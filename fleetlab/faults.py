"""Fault injection — the part of the project that actually proves something.

A dispatch platform that only works when nothing goes wrong is a demo, not a
system.  This module injects the six failures that real deployments hit, each
behind an explicit switch, and counts every injection so a test can prove the
fault *fired* instead of assuming it did (an injection that never triggers turns
a passing test into a meaningless one).

=====================  ==========================================================
Fault                  What it does
=====================  ==========================================================
``duplicate_rate``     delivers a copy of a message (same ``msg_id``)
``reorder``            shuffles messages inside a delivery window
``drop_rate``          removes a message from a delivery, so the log advances
                       while the consumer's committed offset does not
``crash_after``        loses the consumer's speculative cursor mid-run
``cache_expire_rate``  expires hot state early, out from under the scheduler
``network_timeout_rate``makes a handler raise, forcing the retry -> DLQ path
=====================  ==========================================================

All randomness comes from one seeded ``random.Random``, so an injected fault
pattern is reproducible and a failing case can be replayed exactly.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Sequence

from .bus import ConsumerGroup, Message

__all__ = [
    "FaultConfig",
    "FaultInjector",
    "FaultyConsumerGroup",
    "ConsistencyVerdict",
]


@dataclass
class FaultConfig:
    """Switches for every injectable fault.  All off by default.

    ``timeout_budget`` and ``poison_count`` are *counts*, not rates, on purpose:
    a rate makes the number of injected failures depend on how many messages the
    run happens to produce, which turns "the retry path fired 5 times" into a
    number nobody can assert.  A budget gives an exact, reproducible count.
    """

    enabled: bool = False
    duplicate_rate: float = 0.0
    reorder: bool = False
    reorder_window: int = 4
    drop_rate: float = 0.0
    crash_after: int | None = None
    cache_expire_rate: float = 0.0
    network_timeout_rate: float = 0.0
    timeout_budget: int = 0
    poison_count: int = 0
    seed: int = 1234
    name: str = "clean"

    def any_enabled(self) -> bool:
        return bool(
            self.duplicate_rate
            or self.reorder
            or self.drop_rate
            or self.crash_after is not None
            or self.cache_expire_rate
            or self.network_timeout_rate
            or self.timeout_budget
            or self.poison_count
        )

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "enabled": self.enabled,
            "duplicate_rate": self.duplicate_rate,
            "reorder": self.reorder,
            "reorder_window": self.reorder_window,
            "drop_rate": self.drop_rate,
            "crash_after": self.crash_after,
            "cache_expire_rate": self.cache_expire_rate,
            "network_timeout_rate": self.network_timeout_rate,
            "timeout_budget": self.timeout_budget,
            "poison_count": self.poison_count,
            "seed": self.seed,
        }


class FaultInjector:
    """Applies a :class:`FaultConfig` and keeps an audit of what it did."""

    def __init__(self, config: FaultConfig | None = None) -> None:
        self.config = config if config is not None else FaultConfig()
        self._rng = random.Random(self.config.seed)
        self.duplicated: list[str] = []
        self.dropped: list[str] = []
        self.reordered_batches = 0
        self.reordered_messages = 0
        self.crashes = 0
        self.cache_expiries = 0
        self.timeouts = 0
        self.poisons = 0
        self.retries_forced = 0
        self._delivered = 0
        self._crashed_once = False
        self._timeouts_used = 0
        self._poison_used = 0
        self._poisoned_ids: set[str] = set()

    # -- delivery-level faults -------------------------------------------

    def perturb(self, messages: Sequence[Message]) -> list[Message]:
        """Duplicate, reorder and drop inside one delivery batch.

        Order matters: duplicates are added *before* the shuffle so a copy can
        also land out of order, and drops are applied last so a message cannot be
        both duplicated and dropped in the same batch.
        """
        if not self.config.enabled or not messages:
            return list(messages)
        batch = list(messages)

        if self.config.duplicate_rate > 0:
            with_dupes: list[Message] = []
            for message in batch:
                with_dupes.append(message)
                if self._rng.random() < self.config.duplicate_rate:
                    with_dupes.append(message)
                    self.duplicated.append(message.msg_id)
            batch = with_dupes

        if self.config.reorder and len(batch) > 1:
            window = max(2, self.config.reorder_window)
            shuffled: list[Message] = []
            changed = False
            for start in range(0, len(batch), window):
                chunk = batch[start: start + window]
                original = list(chunk)
                self._rng.shuffle(chunk)
                if chunk != original:
                    changed = True
                shuffled.extend(chunk)
            batch = shuffled
            self.reordered_batches += 1
            if changed:
                self.reordered_messages += len(batch)

        if self.config.drop_rate > 0:
            kept: list[Message] = []
            for message in batch:
                if self._rng.random() < self.config.drop_rate:
                    self.dropped.append(message.msg_id)
                    continue
                kept.append(message)
            batch = kept

        self._delivered += len(batch)
        return batch

    # -- consumer crash ---------------------------------------------------

    def maybe_crash(self, group: ConsumerGroup, at_ms: int = 0) -> bool:
        """Crash the consumer once, after roughly ``crash_after`` deliveries.

        The threshold is measured from the group's own delivery counter, not from
        the injector's count of perturbed messages.  Tying it to the injector meant
        the crash only ever fired when ``poll`` went through the faulty proxy — a
        hidden coupling that made the fault silently depend on how the caller was
        wired.

        Crashing *once* is deliberate: repeated crashes would exercise the recovery
        path many times over while hiding whether it converges, and the property
        worth testing is "it recovers", not "it recovers N times".
        """
        if not self.config.enabled or self.config.crash_after is None:
            return False
        if self._crashed_once:
            return False
        if group.counters.get("delivered", 0) < self.config.crash_after:
            return False
        group.crash()
        self._crashed_once = True
        self.crashes += 1
        return True

    # -- cache and network faults ----------------------------------------

    def expire_cache(self, store: Any, keys: Sequence[str], *, at_ms: int = 0) -> list[str]:
        """Expire a fraction of hot keys, immediately.

        Returns the keys actually expired, so an assertion can check that the
        cache really did lose state rather than trusting the rate.  Deliberately
        not gated on ``config.enabled``: expiring a cache is a legitimate thing to
        test on its own, and gating it there silently made the cache-expiry
        scenario a no-op that still reported "consistent".

        Expiry is forced rather than scheduled through a short TTL, because the
        platform clock cannot see sub-millisecond intervals on Windows (see
        :meth:`fleetlab.cache.TTLStore.expire_now`).
        """
        if self.config.cache_expire_rate <= 0:
            return []
        expired: list[str] = []
        for key in sorted(keys):
            if self._rng.random() < self.config.cache_expire_rate:
                if store.expire_now(key, at_ms=at_ms, reason="injected-expiry"):
                    expired.append(key)
                    self.cache_expiries += 1
        return expired

    def finalize_cache_expiry(self, store: Any) -> None:
        """Force the early TTLs to lapse (no sleeping: a 1 ms TTL plus a sweep)."""
        store.sweep()

    def should_timeout(self) -> bool:
        """Decide whether the next handler invocation fails with a timeout."""
        if not self.config.enabled or self.config.network_timeout_rate <= 0:
            return False
        if self._rng.random() < self.config.network_timeout_rate:
            self.timeouts += 1
            self.retries_forced += 1
            return True
        return False

    def should_fail_handler(self, msg_id: str) -> str:
        """Return ``""``/``"timeout"``/``"poison"`` for this message id.

        ``timeout`` is transient: the budget is spent and the retry succeeds, so
        it exercises the retry path end to end.  ``poison`` is *sticky per message
        id*: the same id keeps failing, exhausts ``max_retries`` and lands in the
        dead-letter topic, which is the only way to test the DLQ honestly.
        """
        if not self.config.enabled:
            return ""
        if msg_id in self._poisoned_ids:
            self.poisons += 1
            return "poison"
        if self._poison_used < self.config.poison_count:
            self._poison_used += 1
            self._poisoned_ids.add(msg_id)
            self.poisons += 1
            return "poison"
        if self._timeouts_used < self.config.timeout_budget:
            self._timeouts_used += 1
            self.timeouts += 1
            self.retries_forced += 1
            return "timeout"
        if self.config.network_timeout_rate > 0 and self._rng.random() < self.config.network_timeout_rate:
            self.timeouts += 1
            self.retries_forced += 1
            return "timeout"
        return ""

    def clear_poison(self) -> int:
        """Stop poisoning (used before a DLQ redrive); returns how many were cleared."""
        cleared = len(self._poisoned_ids)
        self._poisoned_ids.clear()
        self.config.poison_count = 0
        self._poison_used = 0
        return cleared

    # -- reporting --------------------------------------------------------

    def fired(self) -> dict[str, int]:
        return {
            "duplicates": len(self.duplicated),
            "drops": len(self.dropped),
            "reordered_batches": self.reordered_batches,
            "reordered_messages": self.reordered_messages,
            "crashes": self.crashes,
            "cache_expiries": self.cache_expiries,
            "timeouts": self.timeouts,
            "poisons": self.poisons,
            "retries_forced": self.retries_forced,
        }

    def stats(self) -> dict:
        return {
            "config": self.config.as_dict(),
            "fired": self.fired(),
            "duplicated_ids": sorted(set(self.duplicated)),
            "dropped_ids": sorted(set(self.dropped)),
            "poisoned_ids": sorted(self._poisoned_ids),
        }


class FaultyConsumerGroup:
    """A :class:`ConsumerGroup` proxy that corrupts its own ``poll`` output.

    Injecting at the *delivery* boundary rather than inside the bus keeps the log
    itself honest: the log stays complete and correctly ordered, so the consumer
    can recover by re-reading, which is exactly what a real consumer does.  If we
    dropped messages from the log instead, no consumer could ever recover and the
    test would only prove that data loss is fatal.
    """

    def __init__(self, group: ConsumerGroup, injector: FaultInjector) -> None:
        object.__setattr__(self, "_group", group)
        object.__setattr__(self, "injector", injector)

    def poll(self, max_messages: int = 10) -> list[Message]:
        raw = self._group.poll(max_messages)
        return self.injector.perturb(raw)

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_group"), name)

    @property
    def inner(self) -> ConsumerGroup:
        return object.__getattribute__(self, "_group")


@dataclass
class ConsistencyVerdict:
    """The outcome of one consistency experiment, in a form a report can print."""

    scenario: str
    consistent: bool
    criterion: str
    live_fingerprint: str
    expected_fingerprint: str
    observed_fingerprint: str = ""
    tasks_completed: int = 0
    tasks_expected: int = 0
    duplicates_suppressed: int = 0
    gaps_recovered: int = 0
    faults: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "scenario": self.scenario,
            "consistent": self.consistent,
            "criterion": self.criterion,
            "live_fingerprint": self.live_fingerprint,
            "expected_fingerprint": self.expected_fingerprint,
            "observed_fingerprint": self.observed_fingerprint,
            "tasks_completed": self.tasks_completed,
            "tasks_expected": self.tasks_expected,
            "duplicates_suppressed": self.duplicates_suppressed,
            "gaps_recovered": self.gaps_recovered,
            "faults": dict(sorted(self.faults.items())),
            "notes": list(self.notes),
        }

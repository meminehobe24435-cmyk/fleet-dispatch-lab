"""Idempotency: deduplication and exactly-once *effects*.

At-least-once delivery is the only guarantee a message bus can actually give, so
"exactly once" has to be built by the consumer.  Two things need it:

1. **Message dedup** — the same ``msg_id`` delivered twice must not produce the
   effect twice.
2. **Command idempotency** — an operator (or a retrying client) sending ``PAUSE``
   ten times must pause the vehicle once.

Both are implemented with the same primitive, :class:`IdempotencyStore`, which
uses a two-phase *claim* rather than a one-shot "seen" set.  The distinction is
important and is the subtlest bug in this project:

* a one-shot "seen" set makes dedup work but **breaks retry** — the first attempt
  marks the id, the handler fails, and the redelivery is then suppressed as a
  duplicate, silently losing the message;
* a claim can be *released* on failure, so the retry is allowed to run, and can be
  *marked* done on success, so later copies are suppressed.

The store is bounded: an unbounded dedup cache is a memory leak with a nice name.
Eviction is insertion-ordered (deterministic), never random.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "IdempotencyStore",
    "CommandDispatcher",
    "CommandOutcome",
    "DEFAULT_CAPACITY",
]

DEFAULT_CAPACITY = 50_000


class IdempotencyStore:
    """Bounded, thread-safe claim/mark/release store keyed by a stable id.

    Parameters
    ----------
    capacity:
        Maximum number of *completed* ids retained.  Oldest entries are evicted
        first once the bound is hit, which trades an ancient duplicate's safety
        for a fixed memory ceiling — the trade almost every real system makes.
    ttl:
        Optional seconds after which a completed id is forgotten.  ``None``
        means "remember until evicted".
    clock:
        Injectable time source (``time.monotonic`` by default) so tests can
        drive expiry without sleeping.
    """

    def __init__(
        self,
        capacity: int = DEFAULT_CAPACITY,
        ttl: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.ttl = ttl
        self._clock = clock
        self._lock = threading.RLock()
        self._done: "OrderedDict[str, float]" = OrderedDict()
        self._inflight: dict[str, float] = {}
        self.counters = {"claims": 0, "claimed": 0, "duplicates": 0, "released": 0, "evictions": 0, "expired": 0}

    # -- internal ---------------------------------------------------------

    def _expire_locked(self, now: float) -> None:
        if self.ttl is None:
            return
        deadline = now - self.ttl
        dead = [k for k, ts in self._done.items() if ts <= deadline]
        for key in dead:
            del self._done[key]
            self.counters["expired"] += 1
        # An in-flight claim that never completed is released after the TTL too,
        # otherwise a crashed handler would block that id forever.
        stale = [k for k, ts in self._inflight.items() if ts <= deadline]
        for key in stale:
            del self._inflight[key]

    def _evict_locked(self) -> None:
        while len(self._done) > self.capacity:
            self._done.popitem(last=False)
            self.counters["evictions"] += 1

    # -- two-phase claim --------------------------------------------------

    def claim(self, key: str) -> bool:
        """Atomically claim ``key``; ``False`` if it is done or already claimed."""
        with self._lock:
            self.counters["claims"] += 1
            self._expire_locked(self._clock())
            if key in self._done or key in self._inflight:
                self.counters["duplicates"] += 1
                return False
            self._inflight[key] = self._clock()
            self.counters["claimed"] += 1
            return True

    def mark(self, key: str) -> None:
        """Promote an in-flight claim to successfully completed."""
        with self._lock:
            self._inflight.pop(key, None)
            self._done[key] = self._clock()
            self._done.move_to_end(key)
            self._evict_locked()

    def release(self, key: str) -> bool:
        """Drop an in-flight claim so a redelivery may run.

        This is what keeps retry working.  Without it, the first (failed) attempt
        would permanently mask every retry of the same message.
        """
        with self._lock:
            existed = self._inflight.pop(key, None) is not None
            if existed:
                self.counters["released"] += 1
            return existed

    # -- convenience ------------------------------------------------------

    def check_and_set(self, key: str) -> bool:
        """One-shot variant: claim and immediately complete.

        Use for operations that must happen once *regardless of outcome* (a
        control command whose effect is a state transition, for instance).
        """
        if not self.claim(key):
            return False
        self.mark(key)
        return True

    def seen(self, key: str) -> bool:
        with self._lock:
            self._expire_locked(self._clock())
            return key in self._done or key in self._inflight

    def forget(self, key: str) -> bool:
        with self._lock:
            removed = self._done.pop(key, None) is not None
            removed = self._inflight.pop(key, None) is not None or removed
            return removed

    def clear(self) -> None:
        with self._lock:
            self._done.clear()
            self._inflight.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._done)

    def in_flight(self) -> int:
        with self._lock:
            return len(self._inflight)

    def stats(self) -> dict:
        with self._lock:
            return {
                "done": len(self._done),
                "in_flight": len(self._inflight),
                "capacity": self.capacity,
                "counters": dict(sorted(self.counters.items())),
            }


@dataclass
class CommandOutcome:
    """Result of submitting a control command."""

    command_id: str
    status: str  # "applied" | "duplicate" | "unconfirmed" | "failed"
    detail: str = ""
    value: Any = None

    def as_dict(self) -> dict:
        return {
            "command_id": self.command_id,
            "status": self.status,
            "detail": self.detail,
            "value": self.value,
        }


@dataclass
class CommandDispatcher:
    """Applies a control command at most once per ``command_id``.

    A retrying client, a duplicated bus message and an operator double-click all
    collapse into one effect.  Repeats return the *original* result so the caller
    cannot tell the difference — which is exactly what makes client retries safe.

    ``verify`` closes the one gap that naive command dedup leaves open.  Marking
    a command "done" merely because it was *accepted* is wrong when acceptance and
    application are separated by a queue: if the effect never lands, the dedup
    key would block every retry and the command would be lost while looking
    successful.  So the caller may pass a verifier; when it returns ``False`` the
    key is released and the outcome is reported as ``unconfirmed``.
    """

    name: str = "commands"
    store: IdempotencyStore = field(default_factory=IdempotencyStore)
    applied: list[str] = field(default_factory=list)
    duplicates: int = 0
    failures: int = 0
    unconfirmed: int = 0

    def submit(
        self,
        command_id: str,
        action: str,
        payload: dict | None = None,
        *,
        handler: Callable[[str, dict], Any],
        verify: Callable[[Any], bool] | None = None,
    ) -> CommandOutcome:
        key = f"{self.name}:{action}:{command_id}"
        if not self.store.claim(key):
            self.duplicates += 1
            return CommandOutcome(command_id, "duplicate", f"{action} already applied")
        try:
            value = handler(action, payload or {})
        except Exception as exc:  # noqa: BLE001 - a failed command must be retryable
            self.store.release(key)
            self.failures += 1
            return CommandOutcome(command_id, "failed", f"{type(exc).__name__}: {exc}")
        if verify is not None and not verify(value):
            self.store.release(key)
            self.unconfirmed += 1
            return CommandOutcome(
                command_id, "unconfirmed", f"{action} accepted but effect not observed"
            )
        self.store.mark(key)
        self.applied.append(key)
        return CommandOutcome(command_id, "applied", "ok", value)

    def stats(self) -> dict:
        return {
            "applied": len(self.applied),
            "duplicates": self.duplicates,
            "failures": self.failures,
            "unconfirmed": self.unconfirmed,
            "store": self.store.stats(),
        }

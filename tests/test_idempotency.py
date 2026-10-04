"""Idempotency tests, including the dedup-vs-retry interaction.

The subtle one is :func:`test_release_allows_a_retry_to_run`.  A naive "seen ids"
set makes duplicates safe and retries *impossible*: the first attempt marks the id,
the handler fails, and the redelivery is suppressed as a duplicate — the message
is silently lost while every counter looks healthy.
"""

from __future__ import annotations

import threading

import pytest

from fleetlab.idempotency import CommandDispatcher, IdempotencyStore


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_claim_succeeds_once_then_reports_a_duplicate():
    store = IdempotencyStore()
    assert store.claim("m1") is True
    assert store.claim("m1") is False
    assert store.counters["duplicates"] == 1


def test_mark_completes_a_claim():
    store = IdempotencyStore()
    store.claim("m1")
    store.mark("m1")
    assert store.seen("m1") is True
    assert store.in_flight() == 0
    assert len(store) == 1


def test_release_allows_a_retry_to_run():
    """The dedup/retry compatibility test."""
    store = IdempotencyStore()
    assert store.claim("m1") is True
    store.release("m1")  # the handler failed
    assert store.claim("m1") is True  # the redelivery is allowed to run
    store.mark("m1")
    assert store.claim("m1") is False


def test_release_of_an_unknown_key_is_false():
    store = IdempotencyStore()
    assert store.release("nope") is False


def test_check_and_set_is_one_shot():
    store = IdempotencyStore()
    assert store.check_and_set("c1") is True
    assert store.check_and_set("c1") is False
    assert store.in_flight() == 0


def test_seen_covers_in_flight_and_done():
    store = IdempotencyStore()
    store.claim("a")
    assert store.seen("a") is True
    store.mark("a")
    assert store.seen("a") is True
    assert store.seen("b") is False


def test_forget_removes_a_key():
    store = IdempotencyStore()
    store.check_and_set("a")
    assert store.forget("a") is True
    assert store.seen("a") is False
    assert store.forget("a") is False


def test_capacity_evicts_the_oldest_first():
    store = IdempotencyStore(capacity=3)
    for i in range(5):
        store.check_and_set(f"k{i}")
    assert len(store) == 3
    assert store.counters["evictions"] == 2
    # Oldest gone, newest kept: eviction is insertion-ordered, never random.
    assert store.seen("k0") is False
    assert store.seen("k4") is True


def test_zero_capacity_is_rejected():
    with pytest.raises(ValueError):
        IdempotencyStore(capacity=0)


def test_ttl_expires_a_completed_key():
    clock = FakeClock()
    store = IdempotencyStore(ttl=10.0, clock=clock)
    store.check_and_set("a")
    clock.advance(9.0)
    assert store.check_and_set("a") is False
    clock.advance(2.0)
    assert store.check_and_set("a") is True
    assert store.counters["expired"] >= 1


def test_ttl_releases_a_stale_in_flight_claim():
    """A crashed handler must not block its key forever."""
    clock = FakeClock()
    store = IdempotencyStore(ttl=5.0, clock=clock)
    assert store.claim("a") is True
    clock.advance(6.0)
    assert store.claim("a") is True


def test_clear_empties_both_maps():
    store = IdempotencyStore()
    store.claim("a")
    store.check_and_set("b")
    store.clear()
    assert len(store) == 0 and store.in_flight() == 0


def test_concurrent_claims_admit_exactly_one_winner():
    store = IdempotencyStore()
    winners: list[int] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        if store.claim("shared"):
            with lock:
                winners.append(index)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(winners) == 1


def test_stats_shape():
    store = IdempotencyStore(capacity=10)
    store.check_and_set("a")
    stats = store.stats()
    assert stats["done"] == 1
    assert stats["capacity"] == 10
    assert "claims" in stats["counters"]


# ----------------------------------------------------------------------
# command dispatcher
# ----------------------------------------------------------------------


def test_command_applies_once_and_reports_duplicates():
    dispatcher = CommandDispatcher()
    calls: list[str] = []

    def handler(action, payload):
        calls.append(action)
        return "done"

    first = dispatcher.submit("cmd-1", "PAUSE", handler=handler)
    second = dispatcher.submit("cmd-1", "PAUSE", handler=handler)
    assert first.status == "applied" and first.value == "done"
    assert second.status == "duplicate"
    assert calls == ["PAUSE"]
    assert dispatcher.duplicates == 1


def test_different_command_ids_are_independent():
    dispatcher = CommandDispatcher()
    calls: list[str] = []
    dispatcher.submit("c1", "PAUSE", handler=lambda a, p: calls.append(a))
    dispatcher.submit("c2", "PAUSE", handler=lambda a, p: calls.append(a))
    assert len(calls) == 2


def test_different_actions_are_independent():
    dispatcher = CommandDispatcher()
    calls: list[str] = []
    dispatcher.submit("c1", "PAUSE", handler=lambda a, p: calls.append(a))
    dispatcher.submit("c1", "RESUME", handler=lambda a, p: calls.append(a))
    assert calls == ["PAUSE", "RESUME"]


def test_a_failing_command_can_be_retried():
    dispatcher = CommandDispatcher()
    attempts = {"n": 0}

    def handler(action, payload):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("first attempt fails")
        return "ok"

    failed = dispatcher.submit("c1", "PAUSE", handler=handler)
    assert failed.status == "failed" and "RuntimeError" in failed.detail
    assert dispatcher.failures == 1
    retried = dispatcher.submit("c1", "PAUSE", handler=handler)
    assert retried.status == "applied"


def test_verify_releases_the_key_when_the_effect_never_landed():
    """An accepted command is not an applied command.

    If the effect cannot be observed, the id must be released, or a retry would be
    silently swallowed and the command lost while reporting success.
    """
    dispatcher = CommandDispatcher()
    calls: list[int] = []
    dispatcher.submit("c1", "PAUSE", handler=lambda a, p: calls.append(1), verify=lambda v: False)
    assert dispatcher.unconfirmed == 1
    again = dispatcher.submit("c1", "PAUSE", handler=lambda a, p: calls.append(1), verify=lambda v: True)
    assert again.status == "applied"
    assert len(calls) == 2


def test_verify_true_keeps_the_key():
    dispatcher = CommandDispatcher()
    dispatcher.submit("c1", "PAUSE", handler=lambda a, p: None, verify=lambda v: True)
    assert dispatcher.submit("c1", "PAUSE", handler=lambda a, p: None).status == "duplicate"


def test_command_stats_shape():
    dispatcher = CommandDispatcher()
    dispatcher.submit("c1", "PAUSE", handler=lambda a, p: None)
    stats = dispatcher.stats()
    assert stats["applied"] == 1
    assert "store" in stats

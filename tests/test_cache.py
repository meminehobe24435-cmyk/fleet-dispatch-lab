"""Cache tests: Redis semantics, TTL boundaries, hashes, and real concurrency.

TTL is tested with an injected clock rather than by sleeping.  That is not merely
faster: ``time.monotonic()`` on Windows advances in ~15.6 ms steps, so a test that
sleeps 1 ms and then asserts expiry is asserting something the platform clock
cannot represent.
"""

from __future__ import annotations

import threading

import pytest

from fleetlab.cache import (
    TTL_MISSING,
    TTL_NO_EXPIRY,
    MemoryStateStore,
    StoreError,
    StoreTypeError,
    TTLStore,
)


class FakeClock:
    """Manually advanced monotonic clock."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def store(clock: FakeClock) -> TTLStore:
    return TTLStore(clock=clock, name="test")


# ----------------------------------------------------------------------
# string commands
# ----------------------------------------------------------------------


def test_set_and_get_round_trip(store):
    assert store.set("k", "v") is True
    assert store.get("k") == "v"


def test_get_missing_key_is_none(store):
    assert store.get("absent") is None


def test_values_are_strings_like_redis(store):
    store.set("int", 42)
    store.set("float", 1.5)
    store.set("bool", True)
    store.set("bytes", b"raw")
    assert store.get("int") == "42"
    assert store.get("float") == "1.5"
    assert store.get("bool") == "1"
    assert store.get("bytes") == "raw"


def test_set_rejects_a_non_positive_ttl(store):
    with pytest.raises(StoreError):
        store.set("k", "v", ttl=0)


def test_delete_counts_only_keys_that_existed(store):
    store.set("a", "1")
    store.set("b", "2")
    assert store.delete("a", "b", "c") == 2


def test_exists_reflects_live_keys(store):
    store.set("a", "1")
    assert store.exists("a") is True
    store.delete("a")
    assert store.exists("a") is False


# ----------------------------------------------------------------------
# TTL boundaries
# ----------------------------------------------------------------------


def test_ttl_reports_missing_and_persistent(store):
    assert store.ttl("nope") == TTL_MISSING
    store.set("k", "v")
    assert store.ttl("k") == TTL_NO_EXPIRY


def test_ttl_counts_down_and_rounds_up(store, clock):
    store.set("k", "v", ttl=10)
    assert store.ttl("k") == 10
    clock.advance(0.5)
    # Redis rounds up, so a key with 9.5 s left reads as 10, never as 9.
    assert store.ttl("k") == 10
    clock.advance(5.0)
    assert store.ttl("k") == 5


def test_key_is_alive_just_before_its_ttl(store, clock):
    store.set("k", "v", ttl=5)
    clock.advance(4.999)
    assert store.get("k") == "v"
    assert store.ttl("k") == 1


def test_key_is_gone_exactly_at_its_ttl(store, clock):
    store.set("k", "v", ttl=5)
    clock.advance(5.0)
    assert store.get("k") is None


def test_expired_key_never_reports_a_positive_ttl(store, clock):
    store.set("k", "v", ttl=1)
    clock.advance(60)
    # Lazy expiry: physically present, logically gone.
    assert store.ttl("k") == TTL_MISSING
    assert store.exists("k") is False


def test_delete_does_not_count_an_expired_key(store, clock):
    store.set("k", "v", ttl=1)
    clock.advance(10)
    assert store.delete("k") == 0


def test_expire_on_a_missing_key_returns_false(store):
    assert store.expire("nope", 10) is False


def test_expire_sets_a_ttl_on_an_existing_key(store):
    store.set("k", "v")
    assert store.expire("k", 30) is True
    assert store.ttl("k") == 30


def test_expire_with_a_non_positive_ttl_deletes(store):
    store.set("k", "v")
    assert store.expire("k", 0) is True
    assert store.exists("k") is False
    assert store.expire("k", -1) is False


def test_persist_removes_the_ttl(store):
    store.set("k", "v", ttl=5)
    assert store.persist("k") is True
    assert store.ttl("k") == TTL_NO_EXPIRY
    assert store.persist("k") is False


def test_sweep_removes_expired_keys_and_records_events(store, clock):
    store.set("a", "1", ttl=1)
    store.set("b", "2", ttl=1)
    store.set("c", "3")
    clock.advance(2)
    assert store.sweep(at_ms=7) == ["a", "b"]
    assert store.keys() == ["c"]
    assert [e.key for e in store.expiries] == ["a", "b"]
    assert store.expiries[0].at_ms == 7


def test_expire_now_removes_a_live_key_and_records_it(store):
    store.set("k", "v", ttl=60)
    assert store.expire_now("k", at_ms=3) is True
    assert store.get("k") is None
    assert store.expiries[-1].source == "forced"
    assert store.expire_now("k") is False


# ----------------------------------------------------------------------
# counters
# ----------------------------------------------------------------------


def test_incr_starts_from_zero_for_a_missing_key(store):
    assert store.incr("n") == 1
    assert store.incr("n") == 2
    assert store.incr("n", 10) == 12


def test_incr_preserves_an_existing_ttl(store):
    store.set("n", 1, ttl=30)
    store.incr("n")
    assert store.ttl("n") == 30


def test_incr_on_a_non_integer_raises(store):
    store.set("s", "hello")
    with pytest.raises(StoreTypeError):
        store.incr("s")


def test_incr_is_negative_capable(store):
    store.set("n", 5)
    assert store.incr("n", -10) == -5


# ----------------------------------------------------------------------
# hashes
# ----------------------------------------------------------------------


def test_hset_returns_the_number_of_new_fields(store):
    assert store.hset("h", {"a": 1, "b": 2}) == 2
    assert store.hset("h", {"b": 3, "c": 4}) == 1


def test_hget_and_hgetall(store):
    store.hset("h", {"a": "1", "b": "2"})
    assert store.hget("h", "a") == "1"
    assert store.hget("h", "zz") is None
    assert store.hgetall("h") == {"a": "1", "b": "2"}


def test_hget_on_a_missing_key_is_none(store):
    assert store.hget("nope", "a") is None
    assert store.hgetall("nope") == {}


def test_hdel_and_last_field_removes_the_key(store):
    store.hset("h", {"a": "1", "b": "2"})
    assert store.hdel("h", "a") == 1
    assert store.hkeys("h") == ["b"]
    assert store.hdel("h", "b") == 1
    assert store.exists("h") is False
    assert store.hlen("h") == 0


def test_hash_operations_on_a_string_key_raise_wrongtype(store):
    store.set("s", "plain")
    for call in (lambda: store.hget("s", "a"), lambda: store.hgetall("s"), lambda: store.hdel("s", "a")):
        with pytest.raises(StoreTypeError):
            call()


def test_hash_survives_many_fields(store):
    store.hset("h", {f"f{i}": i for i in range(50)})
    assert store.hlen("h") == 50
    assert store.hget("h", "f49") == "49"


def test_hash_values_with_separators_are_preserved(store):
    store.hset("h", {"a": "x,y", "b": "p\nq"})
    assert store.hgetall("h") == {"a": "x,y", "b": "p\nq"}


# ----------------------------------------------------------------------
# keyspace and snapshots
# ----------------------------------------------------------------------


def test_keys_supports_glob_patterns(store):
    store.set("veh:1", "a")
    store.set("veh:2", "b")
    store.set("task:1", "c")
    assert store.keys("veh:*") == ["veh:1", "veh:2"]
    assert store.keys("*:1") == ["task:1", "veh:1"]
    assert store.keys() == ["task:1", "veh:1", "veh:2"]


def test_keys_is_sorted_not_iteration_ordered(store):
    for name in ("z", "a", "m", "b"):
        store.set(name, "1")
    assert store.keys() == ["a", "b", "m", "z"]


def test_flush_empties_the_keyspace(store):
    store.set("a", "1")
    store.hset("h", {"f": "v"})
    store.flush()
    assert store.size() == 0


def test_len_matches_size(store):
    store.set("a", "1")
    assert len(store) == 1 == store.size()


def test_snapshot_and_restore_round_trip(store, clock):
    store.set("persistent", "v")
    store.set("short", "v", ttl=10)
    store.hset("hash", {"a": "1"})
    snapshot = store.snapshot(at_ms=5)
    assert snapshot["at_ms"] == 5
    assert len(snapshot["entries"]) == 3

    other = TTLStore(clock=clock)
    assert other.restore(snapshot) == 3
    assert other.get("persistent") == "v"
    assert other.ttl("short") == 10
    assert other.hgetall("hash") == {"a": "1"}


def test_snapshot_skips_expired_keys(store, clock):
    store.set("gone", "v", ttl=1)
    store.set("live", "v")
    clock.advance(5)
    assert [e["key"] for e in store.snapshot()["entries"]] == ["live"]


def test_restore_replaces_previous_contents(store):
    store.set("old", "v")
    store.restore({"entries": [{"key": "new", "value": "1", "ttl": None}]})
    assert store.keys() == ["new"]


def test_float_encoding_is_repr_stable(store):
    store.set("f", 0.1)
    assert store.get("f") == repr(0.1)


def test_memory_alias_is_the_same_class():
    assert MemoryStateStore is TTLStore


# ----------------------------------------------------------------------
# concurrency
# ----------------------------------------------------------------------


def test_concurrent_incr_loses_no_updates(store):
    threads = [threading.Thread(target=lambda: [store.incr("counter") for _ in range(500)]) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert store.get("counter") == "4000"


def test_concurrent_mixed_operations_leave_a_consistent_keyspace(store):
    errors: list[BaseException] = []

    def writer(worker: int) -> None:
        try:
            for i in range(200):
                store.set(f"w{worker}:{i}", i, ttl=60)
                store.hset(f"h{worker}", {f"f{i}": i})
                store.get(f"w{worker}:{i}")
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(store.keys("w0:*")) == 200
    assert store.hlen("h5") == 200


def test_concurrent_expiry_sweep_is_safe(store, clock):
    for i in range(200):
        store.set(f"k{i}", i, ttl=1)
    done = threading.Event()
    errors: list[BaseException] = []

    def sweeper() -> None:
        try:
            while not done.is_set():
                store.sweep()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=sweeper)
    thread.start()
    clock.advance(2)
    store.size()
    done.set()
    thread.join()
    assert errors == []
    assert store.size() == 0

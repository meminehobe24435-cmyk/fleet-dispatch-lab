"""Redis adapter conformance: the same operation sequence, both implementations.

The claim under test is not "the adapter calls redis-py correctly" — it is
**"application code written against the in-memory store behaves identically on
Redis"**.  So the core of this file is one shared sequence of operations, run
against :class:`~fleetlab.cache.TTLStore` and
:class:`~fleetlab.redis_adapter.RedisStateStore`, with every result compared.

What is verified: the *command semantics* of the adapter (TTL values, delete
counts, acknowledge-style return types, hash field counting, WRONGTYPE error
mapping, snapshot/restore), against ``fakeredis`` — an in-process reimplementation
of the Redis command set.

What is **not** verified: anything that requires a real server — the wire protocol,
actual eviction under memory pressure, replication, failover, and real network
timeouts.  No Redis server exists in this environment, so those are listed as
explicit boundaries in the README rather than quietly implied to be covered.
"""

from __future__ import annotations

import pytest

from fleetlab.cache import TTL_MISSING, TTL_NO_EXPIRY, StoreError, StoreTypeError, TTLStore

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis not installed")

from fleetlab.redis_adapter import RedisStateStore, RedisUnavailable  # noqa: E402

pytestmark = pytest.mark.redis_adapter


def make_redis_store(prefix: str = "fleetlab") -> RedisStateStore:
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisStateStore(client, prefix=prefix, name="fakeredis")


# ----------------------------------------------------------------------
# the shared conformance sequence
# ----------------------------------------------------------------------

#: Operations applied to both stores.  Every step is an (op, args) pair whose
#: result is recorded and compared; integer TTLs only, because Redis EX is
#: whole-second and a fractional TTL would compare a rounding decision rather
#: than a semantic one.
CONFORMANCE_SEQUENCE: list[tuple[str, tuple]] = [
    ("set", ("alpha", "1", None)),
    ("set", ("beta", "two", None)),
    ("set", ("gamma", "3", 100)),
    ("set", ("veh:1", "idle", 100)),
    ("get", ("alpha",)),
    ("get", ("missing",)),
    ("exists", ("alpha",)),
    ("exists", ("missing",)),
    ("ttl", ("alpha",)),
    ("ttl", ("missing",)),
    ("ttl", ("gamma",)),
    ("incr", ("counter",)),
    ("incr", ("counter",)),
    ("incr", ("counter", 10)),
    ("incr", ("gamma",)),
    ("hset", ("hash", {"a": "1", "b": "2"})),
    ("hset", ("hash", {"b": "22", "c": "3"})),
    ("hget", ("hash", "a")),
    ("hget", ("hash", "zz")),
    ("hgetall", ("hash",)),
    ("hgetall", ("missing",)),
    ("hkeys", ("hash",)),
    ("hlen", ("hash",)),
    ("hdel", ("hash", "a")),
    ("hgetall", ("hash",)),
    ("expire", ("beta", 250)),
    ("ttl", ("beta",)),
    ("persist", ("beta",)),
    ("ttl", ("beta",)),
    ("expire", ("missing", 5)),
    ("keys", ("*",)),
    ("keys", ("veh:*",)),
    ("keys", ("hash",)),
    ("delete", ("alpha",)),
    ("delete", ("alpha",)),
    ("exists", ("alpha",)),
    ("keys", ("*",)),
]


def run_sequence(store) -> list:
    """Execute the shared sequence, recording every result."""
    results: list = []
    for op, args in CONFORMANCE_SEQUENCE:
        results.append((op, args, getattr(store, op)(*args)))
    return results


def test_conformance_sequence_agrees_between_backends():
    """The whole point: one code path, two backends, identical results."""
    memory = TTLStore(name="mem")
    redis_store = make_redis_store()

    memory_results = run_sequence(memory)
    redis_results = run_sequence(redis_store)

    assert len(memory_results) == len(redis_results)
    mismatches = [
        (m, r) for m, r in zip(memory_results, redis_results) if m[2] != r[2]
    ]
    assert mismatches == [], f"backends disagree at {len(mismatches)} step(s)"


def test_conformance_sequence_covers_a_meaningful_number_of_operations():
    """Guard against the sequence being trimmed to nothing."""
    assert len(CONFORMANCE_SEQUENCE) >= 35


def test_the_two_backends_start_from_an_empty_and_equal_state():
    memory = TTLStore()
    redis_store = make_redis_store()
    assert memory.keys("*") == [] == redis_store.keys("*")
    assert memory.ttl("x") == redis_store.ttl("x") == TTL_MISSING


# ----------------------------------------------------------------------
# individual semantics, Redis-only
# ----------------------------------------------------------------------


@pytest.fixture()
def redis_store() -> RedisStateStore:
    return make_redis_store()


def test_set_and_get(redis_store):
    assert redis_store.set("k", "v") is True
    assert redis_store.get("k") == "v"


def test_get_missing_is_none(redis_store):
    assert redis_store.get("nope") is None


def test_values_are_stringified(redis_store):
    redis_store.set("i", 7)
    redis_store.set("b", True)
    redis_store.set("f", 1.25)
    assert redis_store.get("i") == "7"
    assert redis_store.get("b") == "1"
    assert redis_store.get("f") == "1.25"


def test_ttl_sentinels_match_redis(redis_store):
    assert redis_store.ttl("missing") == TTL_MISSING
    redis_store.set("persistent", "v")
    assert redis_store.ttl("persistent") == TTL_NO_EXPIRY
    redis_store.set("short", "v", ttl=100)
    assert 0 < redis_store.ttl("short") <= 100


def test_expire_returns_bool_not_int(redis_store):
    redis_store.set("k", "v")
    assert redis_store.expire("k", 50) is True
    assert redis_store.expire("missing", 50) is False


def test_expire_with_a_non_positive_ttl_deletes(redis_store):
    redis_store.set("k", "v")
    assert redis_store.expire("k", 0) is True
    assert redis_store.get("k") is None


def test_delete_returns_the_count_removed(redis_store):
    redis_store.set("a", "1")
    redis_store.set("b", "2")
    assert redis_store.delete("a", "b", "c") == 2


def test_exists_returns_bool(redis_store):
    """redis-py returns an integer; the adapter normalises it to bool."""
    redis_store.set("a", "1")
    assert redis_store.exists("a") is True
    assert redis_store.exists("b") is False


def test_incr_and_incrby(redis_store):
    assert redis_store.incr("n") == 1
    assert redis_store.incr("n") == 2
    assert redis_store.incr("n", 5) == 7


def test_incr_on_a_non_integer_raises_store_type_error(redis_store):
    """``redis.ResponseError`` is translated to this package's error type."""
    redis_store.set("s", "hello")
    with pytest.raises(StoreTypeError):
        redis_store.incr("s")


def test_hset_returns_the_new_field_count(redis_store):
    assert redis_store.hset("h", {"a": 1, "b": 2}) == 2
    assert redis_store.hset("h", {"b": 3, "c": 4}) == 1


def test_hash_reads(redis_store):
    redis_store.hset("h", {"a": "1", "b": "2"})
    assert redis_store.hget("h", "a") == "1"
    assert redis_store.hget("h", "nope") is None
    assert redis_store.hgetall("h") == {"a": "1", "b": "2"}
    assert redis_store.hkeys("h") == ["a", "b"]
    assert redis_store.hlen("h") == 2


def test_hdel_and_empty_hash_removes_the_key(redis_store):
    redis_store.hset("h", {"a": "1"})
    assert redis_store.hdel("h", "a") == 1
    assert redis_store.get("h") is None


def test_hash_on_a_string_key_raises_store_type_error(redis_store):
    redis_store.set("s", "plain")
    with pytest.raises(StoreTypeError):
        redis_store.hget("s", "a")


def test_keys_is_sorted_and_stripped_of_the_prefix(redis_store):
    redis_store.set("b", "1")
    redis_store.set("a", "1")
    redis_store.set("veh:1", "1")
    assert redis_store.keys() == ["a", "b", "veh:1"]
    assert redis_store.keys("veh:*") == ["veh:1"]


def test_prefixes_isolate_two_tenants():
    client = fakeredis.FakeRedis(decode_responses=True)
    first = RedisStateStore(client, prefix="tenant-a")
    second = RedisStateStore(client, prefix="tenant-b")
    first.set("k", "a")
    second.set("k", "b")
    assert first.get("k") == "a"
    assert second.get("k") == "b"
    assert first.keys() == ["k"] == second.keys()


def test_flush_only_removes_namespaced_keys():
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStateStore(client, prefix="fleetlab")
    store.set("mine", "1")
    client.set("someone-elses", "1")
    store.flush()
    assert store.keys() == []
    assert client.get("someone-elses") == "1"


def test_ping(redis_store):
    assert redis_store.ping() is True


def test_snapshot_and_restore_round_trip(redis_store):
    redis_store.set("plain", "v")
    redis_store.hset("hash", {"a": "1", "b": "2"})
    redis_store.set("timed", "v", ttl=500)
    snapshot = redis_store.snapshot(at_ms=9)
    assert snapshot["at_ms"] == 9
    assert snapshot["backend"] == "fakeredis"

    restored = make_redis_store()
    assert restored.restore(snapshot) == 3
    assert restored.get("plain") == "v"
    assert restored.hgetall("hash") == {"a": "1", "b": "2"}
    assert 0 < restored.ttl("timed") <= 500


def test_set_rejects_a_non_positive_ttl(redis_store):
    with pytest.raises(StoreError):
        redis_store.set("k", "v", ttl=0)


def test_snapshot_and_restore_agree_across_backends():
    """A snapshot's *content* must not depend on which backend produced it."""
    memory = TTLStore()
    redis_store = make_redis_store()
    for store in (memory, redis_store):
        store.set("plain", "v")
        store.hset("hash", {"a": "1"})
        store.set("timed", "v", ttl=300)

    def shape(snapshot):
        return sorted(
            (entry["key"], entry.get("kind"), entry.get("value") or entry.get("fields"))
            for entry in snapshot["entries"]
        )

    assert shape(memory.snapshot()) == shape(redis_store.snapshot())


def test_a_snapshot_taken_from_memory_restores_into_redis():
    """The snapshot format is a contract *between* backends, not inside one.

    Before the formats were unified, the in-memory store dumped a hash as its
    internal encoded string, so restoring it into Redis silently produced a
    string key instead of a hash — data that looks present and behaves wrong.
    """
    memory = TTLStore()
    memory.set("plain", "v")
    memory.hset("hash", {"a": "1", "b": "2"})
    memory.set("timed", "v", ttl=400)

    redis_store = make_redis_store()
    assert redis_store.restore(memory.snapshot()) == 3
    assert redis_store.get("plain") == "v"
    assert redis_store.hgetall("hash") == {"a": "1", "b": "2"}
    assert 0 < redis_store.ttl("timed") <= 400


def test_a_snapshot_taken_from_redis_restores_into_memory():
    redis_store = make_redis_store()
    redis_store.set("plain", "v")
    redis_store.hset("hash", {"a": "1"})

    memory = TTLStore()
    assert memory.restore(redis_store.snapshot()) == 2
    assert memory.get("plain") == "v"
    assert memory.hgetall("hash") == {"a": "1"}


# ----------------------------------------------------------------------
# error mapping with a client that is deliberately broken
# ----------------------------------------------------------------------


class BrokenClient:
    """Minimal client stub used to exercise the adapter's error translation."""

    class ResponseError(Exception):
        pass

    def __init__(self, mode: str) -> None:
        self.mode = mode

    def _fail(self):
        if self.mode == "wrongtype":
            raise type(self).ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
        if self.mode == "connection":
            error = type("ConnectionError", (Exception,), {})
            raise error("connection refused")

    def get(self, key):
        self._fail()

    def set(self, *args, **kwargs):
        self._fail()

    def exists(self, *args):
        self._fail()

    def ping(self):
        self._fail()


def test_wrongtype_is_mapped_to_store_type_error():
    store = RedisStateStore(BrokenClient("wrongtype"), prefix="p")
    with pytest.raises(StoreTypeError):
        store.get("k")


def test_connection_failure_is_mapped_to_a_store_error():
    store = RedisStateStore(BrokenClient("connection"), prefix="p")
    with pytest.raises(StoreError):
        store.ping()


def test_ping_wraps_a_broken_client():
    store = RedisStateStore(BrokenClient("connection"), prefix="p")
    with pytest.raises(RedisUnavailable):
        store.ping()


def test_repr_mentions_the_prefix():
    store = make_redis_store(prefix="abc")
    assert "abc" in repr(store)

"""Fault injector tests.

Every test here checks that the fault *fired* and was counted, not just that the
system survived.  An injection that never triggers turns a passing test into a
meaningless one — that failure mode happened in this project twice (a 1 ms cache
TTL that the platform clock could not see, and a hidden ``enabled`` gate).
"""

from __future__ import annotations

import random

import pytest

from fleetlab.bus import Message, MessageBus
from fleetlab.cache import TTLStore
from fleetlab.faults import FaultConfig, FaultInjector, FaultyConsumerGroup
from fleetlab.observability import DeterministicIds


def messages(count: int) -> list[Message]:
    return [
        Message(topic="t", partition=0, offset=i, key=f"k{i}", msg_id=f"m{i}", payload={"i": i})
        for i in range(count)
    ]


def enabled(**kwargs) -> FaultConfig:
    base = {"enabled": True, "seed": 7}
    base.update(kwargs)
    return FaultConfig(**base)


# ----------------------------------------------------------------------
# delivery-level faults
# ----------------------------------------------------------------------


def test_a_disabled_injector_changes_nothing():
    injector = FaultInjector(FaultConfig())
    batch = messages(20)
    assert injector.perturb(batch) == batch
    assert injector.fired()["duplicates"] == 0


def test_duplicate_rate_is_deterministic_for_a_seed():
    first = FaultInjector(enabled(duplicate_rate=0.5))
    second = FaultInjector(enabled(duplicate_rate=0.5))
    assert len(first.perturb(messages(40))) == len(second.perturb(messages(40)))


def test_duplicates_keep_the_same_message_id():
    """The duplicate must be a *repeat*, not a new message, or dedup cannot help."""
    injector = FaultInjector(enabled(duplicate_rate=1.0))
    batch = injector.perturb(messages(5))
    assert len(batch) == 10
    ids = [m.msg_id for m in batch]
    assert sorted(ids) == sorted([f"m{i}" for i in range(5)] * 2)
    assert injector.fired()["duplicates"] == 5


def test_reorder_preserves_the_multiset_and_detects_change():
    injector = FaultInjector(enabled(reorder=True, reorder_window=4))
    batch = messages(12)
    shuffled = injector.perturb(batch)
    assert sorted(m.msg_id for m in shuffled) == sorted(m.msg_id for m in batch)
    assert injector.reordered_batches == 1
    assert injector.reordered_messages > 0


def test_reorder_of_a_single_message_is_a_no_op():
    injector = FaultInjector(enabled(reorder=True))
    assert injector.perturb(messages(1))[0].msg_id == "m0"


def test_drop_removes_messages_and_counts_them():
    injector = FaultInjector(enabled(drop_rate=1.0))
    assert injector.perturb(messages(4)) == []
    assert len(injector.dropped) == 4


def test_zero_rates_inject_nothing():
    injector = FaultInjector(enabled(duplicate_rate=0.0, drop_rate=0.0))
    assert len(injector.perturb(messages(10))) == 10
    assert injector.fired()["duplicates"] == 0
    assert injector.fired()["drops"] == 0


def test_perturb_of_an_empty_batch_is_safe():
    injector = FaultInjector(enabled(duplicate_rate=1.0, drop_rate=1.0, reorder=True))
    assert injector.perturb([]) == []


def test_all_faults_together_still_produce_a_list():
    injector = FaultInjector(enabled(duplicate_rate=0.5, reorder=True, drop_rate=0.3))
    result = injector.perturb(messages(30))
    assert isinstance(result, list)
    assert injector.fired()["duplicates"] > 0
    assert injector.fired()["drops"] > 0


# ----------------------------------------------------------------------
# crash injection
# ----------------------------------------------------------------------


def test_crash_fires_once_after_the_threshold():
    bus = MessageBus(default_partitions=1)
    bus.declare_topic("t", 1)
    for index in range(10):
        bus.publish("t", key="k", payload={"i": index})
    group = bus.create_group("t", "g")
    injector = FaultInjector(enabled(crash_after=3))

    group.poll(5)
    assert injector.maybe_crash(group, 100) is True
    assert group.cursor[0] == group.committed[0] == 0
    assert injector.maybe_crash(group, 200) is False  # crashing twice proves nothing
    assert injector.crashes == 1


def test_crash_does_not_fire_before_the_threshold():
    bus = MessageBus(default_partitions=1)
    bus.declare_topic("t", 1)
    bus.publish("t", key="k", payload={})
    group = bus.create_group("t", "g")
    injector = FaultInjector(enabled(crash_after=5))
    group.poll(1)
    assert injector.maybe_crash(group, 0) is False
    assert injector.crashes == 0


def test_crash_is_skipped_when_not_configured():
    bus = MessageBus(default_partitions=1)
    bus.declare_topic("t", 1)
    group = bus.create_group("t", "g")
    assert FaultInjector(enabled()).maybe_crash(group) is False


# ----------------------------------------------------------------------
# cache expiry
# ----------------------------------------------------------------------


def test_cache_expiry_removes_keys_and_counts_them():
    injector = FaultInjector(FaultConfig(cache_expire_rate=1.0, seed=3))
    store = TTLStore()
    for index in range(5):
        store.set(f"k{index}", "v", ttl=60)
    expired = injector.expire_cache(store, store.keys("*"), at_ms=1)
    assert len(expired) == 5
    assert store.size() == 0
    assert injector.cache_expiries == 5


def test_cache_expiry_is_not_gated_on_enabled():
    """Regression: the rate used to be ignored unless the whole config was enabled.

    That made the cache-expiry scenario mark nothing and lose nothing, while still
    reporting itself consistent — a fault test that tested no fault.
    """
    injector = FaultInjector(FaultConfig(enabled=False, cache_expire_rate=1.0))
    store = TTLStore()
    store.set("k", "v", ttl=60)
    assert injector.expire_cache(store, ["k"], at_ms=1) == ["k"]
    assert store.exists("k") is False


def test_cache_expiry_records_the_event_source():
    injector = FaultInjector(FaultConfig(cache_expire_rate=1.0, seed=1))
    store = TTLStore()
    store.set("k", "v")
    injector.expire_cache(store, ["k"], at_ms=42)
    assert store.expiries[-1].source == "injected-expiry"
    assert store.expiries[-1].at_ms == 42


def test_cache_expiry_skips_missing_keys():
    injector = FaultInjector(FaultConfig(cache_expire_rate=1.0, seed=1))
    store = TTLStore()
    assert injector.expire_cache(store, ["absent"], at_ms=0) == []


# ----------------------------------------------------------------------
# handler failure injection
# ----------------------------------------------------------------------


def test_timeout_budget_is_spent_then_exhausted():
    injector = FaultInjector(enabled(timeout_budget=3))
    outcomes = [injector.should_fail_handler(f"m{i}") for i in range(6)]
    assert outcomes == ["timeout", "timeout", "timeout", "", "", ""]
    assert injector.timeouts == 3


def test_poison_is_sticky_per_message_id():
    """Poison must keep failing, or the message never reaches the DLQ."""
    injector = FaultInjector(enabled(poison_count=1))
    assert injector.should_fail_handler("m0") == "poison"
    assert injector.should_fail_handler("m0") == "poison"
    assert injector.should_fail_handler("m0") == "poison"
    assert injector.should_fail_handler("m1") == ""


def test_poison_count_limits_how_many_ids_are_affected():
    injector = FaultInjector(enabled(poison_count=2))
    assert injector.should_fail_handler("a") == "poison"
    assert injector.should_fail_handler("b") == "poison"
    assert injector.should_fail_handler("c") == ""


def test_clear_poison_stops_further_failures():
    injector = FaultInjector(enabled(poison_count=2))
    injector.should_fail_handler("a")
    injector.should_fail_handler("b")
    assert injector.clear_poison() == 2
    assert injector.should_fail_handler("a") == ""


def test_disabled_injector_never_fails_a_handler():
    injector = FaultInjector(FaultConfig(timeout_budget=5, poison_count=5))
    assert [injector.should_fail_handler(f"m{i}") for i in range(6)] == [""] * 6


def test_timeout_rate_is_probabilistic_but_seeded():
    first = FaultInjector(enabled(network_timeout_rate=0.5, seed=11))
    second = FaultInjector(enabled(network_timeout_rate=0.5, seed=11))
    assert [first.should_fail_handler(f"m{i}") for i in range(20)] == [
        second.should_fail_handler(f"m{i}") for i in range(20)
    ]


# ----------------------------------------------------------------------
# reporting and the faulty group proxy
# ----------------------------------------------------------------------


def test_fired_reports_every_counter():
    injector = FaultInjector(enabled(duplicate_rate=1.0, drop_rate=0.0))
    injector.perturb(messages(2))
    fired = injector.fired()
    for key in ("duplicates", "drops", "reordered_batches", "crashes", "cache_expiries", "timeouts", "poisons"):
        assert key in fired
    assert fired["duplicates"] == 2


def test_stats_includes_config_and_ids():
    injector = FaultInjector(enabled(duplicate_rate=1.0))
    injector.perturb(messages(3))
    stats = injector.stats()
    assert stats["config"]["enabled"] is True
    assert stats["duplicated_ids"] == ["m0", "m1", "m2"]


def test_config_as_dict_and_any_enabled():
    assert FaultConfig().any_enabled() is False
    assert FaultConfig(duplicate_rate=0.1).any_enabled() is True
    assert FaultConfig(reorder=True).any_enabled() is True
    assert FaultConfig(crash_after=1).any_enabled() is True
    assert FaultConfig(cache_expire_rate=0.1).any_enabled() is True
    assert FaultConfig(timeout_budget=1).any_enabled() is True
    assert FaultConfig(poison_count=1).any_enabled() is True
    assert "duplicate_rate" in FaultConfig().as_dict()


def test_faulty_group_proxy_perturbs_poll_and_delegates_everything_else():
    bus = MessageBus(default_partitions=1)
    bus.declare_topic("t", 1)
    for index in range(6):
        bus.publish("t", key="k", payload={"i": index})
    group = bus.create_group("t", "g")
    injector = FaultInjector(enabled(duplicate_rate=1.0))
    proxy = FaultyConsumerGroup(group, injector)

    batch = proxy.poll(10)
    assert len(batch) == 12  # every message duplicated
    # Attribute access is forwarded to the real group.
    assert proxy.name == "g"
    assert proxy.lag() == 6
    proxy.ack(batch[0])
    assert group.counters["acked"] == 1
    assert proxy.inner is group


def test_consistency_verdict_is_serialisable():
    from fleetlab.faults import ConsistencyVerdict

    verdict = ConsistencyVerdict(
        scenario="x", consistent=True, criterion="fingerprint equality",
        live_fingerprint="a", expected_fingerprint="a",
    )
    payload = verdict.as_dict()
    assert payload["scenario"] == "x"
    assert payload["consistent"] is True
    assert payload["faults"] == {}


def test_random_module_untouched_by_the_injector():
    """The injector must not consume the global RNG: that would break reproducibility."""
    random.seed(1234)
    before = random.random()
    random.seed(1234)
    FaultInjector(enabled(duplicate_rate=1.0)).perturb(messages(10))
    assert random.random() == before


def test_deterministic_ids_do_not_consume_the_global_rng():
    random.seed(99)
    expected = random.random()
    random.seed(99)
    DeterministicIds(5).next()
    assert random.random() == expected

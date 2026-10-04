"""Message bus tests: partitioning, offsets, ack/nack, ordering, dedup, recovery.

These cover the traps the project exists to demonstrate, and three of them are
"inject a known fault, assert the system recovers":

* :func:`test_crash_then_recover_loses_nothing_and_duplicates_nothing`
* :func:`test_dropped_delivery_is_recovered_by_reseeking_to_the_commit`
* :func:`test_out_of_order_delivery_is_buffered_and_released_in_order`
"""

from __future__ import annotations

import pytest

from fleetlab.bus import (
    DLQ_PREFIX,
    BackpressureError,
    Consumer,
    ConsumerGroup,
    MessageBus,
    Topic,
    TopicNotFound,
    stable_hash,
)
from fleetlab.idempotency import IdempotencyStore


def bus(**kwargs) -> MessageBus:
    instance = MessageBus(**kwargs)
    instance.declare_topic("events", kwargs.get("default_partitions", 4))
    return instance


def publisher(instance: MessageBus, key: str, count: int, topic: str = "events"):
    for index in range(count):
        instance.publish(topic, key=key, payload={"i": index}, at_ms=index)
    return instance


# ----------------------------------------------------------------------
# topics and partitioning
# ----------------------------------------------------------------------


def test_publishing_to_an_undeclared_topic_raises():
    instance = MessageBus()
    with pytest.raises(TopicNotFound):
        instance.publish("nope", key="k", payload={})


def test_unknown_group_raises():
    instance = bus()
    with pytest.raises(TopicNotFound):
        instance.group("nope")


def test_declare_topic_is_idempotent():
    instance = MessageBus()
    first = instance.declare_topic("t", 3)
    second = instance.declare_topic("t", 99)
    assert first is second
    assert instance.counters["topics_declared"] == 1


def test_stable_hash_is_process_independent():
    """Regression: ``hash()`` is salted per process, which would move messages."""
    assert stable_hash("task-0001") == stable_hash("task-0001")
    assert stable_hash("task-0001") != stable_hash("task-0002")
    # A literal value, so any change to the scheme is caught rather than silently
    # repartitioning every existing key (and every stored consumer offset).
    assert stable_hash("k") == 10152434533701879956
    assert stable_hash("task-0001") == 4917764230982328310


def test_same_key_always_lands_in_the_same_partition():
    instance = bus(default_partitions=4)
    for _ in range(20):
        message = instance.publish("events", key="stable-key", payload={})
        assert message.partition == instance.topic("events").partition_for_key("stable-key").index


def test_offsets_are_monotonic_per_partition():
    instance = bus(default_partitions=2)
    publisher(instance, "a", 5)
    partition = instance.topic("events").partitions[instance.topic("events").partition_for_key("a").index]
    assert [m.offset for m in partition.messages] == [0, 1, 2, 3, 4]


def test_message_id_is_derived_from_position():
    instance = bus(default_partitions=2)
    message = instance.publish("events", key="a", payload={})
    assert message.msg_id == f"events-{message.partition}-{message.offset}"


def test_read_log_orders_by_partition_then_offset():
    instance = bus(default_partitions=3)
    for key in ("x", "y", "z", "x", "y"):
        instance.publish("events", key=key, payload={})
    log = instance.read_log("events")
    assert [(m.partition, m.offset) for m in log] == sorted((m.partition, m.offset) for m in log)


def test_read_log_can_target_one_partition():
    instance = bus(default_partitions=3)
    publisher(instance, "x", 3)
    partition = instance.topic("events").partition_for_key("x").index
    assert len(instance.read_log("events", partition)) == 3


# ----------------------------------------------------------------------
# ack / nack / offsets
# ----------------------------------------------------------------------


def test_ack_advances_the_committed_offset_contiguously():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 4)
    group = instance.create_group("events", "g")
    messages = group.poll(10)
    assert len(messages) == 4

    group.ack(messages[0])
    group.ack(messages[2])  # a hole: committed must not jump over it
    assert group.commit_position()["0"] == 1
    group.ack(messages[1])
    assert group.commit_position()["0"] == 3
    group.ack(messages[3])
    assert group.commit_position()["0"] == 4


def test_duplicate_ack_is_a_no_op():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g")
    message = group.poll(1)[0]
    assert group.ack(message) is True
    assert group.ack(message) is False
    assert group.counters["acked"] == 1


def test_nack_requeues_then_dead_letters():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g", max_retries=2)
    message = group.poll(1)[0]
    assert group.nack(message, "boom") == "retry"
    assert group.nack(message, "boom") == "retry"
    assert group.nack(message, "boom") == "dlq"
    assert len(group.dlq) == 1
    assert group.dlq[0]["reason"] == "boom"
    assert group.dlq[0]["attempts"] == 3


def test_dead_letter_creates_a_dlq_topic_with_the_failure_record():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g", max_retries=0)
    group.nack(group.poll(1)[0], "always fails")
    dlq_name = DLQ_PREFIX + "events"
    assert dlq_name in instance.topics
    record = instance.read_log(dlq_name)[0]
    assert record.payload["reason"] == "always fails"
    assert record.payload["topic"] == "events"


def test_retries_are_served_before_fresh_reads():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 3)
    group = instance.create_group("events", "g", max_retries=5)
    first = group.poll(1)[0]
    group.nack(first, "retry me")
    next_batch = group.poll(2)
    assert next_batch[0].msg_id == first.msg_id
    assert next_batch[0].attempt == 1


def test_lag_counts_unacknowledged_messages():
    instance = bus(default_partitions=2)
    publisher(instance, "a", 3)
    publisher(instance, "b", 2)
    group = instance.create_group("events", "g")
    assert group.lag() == 5
    for message in group.poll(10):
        group.ack(message)
    assert group.lag() == 0


def test_backpressure_refuses_a_publish_and_counts_it():
    instance = bus(default_partitions=1, backpressure_limit=2)
    publisher(instance, "k", 2)
    instance.create_group("events", "g")
    with pytest.raises(BackpressureError) as info:
        instance.publish("events", key="k", payload={})
    assert "lag 2" in str(info.value)
    assert instance.counters["backpressure_rejections"] == 1


def test_backpressure_can_be_bypassed():
    instance = bus(default_partitions=1, backpressure_limit=1)
    publisher(instance, "k", 1)
    instance.create_group("events", "g")
    message = instance.publish("events", key="k", payload={}, _bypass_backpressure=True)
    assert message is not None


def test_stats_reports_topics_groups_and_lag():
    instance = bus(default_partitions=2)
    publisher(instance, "k", 2)
    instance.create_group("events", "g")
    stats = instance.stats()
    assert stats["topics"]["events"]["messages"] == 2
    assert stats["groups"]["g"]["lag"] == 2
    assert stats["max_lag"] == 2


# ----------------------------------------------------------------------
# consumer: dedup and ordering
# ----------------------------------------------------------------------


def handler_collect(sink: list):
    return lambda message: sink.append(message.payload["i"])


def test_consumer_processes_and_acknowledges():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 3)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))
    result = consumer.run_once(10)
    assert seen == [0, 1, 2]
    assert result.processed == 3 and result.acked == 3


def test_consumer_suppresses_a_duplicate_delivery():
    """Inject a known fault: the same message id delivered twice."""
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))

    original_poll = group.poll
    duplicated = {"done": False}

    def poll_with_duplicate(max_messages=10):
        messages = original_poll(max_messages)
        if messages and not duplicated["done"]:
            duplicated["done"] = True
            return messages + messages  # the injected duplicate
        return messages

    group.poll = poll_with_duplicate  # type: ignore[assignment]
    result = consumer.run_once(10)
    assert seen == [0]  # one effect
    assert result.duplicates_suppressed == 1
    assert result.processed == 1


def test_consumer_retries_a_failed_handler_and_succeeds():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g", max_retries=3)
    attempts: list[int] = []

    def flaky(message):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient")

    consumer = Consumer(group, flaky)
    first = consumer.run_once(5)
    assert first.nacked == 1 and first.errors
    second = consumer.run_once(5)
    assert second.processed == 1
    assert len(attempts) == 2


def test_consumer_dead_letters_a_permanently_failing_message():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g", max_retries=1)

    def always_fails(message):
        raise ValueError("permanent")

    consumer = Consumer(group, always_fails)
    result = consumer.drain()
    assert result.dlq == 1
    assert len(group.dlq) == 1
    assert group.lag() == 0  # a poison message must not block the queue


def test_out_of_order_delivery_is_buffered_and_released_in_order():
    """Inject a known fault: shuffle a delivery batch, assert order is restored."""
    instance = bus(default_partitions=1)
    publisher(instance, "k", 5)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))

    original_poll = group.poll
    shuffled = {"done": False}

    def poll_shuffled(max_messages=10):
        messages = original_poll(max_messages)
        if messages and not shuffled["done"]:
            shuffled["done"] = True
            return list(reversed(messages))
        return messages

    group.poll = poll_shuffled  # type: ignore[assignment]
    result = consumer.drain()
    assert seen == [0, 1, 2, 3, 4]  # application order is correct
    assert result.out_of_order_detected >= 1


def test_order_guard_can_be_disabled():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 4)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen), order_guard=False)
    original_poll = group.poll
    flipped = {"done": False}

    def poll_flipped(max_messages=10):
        messages = original_poll(max_messages)
        if messages and not flipped["done"]:
            flipped["done"] = True
            return list(reversed(messages))
        return messages

    group.poll = poll_flipped  # type: ignore[assignment]
    result = consumer.run_once(10)
    assert seen == [3, 2, 1, 0]
    assert result.out_of_order_detected == 0


def test_dropped_delivery_is_recovered_by_reseeking_to_the_commit():
    """Inject a known fault: lose a delivery, assert nothing is lost overall."""
    instance = bus(default_partitions=1)
    publisher(instance, "k", 6)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))

    original_poll = group.poll
    dropped = {"done": False}

    def poll_with_drop(max_messages=10):
        messages = original_poll(max_messages)
        if len(messages) >= 3 and not dropped["done"]:
            dropped["done"] = True
            return messages[:2]  # message offset 2 never arrives
        return messages

    group.poll = poll_with_drop  # type: ignore[assignment]
    result = consumer.drain()
    assert seen == [0, 1, 2, 3, 4, 5]
    assert result.gap_recoveries >= 1
    assert group.lag() == 0
    assert len(seen) == len(set(seen))


def test_crash_then_recover_loses_nothing_and_duplicates_nothing():
    """Inject a known fault: kill the consumer mid-batch, assert exactly-once."""
    instance = bus(default_partitions=1)
    publisher(instance, "k", 8)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))

    # Process a prefix without acknowledging it, then die.
    group.poll(3)
    seen.clear()
    group.crash()
    assert group.cursor[0] == group.committed[0] == 0

    result = consumer.drain()
    assert seen == [0, 1, 2, 3, 4, 5, 6, 7]
    assert len(seen) == len(set(seen))
    assert result.duplicates_suppressed == 0
    assert group.counters["crashes"] == 1


def test_crash_after_acknowledgement_does_not_reprocess():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 4)
    group = instance.create_group("events", "g")
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))
    consumer.run_once(2)
    assert seen == [0, 1]
    group.crash()
    result = consumer.drain()
    assert seen == [0, 1, 2, 3]  # 0 and 1 were committed, so they are not replayed
    assert result.duplicates_suppressed == 0


def test_has_gap_reports_a_stalled_partition():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 3)
    group = instance.create_group("events", "g")
    consumer = Consumer(group, lambda m: None)
    assert consumer.has_gap() is False
    group.poll(2)
    assert consumer.has_gap() is True
    assert consumer.recover_gap() == 1
    assert consumer.has_gap() is False


def test_order_window_overflow_forces_a_release():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 10)
    group = instance.create_group("events", "g", max_reorder_window=2)
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen))

    # Deliver everything except offset 0: the window fills and must fail open.
    messages = group.topic.partitions[0].messages[1:]
    group.cursor[0] = 1
    result = consumer.run_once(10)
    del messages
    assert result.order_gap_forced >= 1
    assert seen == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_consumer_own_dedup_store_is_used():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 2)
    group = instance.create_group("events", "g")
    store = IdempotencyStore()
    seen: list[int] = []
    consumer = Consumer(group, handler_collect(seen), dedup=store)
    consumer.drain()
    assert consumer.dedup is store
    assert len(store) == 2


def test_drain_is_bounded_and_terminates_on_an_empty_bus():
    instance = bus(default_partitions=2)
    group = instance.create_group("events", "g")
    result = Consumer(group, lambda m: None).drain()
    assert result.processed == 0


def test_process_result_as_dict_is_json_friendly():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 1)
    group = instance.create_group("events", "g")
    payload = Consumer(group, lambda m: None).run_once(1).as_dict()
    assert payload["processed"] == 1
    assert "duplicates_suppressed" in payload


# ----------------------------------------------------------------------
# replay
# ----------------------------------------------------------------------


def test_replay_reapplies_every_record():
    instance = bus(default_partitions=2)
    for key in ("a", "b", "c"):
        publisher(instance, key, 3)
    replayed: list[tuple] = []
    count = instance.replay("events", lambda m: replayed.append((m.key, m.offset)))
    assert count == 9
    assert len(replayed) == 9


def test_replay_from_an_offset_is_inclusive():
    """``from_offset`` means "start replaying at this offset", as in Kafka."""
    instance = bus(default_partitions=1)
    publisher(instance, "k", 5)
    replayed: list[int] = []
    count = instance.replay("events", lambda m: replayed.append(m.offset), from_offset=2)
    assert replayed == [2, 3, 4]
    assert count == 3


def test_replay_can_be_filtered_by_key():
    instance = bus(default_partitions=2)
    publisher(instance, "a", 3)
    publisher(instance, "b", 3)
    replayed: list[str] = []
    instance.replay("events", lambda m: replayed.append(m.key), keys={"a"})
    assert set(replayed) == {"a"}


def test_replay_to_an_offset_stops_early():
    instance = bus(default_partitions=1)
    publisher(instance, "k", 5)
    replayed: list[int] = []
    instance.replay("events", lambda m: replayed.append(m.offset), to_offset=2)
    assert replayed == [0, 1, 2]


def test_message_as_dict_carries_the_trace_id():
    instance = bus(default_partitions=1)
    message = instance.publish("events", key="k", payload={}, headers={"trace_id": "t1"})
    assert message.trace_id == "t1"
    assert message.as_dict()["headers"]["trace_id"] == "t1"


def test_with_attempt_preserves_identity():
    instance = bus(default_partitions=1)
    message = instance.publish("events", key="k", payload={"a": 1})
    retried = message.with_attempt(3)
    assert retried.msg_id == message.msg_id
    assert retried.attempt == 3
    assert message.attempt == 0


def test_topic_as_dict_shape():
    topic = Topic("t", 3)
    payload = topic.as_dict()
    assert payload["partitions"] == 3
    assert payload["messages"] == 0
    assert sorted(payload["offsets"]) == ["0", "1", "2"]


def test_consumer_group_as_dict_shape():
    instance = bus(default_partitions=2)
    publisher(instance, "k", 2)
    group: ConsumerGroup = instance.create_group("events", "g")
    payload = group.as_dict()
    assert payload["group"] == "g"
    assert payload["lag"] == 2
    assert "counters" in payload

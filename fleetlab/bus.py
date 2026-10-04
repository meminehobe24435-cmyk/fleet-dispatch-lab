"""Self-contained message bus with Kafka/MQTT-shaped semantics.

There is no Kafka, no RabbitMQ and no MQTT broker in the target environment, so
the platform implements the parts of those systems that actually matter for a
dispatch platform, and makes each one testable:

* **topics and partitions** — a message is routed to ``stable_hash(key) % N`` so
  ordering is per-key, which is what a real bus promises.
* **offsets** — monotonic per partition, and the consumer's *committed* offset
  only advances past a contiguous run of acknowledgements, so a crash replays
  at-least-once instead of skipping work.
* **consumer groups** — independent read cursors over the same log.
* **ack / nack** — a nack schedules a redelivery; too many redeliveries route
  the message to a dead-letter topic with the failure reason attached.
* **backpressure** — a bounded lag.  Policy: the *producer* is rejected with
  :class:`BackpressureError` when the group lag crosses the limit, and the
  consumer exposes ``lag`` as a metric.  We deliberately do *not* buffer without
  bound: an unbounded buffer converts a latency problem into an out-of-memory
  problem.
* **ordering** — a consumer-side reorder guard detects out-of-order delivery
  (which the fault injector produces on purpose), holds the message in a window
  and releases it once the gap is filled.  If the window overflows we release the
  lowest offset anyway and count it, because waiting forever on a *lost* message
  is a hang, not a guarantee.
* **dedup** — every message carries a stable ``msg_id``; the consumer suppresses
  repeats so at-least-once delivery still yields exactly-once *effects*.

Everything here is deterministic: keys are hashed with MD5 rather than
``hash()`` (which is salted per process) and every iteration over a mapping is
sorted.
"""

from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .idempotency import IdempotencyStore

__all__ = [
    "Message",
    "Partition",
    "Topic",
    "MessageBus",
    "ConsumerGroup",
    "Consumer",
    "BackpressureError",
    "TopicNotFound",
    "ProcessResult",
    "DLQ_PREFIX",
]

#: Topic-name prefix for dead-letter topics.
DLQ_PREFIX = "__dlq__."


class BackpressureError(Exception):
    """Raised by ``publish`` when the bus is over its lag budget."""


class TopicNotFound(Exception):
    """Raised when publishing to or reading from an undeclared topic."""


def stable_hash(value: str) -> int:
    """Process-independent hash, so partition routing survives a restart.

    Python's builtin ``hash`` is salted by ``PYTHONHASHSEED``; using it here
    would make the same message land in a different partition on every run and
    would destroy both ordering and reproducibility.
    """
    digest = hashlib.md5(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True)
class Message:
    """An immutable bus record."""

    topic: str
    partition: int
    offset: int
    key: str
    msg_id: str
    payload: dict
    headers: dict[str, str] = field(default_factory=dict)
    created_at_ms: int = 0
    attempt: int = 0

    def with_attempt(self, attempt: int) -> "Message":
        return Message(
            topic=self.topic,
            partition=self.partition,
            offset=self.offset,
            key=self.key,
            msg_id=self.msg_id,
            payload=self.payload,
            headers=self.headers,
            created_at_ms=self.created_at_ms,
            attempt=attempt,
        )

    @property
    def trace_id(self) -> str:
        return self.headers.get("trace_id", "")

    def as_dict(self) -> dict:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
            "msg_id": self.msg_id,
            "payload": self.payload,
            "headers": dict(sorted(self.headers.items())),
            "created_at_ms": self.created_at_ms,
            "attempt": self.attempt,
        }


@dataclass
class Partition:
    """An append-only ordered log."""

    topic: str
    index: int
    messages: list[Message] = field(default_factory=list)

    @property
    def next_offset(self) -> int:
        return len(self.messages)

    def append(
        self,
        key: str,
        payload: dict,
        *,
        headers: dict[str, str] | None = None,
        at_ms: int = 0,
        msg_id: str | None = None,
    ) -> Message:
        offset = self.next_offset
        message = Message(
            topic=self.topic,
            partition=self.index,
            offset=offset,
            key=key,
            msg_id=msg_id or f"{self.topic}-{self.index}-{offset}",
            payload=payload,
            headers=dict(headers or {}),
            created_at_ms=at_ms,
        )
        self.messages.append(message)
        return message

    def read_from(self, offset: int, limit: int) -> list[Message]:
        return self.messages[offset: offset + limit]

    def size_bytes(self) -> int:
        """Rough payload footprint, used for reporting only."""
        return sum(len(str(m.payload)) for m in self.messages)


@dataclass
class Topic:
    """A named set of partitions."""

    name: str
    num_partitions: int = 4
    partitions: list[Partition] = field(default_factory=list)
    created_at_ms: int = 0

    def __post_init__(self) -> None:
        if not self.partitions:
            self.partitions = [
                Partition(self.name, i) for i in range(self.num_partitions)
            ]

    def partition_for_key(self, key: str) -> Partition:
        return self.partitions[stable_hash(key) % len(self.partitions)]

    def total_messages(self) -> int:
        return sum(p.next_offset for p in self.partitions)

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "partitions": len(self.partitions),
            "messages": self.total_messages(),
            "offsets": {str(p.index): p.next_offset for p in sorted(self.partitions, key=lambda p: p.index)},
        }


class ConsumerGroup:
    """A durable read cursor plus acknowledgement bookkeeping over one topic."""

    def __init__(
        self,
        bus: "MessageBus",
        topic: Topic,
        name: str,
        *,
        max_retries: int = 3,
        max_reorder_window: int = 64,
    ) -> None:
        self.bus = bus
        self.topic = topic
        self.name = name
        self.max_retries = max_retries
        self.max_reorder_window = max_reorder_window

        #: next offset to *read* per partition (speculative read position).
        self.cursor: dict[int, int] = {p.index: 0 for p in topic.partitions}
        #: contiguous acked prefix per partition (durable position on crash).
        self.committed: dict[int, int] = {p.index: 0 for p in topic.partitions}
        self._acked: dict[int, set[int]] = {p.index: set() for p in topic.partitions}
        self._retry_queue: dict[int, deque[Message]] = {
            p.index: deque() for p in topic.partitions
        }
        self.attempts: dict[str, int] = {}
        self.dlq: list[dict] = []
        self.counters: dict[str, int] = {
            "delivered": 0,
            "acked": 0,
            "nacked": 0,
            "redelivered": 0,
            "dlq": 0,
            "crashes": 0,
            "recoveries": 0,
        }

    # -- reading ----------------------------------------------------------

    def poll(self, max_messages: int = 10) -> list[Message]:
        """Return up to ``max_messages``; retries are served before fresh reads."""
        out: list[Message] = []
        partitions = sorted(self.cursor)
        for index in partitions:
            queue = self._retry_queue[index]
            while queue and len(out) < max_messages:
                out.append(queue.popleft())
                self.counters["redelivered"] += 1
        for index in partitions:
            partition = self.topic.partitions[index]
            while self.cursor[index] < partition.next_offset and len(out) < max_messages:
                out.append(partition.messages[self.cursor[index]])
                self.cursor[index] += 1
        self.counters["delivered"] += len(out)
        return out

    def ack(self, message: Message) -> bool:
        """Acknowledge; only a *contiguous* prefix advances ``committed``."""
        index = message.partition
        if message.offset < self.committed[index] or message.offset in self._acked[index]:
            return False  # duplicate ack is a no-op, not an error
        self._acked[index].add(message.offset)
        while self.committed[index] in self._acked[index]:
            self._acked[index].discard(self.committed[index])
            self.committed[index] += 1
        self.counters["acked"] += 1
        return True

    def nack(self, message: Message, reason: str = "") -> str:
        """Reject a delivery.

        Returns ``"retry"`` when the message was requeued and ``"dlq"`` when it
        exhausted ``max_retries`` and was routed to the dead-letter topic.
        """
        index = message.partition
        attempt = self.attempts.get(message.msg_id, 0) + 1
        self.attempts[message.msg_id] = attempt
        self.counters["nacked"] += 1
        if attempt > self.max_retries:
            self._to_dlq(message, reason, attempt)
            self.ack(message)
            self.counters["dlq"] += 1
            return "dlq"
        self._retry_queue[index].append(message.with_attempt(attempt))
        return "retry"

    def _to_dlq(self, message: Message, reason: str, attempt: int) -> None:
        record = {
            "msg_id": message.msg_id,
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "key": message.key,
            "attempts": attempt,
            "reason": reason,
            "payload": message.payload,
            "trace_id": message.trace_id,
        }
        self.dlq.append(record)
        dlq_topic = DLQ_PREFIX + self.topic.name
        self.bus.declare_topic(dlq_topic, self.topic.num_partitions)
        self.bus.publish(
            dlq_topic,
            key=message.key,
            payload=record,
            headers={"trace_id": message.trace_id, "origin": message.topic},
            at_ms=message.created_at_ms,
            _bypass_backpressure=True,
        )

    # -- lag / recovery ---------------------------------------------------

    def lag(self) -> int:
        """Unacknowledged messages: produced but not yet durably consumed."""
        total = 0
        for index in sorted(self.committed):
            head = self.topic.partitions[index].next_offset
            total += max(0, head - self.committed[index])
        return total

    def lag_by_partition(self) -> dict[str, int]:
        return {
            str(i): max(0, self.topic.partitions[i].next_offset - self.committed[i])
            for i in sorted(self.committed)
        }

    def commit_position(self) -> dict[str, int]:
        return {str(i): self.committed[i] for i in sorted(self.committed)}

    def crash(self) -> None:
        """Simulate process death: lose the speculative cursor, keep the commit.

        This is the whole point of separating ``cursor`` from ``committed`` — an
        abrupt exit cannot lose acknowledged work, and anything unacknowledged is
        re-read from the last commit.
        """
        self.counters["crashes"] += 1
        for index in sorted(self.committed):
            self.cursor[index] = self.committed[index]
            self._retry_queue[index].clear()

    def recover(self) -> dict[str, int]:
        """Restart from the committed offset; returns the resume position."""
        self.counters["recoveries"] += 1
        return self.commit_position()

    def as_dict(self) -> dict:
        return {
            "group": self.name,
            "topic": self.topic.name,
            "lag": self.lag(),
            "committed": self.commit_position(),
            "cursor": {str(i): self.cursor[i] for i in sorted(self.cursor)},
            "counters": dict(sorted(self.counters.items())),
            "dlq": len(self.dlq),
        }


@dataclass
class ProcessResult:
    """Outcome of one consumer pass."""

    processed: int = 0
    acked: int = 0
    nacked: int = 0
    duplicates_suppressed: int = 0
    out_of_order_detected: int = 0
    order_gap_forced: int = 0
    gap_recoveries: int = 0
    dlq: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "processed": self.processed,
            "acked": self.acked,
            "nacked": self.nacked,
            "duplicates_suppressed": self.duplicates_suppressed,
            "out_of_order_detected": self.out_of_order_detected,
            "order_gap_forced": self.order_gap_forced,
            "gap_recoveries": self.gap_recoveries,
            "dlq": self.dlq,
            "errors": list(self.errors),
        }


class Consumer:
    """Turns a raw ``poll`` into correct, deduplicated, ordered processing.

    The consumer is where the "typical distributed-systems traps" are handled,
    and every handling path is counted so the behaviour is assertable:

    ==========================  =========================================
    Trap                        Handling
    ==========================  =========================================
    duplicate delivery          ``msg_id`` dedup, effects applied once
    out-of-order delivery       per-partition expected-offset gate + window
    message loss                nothing is acknowledged until handled
    handler failure              nack -> retry -> dead-letter topic
    ==========================  =========================================
    """

    def __init__(
        self,
        group: ConsumerGroup,
        handler: Callable[[Message], Any],
        *,
        dedup: IdempotencyStore | None = None,
        order_guard: bool = True,
    ) -> None:
        self.group = group
        self.handler = handler
        self.dedup = dedup if dedup is not None else IdempotencyStore()
        self.order_guard = order_guard
        self.expected: dict[int, int] = dict(group.committed)
        self._buffer: dict[int, dict[int, Message]] = {}
        self.calls: list[str] = []
        self.failures: dict[str, int] = {}
        #: msg_ids whose handler must raise, for fault-injection tests.
        self.poison: set[str] = set()

    # -- ordering ---------------------------------------------------------

    def _ready(self, partition: int) -> list[Message]:
        """Release buffered messages in offset order once the gap is filled."""
        buffered = self._buffer.get(partition)
        if not buffered:
            return []
        out: list[Message] = []
        while self.expected[partition] in buffered:
            out.append(buffered.pop(self.expected[partition]))
            self.expected[partition] += 1
        return out

    def _gate(self, message: Message, result: ProcessResult) -> list[Message]:
        """Apply the order gate; returns messages that are now safe to handle."""
        if not self.order_guard:
            return [message]
        partition = message.partition
        want = self.expected.setdefault(partition, 0)
        buffered = self._buffer.setdefault(partition, {})

        if message.offset < want:
            # Already passed this offset: a stale redelivery, not a new ordering
            # violation.  Dedup deals with it; do not double-count as disorder.
            return [message]
        if message.offset == want:
            self.expected[partition] = want + 1
            return [message] + self._ready(partition)

        # A genuine hole: buffer and wait for the missing offset.
        result.out_of_order_detected += 1
        if message.offset in buffered:
            return []
        buffered[message.offset] = message
        if len(buffered) > self.group.max_reorder_window:
            # The window is full, so the gap is probably a *lost* message rather
            # than a late one.  Fail open, and count it loudly.
            lowest = min(buffered)
            released = [buffered.pop(o) for o in sorted(buffered)]
            self.expected[partition] = lowest + len(released)
            result.order_gap_forced += 1
            return released
        return []

    # -- processing -------------------------------------------------------

    def run_once(self, max_messages: int = 10) -> ProcessResult:
        """One poll + handle + ack cycle."""
        result = ProcessResult()
        for message in self.group.poll(max_messages):
            try:
                for ready in self._gate(message, result):
                    self._handle_one(ready, result)
            except Exception as exc:  # noqa: BLE001 - a handler fault must not kill the loop
                result.errors.append(f"{type(exc).__name__}: {exc}")
        return result

    def _handle_one(self, message: Message, result: ProcessResult) -> None:
        # 1. Claim the message id.  Claiming (rather than "mark as done
        #    immediately") is what makes dedup and retry compatible: a handler
        #    failure releases the claim, so the redelivery is allowed to run,
        #    while a *successful* pass keeps the claim and any later copy of the
        #    same message becomes a no-op.
        if not self.dedup.claim(message.msg_id):
            result.duplicates_suppressed += 1
            self.group.ack(message)
            return

        self.calls.append(message.msg_id)
        if message.msg_id in self.poison:
            self.failures[message.msg_id] = self.failures.get(message.msg_id, 0) + 1
            self.dedup.release(message.msg_id)
            outcome = self.group.nack(message, reason="handler error (injected poison)")
            result.nacked += 1
            if outcome == "dlq":
                result.dlq += 1
            return

        try:
            self.handler(message)
        except Exception as exc:  # noqa: BLE001
            self.failures[message.msg_id] = self.failures.get(message.msg_id, 0) + 1
            self.dedup.release(message.msg_id)
            outcome = self.group.nack(message, reason=f"{type(exc).__name__}: {exc}")
            result.nacked += 1
            result.errors.append(f"{message.msg_id}: {exc}")
            if outcome == "dlq":
                result.dlq += 1
            return

        # 2. Success: promote the claim to "done" and only then acknowledge, so
        #    a crash between effect and ack causes a replay, never a loss.
        self.dedup.mark(message.msg_id)
        result.processed += 1
        self.group.ack(message)
        result.acked += 1

    def drain(self, *, max_iterations: int = 10_000, max_gap_recoveries: int = 16) -> ProcessResult:
        """Process until the group has no lag, or until no progress is possible.

        A stalled gap is *not* the same as an empty queue.  If the read cursor has
        moved past an offset that was never acknowledged, that message's delivery
        was lost, so we re-seek the cursor back to the committed offset and let
        the log hand it over again.  Already-handled neighbours come back too,
        but dedup makes their second pass free.  This is the difference between
        "lost message causes a stuck task" and "lost message causes a re-read".
        """
        total = ProcessResult()
        idle = 0
        recoveries = 0
        for _ in range(max_iterations):
            batch = self.run_once()
            _accumulate(total, batch)
            progress = (
                batch.processed
                + batch.nacked
                + batch.duplicates_suppressed
                + batch.out_of_order_detected
            )
            if progress == 0:
                if recoveries < max_gap_recoveries and self.has_gap():
                    if self.recover_gap():
                        recoveries += 1
                        total.gap_recoveries += 1
                        idle = 0
                        continue
                idle += 1
                if idle >= 2:
                    break
            else:
                idle = 0
        return total

    def has_gap(self) -> bool:
        """True when the read cursor has outrun the committed offset."""
        return any(
            self.group.cursor[p] > self.group.committed[p]
            for p in sorted(self.group.committed)
        )

    def recover_gap(self) -> int:
        """Re-seek every lagging partition to its committed offset.

        Returns the number of partitions rewound.  The order gate is re-aligned
        too, so messages already sitting in the reorder window are released once
        the missing predecessor is replayed.
        """
        rewound = 0
        for partition in sorted(self.group.committed):
            committed = self.group.committed[partition]
            if self.group.cursor[partition] > committed:
                self.group.cursor[partition] = committed
                self.group._retry_queue[partition].clear()
                rewound += 1
            if self.expected.get(partition, 0) > committed:
                self.expected[partition] = committed
        return rewound


def _accumulate(total: ProcessResult, batch: ProcessResult) -> None:
    total.processed += batch.processed
    total.acked += batch.acked
    total.nacked += batch.nacked
    total.duplicates_suppressed += batch.duplicates_suppressed
    total.out_of_order_detected += batch.out_of_order_detected
    total.order_gap_forced += batch.order_gap_forced
    total.gap_recoveries += batch.gap_recoveries
    total.dlq += batch.dlq
    total.errors.extend(batch.errors)


class MessageBus:
    """Topic registry plus consumer groups plus a lag budget."""

    def __init__(
        self,
        *,
        default_partitions: int = 4,
        backpressure_limit: int | None = None,
        max_retries: int = 3,
    ) -> None:
        self.topics: dict[str, Topic] = {}
        self.groups: dict[str, ConsumerGroup] = {}
        self.default_partitions = default_partitions
        self.backpressure_limit = backpressure_limit
        self.max_retries = max_retries
        self.counters: dict[str, int] = {
            "published": 0,
            "backpressure_rejections": 0,
            "topics_declared": 0,
        }

    # -- topics -----------------------------------------------------------

    def declare_topic(self, name: str, num_partitions: int | None = None) -> Topic:
        existing = self.topics.get(name)
        if existing is not None:
            return existing
        topic = Topic(name, num_partitions or self.default_partitions)
        self.topics[name] = topic
        self.counters["topics_declared"] += 1
        return topic

    def topic(self, name: str) -> Topic:
        try:
            return self.topics[name]
        except KeyError:
            raise TopicNotFound(f"topic {name!r} is not declared") from None

    # -- publishing -------------------------------------------------------

    def publish(
        self,
        topic: str,
        *,
        key: str,
        payload: dict,
        headers: dict[str, str] | None = None,
        at_ms: int = 0,
        msg_id: str | None = None,
        _bypass_backpressure: bool = False,
    ) -> Message:
        """Append a message, or raise :class:`BackpressureError` if over budget.

        The rejection is deliberate and counted: a producer that is told "no"
        can shed load or retry with backoff, whereas a producer that is silently
        buffered finds out about the backlog only when the process dies.
        """
        record = self.topic(topic)
        if not _bypass_backpressure and self.backpressure_limit is not None:
            if self.max_lag() >= self.backpressure_limit:
                self.counters["backpressure_rejections"] += 1
                raise BackpressureError(
                    f"topic {topic!r} refused publish: lag {self.max_lag()} "
                    f">= limit {self.backpressure_limit}"
                )
        partition = record.partition_for_key(key)
        message = partition.append(
            key, payload, headers=headers, at_ms=at_ms, msg_id=msg_id
        )
        self.counters["published"] += 1
        return message

    # -- consumer groups --------------------------------------------------

    def create_group(
        self,
        topic: str,
        group: str,
        *,
        max_retries: int | None = None,
        max_reorder_window: int = 64,
    ) -> ConsumerGroup:
        record = self.topic(topic)
        if group in self.groups:
            return self.groups[group]
        consumer_group = ConsumerGroup(
            self,
            record,
            group,
            max_retries=self.max_retries if max_retries is None else max_retries,
            max_reorder_window=max_reorder_window,
        )
        self.groups[group] = consumer_group
        return consumer_group

    def group(self, name: str) -> ConsumerGroup:
        try:
            return self.groups[name]
        except KeyError:
            raise TopicNotFound(f"consumer group {name!r} does not exist") from None

    # -- observability ----------------------------------------------------

    def max_lag(self) -> int:
        if not self.groups:
            return 0
        return max(g.lag() for g in self.groups.values())

    def total_lag(self) -> int:
        return sum(g.lag() for g in self.groups.values())

    def dlq_records(self) -> list[dict]:
        out: list[dict] = []
        for name in sorted(self.groups):
            out.extend(self.groups[name].dlq)
        return out

    def stats(self) -> dict:
        live_topics = {n: t for n, t in self.topics.items() if not n.startswith(DLQ_PREFIX)}
        return {
            "topics": {n: live_topics[n].as_dict() for n in sorted(live_topics)},
            "dlq_topics": sorted(n for n in self.topics if n.startswith(DLQ_PREFIX)),
            "groups": {n: self.groups[n].as_dict() for n in sorted(self.groups)},
            "counters": dict(sorted(self.counters.items())),
            "max_lag": self.max_lag(),
            "total_lag": self.total_lag(),
        }

    # -- replay -----------------------------------------------------------

    def read_log(self, topic: str, partition: int | None = None) -> list[Message]:
        """Read the raw log, optionally one partition, ordered by (partition, offset)."""
        record = self.topic(topic)
        if partition is not None:
            return list(record.partitions[partition].messages)
        out: list[Message] = []
        for p in sorted(record.partitions, key=lambda p: p.index):
            out.extend(p.messages)
        return out

    def replay(
        self,
        topic: str,
        handler: Callable[[Message], Any],
        *,
        from_offset: int = 0,
        keys: Iterable[str] | None = None,
        to_offset: int | None = None,
    ) -> int:
        """Re-apply the log to any handler, optionally filtered.

        ``from_offset`` is **inclusive** and ``to_offset`` inclusive, matching the
        way an operator says "replay from offset 500".  Returns the number of
        records replayed.

        This is *the* recovery story of the platform: the event log is the system
        of record, so a rebuilt state store must equal the live one.
        """
        wanted = set(keys) if keys is not None else None
        count = 0
        for message in self.read_log(topic):
            if message.offset < from_offset:
                continue
            if to_offset is not None and message.offset > to_offset:
                continue
            if wanted is not None and message.key not in wanted:
                continue
            handler(message)
            count += 1
        return count

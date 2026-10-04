"""Runtime: wires the platform together and owns the single state applier.

The one architectural rule that makes the rest of the project verifiable lives
here:

    **Every state mutation goes through** :meth:`Runtime.apply`.

Components do not write to the state object.  They emit a *record* — through the
bus for the event pipeline, or directly for things that are not bus traffic, like
a heartbeat result — and :meth:`apply` is what turns a record into state.  The
journal (:mod:`fleetlab.eventlog`) records exactly the records that were applied,
in the order they were applied, so replaying the journal into a fresh state must
produce the same state.  If any component ever bypasses this, the replay
assertion fails, which is precisely the bug it exists to catch.

The division of labour between the two storage layers is also deliberate:

* the **event log** is the source of truth and is never truncated;
* the **cache** holds the "what is true now" projection (latest pose, latest task
  progress) and is *disposable* — :meth:`Runtime.rebuild_cache` reconstructs it
  from state.  Losing the cache costs a rebuild, not correctness.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable

from .bus import Consumer, ConsumerGroup, Message, MessageBus, ProcessResult
from .cache import TTLStore
from .faults import FaultConfig, FaultInjector, FaultyConsumerGroup
from .fleet import FleetManager, Vehicle
from .idempotency import CommandDispatcher, CommandOutcome, IdempotencyStore
from .observability import (
    AlertEngine,
    DeterministicIds,
    JsonLogger,
    Metrics,
    Tracer,
)
from .eventlog import EventLog, EventRecord
from .state import PlatformState, Priority
from .task_fsm import TaskEvent, TaskState

__all__ = ["Runtime", "RuntimeConfig", "TASK_TO_VEHICLE", "PermanentError"]

#: Task event -> the vehicle-side event it implies.
TASK_TO_VEHICLE: dict[str, str] = {
    TaskEvent.DISPATCH: "DISPATCH",
    TaskEvent.ENQUEUE: "ENQUEUE",
    TaskEvent.PROCEED: "PROCEED",
    TaskEvent.START: "START",
    TaskEvent.PAUSE: "PAUSE",
    TaskEvent.RESUME: "RESUME",
    TaskEvent.RECOVER: "RECOVER",
    TaskEvent.COMPLETE: "COMPLETE",
    TaskEvent.CANCEL: "RELEASE",
    # A failed task releases its vehicle rather than faulting it: the task failed,
    # not the truck, and grounding a healthy vehicle would turn one bad job into a
    # fleet-wide capacity loss.
    TaskEvent.FAIL: "RELEASE",
    TaskEvent.REQUEUE: "RELEASE",
}

#: Events after which the vehicle is free again.
FREES_VEHICLE = {
    TaskEvent.COMPLETE,
    TaskEvent.CANCEL,
    TaskEvent.FAIL,
    TaskEvent.REQUEUE,
}


class PermanentError(Exception):
    """A handler failure that retrying cannot fix (used to exercise the DLQ)."""


@dataclass
class RuntimeConfig:
    """Everything tunable about one runtime instance."""

    vehicles: int = 8
    num_partitions: int = 4
    backpressure_limit: int | None = 200
    heartbeat_timeout_ms: int = 5_000
    max_retries: int = 3
    reorder_window: int = 64
    dedup_capacity: int = 20_000
    seed: int = 20240501
    cache_ttl_s: float = 30.0
    fault: FaultConfig = field(default_factory=FaultConfig)
    event_topic: str = "fleet.events"
    command_topic: str = "fleet.commands"


class Runtime:
    """The assembled platform: bus + state + scheduler hooks + observability."""

    def __init__(self, config: RuntimeConfig | None = None) -> None:
        self.config = config if config is not None else RuntimeConfig()
        self.lock = threading.RLock()
        #: Current *virtual* time in ms, advanced by the simulation.  Used to
        #: measure queue latency deterministically instead of with a wall clock.
        self.current_ms = 0

        # -- domain state --------------------------------------------------
        self.fleet = FleetManager(heartbeat_timeout_ms=self.config.heartbeat_timeout_ms)
        self.state = PlatformState(self.fleet)

        # -- plumbing ------------------------------------------------------
        self.journal = EventLog()
        self.bus = MessageBus(
            default_partitions=self.config.num_partitions,
            backpressure_limit=self.config.backpressure_limit,
            max_retries=self.config.max_retries,
        )
        self.bus.declare_topic(self.config.event_topic, self.config.num_partitions)
        self.bus.declare_topic(self.config.command_topic, self.config.num_partitions)

        # -- observability -------------------------------------------------
        ids = DeterministicIds(self.config.seed)
        self.ids = ids
        self.metrics = Metrics()
        self.logger = JsonLogger()
        self.tracer = Tracer(ids)
        self.alerts = AlertEngine()

        # -- idempotency ---------------------------------------------------
        self.dedup = IdempotencyStore(capacity=self.config.dedup_capacity)
        self.commands = CommandDispatcher("cmd", IdempotencyStore(capacity=self.config.dedup_capacity))

        # -- faults --------------------------------------------------------
        self.injector = FaultInjector(self.config.fault)
        self._collectors: list[Callable[[EventRecord], None]] = []
        self.event_sinks: list[Callable[[dict], None]] = []

        self._raw_group: ConsumerGroup = self.bus.create_group(
            self.config.event_topic,
            "fleet-worker",
            max_retries=self.config.max_retries,
            max_reorder_window=self.config.reorder_window,
        )
        self._raw_command_group: ConsumerGroup = self.bus.create_group(
            self.config.command_topic,
            "fleet-command-worker",
            max_retries=self.config.max_retries,
            max_reorder_window=self.config.reorder_window,
        )

        if self.config.fault.enabled:
            self.group: Any = FaultyConsumerGroup(self._raw_group, self.injector)
            self.command_group: Any = FaultyConsumerGroup(self._raw_command_group, self.injector)
        else:
            self.group = self._raw_group
            self.command_group = self._raw_command_group

        self.consumer = Consumer(self.group, self._consume, dedup=self.dedup)
        self.command_consumer = Consumer(
            self.command_group, self._consume, dedup=IdempotencyStore(capacity=4096)
        )

        # -- hot cache -----------------------------------------------------
        self.cache = TTLStore(name="runtime")

        # -- counters ------------------------------------------------------
        self.counters: dict[str, int] = {
            "events_published": 0,
            "events_published_rejected": 0,
            "events_applied": 0,
            "events_direct": 0,
            "commands_issued": 0,
            "commands_duplicate": 0,
            "commands_unconfirmed": 0,
            "dlq_redrives": 0,
            "world_ticks": 0,
        }
        self.dlq_history: list[dict] = []

    # ------------------------------------------------------------------
    # Fleet setup
    # ------------------------------------------------------------------

    def add_vehicle(
        self,
        vehicle_id: str,
        *,
        x: float = 0.0,
        y: float = 0.0,
        battery: float = 100.0,
        speed_mps: float = 12.0,
        capacity_kg: float = 2000.0,
    ) -> Vehicle:
        vehicle = Vehicle(
            vehicle_id=vehicle_id,
            capacity_kg=capacity_kg,
            x=x,
            y=y,
            speed_mps=speed_mps,
            battery=battery,
            online=True,
            last_heartbeat_ms=0,
        )
        return self.fleet.add(vehicle)

    # ------------------------------------------------------------------
    # The single applier
    # ------------------------------------------------------------------

    def apply(self, record: EventRecord) -> None:
        """Turn one journal record into state.  Live path *and* replay path."""
        kind = record.kind
        payload = record.payload
        at_ms = record.at_ms
        trace_id = record.trace_id
        handler = getattr(self, f"_apply_{kind}", None)
        if handler is None:
            raise ValueError(f"no applier for event kind {kind!r}")
        handler(payload, at_ms, trace_id, record)
        self.counters["events_applied"] += 1
        for collector in self._collectors:
            collector(record)

    def _apply_task_created(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        task_id = payload["task_id"]
        if task_id in self.state.tasks:
            return  # idempotent: a redelivered create must not duplicate the task
        self.state.create_task(
            task_id,
            created_ms=payload.get("created_ms", at_ms),
            origin=(payload["origin"][0], payload["origin"][1]),
            destination=(payload["destination"][0], payload["destination"][1]),
            priority=payload.get("priority", Priority.NORMAL),
            payload_kg=payload.get("payload_kg", 100.0),
            deadline_ms=payload.get("deadline_ms", 0),
        )

    def _apply_task_assigned(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        task_id = payload["task_id"]
        vehicle_id = payload["vehicle_id"]
        if not self.state.assign(task_id, vehicle_id, at_ms, trace_id):
            return
        vehicle = self.fleet.vehicles.get(vehicle_id)
        if vehicle is not None:
            if vehicle.apply("ASSIGN", at_ms=at_ms, trace_id=trace_id, note=task_id):
                vehicle.payload = task_id

    def _apply_task_requeued(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        task_id = payload["task_id"]
        task = self.state.tasks.get(task_id)
        if task is None:
            return
        if not task.apply(
            TaskEvent.REQUEUE,
            at_ms=at_ms,
            trace_id=trace_id,
            note=payload.get("note", "requeued"),
        ):
            return
        task.preempted += 1
        vehicle_id = payload.get("vehicle_id") or task.assigned_vehicle
        task.assigned_vehicle = ""
        vehicle = self.fleet.vehicles.get(vehicle_id) if vehicle_id else None
        if vehicle is not None:
            vehicle.payload = ""
            vehicle.apply("RELEASE", at_ms=at_ms, trace_id=trace_id, note=task_id)

    def _apply_task_event(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        task_id = payload["task_id"]
        event = payload["event"]
        task = self.state.tasks.get(task_id)
        if task is None:
            return
        vehicle_id = task.assigned_vehicle
        if not self.state.apply(
            task_id, event, at_ms=at_ms, trace_id=trace_id, note=payload.get("note", "")
        ):
            return
        if task.state == TaskState.COMPLETED:
            self.metrics.incr("tasks_completed")
        vehicle_event = TASK_TO_VEHICLE.get(event)
        if not vehicle_id or not vehicle_event:
            return
        vehicle = self.fleet.vehicles.get(vehicle_id)
        if vehicle is None:
            return
        vehicle.apply(vehicle_event, at_ms=at_ms, trace_id=trace_id, note=task_id)
        if event == TaskEvent.COMPLETE:
            vehicle.tasks_completed += 1
        if event == TaskEvent.PAUSE:
            # Counted on the vehicle so "10 duplicate PAUSE commands apply once"
            # has an observable counter to assert on.
            vehicle.pause_commands += 1
        if event == TaskEvent.RESUME:
            vehicle.resume_commands += 1
        if event in FREES_VEHICLE:
            vehicle.payload = ""
            task.assigned_vehicle = ""

    def _apply_vehicle_offline(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        vehicle = self.fleet.vehicles.get(payload["vehicle_id"])
        if vehicle is None or not vehicle.online:
            return
        vehicle.online = False
        self.fleet.counters["offline_events"] += 1
        self.fleet.counters["timeouts"] += 1
        vehicle.apply("HEARTBEAT_TIMEOUT", at_ms=at_ms, trace_id=trace_id, note=payload.get("note", ""))
        self.fleet.offline_log.append(
            {
                "vehicle_id": vehicle.vehicle_id,
                "at_ms": at_ms,
                "silent_ms": payload.get("silent_ms", 0),
                "state_at_timeout": vehicle.state,
            }
        )

    def _apply_vehicle_online(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        vehicle = self.fleet.vehicles.get(payload["vehicle_id"])
        if vehicle is None or vehicle.online:
            return
        vehicle.online = True
        self.fleet.counters["reconnects"] += 1
        vehicle.apply("ONLINE", at_ms=at_ms, trace_id=trace_id, note="reconnected")

    def _apply_vehicle_event(self, payload: dict, at_ms: int, trace_id: str, record) -> None:
        """Vehicle-internal transitions (charging, repair) as journaled events.

        These were originally applied inline by the simulation loop, which made
        the event log an incomplete description of what happened and broke the
        replay assertion.  Anything that changes a vehicle's *state* belongs in
        the log, even when nothing on the bus caused it.
        """
        vehicle = self.fleet.vehicles.get(payload["vehicle_id"])
        if vehicle is None:
            return
        vehicle.apply(
            payload["event"], at_ms=at_ms, trace_id=trace_id, note=payload.get("note", "")
        )

    # ------------------------------------------------------------------
    # Emitting
    # ------------------------------------------------------------------

    def publish(
        self,
        kind: str,
        subject: str,
        payload: dict,
        *,
        at_ms: int,
        trace_id: str = "",
        topic: str | None = None,
        span_id: str = "",
        msg_id: str | None = None,
    ) -> Message | None:
        """Put an event on the bus; returns ``None`` if the bus refused it.

        A refused publish is a *normal* outcome under backpressure, not an
        exception to be swallowed: the caller is expected to hold the work and
        try again, which is what the simulation loop does.

        ``msg_id`` is normally left ``None`` (the bus derives it from the topic,
        partition and offset).  The dead-letter redrive passes the original id so
        that the deduplicator still recognises a message it has already handled.
        """
        headers = {"trace_id": trace_id, "kind": kind, "subject": subject}
        if span_id:
            headers["span_id"] = span_id
        body = {
            "kind": kind,
            "subject": subject,
            "payload": payload,
            "at_ms": at_ms,
            "trace_id": trace_id,
            "span_id": span_id,
        }
        # A span for the hop itself.  Without it a sim-generated event had only a
        # consumer span, so a trace showed *that* something was applied but not
        # who produced it — the chain was a stump, not a chain.
        with self.tracer.span(
            "bus.publish",
            trace_id=trace_id,
            parent_span_id=span_id,
            at_ms=at_ms,
            kind=kind,
            subject=subject,
        ) as publish_span:
            body["span_id"] = publish_span.span_id
            headers["span_id"] = publish_span.span_id
            try:
                message = self.bus.publish(
                    topic or self.config.event_topic,
                    key=subject,
                    payload=body,
                    headers=headers,
                    at_ms=at_ms,
                    msg_id=msg_id,
                )
            except Exception:  # BackpressureError and friends
                self.counters["events_published_rejected"] += 1
                self.metrics.incr("bus_publish_rejected")
                publish_span.attributes["outcome"] = "rejected"
                return None
            publish_span.attributes["msg_id"] = message.msg_id
            publish_span.attributes["partition"] = message.partition
            publish_span.attributes["offset"] = message.offset
        self.counters["events_published"] += 1
        self.metrics.incr("bus_published")
        return message

    def emit_direct(
        self,
        kind: str,
        subject: str,
        payload: dict,
        *,
        at_ms: int,
        trace_id: str = "",
        span_id: str = "",
    ) -> EventRecord:
        """Journal and apply immediately, bypassing the bus.

        Used for observations that are not bus traffic — a heartbeat result, a
        timeout verdict — but which still change state, and therefore must still
        be journaled for replay to reproduce them.
        """
        record = self.journal.append(
            kind,
            subject,
            at_ms=at_ms,
            payload=payload,
            trace_id=trace_id,
            span_id=span_id,
            origin="direct",
        )
        self.counters["events_direct"] += 1
        self.apply(record)
        return record

    # ------------------------------------------------------------------
    # Consumption
    # ------------------------------------------------------------------

    def _consume(self, message: Message) -> None:
        """Bus handler: journal, apply, log, measure — under one trace id."""
        body = message.payload
        trace_id = body.get("trace_id") or message.trace_id
        span_id = body.get("span_id", "")
        self.metrics.incr("messages_consumed")

        with self.tracer.span(
            "consumer.process",
            trace_id=trace_id,
            parent_span_id=span_id,
            at_ms=body.get("at_ms", 0),
            msg_id=message.msg_id,
            topic=message.topic,
        ) as span:
            # Injected handler failure: raise *before* journaling so nothing is
            # half-applied, which is exactly the guarantee a real handler needs
            # to be safely retryable.
            failure = self.injector.should_fail_handler(message.msg_id)
            if failure:
                span.attributes["injected_fault"] = failure
                self.metrics.incr(f"injected_{failure}")
                raise (TimeoutError if failure == "timeout" else PermanentError)(
                    f"injected {failure} fault on {message.msg_id}"
                )

            record = self.journal.append(
                body["kind"],
                body["subject"],
                at_ms=body.get("at_ms", 0),
                payload=body.get("payload", {}),
                trace_id=trace_id,
                span_id=span.span_id,
                origin="bus",
            )
            self.apply(record)
            span.attributes["kind"] = record.kind
            span.attributes["subject"] = record.subject
            span.attributes["seq"] = record.seq
            # The resulting state closes the "state changed" leg of the trace, so
            # a reader can see the transition without cross-referencing the log.
            span.attributes["result_state"] = self._subject_state(record)
            span.attributes["outcome"] = "applied"
            self.metrics.incr("events_applied_by_consumer")

        self.logger.log(
            "event_applied",
            trace_id=trace_id,
            span_id=span_id,
            at_ms=body.get("at_ms", 0),
            kind=body.get("kind", ""),
            subject=body.get("subject", ""),
            msg_id=message.msg_id,
            partition=message.partition,
            offset=message.offset,
            attempt=message.attempt,
        )
        # Virtual-time latency: how long the event sat in the pipeline.  Measured
        # against the simulation clock so the number is reproducible.
        waited = self.current_ms - body.get("at_ms", 0)
        self.metrics.observe("event_pipeline_latency_ms", max(0, waited))

    def _subject_state(self, record: EventRecord) -> str:
        """Human-readable state of whatever the record was about."""
        if record.kind.startswith("task"):
            task = self.state.tasks.get(record.payload.get("task_id", record.subject))
            return "" if task is None else task.state
        if record.kind.startswith("vehicle"):
            vehicle = self.fleet.vehicles.get(record.payload.get("vehicle_id", record.subject))
            return "" if vehicle is None else vehicle.state
        return ""

    def drain(self, *, max_iterations: int = 64, budget: int | None = None) -> ProcessResult:
        """Consume the bus, recovering gaps if needed.

        ``budget`` caps how many messages one call may handle, which is how the
        simulation models a consumer that simply cannot keep up: an unbounded
        drain makes the backlog always zero, and a backlog metric that is
        structurally always zero measures nothing.
        """
        if budget is not None:
            result = self.consumer.run_once(budget)
            self.command_consumer.run_once(max(1, budget // 2))
        else:
            result = self.consumer.drain(max_iterations=max_iterations)
            self.command_consumer.drain(max_iterations=max_iterations)
        self.metrics.incr("duplicates_suppressed", result.duplicates_suppressed)
        self.metrics.incr("out_of_order_detected", result.out_of_order_detected)
        self.metrics.incr("order_gap_forced", result.order_gap_forced)
        self.metrics.incr("gap_recoveries", result.gap_recoveries)
        self.metrics.incr("consumer_nacked", result.nacked)
        self.metrics.incr("dlq_routed", result.dlq)
        return result

    def maybe_crash(self, at_ms: int) -> bool:
        """Apply the injected consumer crash, if it is due."""
        crashed = self.injector.maybe_crash(self._raw_group, at_ms)
        if crashed:
            self.metrics.incr("consumer_crashes")
            self.logger.log("consumer_crash", at_ms=at_ms, group=self._raw_group.name)
        return crashed

    # ------------------------------------------------------------------
    # Dead-letter queue handling
    # ------------------------------------------------------------------

    def dlq_depth(self) -> int:
        return len(self._raw_group.dlq)

    def redrive_dlq(self, *, max_rounds: int = 3) -> int:
        """Re-publish dead-lettered events, keeping their original message ids.

        Keeping the id is the whole trick.  Minting a fresh one looks harmless —
        the message never got applied, so why would dedup matter? — but it makes
        the redriven copy invisible to the deduplicator, and a message that *had*
        already been applied would then be applied a second time.  In this project
        that showed up as duplicate ``task_created`` records in the journal while
        the state stayed correct, which is exactly the kind of "harmless" bug that
        silently corrupts an audit trail.
        """
        redriven = 0
        for _ in range(max_rounds):
            records = list(self._raw_group.dlq)
            if not records:
                break
            self._raw_group.dlq.clear()
            for record in records:
                self.dlq_history.append(dict(record))
                payload = record.get("payload", {})
                body = payload.get("payload", payload)
                message = self.publish(
                    payload.get("kind", "task_event"),
                    payload.get("subject", record.get("key", "")),
                    body,
                    at_ms=payload.get("at_ms", 0),
                    trace_id=payload.get("trace_id", record.get("trace_id", "")),
                    msg_id=record.get("msg_id"),
                )
                if message is None:
                    continue
                self.counters["dlq_redrives"] += 1
                redriven += 1
            self.drain()
        if redriven:
            self.metrics.incr("dlq_redrives", redriven)
        return redriven

    # ------------------------------------------------------------------
    # Control commands (idempotent)
    # ------------------------------------------------------------------

    def issue_command(
        self,
        command_id: str,
        action: str,
        task_id: str,
        *,
        at_ms: int,
        trace_id: str = "",
        drain: bool = True,
    ) -> CommandOutcome:
        """Issue a PAUSE/RESUME/CANCEL once, no matter how often it is sent.

        The trace id is minted here and carried in the message headers, so the
        whole chain — command, publish, consume, transition — can be retrieved by
        one id later.  That end-to-end retrievability is the point of tracing.
        """
        self.counters["commands_issued"] += 1
        trace_id = trace_id or self.tracer.new_trace_id()
        task = self.state.tasks.get(task_id)
        if task is None:
            return CommandOutcome(command_id, "failed", f"unknown task {task_id}")

        with self.tracer.span(
            "command.issue", trace_id=trace_id, at_ms=at_ms, action=action, task_id=task_id
        ) as span:
            before_state = task.state
            before_pause = self._pause_counter(task_id)

            def handler(_action: str, _payload: dict) -> Any:
                message = self.publish(
                    "task_event",
                    task_id,
                    {"task_id": task_id, "event": action, "note": f"command {command_id}"},
                    at_ms=at_ms,
                    trace_id=trace_id,
                    span_id=span.span_id,
                )
                if message is None:
                    # Bus refused: raise so the command is *not* marked done and a
                    # retry is still allowed.  Marking it done here would lose the
                    # command under backpressure.
                    raise TimeoutError("bus refused publish (backpressure)")
                if drain:
                    self.drain()

            def verify(_value: Any) -> bool:
                current = self.state.tasks.get(task_id)
                if current is None:
                    return False
                changed_state = current.state != before_state
                changed_counter = self._pause_counter(task_id) != before_pause
                return changed_state or changed_counter

            outcome = self.commands.submit(
                command_id,
                action,
                {"task_id": task_id},
                handler=handler,
                verify=verify,
            )
            if outcome.status == "duplicate":
                self.counters["commands_duplicate"] += 1
            elif outcome.status == "unconfirmed":
                self.counters["commands_unconfirmed"] += 1
            span.attributes["status"] = outcome.status
            span.attributes["before"] = before_state
            span.attributes["after"] = self.state.tasks.get(task_id, task).state

        self.logger.log(
            "command_result",
            trace_id=trace_id,
            span_id=span.span_id,
            at_ms=at_ms,
            command_id=command_id,
            action=action,
            task_id=task_id,
            status=outcome.status,
        )
        # The "response" span closes the loop the README's trace example shows.
        with self.tracer.span(
            "command.response",
            trace_id=trace_id,
            parent_span_id=span.span_id,
            at_ms=at_ms,
            status=outcome.status,
        ):
            pass
        return outcome

    def _pause_counter(self, task_id: str) -> int:
        task = self.state.tasks.get(task_id)
        if task is None or not task.assigned_vehicle:
            return 0
        vehicle = self.fleet.vehicles.get(task.assigned_vehicle)
        return 0 if vehicle is None else vehicle.pause_commands

    # ------------------------------------------------------------------
    # Heartbeats / liveness
    # ------------------------------------------------------------------

    def heartbeat(
        self, vehicle_id: str, *, at_ms: int, battery: float | None = None
    ) -> Vehicle:
        """Record liveness.

        Note what this does *not* do: it does not flip ``online`` back by itself.
        Coming back online is a state change, so it goes through the journal and
        the applier like everything else.  Doing it inline here would make the
        replay of ``vehicle_online`` a no-op ("already online") and produce a
        state the log does not describe.
        """
        vehicle = self.fleet.get(vehicle_id)
        self.fleet.counters["heartbeats"] += 1
        vehicle.last_heartbeat_ms = at_ms
        if battery is not None:
            vehicle.battery = battery
        self.metrics.incr("heartbeats")
        if not vehicle.online:
            self.emit_direct(
                "vehicle_online", vehicle_id, {"vehicle_id": vehicle_id}, at_ms=at_ms
            )
        return vehicle

    def check_timeouts(self, at_ms: int) -> list[str]:
        """Detect silent vehicles, journal the verdict, then raise alerts.

        Detection is read-only; the *decision* to take a vehicle offline is
        journaled, so replay reproduces it instead of silently disagreeing.
        """
        stale = self.fleet.stale_vehicles(at_ms)
        for vehicle_id, silent_ms, _state in stale:
            self.emit_direct(
                "vehicle_offline",
                vehicle_id,
                {
                    "vehicle_id": vehicle_id,
                    "silent_ms": silent_ms,
                    "note": "heartbeat timeout",
                },
                at_ms=at_ms,
            )
            self.metrics.incr("vehicles_offline")
            self.alerts.check_vehicle_offline(vehicle_id, silent_ms, at_ms=at_ms)
        return [vehicle_id for vehicle_id, _, _ in stale]

    # ------------------------------------------------------------------
    # Cache projection
    # ------------------------------------------------------------------

    def cache_vehicle(self, vehicle_id: str, at_ms: int) -> None:
        vehicle = self.fleet.vehicles[vehicle_id]
        self.cache.hset(
            f"veh:{vehicle_id}",
            {
                "state": vehicle.state,
                "x": round(vehicle.x, 3),
                "y": round(vehicle.y, 3),
                "battery": round(vehicle.battery, 2),
                "online": "1" if vehicle.online else "0",
                "payload": vehicle.payload,
                "updated_ms": at_ms,
            },
        )
        self.cache.expire(f"veh:{vehicle_id}", self.config.cache_ttl_s)

    def cache_task(self, task_id: str, at_ms: int) -> None:
        task = self.state.tasks[task_id]
        self.cache.hset(
            f"task:{task_id}",
            {
                "state": task.state,
                "vehicle": task.assigned_vehicle,
                "priority": task.priority,
                "updated_ms": at_ms,
            },
        )
        self.cache.expire(f"task:{task_id}", self.config.cache_ttl_s)

    def refresh_cache(self, at_ms: int) -> None:
        for vehicle_id in self.fleet.ids():
            self.cache_vehicle(vehicle_id, at_ms)
        for task_id in self.state.ids():
            self.cache_task(task_id, at_ms)
        self.cache.set("meta:head_seq", self.journal.head(), ttl=self.config.cache_ttl_s)
        self.cache.set("meta:last_refresh_ms", at_ms, ttl=self.config.cache_ttl_s)

    def rebuild_cache(self, at_ms: int = 0) -> int:
        """Reconstruct the cache from state after it was lost or expired."""
        self.cache.flush()
        self.refresh_cache(at_ms)
        self.metrics.incr("cache_rebuilds")
        return self.cache.size()

    # ------------------------------------------------------------------
    # Replay / consistency
    # ------------------------------------------------------------------

    def fresh_state(self) -> "Runtime":
        """A twin runtime with the same fleet skeleton but empty history."""
        twin = Runtime(RuntimeConfig(**{**self.config.__dict__, "fault": FaultConfig()}))
        for vehicle_id in self.fleet.ids():
            source = self.fleet.vehicles[vehicle_id]
            twin.add_vehicle(
                vehicle_id,
                x=source.x,
                y=source.y,
                battery=source.battery,
                speed_mps=source.speed_mps,
                capacity_kg=source.capacity_kg,
            )
        return twin

    def verify_replay(self) -> dict:
        """Replay the journal into a fresh runtime and compare fingerprints.

        The criterion is :meth:`PlatformState.lifecycle_fingerprint` — task
        states, assignments and vehicle lifecycle.  Pose and battery are excluded
        because they are produced by the physics loop rather than by events, and
        pretending otherwise would make the check fail for irrelevant reasons.
        """
        twin = self.fresh_state()
        replayed = self.journal.replay(twin.apply)
        live_fp = self.state.lifecycle_fingerprint()
        replay_fp = twin.state.lifecycle_fingerprint()
        return {
            "criterion": "PlatformState.lifecycle_fingerprint (sha256 of canonical task+vehicle lifecycle)",
            "records_replayed": replayed,
            "journal_head": self.journal.head(),
            "live_fingerprint": live_fp,
            "replay_fingerprint": replay_fp,
            "consistent": live_fp == replay_fp,
            "journal_digest": self.journal.digest(),
        }

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def state_snapshot(self) -> dict:
        return self.state.snapshot(at_ms=self.counters["world_ticks"])

    def metrics_snapshot(self) -> dict:
        self.metrics.set_gauge("bus_lag", self.bus.max_lag())
        self.metrics.set_gauge("dlq_depth", self.dlq_depth())
        return self.metrics.snapshot()

    def summary(self) -> dict:
        return {
            "vehicles": self.fleet.stats(),
            "tasks": self.state.state_counts(),
            "counters": dict(sorted(self.counters.items())),
            "bus": self.bus.stats(),
            "journal": self.journal.stats(),
            "cache_keys": self.cache.size(),
            "alerts": self.alerts.stats(),
            "commands": self.commands.stats(),
            "dedup": self.dedup.stats(),
            "consumer": self._raw_group.as_dict(),
            "tracer": self.tracer.stats(),
            "faults": self.injector.stats(),
        }

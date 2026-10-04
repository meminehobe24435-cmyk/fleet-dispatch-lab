"""Domain state: tasks, their lifecycle, and the consistency fingerprint.

The platform keeps two views of the world and this module bridges them:

* the **state machine** (``task_fsm``) owns legality of a single transition;
* the **platform state** here owns the aggregate — every task, every vehicle —
  and can answer "is this state the same as that state?".

That last question is the one that makes fault-injection testing meaningful, so
it gets a precise answer: :meth:`PlatformState.lifecycle_fingerprint`, a SHA-256
over a canonicalised projection of the state.  Two runs agree iff the digests
agree.  Having an explicit, hashable equality relation is what turns "the system
looked fine" into an assertion.

Two projections exist on purpose:

``lifecycle_fingerprint``
    task states, assignments, vehicle states and liveness.  This is exactly what
    an event replay must reproduce, so it is the criterion used for replay and
    eventual-consistency assertions.
``full_fingerprint``
    additionally includes pose and battery.  Physics is driven by the simulation
    loop rather than by events, so only a snapshot/restore round-trip (which
    copies the pose) can reproduce this one.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from .fleet import FleetManager, Vehicle
from .task_fsm import IllegalTransition, StateMachine, TaskEvent, TaskState

__all__ = ["Task", "PlatformState", "Priority"]


class Priority:
    """Task priority classes; higher wins, and preemption compares them."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5

    ALL = (LOW, NORMAL, HIGH, URGENT, CRITICAL)

    LABELS = {
        LOW: "LOW",
        NORMAL: "NORMAL",
        HIGH: "HIGH",
        URGENT: "URGENT",
        CRITICAL: "CRITICAL",
    }

    @classmethod
    def label(cls, value: int) -> str:
        return cls.LABELS.get(value, f"P{value}")


@dataclass
class Task:
    """One transport job: pick up at ``origin``, deliver to ``destination``."""

    task_id: str
    created_ms: int
    origin: tuple[float, float]
    destination: tuple[float, float]
    priority: int = Priority.NORMAL
    payload_kg: float = 100.0
    deadline_ms: int = 0
    assigned_vehicle: str = ""
    assigned_ms: int = 0
    started_ms: int = 0
    finished_ms: int = 0
    preempted: int = 0
    fsm: StateMachine = field(default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.fsm is None:
            self.fsm = StateMachine(
                self.task_id,
                initial=TaskState.PENDING,
                state=TaskState.PENDING,
            )

    # -- state ------------------------------------------------------------

    @property
    def state(self) -> str:
        return self.fsm.state

    @property
    def is_terminal(self) -> bool:
        return self.state in TaskState.TERMINAL

    @property
    def succeeded(self) -> bool:
        return self.state == TaskState.COMPLETED

    def apply(self, event: str, at_ms: int = 0, trace_id: str = "", note: str = "") -> bool:
        """Apply an event; ``False`` when rejected (state is left untouched)."""
        ok = self.fsm.try_apply(event, at_ms=at_ms, trace_id=trace_id, note=note)
        if not ok:
            return False
        if event == TaskEvent.ASSIGN:
            self.assigned_ms = at_ms
        elif event == TaskEvent.START and not self.started_ms:
            self.started_ms = at_ms
        elif event == TaskEvent.COMPLETE:
            self.finished_ms = at_ms
        elif event in (TaskEvent.CANCEL, TaskEvent.FAIL):
            self.finished_ms = at_ms
        return True

    # -- derived timing ---------------------------------------------------

    def dispatch_latency_ms(self) -> int | None:
        """Creation -> assignment.  ``None`` while still waiting for a vehicle."""
        return None if not self.assigned_ms else self.assigned_ms - self.created_ms

    def cycle_time_ms(self) -> int | None:
        """Creation -> terminal state (whatever the outcome)."""
        return None if not self.finished_ms else self.finished_ms - self.created_ms

    def service_time_ms(self) -> int | None:
        """Assignment -> terminal state: how long a vehicle was occupied."""
        if not self.finished_ms or not self.assigned_ms:
            return None
        return self.finished_ms - self.assigned_ms

    def is_overdue(self, now_ms: int) -> bool:
        return bool(self.deadline_ms) and not self.is_terminal and now_ms > self.deadline_ms

    def as_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "state": self.state,
            "priority": self.priority,
            "priority_label": Priority.label(self.priority),
            "origin": [round(self.origin[0], 3), round(self.origin[1], 3)],
            "destination": [round(self.destination[0], 3), round(self.destination[1], 3)],
            "payload_kg": self.payload_kg,
            "created_ms": self.created_ms,
            "assigned_vehicle": self.assigned_vehicle,
            "assigned_ms": self.assigned_ms,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "deadline_ms": self.deadline_ms,
            "preempted": self.preempted,
            "dispatch_latency_ms": self.dispatch_latency_ms(),
            "cycle_time_ms": self.cycle_time_ms(),
            "transitions": len(self.fsm.history),
        }


class PlatformState:
    """The aggregate: all tasks plus the fleet, with canonical projection."""

    def __init__(self, fleet: FleetManager | None = None) -> None:
        self.tasks: dict[str, Task] = {}
        self.fleet = fleet if fleet is not None else FleetManager()
        self.rejected_events: list[dict] = []
        self.counters = {
            "tasks_created": 0,
            "events_applied": 0,
            "events_rejected": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_cancelled": 0,
            "preemptions": 0,
        }

    # -- tasks ------------------------------------------------------------

    def create_task(
        self,
        task_id: str,
        *,
        created_ms: int,
        origin: tuple[float, float],
        destination: tuple[float, float],
        priority: int = Priority.NORMAL,
        payload_kg: float = 100.0,
        deadline_ms: int = 0,
    ) -> Task:
        if task_id in self.tasks:
            raise ValueError(f"task {task_id!r} already exists")
        task = Task(
            task_id=task_id,
            created_ms=created_ms,
            origin=origin,
            destination=destination,
            priority=priority,
            payload_kg=payload_kg,
            deadline_ms=deadline_ms,
        )
        self.tasks[task_id] = task
        self.counters["tasks_created"] += 1
        return task

    def get(self, task_id: str) -> Task:
        try:
            return self.tasks[task_id]
        except KeyError:
            raise KeyError(f"unknown task {task_id!r}") from None

    def apply(
        self,
        task_id: str,
        event: str,
        *,
        at_ms: int = 0,
        trace_id: str = "",
        note: str = "",
    ) -> bool:
        """Apply an event to a task and update aggregate counters.

        A rejected event is recorded rather than raised: on the bus hot path a
        duplicate or late event is normal traffic, and the interesting signal is
        the *rate* of rejections.
        """
        task = self.get(task_id)
        before = task.state
        ok = task.apply(event, at_ms=at_ms, trace_id=trace_id, note=note)
        if not ok:
            reason = task.fsm.rejections[-1].reason if task.fsm.rejections else "unknown"
            self.counters["events_rejected"] += 1
            self.rejected_events.append(
                {
                    "task_id": task_id,
                    "event": event,
                    "state": before,
                    "reason": reason,
                    "at_ms": at_ms,
                }
            )
            return False
        self.counters["events_applied"] += 1
        if task.state == TaskState.COMPLETED:
            self.counters["tasks_completed"] += 1
        elif task.state == TaskState.FAILED:
            self.counters["tasks_failed"] += 1
        elif task.state == TaskState.CANCELLED:
            self.counters["tasks_cancelled"] += 1
        return True

    def assign(self, task_id: str, vehicle_id: str, at_ms: int, trace_id: str = "") -> bool:
        """Bind a task to a vehicle, keeping both sides of the link consistent."""
        task = self.get(task_id)
        if vehicle_id not in self.fleet.vehicles:
            raise KeyError(f"unknown vehicle {vehicle_id!r}")
        if not task.apply(TaskEvent.ASSIGN, at_ms=at_ms, trace_id=trace_id, note=f"-> {vehicle_id}"):
            return False
        task.assigned_vehicle = vehicle_id
        task.assigned_ms = at_ms
        self.counters["events_applied"] += 1
        return True

    def unassign(self, task_id: str, at_ms: int, reason: str) -> bool:
        """Clear the vehicle binding (preemption, failure, cancellation)."""
        task = self.get(task_id)
        previous = task.assigned_vehicle
        task.assigned_vehicle = ""
        if previous:
            vehicle = self.fleet.vehicles.get(previous)
            if vehicle is not None and vehicle.payload == task_id:
                vehicle.payload = ""
        return bool(previous)

    # -- queries ----------------------------------------------------------

    def ids(self) -> list[str]:
        return sorted(self.tasks)

    def of_state(self, *states: str) -> list[Task]:
        wanted = set(states)
        return [self.tasks[t] for t in self.ids() if self.tasks[t].state in wanted]

    def open_tasks(self) -> list[Task]:
        return [self.tasks[t] for t in self.ids() if not self.tasks[t].is_terminal]

    def completed(self) -> list[Task]:
        return self.of_state(TaskState.COMPLETED)

    def state_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for tid in self.ids():
            state = self.tasks[tid].state
            counts[state] = counts.get(state, 0) + 1
        return {k: counts[k] for k in sorted(counts)}

    def completion_rate(self) -> float:
        """Completed / created.  A cancelled task counts against the rate."""
        total = len(self.tasks)
        return 0.0 if total == 0 else len(self.completed()) / total

    # -- canonical projection / fingerprint -------------------------------

    def lifecycle_canonical(self) -> list:
        """Task + vehicle lifecycle, normalised and sorted.

        Positions and battery are excluded because they are produced by the
        physics loop, not by bus events; including them would make a replay
        assertion impossible to satisfy for reasons unrelated to the bus.
        """
        tasks = [
            [
                self.tasks[t].task_id,
                self.tasks[t].state,
                self.tasks[t].assigned_vehicle,
                self.tasks[t].assigned_ms,
                self.tasks[t].finished_ms,
            ]
            for t in self.ids()
        ]
        vehicles = [
            [v, self.fleet.vehicles[v].state, bool(self.fleet.vehicles[v].online)]
            for v in self.fleet.ids()
        ]
        return [tasks, vehicles]

    def full_canonical(self) -> list:
        tasks = [
            [
                self.tasks[t].task_id,
                self.tasks[t].state,
                self.tasks[t].assigned_vehicle,
                self.tasks[t].assigned_ms,
                self.tasks[t].finished_ms,
            ]
            for t in self.ids()
        ]
        vehicles = [
            [
                v,
                self.fleet.vehicles[v].state,
                bool(self.fleet.vehicles[v].online),
                round(self.fleet.vehicles[v].x, 3),
                round(self.fleet.vehicles[v].y, 3),
                round(self.fleet.vehicles[v].battery, 2),
                self.fleet.vehicles[v].payload,
            ]
            for v in self.fleet.ids()
        ]
        return [tasks, vehicles]

    @staticmethod
    def _digest(payload: list) -> str:
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def lifecycle_fingerprint(self) -> str:
        """Consistency criterion for replay and fault-injection assertions."""
        return self._digest(self.lifecycle_canonical())

    def full_fingerprint(self) -> str:
        """Consistency criterion for snapshot/restore round-trips."""
        return self._digest(self.full_canonical())

    # -- snapshot / restore ----------------------------------------------

    def snapshot(self, at_ms: int = 0) -> dict:
        """Serialise the whole aggregate (history excluded; it is append-only)."""
        return {
            "at_ms": at_ms,
            "tasks": [
                {
                    "task_id": t.task_id,
                    "state": t.state,
                    "priority": t.priority,
                    "origin": list(t.origin),
                    "destination": list(t.destination),
                    "payload_kg": t.payload_kg,
                    "created_ms": t.created_ms,
                    "assigned_vehicle": t.assigned_vehicle,
                    "assigned_ms": t.assigned_ms,
                    "started_ms": t.started_ms,
                    "finished_ms": t.finished_ms,
                    "deadline_ms": t.deadline_ms,
                    "preempted": t.preempted,
                }
                for t in (self.tasks[k] for k in self.ids())
            ],
            "vehicles": [
                {
                    "vehicle_id": v.vehicle_id,
                    "state": v.state,
                    "online": bool(v.online),
                    "x": v.x,
                    "y": v.y,
                    "battery": v.battery,
                    "payload": v.payload,
                    "last_heartbeat_ms": v.last_heartbeat_ms,
                    "odometer_m": v.odometer_m,
                    "tasks_completed": v.tasks_completed,
                    "pause_commands": v.pause_commands,
                    "resume_commands": v.resume_commands,
                    "speed_mps": v.speed_mps,
                    "capacity_kg": v.capacity_kg,
                }
                for v in (self.fleet.vehicles[k] for k in self.fleet.ids())
            ],
            "counters": dict(sorted(self.counters.items())),
        }

    def restore(self, snapshot: dict) -> None:
        """Rebuild state from ``snapshot`` (used by crash-recovery tests)."""
        self.tasks.clear()
        self.fleet.vehicles.clear()
        for item in snapshot.get("tasks", []):
            task = Task(
                task_id=item["task_id"],
                created_ms=item["created_ms"],
                origin=tuple(item["origin"]),
                destination=tuple(item["destination"]),
                priority=item["priority"],
                payload_kg=item["payload_kg"],
                deadline_ms=item["deadline_ms"],
                assigned_vehicle=item["assigned_vehicle"],
                assigned_ms=item["assigned_ms"],
                started_ms=item["started_ms"],
                finished_ms=item["finished_ms"],
                preempted=item["preempted"],
            )
            task.fsm.force_state(item["state"])
            self.tasks[task.task_id] = task
        for item in snapshot.get("vehicles", []):
            vehicle = Vehicle(
                vehicle_id=item["vehicle_id"],
                capacity_kg=item.get("capacity_kg", 2000.0),
                x=item["x"],
                y=item["y"],
                speed_mps=item.get("speed_mps", 2.0),
                battery=item["battery"],
                payload=item["payload"],
                online=item["online"],
                last_heartbeat_ms=item["last_heartbeat_ms"],
                odometer_m=item.get("odometer_m", 0.0),
                tasks_completed=item.get("tasks_completed", 0),
                pause_commands=item.get("pause_commands", 0),
                resume_commands=item.get("resume_commands", 0),
            )
            vehicle.fsm.force_state(item["state"])
            self.fleet.vehicles[vehicle.vehicle_id] = vehicle
        self.counters.update(snapshot.get("counters", {}))

    # -- reporting --------------------------------------------------------

    def as_dict(self) -> dict:
        return {
            "tasks": [self.tasks[t].as_dict() for t in self.ids()],
            "vehicles": [self.fleet.vehicles[v].as_dict() for v in self.fleet.ids()],
            "task_state_counts": self.state_counts(),
            "vehicle_state_counts": self.fleet.state_counts(),
            "counters": dict(sorted(self.counters.items())),
            "completion_rate": self.completion_rate(),
        }

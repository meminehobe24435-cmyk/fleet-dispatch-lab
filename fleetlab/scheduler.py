"""Task-to-vehicle assignment, priority and preemption.

The scheduler answers one question — *which vehicle should take this job?* — and
does it in a way that is explainable and reproducible:

* **explainable**: every decision carries the score and the human-readable
  reasons that produced it, because "the algorithm said so" is not an answer an
  operations team can act on;
* **reproducible**: ties are broken by vehicle id, never by dict order, so the
  same input always gives the same assignment (the two-run reproducibility check
  in CI depends on this);
* **priority-aware**: a higher-priority task may *preempt* a lower-priority one,
  and the victim is requeued rather than dropped, so preemption is a delay and
  not a data loss.

Crucially, :meth:`Scheduler.decide` is **pure**: it reads state and returns a
decision, and never writes.  The caller turns the decision into an event that the
journal records and the applier executes.  That is what keeps "the event log is
the system of record" true — a scheduler that mutated state directly would make
an event log replay silently diverge from reality.

Scoring is a linear combination of weighted terms::

    score = W_PRIORITY * priority
          + W_IDLE * (1 if vehicle is idle else 0)
          + W_BATTERY * battery
          - W_DISTANCE * distance_to_pickup
          + W_AGING * waiting_time_s

The weights are module constants precisely so they can be argued about and tuned
without reading the code.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .fleet import LOW_BATTERY, Vehicle
from .state import PlatformState, Priority, Task
from .task_fsm import TaskState, VehicleState

__all__ = ["Scheduler", "DispatchDecision", "WEIGHTS", "PREEMPTION_MIN_PRIORITY"]

#: Scoring weights.  Priority dominates distance by design: a CRITICAL job should
#: not lose to a NORMAL one just because the normal vehicle happens to be closer.
WEIGHTS: dict[str, float] = {
    "priority": 1_000.0,
    "idle": 50.0,
    "battery": 0.5,
    "distance": 1.0,
    "aging": 0.02,
}

#: Preemption only happens for a task at least this urgent.
PREEMPTION_MIN_PRIORITY = Priority.HIGH


@dataclass
class DispatchDecision:
    """Why a particular vehicle should get a particular task."""

    task_id: str
    vehicle_id: str
    score: float
    at_ms: int
    dispatch_latency_ms: int
    reasons: list[str] = field(default_factory=list)
    candidates_considered: int = 0
    preempt_task_id: str = ""
    preempt_vehicle_id: str = ""
    trace_id: str = ""

    def as_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "vehicle_id": self.vehicle_id,
            "score": round(self.score, 4),
            "at_ms": self.at_ms,
            "dispatch_latency_ms": self.dispatch_latency_ms,
            "reasons": list(self.reasons),
            "candidates_considered": self.candidates_considered,
            "preempt_task_id": self.preempt_task_id,
            "preempt_vehicle_id": self.preempt_vehicle_id,
            "trace_id": self.trace_id,
        }


class Scheduler:
    """Priority-, distance- and battery-aware dispatcher with preemption."""

    def __init__(
        self,
        *,
        weights: dict[str, float] | None = None,
        battery_floor: float = LOW_BATTERY,
        max_preemptions_per_task: int = 2,
    ) -> None:
        self.weights = dict(WEIGHTS if weights is None else weights)
        self.battery_floor = battery_floor
        self.max_preemptions_per_task = max_preemptions_per_task
        self.decisions: list[DispatchDecision] = []
        self.counters = {
            "considered": 0,
            "assigned": 0,
            "no_free_vehicle": 0,
            "preemptions": 0,
            "starved_because_preempted": 0,
        }

    # -- scoring ----------------------------------------------------------

    def score(self, task: Task, vehicle: Vehicle, now_ms: int) -> tuple[float, list[str]]:
        """Return ``(score, reasons)`` for putting ``task`` on ``vehicle``."""
        distance = vehicle.distance_to(task.origin)
        waiting_s = max(0.0, (now_ms - task.created_ms) / 1000.0)
        parts = {
            "priority": self.weights["priority"] * task.priority,
            "idle": self.weights["idle"] * (1.0 if vehicle.state == VehicleState.IDLE else 0.0),
            "battery": self.weights["battery"] * vehicle.battery,
            "distance": -self.weights["distance"] * distance,
            "aging": self.weights["aging"] * waiting_s,
        }
        reasons = [
            f"priority={Priority.label(task.priority)}(+{parts['priority']:.1f})",
            f"distance={distance:.1f}m({parts['distance']:.1f})",
            f"battery={vehicle.battery:.1f}%(+{parts['battery']:.1f})",
            f"state={vehicle.state}(+{parts['idle']:.1f})",
            f"waiting={waiting_s:.1f}s(+{parts['aging']:.1f})",
        ]
        return sum(parts.values()), reasons

    def candidates(self, task: Task, state: PlatformState) -> list[Vehicle]:
        """Vehicles eligible for ``task`` right now, best first.

        Eligibility is enforced, not merely scored: an offline vehicle, one in
        FAULT/MAINTENANCE, or one below the battery floor is excluded outright
        rather than given a very low score.  A low score can still win when
        nothing else is available, and that would be the wrong outcome.
        """
        eligible: list[tuple[float, str, Vehicle]] = []
        for vehicle_id in state.fleet.ids():
            vehicle = state.fleet.vehicles[vehicle_id]
            if not vehicle.online:
                continue
            if vehicle.state in (
                VehicleState.FAULT,
                VehicleState.MAINTENANCE,
                VehicleState.OFFLINE,
            ):
                continue
            if vehicle.state not in VehicleState.DISPATCHABLE:
                continue
            if vehicle.battery < self.battery_floor:
                continue
            if vehicle.payload and vehicle.payload != task.task_id:
                continue
            score, _ = self.score(task, vehicle, task.created_ms)
            eligible.append((-score, vehicle_id, vehicle))
        eligible.sort(key=lambda row: (row[0], row[1]))
        return [row[2] for row in eligible]

    # -- task ordering ----------------------------------------------------

    def order_pending(self, state: PlatformState, now_ms: int) -> list[Task]:
        """Pending tasks, most deserving first.

        Sort key: priority desc, then longest waiting first, then task id for a
        stable tie-break — so that a low-priority task cannot be starved simply
        because it was created late.
        """
        pending = state.of_state(TaskState.PENDING)
        pending.sort(key=lambda t: (-t.priority, t.created_ms, t.task_id))
        return pending

    # -- pure decision ----------------------------------------------------

    def decide(
        self,
        state: PlatformState,
        task_id: str,
        *,
        now_ms: int,
        trace_id: str = "",
        taken_vehicles: set[str] | None = None,
        taken_tasks: set[str] | None = None,
    ) -> DispatchDecision | None:
        """Choose a vehicle for ``task_id`` without modifying anything.

        ``taken_vehicles`` / ``taken_tasks`` let :meth:`plan` reason about the
        decisions it has already made in the same batch, which is necessary now
        that the decision no longer writes to state.
        """
        taken_vehicles = taken_vehicles if taken_vehicles is not None else set()
        taken_tasks = taken_tasks if taken_tasks is not None else set()
        task = state.get(task_id)
        self.counters["considered"] += 1

        pool = [v for v in self.candidates(task, state) if v.vehicle_id not in taken_vehicles]
        if pool:
            vehicle = pool[0]
            score, reasons = self.score(task, vehicle, now_ms)
            self.counters["assigned"] += 1
            decision = DispatchDecision(
                task_id=task_id,
                vehicle_id=vehicle.vehicle_id,
                score=score,
                at_ms=now_ms,
                dispatch_latency_ms=now_ms - task.created_ms,
                reasons=reasons,
                candidates_considered=len(pool),
                trace_id=trace_id,
            )
            self.decisions.append(decision)
            return decision

        self.counters["no_free_vehicle"] += 1
        victim = self._pick_victim(state, task, taken_tasks)
        if victim is None:
            return None
        vehicle = state.fleet.vehicles[victim.assigned_vehicle]
        score, reasons = self.score(task, vehicle, now_ms)
        reasons.insert(
            0,
            f"preempt {victim.task_id} (priority {Priority.label(victim.priority)})",
        )
        self.counters["preemptions"] += 1
        self.counters["assigned"] += 1
        decision = DispatchDecision(
            task_id=task_id,
            vehicle_id=vehicle.vehicle_id,
            score=score,
            at_ms=now_ms,
            dispatch_latency_ms=now_ms - task.created_ms,
            reasons=reasons,
            candidates_considered=0,
            preempt_task_id=victim.task_id,
            preempt_vehicle_id=vehicle.vehicle_id,
            trace_id=trace_id,
        )
        self.decisions.append(decision)
        return decision

    def _pick_victim(
        self, state: PlatformState, task: Task, taken_tasks: set[str]
    ) -> Task | None:
        """Lowest-value running task this task is allowed to preempt.

        Deliberately conservative: never preempt an equal or higher priority, and
        never preempt a task that has already been preempted too often — that is
        how a starvation bug turns into an infinite preemption loop.
        """
        if task.priority < PREEMPTION_MIN_PRIORITY:
            return None
        victims: list[Task] = []
        for other in state.of_state(
            TaskState.ASSIGNED, TaskState.ENROUTE, TaskState.QUEUED, TaskState.EXECUTING
        ):
            if other.task_id in taken_tasks:
                continue
            if other.priority >= task.priority:
                continue
            if other.preempted >= self.max_preemptions_per_task:
                self.counters["starved_because_preempted"] += 1
                continue
            vehicle = state.fleet.vehicles.get(other.assigned_vehicle)
            if vehicle is None or not vehicle.online:
                continue
            victims.append(other)
        if not victims:
            return None
        # Lowest priority loses first; among equals the most recently assigned
        # one loses (it has invested the least work), then task id for stability.
        victims.sort(key=lambda t: (t.priority, -t.assigned_ms, t.task_id))
        return victims[0]

    # -- batch ------------------------------------------------------------

    def plan(
        self,
        state: PlatformState,
        *,
        now_ms: int,
        trace_id: str = "",
        max_assignments: int | None = None,
    ) -> list[DispatchDecision]:
        """Decide for as many pending tasks as there is capacity to serve.

        Stops at the first task that cannot be placed: pending tasks are already
        in priority order, so if the most deserving one cannot go, nothing behind
        it can either.  Without that check the loop would keep preempting and
        undo its own decisions.
        """
        out: list[DispatchDecision] = []
        taken_vehicles: set[str] = set()
        taken_tasks: set[str] = set()
        limit = len(state.fleet.vehicles) if max_assignments is None else max_assignments
        for task in self.order_pending(state, now_ms):
            if len(out) >= limit:
                break
            decision = self.decide(
                state,
                task.task_id,
                now_ms=now_ms,
                trace_id=trace_id,
                taken_vehicles=taken_vehicles,
                taken_tasks=taken_tasks,
            )
            if decision is None:
                break
            taken_vehicles.add(decision.vehicle_id)
            taken_tasks.add(decision.task_id)
            if decision.preempt_task_id:
                taken_tasks.add(decision.preempt_task_id)
            out.append(decision)
        return out

    # -- metrics ----------------------------------------------------------

    def dispatch_latencies(self) -> list[int]:
        return [d.dispatch_latency_ms for d in self.decisions]

    def preemption_rate(self) -> float:
        total = self.counters["assigned"]
        return 0.0 if not total else self.counters["preemptions"] / total

    def stats(self) -> dict:
        return {
            "decisions": len(self.decisions),
            "preemption_rate": round(self.preemption_rate(), 4),
            "counters": dict(sorted(self.counters.items())),
        }

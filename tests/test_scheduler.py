"""Scheduler tests: eligibility, scoring, determinism, and preemption.

The purity test matters most: :meth:`Scheduler.decide` must not touch state.  If it
did, the scheduling decision would not be in the event journal and a replay would
diverge from the live state — a bug that only shows up as a mysterious
consistency failure much later.
"""

from __future__ import annotations

import pytest

from fleetlab.fleet import LOW_BATTERY, FleetManager, Vehicle
from fleetlab.scheduler import PREEMPTION_MIN_PRIORITY, Scheduler
from fleetlab.state import PlatformState, Priority
from fleetlab.task_fsm import TaskEvent, TaskState, VehicleState


def build(vehicles: list[tuple[str, float, float, float]] | None = None) -> PlatformState:
    fleet = FleetManager()
    # ``vehicles or [...]`` would treat an empty list as "use the default", which
    # silently gave a fleet-less test a vehicle and made it assert the opposite.
    for vehicle_id, x, y, battery in (
        [("AGV-01", 0.0, 0.0, 100.0)] if vehicles is None else vehicles
    ):
        fleet.add(Vehicle(vehicle_id=vehicle_id, x=x, y=y, battery=battery))
    return PlatformState(fleet)


def add_task(state: PlatformState, task_id="T1", origin=(0.0, 0.0), destination=(100.0, 0.0),
             priority=Priority.NORMAL, created_ms=0) -> str:
    state.create_task(
        task_id,
        created_ms=created_ms,
        origin=origin,
        destination=destination,
        priority=priority,
    )
    return task_id


# ----------------------------------------------------------------------
# scoring
# ----------------------------------------------------------------------


def test_score_reports_its_reasoning():
    state = build([("AGV-01", 30.0, 40.0, 80.0)])
    task_id = add_task(state)
    score, reasons = Scheduler().score(state.get(task_id), state.fleet.vehicles["AGV-01"], 1000)
    assert isinstance(score, float)
    joined = " ".join(reasons)
    assert "distance=50.0m" in joined
    assert "battery=80.0%" in joined
    assert "priority=NORMAL" in joined


def test_a_closer_vehicle_scores_higher():
    state = build([("AGV-near", 5.0, 0.0, 100.0), ("AGV-far", 90.0, 0.0, 100.0)])
    task_id = add_task(state)
    scheduler = Scheduler()
    task = state.get(task_id)
    near, _ = scheduler.score(task, state.fleet.vehicles["AGV-near"], 0)
    far, _ = scheduler.score(task, state.fleet.vehicles["AGV-far"], 0)
    assert near > far


def test_a_full_battery_vehicle_scores_higher():
    state = build([("AGV-full", 0.0, 0.0, 100.0), ("AGV-low", 0.0, 0.0, 25.0)])
    task_id = add_task(state)
    scheduler = Scheduler()
    task = state.get(task_id)
    assert scheduler.score(task, state.fleet.vehicles["AGV-full"], 0)[0] > scheduler.score(
        task, state.fleet.vehicles["AGV-low"], 0
    )[0]


def test_priority_dominates_distance():
    """A CRITICAL task must not lose to a NORMAL one just because of proximity."""
    state = build([("AGV-01", 100.0, 0.0, 100.0)])
    state.create_task("T-critical", created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0),
                      priority=Priority.CRITICAL)
    state.create_task("T-normal", created_ms=0, origin=(100.0, 0.0), destination=(101.0, 0.0),
                      priority=Priority.NORMAL)
    scheduler = Scheduler()
    vehicle = state.fleet.vehicles["AGV-01"]
    assert scheduler.score(state.get("T-critical"), vehicle, 0)[0] > scheduler.score(
        state.get("T-normal"), vehicle, 0
    )[0]


# ----------------------------------------------------------------------
# eligibility
# ----------------------------------------------------------------------


def test_offline_vehicles_are_not_candidates():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    state.fleet.vehicles["AGV-01"].online = False
    task_id = add_task(state)
    assert Scheduler().candidates(state.get(task_id), state) == []


def test_low_battery_vehicles_are_not_candidates():
    state = build([("AGV-01", 0.0, 0.0, LOW_BATTERY - 1)])
    task_id = add_task(state)
    assert Scheduler().candidates(state.get(task_id), state) == []


def test_busy_vehicles_are_not_candidates():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    state.fleet.vehicles["AGV-01"].apply("ASSIGN")
    task_id = add_task(state)
    assert Scheduler().candidates(state.get(task_id), state) == []


def test_faulted_vehicles_are_not_candidates():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    state.fleet.vehicles["AGV-01"].apply("FAULT")
    task_id = add_task(state)
    assert Scheduler().candidates(state.get(task_id), state) == []


def test_candidates_are_ordered_best_first_with_a_stable_tie_break():
    state = build([("AGV-b", 0.0, 0.0, 100.0), ("AGV-a", 0.0, 0.0, 100.0)])
    task_id = add_task(state)
    candidates = Scheduler().candidates(state.get(task_id), state)
    assert [v.vehicle_id for v in candidates] == ["AGV-a", "AGV-b"]


# ----------------------------------------------------------------------
# decisions
# ----------------------------------------------------------------------


def test_decide_is_pure():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    task_id = add_task(state)
    before_tasks = state.lifecycle_canonical()
    before_vehicles = state.fleet.vehicles["AGV-01"].state

    decision = Scheduler().decide(state, task_id, now_ms=500)

    assert decision is not None
    assert decision.vehicle_id == "AGV-01"
    assert decision.dispatch_latency_ms == 500
    assert state.lifecycle_canonical() == before_tasks
    assert state.fleet.vehicles["AGV-01"].state == before_vehicles
    assert state.get(task_id).state == TaskState.PENDING


def test_decide_returns_none_when_no_vehicle_is_free():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    state.fleet.vehicles["AGV-01"].apply("ASSIGN")
    task_id = add_task(state)
    assert Scheduler().decide(state, task_id, now_ms=0) is None


def test_order_pending_prefers_priority_then_age():
    state = build()
    state.create_task("T-low", created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0), priority=Priority.LOW)
    state.create_task("T-high", created_ms=100, origin=(0.0, 0.0), destination=(1.0, 0.0), priority=Priority.HIGH)
    state.create_task("T-old", created_ms=50, origin=(0.0, 0.0), destination=(1.0, 0.0), priority=Priority.HIGH)
    ordered = [t.task_id for t in Scheduler().order_pending(state, 1000)]
    assert ordered[0] == "T-old"
    assert ordered[1] == "T-high"
    assert ordered[2] == "T-low"


def test_plan_reserves_a_vehicle_for_only_one_task_per_batch():
    """Without reservation, one idle vehicle could be promised to two tasks."""
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    add_task(state, "T1")
    add_task(state, "T2")
    decisions = Scheduler().plan(state, now_ms=0)
    assert len(decisions) == 1
    assert decisions[0].task_id == "T1"


def test_plan_assigns_up_to_the_fleet_size():
    state = build([("AGV-01", 0.0, 0.0, 100.0), ("AGV-02", 10.0, 0.0, 100.0), ("AGV-03", 20.0, 0.0, 100.0)])
    for index in range(5):
        add_task(state, f"T{index}")
    decisions = Scheduler().plan(state, now_ms=0)
    assert len(decisions) == 3
    assert len({d.vehicle_id for d in decisions}) == 3


def test_plan_stops_when_nothing_more_can_be_placed():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    add_task(state, "T1", priority=Priority.HIGH)
    add_task(state, "T2", priority=Priority.NORMAL)
    decisions = Scheduler().plan(state, now_ms=0)
    assert len(decisions) == 1


def test_max_assignments_caps_a_batch():
    state = build([("AGV-01", 0.0, 0.0, 100.0), ("AGV-02", 0.0, 0.0, 100.0)])
    add_task(state, "T1")
    add_task(state, "T2")
    assert len(Scheduler().plan(state, now_ms=0, max_assignments=1)) == 1


# ----------------------------------------------------------------------
# preemption
# ----------------------------------------------------------------------


def running_state(priority: int = Priority.LOW):
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    state.create_task("T-running", created_ms=0, origin=(0.0, 0.0), destination=(10.0, 0.0),
                      priority=priority)
    assert state.assign("T-running", "AGV-01", 100)
    state.fleet.vehicles["AGV-01"].apply("ASSIGN")
    return state


def test_a_normal_task_never_preempts():
    state = running_state(Priority.LOW)
    add_task(state, "T-new", priority=Priority.NORMAL)
    assert PREEMPTION_MIN_PRIORITY > Priority.NORMAL
    decision = Scheduler().decide(state, "T-new", now_ms=500)
    # The vehicle is busy and the new task is not urgent enough: no decision.
    assert decision is None


def test_an_urgent_task_preempts_a_low_priority_one():
    state = running_state(Priority.LOW)
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    decision = Scheduler().decide(state, "T-urgent", now_ms=500)
    assert decision is not None
    assert decision.preempt_task_id == "T-running"
    assert decision.preempt_vehicle_id == "AGV-01"
    assert "preempt T-running" in decision.reasons[0]
    # Still pure: the caller applies the preemption, not the scheduler.
    assert state.get("T-running").state == TaskState.ASSIGNED


def test_equal_priority_is_never_preempted():
    state = running_state(Priority.CRITICAL)
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    assert Scheduler().decide(state, "T-urgent", now_ms=500) is None


def test_a_task_already_preempted_too_often_is_spared():
    state = running_state(Priority.LOW)
    state.get("T-running").preempted = 99
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    scheduler = Scheduler(max_preemptions_per_task=2)
    assert scheduler.decide(state, "T-urgent", now_ms=500) is None
    assert scheduler.counters["starved_because_preempted"] == 1


def test_preemption_picks_the_lowest_priority_victim():
    state = build([("AGV-01", 0.0, 0.0, 100.0), ("AGV-02", 0.0, 0.0, 100.0)])
    state.create_task("T-low", created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0), priority=Priority.LOW)
    state.create_task("T-mid", created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0), priority=Priority.HIGH)
    for task_id, vehicle_id in (("T-low", "AGV-01"), ("T-mid", "AGV-02")):
        state.assign(task_id, vehicle_id, 100)
        state.fleet.vehicles[vehicle_id].apply("ASSIGN")
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    decision = Scheduler().decide(state, "T-urgent", now_ms=500)
    assert decision.preempt_task_id == "T-low"


def test_offline_vehicle_cannot_have_its_task_preempted():
    state = running_state(Priority.LOW)
    state.fleet.vehicles["AGV-01"].online = False
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    assert Scheduler().decide(state, "T-urgent", now_ms=500) is None


def test_preemption_rate_and_stats():
    state = running_state(Priority.LOW)
    add_task(state, "T-urgent", priority=Priority.CRITICAL)
    scheduler = Scheduler()
    scheduler.decide(state, "T-urgent", now_ms=500)
    stats = scheduler.stats()
    assert stats["preemption_rate"] == 1.0
    assert stats["counters"]["preemptions"] == 1


def test_dispatch_latencies_are_collected():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    add_task(state, "T1", created_ms=1000)
    scheduler = Scheduler()
    scheduler.decide(state, "T1", now_ms=4000)
    assert scheduler.dispatch_latencies() == [3000]


def test_decision_as_dict_shape():
    state = build([("AGV-01", 0.0, 0.0, 100.0)])
    add_task(state)
    decision = Scheduler().decide(state, "T1", now_ms=10)
    payload = decision.as_dict()
    assert payload["task_id"] == "T1"
    assert payload["dispatch_latency_ms"] == 10
    assert isinstance(payload["reasons"], list)


def test_task_without_a_vehicle_is_reported_not_crashed():
    state = build([])
    add_task(state)
    scheduler = Scheduler()
    assert scheduler.decide(state, "T1", now_ms=0) is None
    assert scheduler.counters["no_free_vehicle"] == 1

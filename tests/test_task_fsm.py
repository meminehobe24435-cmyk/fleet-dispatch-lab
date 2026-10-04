"""State machine tests: legality, rejection reasons, and the two dynamic edges.

The regression tests at the bottom are the important ones.  Both encode bugs that
actually happened while building this project, and both were invisible until
throughput collapsed for reasons unrelated to the code that was wrong.
"""

from __future__ import annotations

import pytest

from fleetlab.task_fsm import (
    QUEUE_RETURN,
    RESUME_TARGET,
    TRANSITIONS,
    VEHICLE_TRANSITIONS,
    IllegalTransition,
    StateMachine,
    TaskEvent,
    TaskState,
    VehicleState,
    check_table_consistency,
    transition_table,
)

NON_TERMINAL_TASK_STATES = [s for s in TaskState.ALL if s not in TaskState.TERMINAL]
ACTIVE_TASK_STATES = [
    TaskState.ASSIGNED,
    TaskState.ENROUTE,
    TaskState.QUEUED,
    TaskState.EXECUTING,
    TaskState.PAUSED,
    TaskState.RECOVERING,
]


def make(state: str = TaskState.PENDING) -> StateMachine:
    return StateMachine("T1", initial=state, state=state)


# ----------------------------------------------------------------------
# table shape
# ----------------------------------------------------------------------


def test_task_has_at_least_ten_states():
    assert len(TaskState.ALL) == 10
    assert len(set(TaskState.ALL)) == 10


def test_vehicle_has_at_least_ten_states():
    assert len(VEHICLE_TRANSITIONS) >= 10


def test_transition_table_is_self_consistent():
    assert check_table_consistency(TRANSITIONS) == []
    assert check_table_consistency(VEHICLE_TRANSITIONS) == []


def test_table_check_catches_an_edge_to_a_missing_state():
    # "NOWHERE" must not be a key of the table, or the check is vacuous.
    broken = {"A": {"GO": "NOWHERE"}, "B": {}}
    problems = check_table_consistency(broken)
    assert len(problems) == 1
    assert "unknown state" in problems[0]


def test_table_check_rejects_a_non_dict_edge_set():
    problems = check_table_consistency({"A": ["GO"]})  # type: ignore[dict-item]
    assert problems == ["A: edges is not a dict"]


def test_transition_table_expands_dynamic_targets():
    rows = transition_table()
    resume_rows = [r for r in rows if r["from"] == TaskState.RECOVERING and r["event"] == TaskEvent.RECOVER]
    assert resume_rows and resume_rows[0]["to"] == "ASSIGNED|ENROUTE|QUEUED|EXECUTING"
    queue_rows = [r for r in rows if r["from"] == TaskState.QUEUED and r["event"] == TaskEvent.PROCEED]
    assert queue_rows and queue_rows[0]["to"] == "ASSIGNED|ENROUTE|EXECUTING"
    # No raw sentinel should ever leak into a report.
    assert all(not row["to"].startswith("__") for row in rows)


def test_every_task_state_is_reachable_from_pending():
    reachable = {TaskState.PENDING}
    frontier = [TaskState.PENDING]
    while frontier:
        state = frontier.pop()
        for target in TRANSITIONS[state].values():
            if target in (RESUME_TARGET, QUEUE_RETURN):
                # Dynamic edges are resolved from a stack; treat them as reaching
                # every state that can legitimately be on the stack.
                targets = [TaskState.ASSIGNED, TaskState.ENROUTE, TaskState.EXECUTING]
            else:
                targets = [target]
            for candidate in targets:
                if candidate not in reachable:
                    reachable.add(candidate)
                    frontier.append(candidate)
    assert reachable == set(TaskState.ALL)


def test_every_non_terminal_task_state_can_reach_a_terminal_state():
    def can_finish(start: str) -> bool:
        seen = {start}
        frontier = [start]
        while frontier:
            state = frontier.pop()
            for target in TRANSITIONS[state].values():
                if target in TaskState.TERMINAL:
                    return True
                if target in (RESUME_TARGET, QUEUE_RETURN):
                    continue
                if target not in seen:
                    seen.add(target)
                    frontier.append(target)
        return False

    for state in NON_TERMINAL_TASK_STATES:
        assert can_finish(state), f"{state} cannot reach a terminal state"


# ----------------------------------------------------------------------
# happy path and rejection
# ----------------------------------------------------------------------


def test_happy_path_from_pending_to_completed():
    fsm = make()
    for event, expected in (
        (TaskEvent.ASSIGN, TaskState.ASSIGNED),
        (TaskEvent.DISPATCH, TaskState.ENROUTE),
        (TaskEvent.ENQUEUE, TaskState.QUEUED),
        (TaskEvent.PROCEED, TaskState.ENROUTE),
        (TaskEvent.START, TaskState.EXECUTING),
        (TaskEvent.COMPLETE, TaskState.COMPLETED),
    ):
        assert fsm.apply(event) == expected
    assert fsm.is_terminal()


def test_illegal_transition_raises_and_leaves_state_untouched():
    fsm = make(TaskState.PENDING)
    with pytest.raises(IllegalTransition) as info:
        fsm.apply(TaskEvent.COMPLETE)
    assert fsm.state == TaskState.PENDING
    assert fsm.history == []
    assert info.value.from_state == TaskState.PENDING
    assert info.value.event == TaskEvent.COMPLETE


def test_rejection_reason_names_the_allowed_events():
    fsm = make(TaskState.PENDING)
    with pytest.raises(IllegalTransition):
        fsm.apply(TaskEvent.PAUSE)
    reason = fsm.rejections[-1].reason
    assert "PAUSE" in reason
    assert "ASSIGN" in reason


def test_rejection_is_recorded_with_a_count():
    fsm = make(TaskState.PENDING)
    for _ in range(3):
        assert fsm.try_apply(TaskEvent.COMPLETE) is False
    assert len(fsm.rejections) == 3
    assert fsm.history == []


@pytest.mark.parametrize("terminal", TaskState.TERMINAL)
def test_terminal_states_reject_every_event(terminal):
    fsm = make(terminal)
    assert fsm.is_terminal()
    for event in TaskEvent.ALL:
        assert fsm.try_apply(event) is False
    assert len(fsm.rejections) == len(TaskEvent.ALL)


@pytest.mark.parametrize("state", NON_TERMINAL_TASK_STATES)
def test_cancel_and_fail_are_legal_from_every_non_terminal_state(state):
    assert make(state).try_apply(TaskEvent.CANCEL) is True
    assert make(state).try_apply(TaskEvent.FAIL) is True


@pytest.mark.parametrize("state", ACTIVE_TASK_STATES)
def test_requeue_returns_to_pending_from_every_active_state(state):
    fsm = make(state)
    assert fsm.apply(TaskEvent.REQUEUE) == TaskState.PENDING


def test_try_apply_reports_success_without_raising():
    fsm = make()
    assert fsm.try_apply(TaskEvent.ASSIGN) is True
    assert fsm.state == TaskState.ASSIGNED


def test_allowed_events_are_sorted_and_current():
    fsm = make(TaskState.PENDING)
    assert fsm.allowed_events() == tuple(sorted(fsm.allowed_events()))
    assert TaskEvent.ASSIGN in fsm.allowed_events()
    fsm.apply(TaskEvent.ASSIGN)
    assert TaskEvent.DISPATCH in fsm.allowed_events()
    assert TaskEvent.ASSIGN not in fsm.allowed_events()


def test_can_predicate_matches_the_table():
    fsm = make(TaskState.EXECUTING)
    assert fsm.can(TaskEvent.COMPLETE)
    assert not fsm.can(TaskEvent.ASSIGN)


def test_history_records_sequence_and_trace_id():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN, at_ms=100, trace_id="trace-a", note="hello")
    fsm.apply(TaskEvent.DISPATCH, at_ms=200, trace_id="trace-a")
    assert [r.seq for r in fsm.history] == [1, 2]
    assert fsm.history[0].trace_id == "trace-a"
    assert fsm.history[0].at_ms == 100
    assert fsm.history[0].note == "hello"
    assert fsm.history[1].from_state == TaskState.ASSIGNED
    assert fsm.history[1].to_state == TaskState.ENROUTE


def test_as_dict_is_json_friendly():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN)
    payload = fsm.as_dict()
    assert payload["state"] == TaskState.ASSIGNED
    assert payload["transitions"] == 1
    assert payload["rejections"] == 0
    assert payload["history"][0]["event"] == TaskEvent.ASSIGN


def test_force_state_validates_the_state_name():
    fsm = make()
    fsm.force_state(TaskState.EXECUTING)
    assert fsm.state == TaskState.EXECUTING
    with pytest.raises(ValueError):
        fsm.force_state("NOT_A_STATE")


# ----------------------------------------------------------------------
# dynamic edges
# ----------------------------------------------------------------------


def test_resume_returns_to_the_state_it_was_paused_from():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN)
    fsm.apply(TaskEvent.DISPATCH)
    fsm.apply(TaskEvent.PAUSE)
    assert fsm.state == TaskState.PAUSED
    assert fsm.apply(TaskEvent.RESUME) == TaskState.RECOVERING
    assert fsm.apply(TaskEvent.RECOVER) == TaskState.ENROUTE


def test_pause_from_executing_resumes_into_executing():
    fsm = make()
    for event in (TaskEvent.ASSIGN, TaskEvent.DISPATCH, TaskEvent.START):
        fsm.apply(event)
    fsm.apply(TaskEvent.PAUSE)
    fsm.apply(TaskEvent.RESUME)
    assert fsm.apply(TaskEvent.RECOVER) == TaskState.EXECUTING


def test_resume_without_a_paused_state_is_rejected():
    fsm = make(TaskState.RECOVERING)
    with pytest.raises(IllegalTransition) as info:
        fsm.apply(TaskEvent.RECOVER)
    assert "no paused state" in info.value.reason


def test_proceed_returns_to_the_state_it_was_queued_from():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN)
    fsm.apply(TaskEvent.DISPATCH)
    fsm.apply(TaskEvent.ENQUEUE)
    assert fsm.apply(TaskEvent.PROCEED) == TaskState.ENROUTE


def test_queue_while_executing_returns_to_executing():
    fsm = make()
    for event in (TaskEvent.ASSIGN, TaskEvent.DISPATCH, TaskEvent.START, TaskEvent.ENQUEUE):
        fsm.apply(event)
    assert fsm.state == TaskState.QUEUED
    assert fsm.apply(TaskEvent.PROCEED) == TaskState.EXECUTING


def test_proceed_without_a_queued_state_is_rejected():
    fsm = make(TaskState.QUEUED)
    with pytest.raises(IllegalTransition) as info:
        fsm.apply(TaskEvent.PROCEED)
    assert "no queued state" in info.value.reason


def test_requeue_clears_both_stacks():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN)
    fsm.apply(TaskEvent.PAUSE)
    fsm.apply(TaskEvent.REQUEUE)
    assert fsm.state == TaskState.PENDING
    # A stale pause position would let RECOVER jump somewhere nonsensical.
    assert fsm._pause_stack == []
    assert fsm._queue_stack == []


def test_pause_then_queue_then_resume_uses_the_pause_stack():
    fsm = make()
    fsm.apply(TaskEvent.ASSIGN)
    fsm.apply(TaskEvent.DISPATCH)
    fsm.apply(TaskEvent.START)
    fsm.apply(TaskEvent.PAUSE)  # from EXECUTING
    fsm.apply(TaskEvent.RESUME)
    assert fsm.apply(TaskEvent.RECOVER) == TaskState.EXECUTING


# ----------------------------------------------------------------------
# vehicle machine
# ----------------------------------------------------------------------


def vehicle(state: str = VehicleState.IDLE) -> StateMachine:
    return StateMachine("AGV-1", transitions=VEHICLE_TRANSITIONS, initial=state, state=state)


def test_vehicle_charge_cycle():
    fsm = vehicle()
    assert fsm.apply("CHARGE") == VehicleState.CHARGING
    assert fsm.apply("CHARGE_DONE") == VehicleState.IDLE


def test_vehicle_offline_and_back_online():
    fsm = vehicle(VehicleState.EXECUTING)
    assert fsm.apply("HEARTBEAT_TIMEOUT") == VehicleState.OFFLINE
    assert fsm.apply("ONLINE") == VehicleState.IDLE


def test_vehicle_maintenance_is_recoverable_not_terminal():
    fsm = vehicle(VehicleState.FAULT)
    assert fsm.apply("REPAIR") == VehicleState.MAINTENANCE
    assert fsm.apply("REPAIR_DONE") == VehicleState.IDLE
    assert not fsm.is_terminal()


@pytest.mark.parametrize(
    "state",
    [
        # Only states that can be *holding a task* need RELEASE.  An idle vehicle
        # has nothing to release, and a charging or faulted one is already out of
        # service, so requiring RELEASE there would be wrong rather than safe.
        VehicleState.ASSIGNED,
        VehicleState.ENROUTE,
        VehicleState.QUEUED,
        VehicleState.EXECUTING,
        VehicleState.PAUSED,
        VehicleState.RECOVERING,
    ],
)
def test_every_task_holding_vehicle_state_accepts_release(state):
    """Regression: a vehicle that cannot be released is lost to the fleet.

    QUEUED, ENROUTE and EXECUTING originally had no RELEASE edge.  A vehicle whose
    task ended while it was queued at an intersection stayed QUEUED forever, so the
    usable fleet shrank with every blocked task and the completion rate fell to
    0.275 for reasons that looked like a traffic problem.
    """
    fsm = vehicle(state)
    assert "RELEASE" in fsm.allowed_events()
    assert fsm.apply("RELEASE") == VehicleState.IDLE


@pytest.mark.parametrize("state", list(VEHICLE_TRANSITIONS))
def test_every_vehicle_state_can_return_to_service(state):
    """Every state must have a route back to IDLE, or a vehicle is stranded.

    OFFLINE reaches IDLE through ONLINE, CHARGING through CHARGE_DONE, FAULT
    through RECOVER or REPAIR -> MAINTENANCE -> REPAIR_DONE.  If any of those edges
    were missing, a vehicle would silently leave the fleet for the rest of the run.
    """
    assert VehicleState.IDLE in _reachable(VEHICLE_TRANSITIONS, state)


def _reachable(table: dict, start: str) -> set[str]:
    seen = {start}
    frontier = [start]
    while frontier:
        node = frontier.pop()
        for target in table.get(node, {}).values():
            if target == QUEUE_RETURN:
                continue
            if target not in seen:
                seen.add(target)
                frontier.append(target)
    return seen


def test_vehicle_queue_return_edge_exists():
    fsm = vehicle(VehicleState.ENROUTE)
    fsm.apply("ENQUEUE")
    assert fsm.state == VehicleState.QUEUED
    assert fsm.apply("PROCEED") == VehicleState.ENROUTE

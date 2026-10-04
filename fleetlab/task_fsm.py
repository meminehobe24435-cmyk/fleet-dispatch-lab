"""Event-driven task lifecycle state machine.

This module owns the *rules* of the platform: which task/vehicle state may react
to which event, and which transitions are illegal.  Everything else in the
project (bus, scheduler, traffic, HTTP layer) may only change task state by
submitting an event here, so the state machine is the single source of truth for
legality and for the transition audit trail.

Design notes
------------
* Transitions are declared as a plain table so they can be printed, diffed and
  drawn (``state_diagram`` / ``transition_table``) without executing anything.
* An illegal transition never mutates state.  It raises :class:`IllegalTransition`
  and appends a machine-readable record to ``rejections`` so that the rejection
  rate becomes an observable metric instead of a silent no-op.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

# --------------------------------------------------------------------------
# Task states (10 total, including 3 terminal states)
# --------------------------------------------------------------------------


class TaskState:
    """Task lifecycle states.

    ``PENDING -> ASSIGNED -> ENROUTE -> QUEUED -> EXECUTING -> COMPLETED`` is the
    happy path; ``PAUSED`` / ``RECOVERING`` model stop-and-resume, and
    ``FAILED`` / ``CANCELLED`` are terminal.
    """

    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    ENROUTE = "ENROUTE"
    QUEUED = "QUEUED"
    EXECUTING = "EXECUTING"
    PAUSED = "PAUSED"
    RECOVERING = "RECOVERING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    ALL: tuple[str, ...] = (
        PENDING,
        ASSIGNED,
        ENROUTE,
        QUEUED,
        EXECUTING,
        PAUSED,
        RECOVERING,
        COMPLETED,
        FAILED,
        CANCELLED,
    )

    TERMINAL: tuple[str, ...] = (
        COMPLETED,
        FAILED,
        CANCELLED,
    )

    #: States in which a task still holds a vehicle assignment.
    ACTIVE: tuple[str, ...] = (ASSIGNED, ENROUTE, QUEUED, EXECUTING, PAUSED, RECOVERING)


class VehicleState:
    """Vehicle states.  Kept in this module so task/vehicle pairs stay consistent."""

    OFFLINE = "OFFLINE"
    IDLE = "IDLE"
    ASSIGNED = "ASSIGNED"
    ENROUTE = "ENROUTE"
    QUEUED = "QUEUED"
    EXECUTING = "EXECUTING"
    PAUSED = "PAUSED"
    RECOVERING = "RECOVERING"
    CHARGING = "CHARGING"
    FAULT = "FAULT"
    MAINTENANCE = "MAINTENANCE"

    ALL: tuple[str, ...] = (
        OFFLINE,
        IDLE,
        ASSIGNED,
        ENROUTE,
        QUEUED,
        EXECUTING,
        PAUSED,
        RECOVERING,
        CHARGING,
        FAULT,
        MAINTENANCE,
    )

    #: Terminal for *task assignment* purposes: a vehicle parked in maintenance
    #: is not coming back on its own, even though the vehicle itself is repairable.
    TERMINAL: tuple[str, ...] = (MAINTENANCE,)

    #: Vehicles in these states can take a new task immediately.
    DISPATCHABLE: tuple[str, ...] = (IDLE,)


# --------------------------------------------------------------------------
# Task events
# --------------------------------------------------------------------------


class TaskEvent:
    """Events that may be delivered to a task state machine."""

    ASSIGN = "ASSIGN"
    DISPATCH = "DISPATCH"  # start moving towards the pick-up point
    ENQUEUE = "ENQUEUE"  # blocked at an intersection / lane: hold position
    PROCEED = "PROCEED"  # the resource was granted: resume where we were
    START = "START"  # begin the actual work
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    RECOVER = "RECOVER"  # finish recovery, go back to the state paused from
    REQUEUE = "REQUEUE"  # preemption / vehicle loss: give the vehicle back, wait again
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"
    CANCEL = "CANCEL"

    ALL: tuple[str, ...] = (
        ASSIGN,
        DISPATCH,
        ENQUEUE,
        PROCEED,
        START,
        PAUSE,
        RESUME,
        RECOVER,
        REQUEUE,
        COMPLETE,
        FAIL,
        CANCEL,
    )


#: Sentinels meaning "target depends on where we came from".
RESUME_TARGET = "__RESUME_TARGET__"
QUEUE_RETURN = "__QUEUE_RETURN__"

#: The transition table.  ``TRANSITIONS[state][event] = next_state``.
TRANSITIONS: dict[str, dict[str, str]] = {
    TaskState.PENDING: {
        TaskEvent.ASSIGN: TaskState.ASSIGNED,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    TaskState.ASSIGNED: {
        TaskEvent.DISPATCH: TaskState.ENROUTE,
        # A task may start directly from ASSIGNED when the vehicle is already
        # standing at the pick-up point, so there is no ENROUTE leg to observe.
        TaskEvent.START: TaskState.EXECUTING,
        TaskEvent.ENQUEUE: TaskState.QUEUED,
        TaskEvent.PAUSE: TaskState.PAUSED,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    TaskState.ENROUTE: {
        TaskEvent.ENQUEUE: TaskState.QUEUED,
        TaskEvent.START: TaskState.EXECUTING,
        TaskEvent.PAUSE: TaskState.PAUSED,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    # QUEUED is a real state, not a flag: a task blocked at an intersection has
    # stopped making progress, and operations needs to see that.  PROCEED returns
    # it to *wherever it was* when it got stuck, which is why the target is a
    # sentinel resolved against the queue stack rather than a fixed state.
    TaskState.QUEUED: {
        TaskEvent.PROCEED: QUEUE_RETURN,
        TaskEvent.PAUSE: TaskState.PAUSED,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    TaskState.EXECUTING: {
        TaskEvent.COMPLETE: TaskState.COMPLETED,
        TaskEvent.ENQUEUE: TaskState.QUEUED,
        TaskEvent.PAUSE: TaskState.PAUSED,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    TaskState.PAUSED: {
        TaskEvent.RESUME: TaskState.RECOVERING,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    TaskState.RECOVERING: {
        TaskEvent.RECOVER: RESUME_TARGET,
        TaskEvent.REQUEUE: TaskState.PENDING,
        TaskEvent.CANCEL: TaskState.CANCELLED,
        TaskEvent.FAIL: TaskState.FAILED,
    },
    # Terminal states intentionally have no outgoing transitions.
    TaskState.COMPLETED: {},
    TaskState.FAILED: {},
    TaskState.CANCELLED: {},
}

#: Same table for vehicles (used by the fleet manager).
VEHICLE_TRANSITIONS: dict[str, dict[str, str]] = {
    VehicleState.OFFLINE: {
        "ONLINE": VehicleState.IDLE,
    },
    VehicleState.IDLE: {
        "ASSIGN": VehicleState.ASSIGNED,
        "CHARGE": VehicleState.CHARGING,
        "FAULT": VehicleState.FAULT,
        "MAINTAIN": VehicleState.MAINTENANCE,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.ASSIGNED: {
        "DISPATCH": VehicleState.ENROUTE,
        "ENQUEUE": VehicleState.QUEUED,
        "RELEASE": VehicleState.IDLE,
        "PAUSE": VehicleState.PAUSED,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    # Every non-terminal state must accept RELEASE.  A vehicle whose task ended
    # (cancelled, failed, preempted) has to become dispatchable again; a state
    # that cannot be released from is a vehicle that is lost to the fleet for the
    # rest of the run, and the failure is invisible until throughput collapses.
    VehicleState.ENROUTE: {
        "ENQUEUE": VehicleState.QUEUED,
        "START": VehicleState.EXECUTING,
        "RELEASE": VehicleState.IDLE,
        "PAUSE": VehicleState.PAUSED,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.QUEUED: {
        "PROCEED": QUEUE_RETURN,
        "START": VehicleState.EXECUTING,
        "RELEASE": VehicleState.IDLE,
        "PAUSE": VehicleState.PAUSED,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.EXECUTING: {
        "COMPLETE": VehicleState.IDLE,
        "ENQUEUE": VehicleState.QUEUED,
        "RELEASE": VehicleState.IDLE,
        "PAUSE": VehicleState.PAUSED,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.PAUSED: {
        "RESUME": VehicleState.RECOVERING,
        "RELEASE": VehicleState.IDLE,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.RECOVERING: {
        "RECOVER": RESUME_TARGET,
        "RELEASE": VehicleState.IDLE,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.CHARGING: {
        "CHARGE_DONE": VehicleState.IDLE,
        "FAULT": VehicleState.FAULT,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.FAULT: {
        "REPAIR": VehicleState.MAINTENANCE,
        "RECOVER": VehicleState.IDLE,
        "HEARTBEAT_TIMEOUT": VehicleState.OFFLINE,
    },
    VehicleState.MAINTENANCE: {
        # Maintenance is *not* terminal: a repaired vehicle returns to service.
        "REPAIR_DONE": VehicleState.IDLE,
    },
}


class IllegalTransition(Exception):
    """Raised when an event cannot be applied in the current state.

    The message always carries *why*, because "rejected" without a reason is
    useless in an operations console.
    """

    def __init__(self, subject: str, from_state: str, event: str, reason: str) -> None:
        self.subject = subject
        self.from_state = from_state
        self.event = event
        self.reason = reason
        super().__init__(
            f"illegal transition: subject={subject} state={from_state} "
            f"event={event} reason={reason}"
        )


@dataclass
class TransitionRecord:
    """One accepted state change."""

    subject: str
    seq: int
    from_state: str
    event: str
    to_state: str
    at_ms: int
    trace_id: str = ""
    note: str = ""

    def as_dict(self) -> dict:
        return {
            "subject": self.subject,
            "seq": self.seq,
            "from": self.from_state,
            "event": self.event,
            "to": self.to_state,
            "at_ms": self.at_ms,
            "trace_id": self.trace_id,
            "note": self.note,
        }


@dataclass
class RejectionRecord:
    """One refused state change, with the reason."""

    subject: str
    from_state: str
    event: str
    reason: str
    at_ms: int

    def as_dict(self) -> dict:
        return {
            "subject": self.subject,
            "from": self.from_state,
            "event": self.event,
            "reason": self.reason,
            "at_ms": self.at_ms,
        }


@dataclass
class StateMachine:
    """A single event-driven state machine instance.

    Parameters
    ----------
    subject:
        Identifier used in records/logs (a task id or a vehicle id).
    transitions:
        Transition table; defaults to the task table.
    initial:
        Starting state.
    """

    subject: str
    transitions: dict[str, dict[str, str]] = field(default_factory=lambda: TRANSITIONS)
    initial: str = TaskState.PENDING
    state: str = ""
    history: list[TransitionRecord] = field(default_factory=list)
    rejections: list[RejectionRecord] = field(default_factory=list)
    #: Stack of states we were paused from, so RESUME can restore precisely.
    _pause_stack: list[str] = field(default_factory=list)
    #: Stack of states we were queued from, so PROCEED can restore precisely.
    _queue_stack: list[str] = field(default_factory=list)
    _seq: int = 0

    def __post_init__(self) -> None:
        if not self.state:
            self.state = self.initial

    # -- introspection ----------------------------------------------------

    def allowed_events(self) -> tuple[str, ...]:
        """Events that would be accepted right now (sorted for reproducibility)."""
        return tuple(sorted(self.transitions.get(self.state, {}).keys()))

    def is_terminal(self) -> bool:
        return not self.transitions.get(self.state, {})

    def can(self, event: str) -> bool:
        return event in self.transitions.get(self.state, {})

    def _resolve_target(self, event: str) -> str:
        target = self.transitions[self.state][event]
        if target == RESUME_TARGET:
            if not self._pause_stack:
                raise IllegalTransition(
                    self.subject, self.state, event, "no paused state to resume to"
                )
            return self._pause_stack[-1]
        if target == QUEUE_RETURN:
            if not self._queue_stack:
                raise IllegalTransition(
                    self.subject, self.state, event, "no queued state to return to"
                )
            return self._queue_stack[-1]
        return target

    # -- mutation ---------------------------------------------------------

    def apply(
        self,
        event: str,
        *,
        at_ms: int = 0,
        trace_id: str = "",
        note: str = "",
    ) -> str:
        """Apply ``event``; return the new state.

        Raises
        ------
        IllegalTransition
            If the event is unknown or not allowed in the current state.  State
            is guaranteed unchanged in that case.
        """
        table = self.transitions.get(self.state)
        if table is None:  # pragma: no cover - defensive, table covers all states
            raise IllegalTransition(
                self.subject, self.state, event, "state not present in transition table"
            )
        if event not in table:
            reason = (
                f"event {event} not allowed in {self.state}; "
                f"allowed={list(self.allowed_events()) or 'none (terminal state)'}"
            )
            self.rejections.append(
                RejectionRecord(self.subject, self.state, event, reason, at_ms)
            )
            raise IllegalTransition(self.subject, self.state, event, reason)

        target = self._resolve_target(event)
        previous = self.state
        self._seq += 1
        self.state = target
        if event == TaskEvent.PAUSE:
            self._pause_stack.append(previous)
        elif event == TaskEvent.RECOVER:
            if self._pause_stack:
                self._pause_stack.pop()
        elif event == TaskEvent.ENQUEUE:
            self._queue_stack.append(previous)
        elif event == TaskEvent.PROCEED:
            if self._queue_stack:
                self._queue_stack.pop()
        elif event == TaskEvent.REQUEUE:
            # A requeued task starts over: any remembered pause/queue position
            # describes a journey that is no longer being made.
            self._pause_stack.clear()
            self._queue_stack.clear()
        self.history.append(
            TransitionRecord(
                subject=self.subject,
                seq=self._seq,
                from_state=previous,
                event=event,
                to_state=target,
                at_ms=at_ms,
                trace_id=trace_id,
                note=note,
            )
        )
        return self.state

    def try_apply(self, event: str, **kwargs) -> bool:
        """Apply ``event`` and report success instead of raising.

        Used on the hot path (bus consumers) where a rejected event is an
        expected condition, not an error.
        """
        try:
            self.apply(event, **kwargs)
            return True
        except IllegalTransition:
            return False

    def force_state(self, state: str) -> None:
        """Test/diagnostic helper: jump to a state without recording a transition."""
        if state not in self.transitions:
            raise ValueError(f"unknown state {state!r}")
        self.state = state

    # -- reporting --------------------------------------------------------

    def as_dict(self) -> dict:
        return {
            "subject": self.subject,
            "state": self.state,
            "transitions": len(self.history),
            "rejections": len(self.rejections),
            "history": [r.as_dict() for r in self.history],
        }


# --------------------------------------------------------------------------
# Static helpers shared by the report generator and by tests
# --------------------------------------------------------------------------


def transition_table(
    transitions: dict[str, dict[str, str]] | None = None,
    dynamic_targets: dict[str, list[str]] | None = None,
) -> list[dict]:
    """Return the transition table as a sorted list of dicts.

    Dynamic sentinels are expanded to the concrete set of states they can resolve
    to, which is what a reader of the report actually wants to see.
    """
    table = TRANSITIONS if transitions is None else transitions
    expansion = {
        RESUME_TARGET: ["ASSIGNED", "ENROUTE", "QUEUED", "EXECUTING"],
        QUEUE_RETURN: ["ASSIGNED", "ENROUTE", "EXECUTING"],
    }
    if dynamic_targets:
        expansion.update({k: sorted(v) for k, v in dynamic_targets.items()})
    rows: list[dict] = []
    for state in sorted(table):
        for event in sorted(table[state]):
            target = table[state][event]
            rows.append(
                {
                    "from": state,
                    "event": event,
                    "to": "|".join(expansion[target]) if target in expansion else target,
                }
            )
    return rows


def check_table_consistency(transitions: dict[str, dict[str, str]] | None = None) -> list[str]:
    """Validate the table itself and return a list of problems (empty == fine).

    Catches the classic copy/paste bug of pointing an edge at a state that does
    not exist.
    """
    table = TRANSITIONS if transitions is None else transitions
    problems: list[str] = []
    dynamic = {RESUME_TARGET, QUEUE_RETURN}
    for state, edges in table.items():
        if not isinstance(edges, dict):
            problems.append(f"{state}: edges is not a dict")
            continue
        for event, target in edges.items():
            if target not in dynamic and target not in table:
                problems.append(f"{state}--{event}--> unknown state {target!r}")
    return problems

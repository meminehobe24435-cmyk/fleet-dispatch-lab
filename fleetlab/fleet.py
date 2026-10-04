"""Real-time vehicle state: pose, battery, load, liveness.

Two different kinds of truth live in the platform and mixing them up is a
classic source of bugs, so this module is explicit about which is which:

* the **vehicle table** here is *current state* — it is overwritten in place and
  answers "where is AGV-07 right now";
* the **event log** (see :mod:`fleetlab.bus` and :mod:`fleetlab.eventlog`) is
  *history* — it is append-only and answers "how did AGV-07 get here".

Liveness is a third thing again: a vehicle is *online* when a heartbeat arrived
within the timeout.  Being online is not a state in the state machine, because a
vehicle can be in any state while offline; conflating the two is how a dispatch
system ends up assigning work to a dismounted truck.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .task_fsm import (
    VEHICLE_TRANSITIONS,
    IllegalTransition,
    StateMachine,
    VehicleState,
)

__all__ = ["Vehicle", "FleetManager", "FLEET_TRANSITIONS"]

FLEET_TRANSITIONS = VEHICLE_TRANSITIONS

#: Battery percentage below which a vehicle is withheld from dispatch.
LOW_BATTERY = 20.0
#: Battery consumed per simulated metre of travel.  Tuned so that a typical
#: two-leg trip costs a few percent: an earlier value of 0.05 made every trip eat
#: a third of the battery and the fleet spent its life on the charger.
BATTERY_DRAIN_PER_METRE = 0.01


@dataclass
class Vehicle:
    """One automated guided vehicle's live state."""

    vehicle_id: str
    capacity_kg: float = 2000.0
    x: float = 0.0
    y: float = 0.0
    speed_mps: float = 2.0
    battery: float = 100.0
    payload: str = ""
    online: bool = True
    last_heartbeat_ms: int = 0
    odometer_m: float = 0.0
    tasks_completed: int = 0
    pause_commands: int = 0
    resume_commands: int = 0
    preempted: int = 0
    faults: int = 0
    fsm: StateMachine = field(default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.fsm is None:
            self.fsm = StateMachine(
                self.vehicle_id,
                transitions=FLEET_TRANSITIONS,
                initial=VehicleState.IDLE,
                state=VehicleState.IDLE,
            )

    # -- state helpers ----------------------------------------------------

    @property
    def state(self) -> str:
        return self.fsm.state

    @property
    def dispatchable(self) -> bool:
        """Available for a brand-new task right now?"""
        return (
            self.online
            and self.state in VehicleState.DISPATCHABLE
            and self.battery >= LOW_BATTERY
        )

    def apply(self, event: str, at_ms: int = 0, trace_id: str = "", note: str = "") -> bool:
        """Apply a vehicle event; ``False`` if the transition was illegal."""
        return self.fsm.try_apply(event, at_ms=at_ms, trace_id=trace_id, note=note)

    def distance_to(self, target: tuple[float, float]) -> float:
        # ``math.sqrt(dx*dx + dy*dy)`` rather than ``math.hypot``: sqrt is
        # correctly rounded by IEEE-754 and therefore bit-identical on every
        # platform, while hypot's overflow-scaling algorithm is libm-dependent.
        # A one-ULP disagreement changes routing decisions, which would make the
        # committed metrics unreproducible on another OS for no good reason.
        return math.sqrt((self.x - target[0]) ** 2 + (self.y - target[1]) ** 2)

    def move_towards(self, target: tuple[float, float], dt_s: float) -> float:
        """Advance towards ``target`` for ``dt_s``; returns metres travelled."""
        dx = target[0] - self.x
        dy = target[1] - self.y
        span = math.sqrt(dx * dx + dy * dy)
        if span <= 1e-9:
            return 0.0
        step = min(self.speed_mps * dt_s, span)
        self.x += dx / span * step
        self.y += dy / span * step
        self.odometer_m += step
        self.battery = max(0.0, self.battery - step * BATTERY_DRAIN_PER_METRE)
        return step

    def at(self, target: tuple[float, float], tolerance: float = 0.5) -> bool:
        return self.distance_to(target) <= tolerance

    def as_dict(self) -> dict:
        return {
            "vehicle_id": self.vehicle_id,
            "state": self.state,
            "online": self.online,
            "x": round(self.x, 3),
            "y": round(self.y, 3),
            "battery": round(self.battery, 2),
            "payload": self.payload,
            "last_heartbeat_ms": self.last_heartbeat_ms,
            "odometer_m": round(self.odometer_m, 3),
            "tasks_completed": self.tasks_completed,
            "pause_commands": self.pause_commands,
            "resume_commands": self.resume_commands,
            "preempted": self.preempted,
            "faults": self.faults,
        }


class FleetManager:
    """Owns vehicles, heartbeats and offline detection."""

    def __init__(self, heartbeat_timeout_ms: int = 5_000) -> None:
        self.vehicles: dict[str, Vehicle] = {}
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.counters = {
            "heartbeats": 0,
            "timeouts": 0,
            "reconnects": 0,
            "offline_events": 0,
        }
        self.offline_log: list[dict] = []

    # -- membership -------------------------------------------------------

    def add(self, vehicle: Vehicle) -> Vehicle:
        self.vehicles[vehicle.vehicle_id] = vehicle
        return vehicle

    def get(self, vehicle_id: str) -> Vehicle:
        try:
            return self.vehicles[vehicle_id]
        except KeyError:
            raise KeyError(f"unknown vehicle {vehicle_id!r}") from None

    def ids(self) -> list[str]:
        return sorted(self.vehicles)

    def of_state(self, *states: str) -> list[Vehicle]:
        wanted = set(states)
        return [self.vehicles[v] for v in self.ids() if self.vehicles[v].state in wanted]

    def dispatchable(self) -> list[Vehicle]:
        return [self.vehicles[v] for v in self.ids() if self.vehicles[v].dispatchable]

    def online(self) -> list[Vehicle]:
        return [self.vehicles[v] for v in self.ids() if self.vehicles[v].online]

    # -- liveness ---------------------------------------------------------

    def heartbeat(
        self,
        vehicle_id: str,
        *,
        at_ms: int,
        x: float | None = None,
        y: float | None = None,
        battery: float | None = None,
    ) -> Vehicle:
        """Record a heartbeat, bringing a previously offline vehicle back online."""
        vehicle = self.get(vehicle_id)
        self.counters["heartbeats"] += 1
        vehicle.last_heartbeat_ms = at_ms
        if x is not None:
            vehicle.x = x
        if y is not None:
            vehicle.y = y
        if battery is not None:
            vehicle.battery = battery
        if not vehicle.online:
            vehicle.online = True
            self.counters["reconnects"] += 1
            # Coming back from OFFLINE resumes at IDLE: we can no longer trust the
            # in-progress task's progress, so the scheduler must re-evaluate it.
            if vehicle.state == VehicleState.OFFLINE:
                vehicle.apply("ONLINE", at_ms=at_ms, note="heartbeat after offline")
        return vehicle

    def stale_vehicles(self, now_ms: int) -> list[tuple[str, int, str]]:
        """Report vehicles whose heartbeat is stale, *without mutating anything*.

        Read-only detection matters for replayability: the decision to take a
        vehicle offline has to be journaled as an event and applied by the single
        applier, so this method must not quietly set ``online = False`` on its own
        and desynchronise the live state from the event log.
        """
        stale: list[tuple[str, int, str]] = []
        for vehicle_id in self.ids():
            vehicle = self.vehicles[vehicle_id]
            if not vehicle.online:
                continue
            silent_ms = now_ms - vehicle.last_heartbeat_ms
            if silent_ms <= self.heartbeat_timeout_ms:
                continue
            stale.append((vehicle_id, silent_ms, vehicle.state))
        return stale

    def check_timeouts(self, now_ms: int) -> list[str]:
        """Mark vehicles whose heartbeat is stale as OFFLINE.

        Returns the affected ids (sorted) so callers can raise alerts without
        re-scanning the fleet.  Detection is *pull-based* rather than driven by a
        timer thread, which keeps the simulation deterministic.
        """
        stale: list[str] = []
        for vehicle_id, silent_ms, state_at_timeout in self.stale_vehicles(now_ms):
            vehicle = self.vehicles[vehicle_id]
            vehicle.online = False
            self.counters["timeouts"] += 1
            self.counters["offline_events"] += 1
            vehicle.apply(
                "HEARTBEAT_TIMEOUT",
                at_ms=now_ms,
                note=f"no heartbeat for {silent_ms} ms",
            )
            self.offline_log.append(
                {
                    "vehicle_id": vehicle_id,
                    "at_ms": now_ms,
                    "silent_ms": silent_ms,
                    "state_at_timeout": state_at_timeout,
                }
            )
            stale.append(vehicle_id)
        return sorted(stale)

    def set_offline(self, vehicle_id: str, at_ms: int, note: str = "injected") -> bool:
        """Force a vehicle offline (used by the fault injector)."""
        vehicle = self.get(vehicle_id)
        if not vehicle.online:
            return False
        vehicle.online = False
        self.counters["offline_events"] += 1
        vehicle.apply("HEARTBEAT_TIMEOUT", at_ms=at_ms, note=note)
        self.offline_log.append(
            {"vehicle_id": vehicle_id, "at_ms": at_ms, "silent_ms": 0, "state_at_timeout": vehicle.state}
        )
        return True

    # -- reporting --------------------------------------------------------

    def snapshot(self) -> dict:
        return {vid: self.vehicles[vid].as_dict() for vid in self.ids()}

    def state_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for vid in self.ids():
            state = self.vehicles[vid].state
            counts[state] = counts.get(state, 0) + 1
        return {k: counts[k] for k in sorted(counts)}

    def stats(self) -> dict:
        return {
            "vehicles": len(self.vehicles),
            "online": len(self.online()),
            "dispatchable": len(self.dispatchable()),
            "state_counts": self.state_counts(),
            "counters": dict(sorted(self.counters.items())),
        }

"""Discrete-event simulation of a port yard: the synthetic workload driver.

**All data produced here is synthetic.**  Vehicle positions, task arrivals,
priorities and deadlines come from a seeded PRNG and a hand-drawn six-hub grid.
Nothing in this file describes a real port, terminal or fleet.

Why simulate at all?  Because the properties worth demonstrating in this project
— eventual consistency under duplication/reordering/loss, consumer recovery from
a committed offset, idempotent commands, deadlock breaking — are *timing*
properties.  They need a workload that produces contention, contention needs a
world, and a world needs to be reproducible, which a recorded trace would not be.

Time is virtual.  One tick is ``tick_ms`` of simulated time, and every timestamp
in the output is derived from the tick counter, never from ``time.time()``.  That
is what lets the two-run reproducibility check compare whole metric files instead
of a handful of counters.

Resource discipline
-------------------
A vehicle's reservations are **never cached in a local field**.  Bookkeeping that
mirrors the traffic controller is bookkeeping that can disagree with it, and a
disagreement there produces a yard that silently stops moving.  Every decision
therefore asks the controller what is actually held, and the reservations are the
single source of truth.
"""

from __future__ import annotations

import heapq
import math
import random
from dataclasses import dataclass, field

from .faults import FaultConfig
from .fleet import Vehicle
from .observability import percentile
from .runtime import Runtime, RuntimeConfig
from .scheduler import Scheduler
from .task_fsm import TaskEvent, TaskState, VehicleState
from .traffic import TrafficController

__all__ = ["HUBS", "EDGES", "World", "SimConfig", "Simulation", "Trip", "distribution"]

#: Six hubs of a 2x3 yard grid, in metres.  Hand-drawn, not surveyed.
HUBS: dict[str, tuple[float, float]] = {
    "A": (60.0, 60.0),
    "B": (300.0, 60.0),
    "C": (540.0, 60.0),
    "D": (60.0, 340.0),
    "E": (300.0, 340.0),
    "F": (540.0, 340.0),
}

#: Lane segments.  Every lane is single-occupancy in this model, which is what
#: makes opposing traffic a real conflict rather than a non-event.
EDGES: tuple[tuple[str, str], ...] = (
    ("A", "B"),
    ("B", "C"),
    ("A", "D"),
    ("D", "E"),
    ("E", "F"),
    ("C", "F"),
    ("B", "E"),
)

CHARGE_BELOW = 30.0
CHARGE_UNTIL = 85.0
CHARGE_PER_TICK = 4.0

#: Extra metres of perceived cost per vehicle occupying a lane or intersection.
#: Large enough that a two-leg detour can beat a congested one-leg hop.
CONGESTION_PENALTY = 260.0

#: Vehicles that may share one aisle segment simultaneously.  Capacity 1 makes an
#: aisle a genuine mutex, which is what produces the give-way decisions; capacity
#: 2 was tried and quietly removed most of the contention the traffic controller
#: exists to arbitrate.
LANE_CAPACITY = 1

#: Deadline model: a fixed handling allowance plus a per-leg travel allowance,
#: multiplied by a random slack.  Deadlines drawn uniformly at random were
#: unrelated to how long a job actually takes, so easy jobs blew deadlines and
#: hard ones never could — a measurement artefact, not a scheduling result.
DEADLINE_BASE_MS = 120_000
DEADLINE_PER_LEG_MS = 90_000

#: How long a vehicle may sit blocked on the same resource before it gives up,
#: releases its reservations and asks to be re-dispatched.
WAIT_TIMEOUT_TICKS = 60


class World:
    """The hub graph plus its shared resources.

    Three kinds of exclusive resource, matching how a real yard is laid out:

    ``BAY_<hub>``
        Berths, capacity > 1.  A vehicle *parks* here.  Modelling the parking
        area as capacity-1 was the source of a structural starvation bug: with
        more vehicles than hubs, some vehicle could never take a hub and the yard
        throttled itself for no physical reason.
    ``INT_<hub>``
        The intersection, capacity 1.  Genuinely mutually exclusive.
    ``LANE_<a>_<b>``
        A one-way aisle, capacity 2.  Direction-specific, so opposing traffic is
        not treated as an intruder on the same resource, and capacity 2 because
        vehicles *follow* each other down an aisle — modelling a lane as a mutex
        made the yard queue behind a single vehicle on every edge.
    """

    def __init__(self, traffic: TrafficController, vehicles: int = 8) -> None:
        self.traffic = traffic
        self.adjacency: dict[str, list[str]] = {hub: [] for hub in HUBS}
        for a, b in EDGES:
            self.adjacency[a].append(b)
            self.adjacency[b].append(a)
        for hub in sorted(self.adjacency):
            self.adjacency[hub].sort()
        self.lane_id = {(a, b): f"LANE_{a}_{b}" for a, b in EDGES}
        self.lane_id.update({(b, a): f"LANE_{b}_{a}" for a, b in EDGES})
        self.bay_capacity = max(2, math.ceil(vehicles / len(HUBS)) + 2)
        for hub in sorted(HUBS):
            traffic.add_resource(f"BAY_{hub}", kind="bay", capacity=self.bay_capacity)
            traffic.add_resource(f"INT_{hub}", kind="intersection", capacity=1)
        for a, b in EDGES:
            traffic.add_resource(f"LANE_{a}_{b}", kind="lane", capacity=LANE_CAPACITY)
            traffic.add_resource(f"LANE_{b}_{a}", kind="lane", capacity=LANE_CAPACITY)

    def bay(self, hub: str) -> str:
        return f"BAY_{hub}"

    def intersection(self, hub: str) -> str:
        return f"INT_{hub}"

    def lane(self, a: str, b: str) -> str:
        return self.lane_id[(a, b)]

    def nearest_hub(self, position: tuple[float, float]) -> str:
        return min(
            sorted(HUBS),
            key=lambda hub: (
                math.sqrt(
                    (position[0] - HUBS[hub][0]) ** 2 + (position[1] - HUBS[hub][1]) ** 2
                ),
                hub,
            ),
        )

    def edge_cost(self, u: str, v: str, penalty: float = CONGESTION_PENALTY) -> float:
        """Travel cost of the hop ``u -> v``, including a congestion surcharge.

        The surcharge is what stops every vehicle from choosing the geometric
        shortest path into the same aisle.  Profiling the first working version
        showed 1299 vehicle-ticks — a quarter of the whole run — blocked on a
        single lane, the central B-E connector: pure shortest-path routing funnels
        all north-south traffic through one edge, and no amount of scheduling
        fixes a self-inflicted traffic jam.
        """
        lane = self.traffic.resources[self.lane_id[(u, v)]]
        crossing = self.traffic.resources[f"INT_{v}"]
        load = (
            len(lane.holders) + len(lane.queue) + len(crossing.holders) + len(crossing.queue)
        )
        base = math.sqrt((HUBS[u][0] - HUBS[v][0]) ** 2 + (HUBS[u][1] - HUBS[v][1]) ** 2)
        return base + penalty * load

    def route(self, start: str, goal: str, penalty: float = CONGESTION_PENALTY) -> list[str]:
        """Least-cost path, weighted by current congestion.

        Dijkstra with a deterministic tie-break: heap entries compare
        ``(cost, hub_name)``, so equal-cost routes always resolve the same way and
        the simulation stays reproducible.
        """
        if start == goal:
            return [start]
        dist: dict[str, float] = {start: 0.0}
        previous: dict[str, str] = {}
        queue: list[tuple[float, str]] = [(0.0, start)]
        while queue:
            cost, node = heapq.heappop(queue)
            if cost > dist.get(node, math.inf) + 1e-9:
                continue
            if node == goal:
                break
            for neighbour in self.adjacency[node]:
                candidate = cost + self.edge_cost(node, neighbour, penalty)
                if candidate < dist.get(neighbour, math.inf) - 1e-9:
                    dist[neighbour] = candidate
                    previous[neighbour] = node
                    heapq.heappush(queue, (candidate, neighbour))
        if goal not in previous:
            return [start]
        path = [goal]
        while path[-1] != start:
            path.append(previous[path[-1]])
        path.reverse()
        return path


@dataclass
class Trip:
    """A vehicle's progress along a route.  Scratch state, never journaled.

    A trip is *derived* from an assignment plus physics, so journaling it would
    bloat the log with records no replay needs.
    """

    task_id: str
    vehicle_id: str
    route: list[str]
    index: int = 0
    phase: str = "negotiate"  # "negotiate" | "travel"
    picked_up: bool = False
    dispatched: bool = False
    waiting: bool = False
    blocked_on: str = ""
    blocked_since: int = 0

    def current_hub(self) -> str:
        return self.route[min(self.index, len(self.route) - 1)]

    def target_hub(self) -> str:
        return self.route[min(self.index + 1, len(self.route) - 1)]


@dataclass
class SimConfig:
    """Everything that defines a scenario.  Fully deterministic given the seed."""

    vehicles: int = 8
    tasks: int = 40
    seed: int = 20240501
    tick_ms: int = 1_000
    max_ticks: int = 3_000
    task_interval_ticks: int = 4
    num_partitions: int = 4
    backpressure_limit: int | None = 200
    heartbeat_timeout_ms: int = 5_000
    max_retries: int = 3
    vehicle_speed_mps: float = 12.0
    #: Messages one tick may consume.  ``None`` means "drain completely", which
    #: keeps the backlog pinned at zero by construction; a finite budget lets a
    #: burst build real lag, real backpressure and a real backlog peak.
    consume_budget_per_tick: int | None = None
    #: Extra tasks published in a single tick, to create a burst on purpose.
    burst_tasks: int = 0
    burst_at_tick: int = 0
    #: (start_tick, vehicle_id, duration_ticks) — forces a vehicle to fall silent.
    offline_plan: tuple[tuple[int, str, int], ...] = ()
    #: Ticks at which to expire the hot cache on purpose.
    cache_expire_ticks: tuple[int, ...] = ()
    #: Hold the hub's intersection while negotiating the next leg.  This is the
    #: deadlock-prone mode: it creates genuine hold-and-wait between vehicles, and
    #: the baseline leaves it off precisely because a real yard avoids it.
    hold_hub_intersection: bool = False
    fault: FaultConfig = field(default_factory=FaultConfig)
    name: str = "baseline"

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "vehicles": self.vehicles,
            "tasks": self.tasks,
            "seed": self.seed,
            "tick_ms": self.tick_ms,
            "max_ticks": self.max_ticks,
            "task_interval_ticks": self.task_interval_ticks,
            "num_partitions": self.num_partitions,
            "backpressure_limit": self.backpressure_limit,
            "heartbeat_timeout_ms": self.heartbeat_timeout_ms,
            "max_retries": self.max_retries,
            "vehicle_speed_mps": self.vehicle_speed_mps,
            "consume_budget_per_tick": self.consume_budget_per_tick,
            "burst_tasks": self.burst_tasks,
            "burst_at_tick": self.burst_at_tick,
            "offline_plan": [list(entry) for entry in self.offline_plan],
            "cache_expire_ticks": list(self.cache_expire_ticks),
            "hold_hub_intersection": self.hold_hub_intersection,
            "fault": self.fault.as_dict(),
        }


class Simulation:
    """Drives the runtime through a scenario and collects the report."""

    def __init__(self, config: SimConfig | None = None) -> None:
        self.config = config if config is not None else SimConfig()
        self.rt = Runtime(
            RuntimeConfig(
                vehicles=self.config.vehicles,
                num_partitions=self.config.num_partitions,
                backpressure_limit=self.config.backpressure_limit,
                heartbeat_timeout_ms=self.config.heartbeat_timeout_ms,
                max_retries=self.config.max_retries,
                seed=self.config.seed,
                fault=self.config.fault,
            )
        )
        self.traffic = TrafficController()
        self.world = World(self.traffic, vehicles=self.config.vehicles)
        self.scheduler = Scheduler()
        self.trips: dict[str, Trip] = {}
        self.gen_rng = random.Random(self.config.seed ^ 0x5EED)
        self.tick_index = 0
        self.now_ms = 0
        self._pending: dict[str, list[tuple]] = {}
        self._silent: dict[str, int] = {}
        self._task_seq = 0
        self.deadlock_events: list[dict] = []
        self.cache_expiry_events: list[dict] = []
        self.backpressure_events: list[dict] = []
        self.rejections: list[dict] = []
        self.offline_cycles: list[dict] = []
        self.wait_timeout_events: list[dict] = []
        self.deadline_failures: list[dict] = []
        self.lag_series: list[list[int]] = []
        self.extra_counters: dict[str, int] = {
            "wait_timeouts": 0,
            "deadline_failures": 0,
            "traffic_blocks": 0,
            "cache_expiries": 0,
            "backpressure_deferrals": 0,
        }
        self._setup_fleet()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _setup_fleet(self) -> None:
        hubs = sorted(HUBS)
        for index in range(self.config.vehicles):
            hub = hubs[index % len(hubs)]
            offset = 12.0 * (index // len(hubs))
            self.rt.add_vehicle(
                f"AGV-{index + 1:02d}",
                x=HUBS[hub][0] + offset,
                y=HUBS[hub][1] + offset,
                battery=100.0 - 3.0 * (index % 5),
                speed_mps=self.config.vehicle_speed_mps,
            )

    # ------------------------------------------------------------------
    # Publish with per-subject ordering under backpressure
    # ------------------------------------------------------------------

    def _publish(
        self, kind: str, subject: str, payload: dict, *, at_ms: int, trace_id: str = ""
    ) -> None:
        """Queue and try to publish, preserving order per subject.

        A refused publish must not be dropped and must not be overtaken: if a
        later event for the same task went out while an earlier one was still
        waiting, the task would be paused before it was assigned.  So each subject
        owns a FIFO and only its head is ever offered to the bus.
        """
        self._pending.setdefault(subject, []).append((kind, subject, payload, at_ms, trace_id))
        self._flush_subject(subject)

    def _flush_subject(self, subject: str) -> None:
        queue = self._pending.get(subject)
        if not queue:
            return
        while queue:
            kind, subj, payload, at_ms, trace_id = queue[0]
            message = self.rt.publish(kind, subj, payload, at_ms=at_ms, trace_id=trace_id)
            if message is None:
                self.extra_counters["backpressure_deferrals"] += 1
                self.backpressure_events.append(
                    {"subject": subject, "kind": kind, "at_ms": at_ms, "queue_depth": len(queue)}
                )
                return
            queue.pop(0)
        self._pending.pop(subject, None)

    def _flush_all(self) -> None:
        for subject in sorted(self._pending):
            self._flush_subject(subject)

    def _direct(self, kind: str, subject: str, payload: dict, *, at_ms: int) -> None:
        self.rt.emit_direct(kind, subject, payload, at_ms=at_ms)

    # ------------------------------------------------------------------
    # Task generation
    # ------------------------------------------------------------------

    def _make_task(self) -> str:
        self._task_seq += 1
        task_id = f"T{self._task_seq:04d}"
        hubs = sorted(HUBS)
        origin = self.gen_rng.choice(hubs)
        destination = self.gen_rng.choice([h for h in hubs if h != origin])
        priority = self.gen_rng.choices(
            [1, 2, 3, 4, 5], weights=[10, 45, 25, 15, 5]
        )[0]
        payload_kg = round(self.gen_rng.uniform(50.0, 1800.0), 1)
        legs = max(1, len(self.world.route(origin, destination)) - 1)
        allowance = DEADLINE_BASE_MS + DEADLINE_PER_LEG_MS * legs
        deadline_ms = self.now_ms + int(allowance * self.gen_rng.uniform(1.6, 3.2))
        self._publish(
            "task_created",
            task_id,
            {
                "task_id": task_id,
                "origin": list(HUBS[origin]),
                "destination": list(HUBS[destination]),
                "origin_hub": origin,
                "destination_hub": destination,
                "priority": priority,
                "payload_kg": payload_kg,
                "created_ms": self.now_ms,
                "deadline_ms": deadline_ms,
            },
            at_ms=self.now_ms,
            trace_id=self.rt.tracer.new_trace_id(),
        )
        return task_id

    # ------------------------------------------------------------------
    # Traffic
    # ------------------------------------------------------------------

    def _request(self, vehicle_id: str, resource_id: str, priority: int) -> bool:
        """Request once per blocking episode; do not re-queue while already waiting."""
        resource = self.traffic.resources[resource_id]
        if vehicle_id in resource.holders:
            return True
        if any(w.vehicle_id == vehicle_id for w in resource.queue):
            return False
        return self.traffic.request(
            vehicle_id, resource_id, priority=priority, at_ms=self.now_ms
        )

    def _held(self, vehicle_id: str, resource_id: str) -> bool:
        resource = self.traffic.resources.get(resource_id)
        return resource is not None and vehicle_id in resource.holders

    def _handle_deadlocks(self) -> None:
        """Break every deadlock the yard is in, and re-plan the victims.

        A broken cycle is only half a recovery: the victim has lost its
        reservations, so its task must go back to the queue.  Leaving it assigned
        would strand a task on a vehicle that is no longer moving — a silent
        failure that looks exactly like a slow one.
        """
        for _ in range(len(self.rt.fleet.vehicles) + 1):
            record = self.traffic.break_deadlock(self.now_ms)
            if record is None:
                break
            record["tick"] = self.tick_index
            self.deadlock_events.append(record)
            self.rt.metrics.incr("deadlock_breaks")
            victim = record["victim"]
            task_id = self._task_of_vehicle(victim)
            if task_id:
                self.trips.pop(task_id, None)
                self._publish(
                    "task_requeued",
                    task_id,
                    {"task_id": task_id, "vehicle_id": victim, "note": "deadlock break"},
                    at_ms=self.now_ms,
                )

    def _task_of_vehicle(self, vehicle_id: str) -> str:
        for task in self.rt.state.open_tasks():
            if task.assigned_vehicle == vehicle_id:
                return task.task_id
        return ""

    # ------------------------------------------------------------------
    # Movement
    # ------------------------------------------------------------------

    def _trip_for(self, task_id: str, vehicle: Vehicle) -> Trip:
        task = self.rt.state.get(task_id)
        origin_hub = self.world.nearest_hub(task.origin)
        destination_hub = self.world.nearest_hub(task.destination)
        start_hub = self.world.nearest_hub((vehicle.x, vehicle.y))
        legs = self.world.route(start_hub, origin_hub)
        if origin_hub != destination_hub:
            legs = legs + self.world.route(origin_hub, destination_hub)[1:]
        trip = Trip(task_id=task_id, vehicle_id=vehicle.vehicle_id, route=legs)
        self.trips[task_id] = trip
        return trip

    def _move_vehicles(self) -> None:
        active = self.rt.state.of_state(
            TaskState.ASSIGNED,
            TaskState.ENROUTE,
            TaskState.QUEUED,
            TaskState.EXECUTING,
            TaskState.PAUSED,
            TaskState.RECOVERING,
        )
        for task in active:
            vehicle_id = task.assigned_vehicle
            if not vehicle_id:
                continue
            vehicle = self.rt.fleet.vehicles.get(vehicle_id)
            if vehicle is None or not vehicle.online:
                continue
            if vehicle.state in (VehicleState.PAUSED, VehicleState.RECOVERING):
                continue

            trip = self.trips.get(task.task_id)
            if trip is not None and trip.vehicle_id != vehicle_id:
                # The task moved to a different vehicle: the old vehicle's
                # reservations belong to a trip nobody is driving any more.
                self.traffic.release_all(trip.vehicle_id, self.now_ms)
                self.trips.pop(task.task_id, None)
                trip = None
            if trip is None:
                trip = self._trip_for(task.task_id, vehicle)
                self._start_trip(task, trip, vehicle)

            self._advance(task, trip, vehicle)

    def _start_trip(self, task, trip: Trip, vehicle: Vehicle) -> None:
        """Announce the trip and load immediately if we are already at the pick-up.

        DISPATCH is published here rather than when the berth is acquired.  Doing
        it later meant that a vehicle blocked before it got its berth would emit
        ENQUEUE first — leaving the task in QUEUED, where DISPATCH is not a legal
        event, so the task could never leave ASSIGNED.  The FSM correctly rejected
        those events, which is how the ordering bug surfaced at all.
        """
        origin_hub = self.world.nearest_hub(task.origin)
        trip.dispatched = True
        self._publish(
            "task_event",
            task.task_id,
            {"task_id": task.task_id, "event": TaskEvent.DISPATCH, "note": "trip start"},
            at_ms=self.now_ms,
        )
        if trip.route[0] != origin_hub or trip.picked_up:
            return
        trip.picked_up = True
        self._publish(
            "task_event",
            task.task_id,
            {
                "task_id": task.task_id,
                "event": TaskEvent.START,
                "note": f"already at pick-up hub {origin_hub}",
            },
            at_ms=self.now_ms,
        )

    def _advance(self, task, trip: Trip, vehicle: Vehicle) -> None:
        vid = vehicle.vehicle_id
        if trip.waiting and self.tick_index - trip.blocked_since > WAIT_TIMEOUT_TICKS:
            self._abandon_wait(task, trip, vehicle)
            return

        if trip.phase == "travel":
            self._travel(task, trip, vehicle)
            return

        current = trip.current_hub()
        target = trip.target_hub()
        if target == current:
            self._arrive(task, trip, vehicle, current)
            return

        # Parked at the current hub: hold a berth while negotiating the next leg.
        bay = self.world.bay(current)
        if not self._held(vid, bay):
            if not self._request(vid, bay, task.priority):
                self._note_waiting(task, trip, bay, vid)
                return

        # Optional hold-and-wait on the intersection we are standing in.  Off in
        # the baseline; on in the deadlock scenario, where it is the whole point.
        int_current = self.world.intersection(current)
        if self.config.hold_hub_intersection and not self._held(vid, int_current):
            if not self._request(vid, int_current, task.priority):
                self._note_waiting(task, trip, int_current, vid)
                return

        lane = self.world.lane(current, target)
        if not self._held(vid, lane):
            if not self._request(vid, lane, task.priority):
                self._note_waiting(task, trip, lane, vid)
                return

        # Cleared to go.  Hand back the berth (and the intersection if we held it)
        # so the yard behind us keeps moving; holding a resource while driving is
        # what turns a busy terminal into a stopped one.
        #
        # The *destination* intersection is deliberately NOT reserved here.  An
        # earlier version reserved it before setting off, which meant it stayed
        # locked for the entire approach — 12 ticks per leg on a 6-hub graph. The
        # intersection is now taken on arrival and released on the same tick, so it
        # behaves like a crossing rather than a parking space.
        self.traffic.release(vid, bay, self.now_ms)
        if self.config.hold_hub_intersection:
            self.traffic.release(vid, int_current, self.now_ms)
        trip.phase = "travel"
        self._clear_wait(task, trip)
        self._travel(task, trip, vehicle)

    def _travel(self, task, trip: Trip, vehicle: Vehicle) -> None:
        vid = vehicle.vehicle_id
        target = trip.target_hub()
        previous = trip.current_hub()
        vehicle.move_towards(HUBS[target], self.config.tick_ms / 1000.0)
        if not vehicle.at(HUBS[target], tolerance=1.0):
            return
        # At the hub boundary: the crossing needs the intersection, briefly.
        int_target = self.world.intersection(target)
        if not self._held(vid, int_target):
            if not self._request(vid, int_target, task.priority):
                # Queue on the lane we already hold, exactly as a real vehicle
                # would, rather than releasing the aisle and losing our place.
                self._note_waiting(task, trip, int_target, vid)
                return
        self.traffic.release(vid, self.world.lane(previous, target), self.now_ms)
        self.traffic.release(vid, int_target, self.now_ms)
        trip.index += 1
        trip.phase = "negotiate"
        self._clear_wait(task, trip)
        self._arrive(task, trip, vehicle, target)

    def _arrive(self, task, trip: Trip, vehicle: Vehicle, hub: str) -> None:
        origin_hub = self.world.nearest_hub(task.origin)
        destination_hub = self.world.nearest_hub(task.destination)
        if not trip.picked_up and hub == origin_hub:
            trip.picked_up = True
            self._publish(
                "task_event",
                task.task_id,
                {
                    "task_id": task.task_id,
                    "event": TaskEvent.START,
                    "note": f"loaded at hub {hub}",
                },
                at_ms=self.now_ms,
            )
            return
        if trip.picked_up and hub == destination_hub:
            self.traffic.release_all(vehicle.vehicle_id, self.now_ms)
            self.trips.pop(task.task_id, None)
            self._publish(
                "task_event",
                task.task_id,
                {
                    "task_id": task.task_id,
                    "event": TaskEvent.COMPLETE,
                    "note": f"delivered at hub {hub}",
                },
                at_ms=self.now_ms,
            )

    def _note_waiting(self, task, trip: Trip, resource_id: str, vehicle_id: str) -> None:
        """Enter QUEUED once per blocking episode, not once per tick.

        The task really does change state here — a blocked task is not making
        progress and dispatch needs to see that — and ``PROCEED`` returns it to
        the state it was blocked in, which the FSM resolves from its queue stack.
        """
        trip.blocked_on = resource_id
        if trip.waiting:
            return
        trip.waiting = True
        trip.blocked_since = self.tick_index
        self.extra_counters["traffic_blocks"] += 1
        self.rt.metrics.incr("traffic_blocks")
        self._publish(
            "task_event",
            task.task_id,
            {
                "task_id": task.task_id,
                "event": TaskEvent.ENQUEUE,
                "note": f"blocked on {resource_id}",
            },
            at_ms=self.now_ms,
        )

    def _clear_wait(self, task, trip: Trip) -> None:
        """Leave QUEUED, returning to whatever state the block interrupted."""
        if not trip.waiting:
            return
        trip.waiting = False
        blocked_on = trip.blocked_on
        trip.blocked_on = ""
        if task.state == TaskState.QUEUED:
            self._publish(
                "task_event",
                task.task_id,
                {
                    "task_id": task.task_id,
                    "event": TaskEvent.PROCEED,
                    "note": f"{blocked_on} granted",
                },
                at_ms=self.now_ms,
            )

    def _abandon_wait(self, task, trip: Trip, vehicle: Vehicle) -> None:
        """Back off from a blocked leg without throwing away the journey.

        Deadlock breaking only finds *cycles*; a vehicle can also be starved by
        ordinary contention on a resource that is never released, and no cycle
        detector will see that.  Timing out keeps the system live.

        Crucially the back-off gives up the *queue position and the lane*, not the
        load.  Requeueing the whole task reset its progress, so a vehicle that had
        already picked up cargo drove back to the pick-up point to fetch it again
        — wasted trips that showed up as a terrible dispatch-latency tail.
        """
        vid = vehicle.vehicle_id
        keep = self.world.bay(trip.current_hub())
        freed: list[str] = []
        contended = sorted(set(self.traffic.holds(vid)) | set(self.traffic.blocked_on(vid)))
        for resource_id in contended:
            if resource_id == keep:
                continue
            if self.traffic.release_resource(vid, resource_id, self.now_ms):
                freed.append(resource_id)
        self.extra_counters["wait_timeouts"] += 1
        self.rt.metrics.incr("wait_timeouts")
        self.wait_timeout_events.append(
            {
                "tick": self.tick_index,
                "at_ms": self.now_ms,
                "task_id": task.task_id,
                "vehicle_id": vid,
                "blocked_on": trip.blocked_on,
                "freed": freed,
                "picked_up": trip.picked_up,
                "waited_ticks": self.tick_index - trip.blocked_since,
            }
        )
        trip.phase = "negotiate"
        self._clear_wait(task, trip)

    def _recharge(self) -> None:
        """Battery and repair handling — every transition journaled as an event.

        These look like pure physics, but they change ``Vehicle.state``, which is
        part of the consistency fingerprint.  Applying them inline made the live
        state diverge from a replay of the log, which is exactly the bug the
        replay assertion exists to catch.
        """
        for vehicle_id in self.rt.fleet.ids():
            vehicle = self.rt.fleet.vehicles[vehicle_id]
            if not vehicle.online:
                continue
            if vehicle.state == VehicleState.CHARGING:
                vehicle.battery = min(100.0, vehicle.battery + CHARGE_PER_TICK)
                if vehicle.battery >= CHARGE_UNTIL:
                    self._direct(
                        "vehicle_event",
                        vehicle_id,
                        {"vehicle_id": vehicle_id, "event": "CHARGE_DONE"},
                        at_ms=self.now_ms,
                    )
            elif vehicle.state == VehicleState.IDLE and vehicle.battery < CHARGE_BELOW:
                self._direct(
                    "vehicle_event",
                    vehicle_id,
                    {"vehicle_id": vehicle_id, "event": "CHARGE", "note": "low battery"},
                    at_ms=self.now_ms,
                )
            elif vehicle.state == VehicleState.FAULT:
                vehicle.faults += 1
                self._direct(
                    "vehicle_event",
                    vehicle_id,
                    {"vehicle_id": vehicle_id, "event": "REPAIR", "note": "fault repair"},
                    at_ms=self.now_ms,
                )
            elif vehicle.state == VehicleState.MAINTENANCE:
                self._direct(
                    "vehicle_event",
                    vehicle_id,
                    {"vehicle_id": vehicle_id, "event": "REPAIR_DONE"},
                    at_ms=self.now_ms,
                )

    def _enforce_deadlines(self) -> None:
        """Fail tasks that blew their deadline, so the run cannot wait forever."""
        for task in self.rt.state.open_tasks():
            if not task.is_overdue(self.now_ms):
                continue
            self.deadline_failures.append(
                {
                    "tick": self.tick_index,
                    "at_ms": self.now_ms,
                    "task_id": task.task_id,
                    "state": task.state,
                    "deadline_ms": task.deadline_ms,
                    "overdue_ms": self.now_ms - task.deadline_ms,
                }
            )
            self.extra_counters["deadline_failures"] += 1
            self.rt.metrics.incr("deadline_failures")
            if task.assigned_vehicle:
                self.traffic.release_all(task.assigned_vehicle, self.now_ms)
            self.trips.pop(task.task_id, None)
            self._publish(
                "task_event",
                task.task_id,
                {"task_id": task.task_id, "event": TaskEvent.FAIL, "note": "deadline exceeded"},
                at_ms=self.now_ms,
            )

    # ------------------------------------------------------------------
    # Tick loop
    # ------------------------------------------------------------------

    def _silence_due(self) -> list[dict]:
        """Start and end the planned vehicle-silence windows."""
        events: list[dict] = []
        for start_tick, vehicle_id, duration in self.config.offline_plan:
            if self.tick_index == start_tick and vehicle_id not in self._silent:
                self._silent[vehicle_id] = start_tick + duration
                events.append({"phase": "silent", "vehicle_id": vehicle_id, "tick": self.tick_index})
            elif vehicle_id in self._silent and self.tick_index >= self._silent[vehicle_id]:
                del self._silent[vehicle_id]
                events.append({"phase": "resume", "vehicle_id": vehicle_id, "tick": self.tick_index})
        for event in events:
            self.offline_cycles.append(event)
        return events

    def _sample_lag(self) -> int:
        """Record the bus lag and keep the high-water mark.

        Sampled at several points inside the tick, not just at the end.  Backlog
        peaks immediately *after* a burst is published and before the consumer has
        caught up; measuring only at the end of the tick reported a peak of 3 on a
        scenario whose real peak was far higher.
        """
        lag = self.rt.bus.max_lag()
        self.rt.metrics.max_gauge("bus_lag_peak", lag)
        self.rt.metrics.set_gauge("bus_lag", lag)
        self.lag_series.append([self.tick_index, lag, len(self.rt.state.open_tasks())])
        return lag
    def _drain(self) -> None:
        """Drain the bus, honouring the per-tick consume budget.

        Every drain inside the tick loop goes through here.  An earlier version
        budgeted only the first of the three drains, so the other two quietly
        caught up and the backlog metric stayed at zero — the budget looked
        implemented while measuring nothing.
        """
        self.rt.drain(budget=self.config.consume_budget_per_tick)
        self._sample_lag()

    def tick(self) -> None:
        self.tick_index += 1
        self.now_ms = self.tick_index * self.config.tick_ms
        rt = self.rt
        rt.current_ms = self.now_ms
        rt.counters["world_ticks"] = self.tick_index

        # 1. Backpressure recovery: retry the deferred publishes first.
        self._flush_all()
        self._sample_lag()

        # 2. Liveness.
        for event in self._silence_due():
            if event["phase"] == "resume":
                rt.fleet.vehicles[event["vehicle_id"]].last_heartbeat_ms = self.now_ms
                rt.heartbeat(event["vehicle_id"], at_ms=self.now_ms)
        for vehicle_id in rt.fleet.ids():
            if vehicle_id in self._silent:
                continue
            rt.heartbeat(vehicle_id, at_ms=self.now_ms)
        rt.check_timeouts(self.now_ms)

        # 3. Clear deadlocks before anyone tries to move.
        self._handle_deadlocks()

        # 4. New work.
        if (
            self.tick_index % self.config.task_interval_ticks == 0
            and self._task_seq < self.config.tasks
        ):
            self._make_task()
        if self.config.burst_tasks and self.tick_index == self.config.burst_at_tick:
            # A burst is what actually tests the backlog path: a steady trickle is
            # always consumed before the next tick, so lag never leaves zero and
            # the backpressure branch is never exercised.
            for _ in range(self.config.burst_tasks):
                if self._task_seq >= self.config.tasks:
                    break
                self._make_task()
        self._enforce_deadlines()
        # Publish first, then sample: this is where a burst makes the backlog peak.
        self._sample_lag()
        self._drain()

        # 5. Dispatch: pure decisions, then events.
        decisions = self.scheduler.plan(rt.state, now_ms=self.now_ms)
        for decision in decisions:
            rt.metrics.observe("dispatch_latency_ms", decision.dispatch_latency_ms)
            if decision.preempt_task_id:
                self.trips.pop(decision.preempt_task_id, None)
                if decision.preempt_vehicle_id:
                    self.traffic.release_all(decision.preempt_vehicle_id, self.now_ms)
                self._publish(
                    "task_requeued",
                    decision.preempt_task_id,
                    {
                        "task_id": decision.preempt_task_id,
                        "vehicle_id": decision.preempt_vehicle_id,
                        "note": f"preempted by {decision.task_id}",
                    },
                    at_ms=self.now_ms,
                    trace_id=decision.trace_id,
                )
            self._publish(
                "task_assigned",
                decision.task_id,
                {
                    "task_id": decision.task_id,
                    "vehicle_id": decision.vehicle_id,
                    "score": round(decision.score, 4),
                },
                at_ms=self.now_ms,
                trace_id=decision.trace_id,
            )
        self._drain()

        # 6. Traffic + movement.
        self._move_vehicles()
        self._drain()

        # 7. Physics bookkeeping.
        self._recharge()

        # 8. Hot-state projection, then expire it on purpose if scheduled.
        rt.refresh_cache(self.now_ms)
        if self.tick_index in self.config.cache_expire_ticks:
            keys = rt.cache.keys("veh:*") + rt.cache.keys("task:*")
            expired = rt.injector.expire_cache(rt.cache, keys, at_ms=self.now_ms)
            if expired:
                self.extra_counters["cache_expiries"] += len(expired)
                self.cache_expiry_events.append(
                    {
                        "tick": self.tick_index,
                        "at_ms": self.now_ms,
                        "lost": len(expired),
                        "keys_before": len(keys),
                        "keys_after": rt.cache.size(),
                    }
                )
                # The cache lost live state.  Rebuilding it from the state of
                # record is the whole reason the cache is allowed to be lossy.
                rt.rebuild_cache(self.now_ms)

        # 9. Dead-letter redrive.  A real deployment runs this on a timer, and it
        #    cannot be deferred to the end of the run: the events sitting in the
        #    DLQ include task creations, so deferring them means the tasks they
        #    describe do not exist yet — and a run that stops because "everything
        #    is finished" would stop before ever seeing them.
        if rt.dlq_depth():
            rt.injector.clear_poison()
            rt.redrive_dlq()

        # 10. Observability.
        lag = self._sample_lag()
        rt.metrics.set_gauge("open_tasks", len(rt.state.open_tasks()))
        rt.alerts.check_backlog(lag, at_ms=self.now_ms)
        rt.alerts.check_dlq(rt.dlq_depth(), at_ms=self.now_ms)
        for task in rt.state.open_tasks():
            rt.alerts.check_task_timeout(
                task.task_id, self.now_ms - task.created_ms, at_ms=self.now_ms
            )

        # 11. Faults that must happen mid-run.
        rt.maybe_crash(self.now_ms)

    def done(self) -> bool:
        if self._task_seq < self.config.tasks or self._pending:
            return False
        return not self.rt.state.open_tasks()

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> "Simulation":
        for _ in range(self.config.max_ticks):
            self.tick()
            if self.done():
                break
        self.finish()
        # The final redrive can surface work that was never applied — brand new
        # tasks that did not exist when the loop decided it was finished.  Give
        # the yard a second, bounded window rather than reporting them as
        # "stopped, everything terminal" while they sit in PENDING.
        for _ in range(self.config.max_ticks):
            if self.done():
                break
            self.tick()
        self.finish()
        return self

    def finish(self) -> None:
        """Drain everything, recover the DLQ, and reconcile the final state."""
        self._flush_all()
        self.rt.drain()
        if self.rt.dlq_depth():
            self.rt.injector.clear_poison()
            self.rt.redrive_dlq()
        self._flush_all()
        self.rt.drain()
        self.rt.refresh_cache(self.now_ms)
        self.rt.metrics.set_gauge("bus_lag", self.rt.bus.max_lag())
        self.rt.metrics.set_gauge("dlq_depth", self.rt.dlq_depth())

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def _task_timing(self) -> dict:
        tasks = list(self.rt.state.tasks.values())
        return {
            "dispatch_latency_ms": distribution(
                [t.dispatch_latency_ms() for t in tasks if t.dispatch_latency_ms() is not None]
            ),
            "cycle_time_ms": distribution(
                [t.cycle_time_ms() for t in tasks if t.cycle_time_ms() is not None]
            ),
            "service_time_ms": distribution(
                [t.service_time_ms() for t in tasks if t.service_time_ms() is not None]
            ),
        }

    def _transition_stats(self) -> dict:
        by_event: dict[str, int] = {}
        by_state: dict[str, int] = {}
        for task_id in self.rt.state.ids():
            for record in self.rt.state.tasks[task_id].fsm.history:
                by_event[record.event] = by_event.get(record.event, 0) + 1
                by_state[record.to_state] = by_state.get(record.to_state, 0) + 1
        return {
            "total_transitions": sum(by_event.values()),
            "by_event": {k: by_event[k] for k in sorted(by_event)},
            "by_target_state": {k: by_state[k] for k in sorted(by_state)},
            "rejected": len(self.rt.state.rejected_events),
            "rejected_reasons": _rejection_summary(self.rt.state.rejected_events),
        }

    def report(self) -> dict:
        """The fully deterministic metrics document.

        Nothing here may depend on wall-clock time.  Runtime duration is written
        separately (see ``timing.json``) precisely so this file can be compared
        byte-for-byte between two runs.
        """
        rt = self.rt
        state = rt.state
        fleet = rt.fleet
        consistency = rt.verify_replay()
        cache_snapshot = rt.cache.snapshot(at_ms=self.now_ms)
        tasks_total = len(state.tasks)
        completed = len(state.completed())
        bus_stats = rt.bus.stats()

        return {
            "meta": {
                "project": "fleet-dispatch-lab",
                "scenario": self.config.name,
                "synthetic_data": True,
                "synthetic_notice": (
                    "All vehicle positions, tasks, priorities and timings are generated by "
                    "a seeded PRNG. This is not data from any real port or fleet."
                ),
                "clock": "virtual (tick_ms x ticks); no wall-clock value is used in metrics",
                "tick_ms": self.config.tick_ms,
                "ticks": self.tick_index,
                "stopped_because": "all tasks terminal" if self.done() else "max_ticks reached",
            },
            "config": self.config.as_dict(),
            "scale": {
                "vehicles": len(fleet.vehicles),
                "tasks_created": tasks_total,
                "hubs": len(HUBS),
                "edges": len(EDGES),
                "resources": len(self.traffic.resources),
                "partitions": self.config.num_partitions,
            },
            "tasks": {
                "created": tasks_total,
                "completed": completed,
                "failed": state.counters["tasks_failed"],
                "cancelled": state.counters["tasks_cancelled"],
                "open": len(state.open_tasks()),
                "completion_rate": round(state.completion_rate(), 4),
                "completion_rate_definition": (
                    "completed / created; failed and cancelled count against it"
                ),
                "state_counts": state.state_counts(),
                "timing": self._task_timing(),
            },
            "dispatch": {
                "decisions": len(self.scheduler.decisions),
                "assignments": self.scheduler.counters["assigned"],
                "preemptions": self.scheduler.counters["preemptions"],
                "preemption_rate": round(self.scheduler.preemption_rate(), 4),
                "candidates_considered": sum(
                    d.candidates_considered for d in self.scheduler.decisions
                ),
                "counters": dict(sorted(self.scheduler.counters.items())),
            },
            "state_machine": self._transition_stats(),
            "bus": {
                "published": bus_stats["counters"]["published"],
                "backpressure_rejections": bus_stats["counters"]["backpressure_rejections"],
                "topics": bus_stats["topics"],
                "dlq_topics": bus_stats["dlq_topics"],
                "max_lag": bus_stats["max_lag"],
                "total_lag": bus_stats["total_lag"],
                "lag_peak": rt.metrics.gauge("bus_lag_peak"),
                "consumer": rt._raw_group.as_dict(),
                "partitions_used": _partitions_used(rt),
            },
            "consumer": {
                "processed": rt.metrics.counter("messages_consumed"),
                "duplicates_suppressed": rt.metrics.counter("duplicates_suppressed"),
                "out_of_order_detected": rt.metrics.counter("out_of_order_detected"),
                "order_gap_forced": rt.metrics.counter("order_gap_forced"),
                "gap_recoveries": rt.metrics.counter("gap_recoveries"),
                "nacked": rt.metrics.counter("consumer_nacked"),
                "dlq_routed": rt.metrics.counter("dlq_routed"),
                "crashes": rt.metrics.counter("consumer_crashes"),
                "dlq_redrives": rt.counters["dlq_redrives"],
                "dlq_remaining": rt.dlq_depth(),
            },
            "idempotency": {
                "message_dedup": rt.dedup.stats(),
                "commands": rt.commands.stats(),
                "commands_duplicate": rt.counters["commands_duplicate"],
                "commands_unconfirmed": rt.counters["commands_unconfirmed"],
            },
            "traffic": {
                **self.traffic.stats(),
                "deadlock_events": len(self.deadlock_events),
                "wait_timeout_events": len(self.wait_timeout_events),
                "road_network": {
                    "hubs": sorted(HUBS),
                    "edges": [f"{a}-{b}" for a, b in EDGES],
                },
            },
            "fleet": {
                **fleet.stats(),
                "offline_cycles": self.offline_cycles,
                "cache_expiry_events": self.cache_expiry_events,
            },
            "cache": {
                "keys": rt.cache.size(),
                "snapshot_entries": len(cache_snapshot["entries"]),
                "expired_observed": len(rt.cache.expiries),
                "keys_injected_expired": self.extra_counters["cache_expiries"],
                "keys_lost_by_fault": sum(e["lost"] for e in self.cache_expiry_events),
                "keys_after_rebuild": rt.cache.size(),
                "rebuilds": rt.metrics.counter("cache_rebuilds"),
            },
            "faults": {
                "config": rt.injector.config.as_dict(),
                "fired": rt.injector.fired(),
                "deferred_publishes": self.extra_counters["backpressure_deferrals"],
            },
            "recovery": {
                "deadlock_breaks": len(self.deadlock_events),
                "wait_timeouts": len(self.wait_timeout_events),
                "deadline_failures": len(self.deadline_failures),
                "cache_expiries": self.extra_counters["cache_expiries"],
                "consumer_crashes": rt.metrics.counter("consumer_crashes"),
                "dlq_redrives": rt.counters["dlq_redrives"],
            },
            "consistency": {
                "criterion": consistency["criterion"],
                "records_replayed": consistency["records_replayed"],
                "live_fingerprint": consistency["live_fingerprint"],
                "replay_fingerprint": consistency["replay_fingerprint"],
                "consistent": consistency["consistent"],
                "journal_digest": consistency["journal_digest"],
                "journal_head": consistency["journal_head"],
            },
            "observability": {
                "journal": rt.journal.stats(),
                "log_records": len(rt.logger),
                "log_events": rt.logger.counts_by_event(),
                "traces": rt.tracer.stats(),
                "alerts": {**rt.alerts.stats(), "list": rt.alerts.as_list()},
                "metrics": rt.metrics_snapshot(),
            },
            "series": {
                "lag": self.lag_series,
                "note": "each row is [tick, bus_lag, open_tasks]",
            },
            "timeline": [
                {
                    "task_id": task.task_id,
                    "priority": task.priority,
                    "state": task.state,
                    "vehicle": task.assigned_vehicle,
                    "created_ms": task.created_ms,
                    "assigned_ms": task.assigned_ms,
                    "started_ms": task.started_ms,
                    "finished_ms": task.finished_ms,
                    "deadline_ms": task.deadline_ms,
                    "preempted": task.preempted,
                }
                for task in (state.tasks[t] for t in state.ids())
            ],
            "runtime_counters": dict(sorted(rt.counters.items())),
        }


# ----------------------------------------------------------------------
# Helpers (free functions so they are trivially unit-testable)
# ----------------------------------------------------------------------


def distribution(values: list[int]) -> dict:
    """Summary of a set of observed values, with real (nearest-rank) percentiles."""
    if not values:
        return {
            "count": 0,
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
            "values": [],
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "mean": round(sum(ordered) / len(ordered), 3),
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": ordered[-1],
        "values": ordered,
    }


def _rejection_summary(rejections: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for record in rejections:
        key = f"{record['state']}<-{record['event']}"
        counts[key] = counts.get(key, 0) + 1
    return {k: counts[k] for k in sorted(counts)}


def _partitions_used(runtime: Runtime) -> dict[str, int]:
    out: dict[str, int] = {}
    for name in sorted(runtime.bus.topics):
        topic = runtime.bus.topics[name]
        out[name] = sum(1 for p in topic.partitions if p.next_offset > 0)
    return out

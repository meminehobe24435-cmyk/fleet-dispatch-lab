"""Traffic control: mutual exclusion at intersections and lane segments.

A port is a shared resource graph.  Two AGVs that both enter the same
intersection do not merely collide — they deadlock, block the aisle behind them
and stop the whole quay.  So "traffic control" here means three things, all of
which are testable without any physics:

1. **Mutual exclusion** — a resource with ``capacity`` slots hands out at most
    that many grants; the default of 1 makes an intersection a mutex.
2. **Give-way decision** — when several vehicles contend, the controller decides
   who goes first and records *why*.  The rule is total and deterministic
   (priority desc, arrival asc, vehicle id asc) so the decision never depends on
   dictionary iteration order.
3. **Deadlock detection and breaking** — a wait-for cycle is detected on the
   graph and broken by revoking the reservation of the least valuable vehicle in
   the cycle, which is how a real yard recovers without human intervention.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = [
    "Resource",
    "WaitingRequest",
    "Reservation",
    "ConflictDecision",
    "TrafficController",
]


@dataclass
class WaitingRequest:
    """A vehicle queued for a resource."""

    vehicle_id: str
    priority: int
    requested_ms: int
    sequence: int


@dataclass
class Reservation:
    """A granted hold on a resource."""

    vehicle_id: str
    resource_id: str
    granted_ms: int
    priority: int


@dataclass
class Resource:
    """An intersection (capacity 1) or a lane segment."""

    resource_id: str
    kind: str = "intersection"
    capacity: int = 1
    holders: dict[str, Reservation] = field(default_factory=dict)
    queue: list[WaitingRequest] = field(default_factory=list)

    @property
    def free_slots(self) -> int:
        return max(0, self.capacity - len(self.holders))

    @property
    def contended(self) -> bool:
        return bool(self.queue)

    def as_dict(self) -> dict:
        return {
            "resource_id": self.resource_id,
            "kind": self.kind,
            "capacity": self.capacity,
            "holders": sorted(self.holders),
            "queue": [w.vehicle_id for w in self.queue],
        }


@dataclass
class ConflictDecision:
    """Who goes first, and under which rule."""

    resource_id: str
    at_ms: int
    winner: str
    losers: list[str]
    rule: str
    waiting: list[str]

    def as_dict(self) -> dict:
        return {
            "resource_id": self.resource_id,
            "at_ms": self.at_ms,
            "winner": self.winner,
            "losers": list(self.losers),
            "rule": self.rule,
            "waiting": list(self.waiting),
        }


class TrafficController:
    """Owns the resource graph and arbitrates access to it."""

    def __init__(self) -> None:
        self.resources: dict[str, Resource] = {}
        self.decisions: list[ConflictDecision] = []
        self.deadlock_breaks: list[dict] = []
        self._sequence = 0
        #: High-water mark of queue depth.  A depth read at the end of a run is
        #: always zero, because by then everything has drained — reporting that
        #: as "max queue depth" would be measuring the wrong moment.
        self.max_queue_depth_seen = 0
        self.max_blocked_seen = 0
        self.counters = {
            "requests": 0,
            "grants": 0,
            "blocks": 0,
            "releases": 0,
            "conflicts": 0,
            "deadlocks_detected": 0,
            "deadlocks_broken": 0,
        }

    # -- graph ------------------------------------------------------------

    def add_resource(
        self, resource_id: str, kind: str = "intersection", capacity: int = 1
    ) -> Resource:
        resource = Resource(resource_id, kind, capacity)
        self.resources[resource_id] = resource
        return resource

    def get(self, resource_id: str) -> Resource:
        try:
            return self.resources[resource_id]
        except KeyError:
            raise KeyError(f"unknown resource {resource_id!r}") from None

    def ids(self) -> list[str]:
        return sorted(self.resources)

    # -- arbitration ------------------------------------------------------

    @staticmethod
    def _rank(request: WaitingRequest) -> tuple:
        """Total order for give-way: higher priority, then earlier arrival, then id."""
        return (-request.priority, request.requested_ms, request.sequence, request.vehicle_id)

    def request(
        self,
        vehicle_id: str,
        resource_id: str,
        *,
        priority: int = 2,
        at_ms: int = 0,
        trace_id: str = "",
    ) -> bool:
        """Queue for a resource and try to grant it immediately.

        Returns ``True`` when the caller may enter.  A blocked request *stays
        queued*, so a later :meth:`release` promotes it; a blocked vehicle is
        never silently forgotten.
        """
        resource = self.get(resource_id)
        self.counters["requests"] += 1
        if vehicle_id in resource.holders:
            return True  # re-entrant request: already inside, not a second grant

        existing = next((w for w in resource.queue if w.vehicle_id == vehicle_id), None)
        if existing is None:
            self._sequence += 1
            resource.queue.append(WaitingRequest(vehicle_id, priority, at_ms, self._sequence))
        else:
            existing.priority = priority

        self._try_grant(resource, at_ms=at_ms)
        if vehicle_id in resource.holders:
            return True

        self.counters["blocks"] += 1
        self._observe_pressure()
        self._record_decision(resource, at_ms)
        return False

    def _observe_pressure(self) -> None:
        """Update the congestion high-water marks after a block."""
        depth = max((len(r.queue) for r in self.resources.values()), default=0)
        blocked = sum(len(r.queue) for r in self.resources.values())
        self.max_queue_depth_seen = max(self.max_queue_depth_seen, depth)
        self.max_blocked_seen = max(self.max_blocked_seen, blocked)

    def _try_grant(self, resource: Resource, *, at_ms: int) -> bool:
        """Promote queued requests in give-way order while slots remain."""
        any_grant = False
        resource.queue.sort(key=self._rank)
        while resource.free_slots > 0 and resource.queue:
            candidate = resource.queue.pop(0)
            resource.holders[candidate.vehicle_id] = Reservation(
                candidate.vehicle_id, resource.resource_id, at_ms, candidate.priority
            )
            self.counters["grants"] += 1
            any_grant = True
        return any_grant

    def _record_decision(self, resource: Resource, at_ms: int) -> None:
        """Record the give-way verdict for observability."""
        waiting = [w.vehicle_id for w in sorted(resource.queue, key=self._rank)]
        holders = sorted(resource.holders)
        if not waiting or not holders:
            return
        self.counters["conflicts"] += 1
        self.decisions.append(
            ConflictDecision(
                resource_id=resource.resource_id,
                at_ms=at_ms,
                winner=holders[0],
                losers=waiting,
                rule="holder in progress; queue ordered by priority desc, arrival asc, id asc",
                waiting=waiting,
            )
        )

    def release(self, vehicle_id: str, resource_id: str, at_ms: int = 0) -> bool:
        """Release a hold and promote the next waiter."""
        resource = self.get(resource_id)
        if vehicle_id not in resource.holders:
            return False
        del resource.holders[vehicle_id]
        self.counters["releases"] += 1
        self._try_grant(resource, at_ms=at_ms)
        return True

    def release_all(self, vehicle_id: str, at_ms: int = 0) -> list[str]:
        """Release every hold and drop every queued request for a vehicle.

        Used when a vehicle is preempted or goes offline: a disappeared vehicle
        must not keep a mutex, or the yard deadlocks behind a vehicle that is not
        there any more.
        """
        freed: list[str] = []
        for resource_id in self.ids():
            if vehicle_id in self.resources[resource_id].holders:
                self.release(vehicle_id, resource_id, at_ms)
                freed.append(resource_id)
        for resource_id in self.ids():
            resource = self.resources[resource_id]
            before = len(resource.queue)
            resource.queue = [w for w in resource.queue if w.vehicle_id != vehicle_id]
            if len(resource.queue) != before:
                freed.append(f"queue:{resource_id}")
        return sorted(freed)

    def holds(self, vehicle_id: str) -> list[str]:
        return sorted(r for r in self.ids() if vehicle_id in self.resources[r].holders)

    def release_resource(self, vehicle_id: str, resource_id: str, at_ms: int = 0) -> bool:
        """Give up one resource: drop the hold *and* any queued request for it.

        Needed for back-off.  Releasing the hold alone leaves the vehicle in the
        queue, so it would be promoted straight back into the resource it just
        decided to stop waiting for.
        """
        resource = self.get(resource_id)
        acted = False
        if vehicle_id in resource.holders:
            del resource.holders[vehicle_id]
            self.counters["releases"] += 1
            acted = True
        before = len(resource.queue)
        resource.queue = [w for w in resource.queue if w.vehicle_id != vehicle_id]
        if len(resource.queue) != before:
            acted = True
        if acted:
            self._try_grant(resource, at_ms=at_ms)
        return acted

    def blocked_on(self, vehicle_id: str) -> list[str]:
        return sorted(
            r
            for r in self.ids()
            if any(w.vehicle_id == vehicle_id for w in self.resources[r].queue)
        )

    def free_resources(self) -> list[str]:
        return sorted(r for r in self.ids() if self.resources[r].free_slots > 0)

    # -- deadlock ---------------------------------------------------------

    def wait_for_graph(self) -> dict[str, list[str]]:
        """``vehicle -> vehicles it is blocked by`` (sorted, for determinism)."""
        graph: dict[str, set[str]] = {}
        for resource_id in self.ids():
            resource = self.resources[resource_id]
            if resource.free_slots > 0:
                continue
            blockers = sorted(resource.holders)
            for waiter in sorted(resource.queue, key=self._rank):
                if waiter.vehicle_id in resource.holders:
                    continue
                graph.setdefault(waiter.vehicle_id, set()).update(blockers)
        return {k: sorted(v) for k, v in sorted(graph.items())}

    def detect_deadlock(self) -> list[list[str]]:
        """Return the cycles in the wait-for graph, each canonicalised.

        Iterative depth-first search with an explicit stack: a recursive version
        overflows on the long dependency chains a real yard produces.
        """
        graph = self.wait_for_graph()
        cycles: set[tuple[str, ...]] = set()

        def canonical(cycle: list[str]) -> tuple[str, ...]:
            body = cycle[:-1] if len(cycle) > 1 and cycle[0] == cycle[-1] else cycle
            if not body:
                return ()
            pivot = min(range(len(body)), key=lambda i: body[i])
            return tuple(body[pivot:] + body[:pivot])

        for start in sorted(graph):
            stack: list[tuple[str, list[str], frozenset[str]]] = [
                (start, [start], frozenset({start}))
            ]
            while stack:
                node, path, on_path = stack.pop()
                for neighbour in graph.get(node, []):
                    if neighbour in on_path:
                        key = canonical(path[path.index(neighbour):] + [neighbour])
                        if len(key) > 1:
                            cycles.add(key)
                        continue
                    stack.append((neighbour, path + [neighbour], on_path | {neighbour}))
        return [list(c) for c in sorted(cycles)]

    def _priority_of(self, vehicle_id: str) -> int:
        best = 0
        for resource_id in self.holds(vehicle_id):
            best = max(best, self.resources[resource_id].holders[vehicle_id].priority)
        for resource_id in self.blocked_on(vehicle_id):
            for waiter in self.resources[resource_id].queue:
                if waiter.vehicle_id == vehicle_id:
                    best = max(best, waiter.priority)
        return best

    def break_deadlock(self, at_ms: int = 0) -> dict | None:
        """Break one deadlock cycle by revoking its least valuable member.

        Selection rule: lowest priority in the cycle, then highest vehicle id — a
        stable, if arbitrary, tie-break.  The victim loses its reservations *and*
        its queued requests, which is a real cost, so every break is recorded
        rather than logged and forgotten.
        """
        cycles = self.detect_deadlock()
        if not cycles:
            return None
        self.counters["deadlocks_detected"] += 1
        cycle = cycles[0]
        priorities = {v: self._priority_of(v) for v in cycle}
        victim = min(cycle, key=lambda v: (priorities[v], v))
        freed = self.release_all(victim, at_ms)
        record = {
            "at_ms": at_ms,
            "cycle": list(cycle),
            "victim": victim,
            "victim_priority": priorities[victim],
            "freed": freed,
            "rule": "lowest priority in cycle, then highest vehicle id",
        }
        self.deadlock_breaks.append(record)
        self.counters["deadlocks_broken"] += 1
        return record

    # -- reporting --------------------------------------------------------

    def snapshot(self) -> dict:
        return {r: self.resources[r].as_dict() for r in self.ids()}

    def stats(self) -> dict:
        contended = [r for r in self.ids() if self.resources[r].contended]
        return {
            "resources": len(self.resources),
            "contended_resources": len(contended),
            "max_queue_depth": self.max_queue_depth_seen,
            "max_blocked_vehicles": self.max_blocked_seen,
            "conflicts": len(self.decisions),
            "deadlock_breaks": len(self.deadlock_breaks),
            "counters": dict(sorted(self.counters.items())),
        }

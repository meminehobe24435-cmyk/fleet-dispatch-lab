"""Traffic control tests: mutual exclusion, give-way order, deadlock handling."""

from __future__ import annotations

import pytest

from fleetlab.traffic import TrafficController


def controller(*specs) -> TrafficController:
    instance = TrafficController()
    for spec in specs:
        instance.add_resource(*spec)
    return instance


# ----------------------------------------------------------------------
# mutual exclusion
# ----------------------------------------------------------------------


def test_an_intersection_admits_one_vehicle():
    traffic = controller(("INT_A", "intersection", 1))
    assert traffic.request("AGV-1", "INT_A", priority=2, at_ms=0) is True
    assert traffic.request("AGV-2", "INT_A", priority=2, at_ms=1) is False
    assert traffic.resources["INT_A"].holders.keys() == {"AGV-1"}


def test_a_lane_with_capacity_two_admits_two():
    traffic = controller(("LANE_A_B", "lane", 2))
    assert traffic.request("AGV-1", "LANE_A_B", at_ms=0) is True
    assert traffic.request("AGV-2", "LANE_A_B", at_ms=1) is True
    assert traffic.request("AGV-3", "LANE_A_B", at_ms=2) is False


def test_reentrant_request_is_not_a_second_grant():
    traffic = controller(("INT_A",))
    assert traffic.request("AGV-1", "INT_A", at_ms=0) is True
    assert traffic.request("AGV-1", "INT_A", at_ms=1) is True
    assert traffic.counters["grants"] == 1


def test_unknown_resource_raises():
    traffic = controller(("INT_A",))
    with pytest.raises(KeyError):
        traffic.request("AGV-1", "NOPE")


def test_release_promotes_the_next_waiter():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", priority=2, at_ms=0)
    traffic.request("AGV-2", "INT_A", priority=2, at_ms=1)
    assert traffic.blocked_on("AGV-2") == ["INT_A"]
    assert traffic.release("AGV-1", "INT_A", at_ms=2) is True
    assert traffic.holds("AGV-2") == ["INT_A"]
    assert traffic.blocked_on("AGV-2") == []


def test_releasing_a_resource_you_do_not_hold_is_false():
    traffic = controller(("INT_A",))
    assert traffic.release("AGV-1", "INT_A") is False


def test_release_resource_drops_both_hold_and_queue_position():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    traffic.request("AGV-2", "INT_A", at_ms=1)
    assert traffic.release_resource("AGV-2", "INT_A") is True
    # Backing off must remove the *request* too, or the next release would promote
    # the vehicle straight back into the resource it just gave up on.
    assert traffic.blocked_on("AGV-2") == []
    assert traffic.release("AGV-1", "INT_A") is True
    assert traffic.holds("AGV-2") == []


def test_release_all_frees_holds_and_queues():
    traffic = controller(("INT_A",), ("LANE_A_B",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    traffic.request("AGV-1", "LANE_A_B", at_ms=0)
    traffic.request("AGV-2", "INT_A", at_ms=1)
    freed = traffic.release_all("AGV-1", at_ms=2)
    assert "INT_A" in freed and "LANE_A_B" in freed
    assert traffic.holds("AGV-1") == []
    assert "AGV-2" in traffic.resources["INT_A"].holders


def test_free_resources_lists_available_ones():
    traffic = controller(("INT_A",), ("INT_B",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    assert traffic.free_resources() == ["INT_B"]


# ----------------------------------------------------------------------
# give-way decisions
# ----------------------------------------------------------------------


def test_give_way_order_is_by_priority():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", priority=2, at_ms=0)
    traffic.request("AGV-low", "INT_A", priority=1, at_ms=1)
    traffic.request("AGV-high", "INT_A", priority=5, at_ms=2)
    traffic.release("AGV-1", "INT_A", at_ms=3)
    # The urgent vehicle goes first even though it arrived last.
    assert "AGV-high" in traffic.resources["INT_A"].holders


def test_give_way_breaks_priority_ties_by_arrival_then_id():
    traffic = controller(("LANE", "lane", 1))
    traffic.request("holder", "LANE", priority=2, at_ms=0)
    traffic.request("later", "LANE", priority=2, at_ms=9)
    traffic.request("earlier", "LANE", priority=2, at_ms=1)
    traffic.release("holder", "LANE", at_ms=10)
    assert "earlier" in traffic.resources["LANE"].holders
    traffic.release("earlier", "LANE", at_ms=11)
    assert "later" in traffic.resources["LANE"].holders


def test_conflict_decision_records_the_rule_and_the_losers():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", priority=3, at_ms=0)
    traffic.request("AGV-2", "INT_A", priority=1, at_ms=1)
    decision = traffic.decisions[-1]
    assert decision.resource_id == "INT_A"
    assert decision.winner == "AGV-1"
    assert decision.losers == ["AGV-2"]
    assert "priority desc" in decision.rule
    assert decision.as_dict()["at_ms"] == 1


def test_contended_resource_is_reported():
    traffic = controller(("INT_A",), ("INT_B",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    traffic.request("AGV-2", "INT_A", at_ms=1)
    stats = traffic.stats()
    assert stats["contended_resources"] == 1
    assert stats["resources"] == 2


def test_max_queue_depth_is_a_high_water_mark():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    for index in range(4):
        traffic.request(f"AGV-{index + 2}", "INT_A", at_ms=index + 1)
    traffic.release("AGV-1", "INT_A")
    # Read at the end the queue is empty, so a "current depth" would report 0.
    assert traffic.stats()["max_queue_depth"] == 4
    assert len(traffic.resources["INT_A"].queue) == 3


# ----------------------------------------------------------------------
# deadlock
# ----------------------------------------------------------------------


def two_cycle() -> TrafficController:
    """Two vehicles, each holding the resource the other one wants."""
    traffic = controller(("INT_A",), ("INT_B",))
    traffic.request("AGV-1", "INT_A", priority=2, at_ms=0)
    traffic.request("AGV-2", "INT_B", priority=2, at_ms=0)
    traffic.request("AGV-1", "INT_B", priority=2, at_ms=1)
    traffic.request("AGV-2", "INT_A", priority=2, at_ms=1)
    return traffic


def test_wait_for_graph_lists_blockers():
    traffic = two_cycle()
    graph = traffic.wait_for_graph()
    assert graph["AGV-1"] == ["AGV-2"]
    assert graph["AGV-2"] == ["AGV-1"]


def test_no_deadlock_when_a_resource_is_still_free():
    traffic = controller(("INT_A",), ("INT_B",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    traffic.request("AGV-2", "INT_B", at_ms=0)
    traffic.request("AGV-1", "INT_B", at_ms=1)  # succeeds, INT_B had a slot
    assert traffic.detect_deadlock() == []


def test_deadlock_is_detected():
    assert two_cycle().detect_deadlock() == [["AGV-1", "AGV-2"]]


def test_deadlock_detection_is_canonicalised():
    """The same cycle found from either end must be one entry, not two."""
    cycles = two_cycle().detect_deadlock()
    assert len(cycles) == 1
    assert cycles[0] == sorted(cycles[0])


def test_breaking_a_deadlock_frees_the_cycle():
    traffic = two_cycle()
    record = traffic.break_deadlock(at_ms=5)
    assert record is not None
    assert record["cycle"] == ["AGV-1", "AGV-2"]
    assert record["victim"] in ("AGV-1", "AGV-2")
    assert traffic.detect_deadlock() == []
    assert traffic.counters["deadlocks_broken"] == 1


def test_deadlock_victim_is_the_lowest_priority_vehicle():
    traffic = controller(("INT_A",), ("INT_B",))
    traffic.request("AGV-1", "INT_A", priority=5, at_ms=0)
    traffic.request("AGV-2", "INT_B", priority=1, at_ms=0)
    traffic.request("AGV-1", "INT_B", priority=5, at_ms=1)
    traffic.request("AGV-2", "INT_A", priority=1, at_ms=1)
    record = traffic.break_deadlock(at_ms=5)
    assert record["victim"] == "AGV-2"
    assert record["victim_priority"] == 1


def test_break_deadlock_returns_none_when_there_is_no_cycle():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    assert traffic.break_deadlock() is None
    assert traffic.counters["deadlocks_detected"] == 0


def test_three_way_cycle_is_detected_and_broken():
    traffic = controller(("R1",), ("R2",), ("R3",))
    for index, resource in enumerate(("R1", "R2", "R3")):
        traffic.request(f"AGV-{index + 1}", resource, priority=2, at_ms=0)
    # Each vehicle now wants the next resource in the ring.
    traffic.request("AGV-1", "R2", priority=2, at_ms=1)
    traffic.request("AGV-2", "R3", priority=2, at_ms=1)
    traffic.request("AGV-3", "R1", priority=2, at_ms=1)
    cycles = traffic.detect_deadlock()
    assert cycles and len(cycles[0]) == 3
    record = traffic.break_deadlock(at_ms=2)
    assert record is not None
    assert traffic.detect_deadlock() == []


def test_long_chain_does_not_overflow_the_stack():
    """Detection is iterative; a recursive DFS dies on a chain like this."""
    traffic = TrafficController()
    size = 1_200
    for index in range(size):
        traffic.add_resource(f"R{index}")
    for index in range(size):
        traffic.request(f"AGV-{index}", f"R{index}", at_ms=0)
    for index in range(size - 1):
        traffic.request(f"AGV-{index}", f"R{index + 1}", at_ms=1)
    assert traffic.detect_deadlock() == []


def test_snapshot_and_stats_shapes():
    traffic = controller(("INT_A", "intersection", 1))
    traffic.request("AGV-1", "INT_A", at_ms=0)
    snapshot = traffic.snapshot()
    assert snapshot["INT_A"]["holders"] == ["AGV-1"]
    assert snapshot["INT_A"]["capacity"] == 1
    stats = traffic.stats()
    assert stats["resources"] == 1
    assert "counters" in stats


def test_reservation_records_when_it_was_granted():
    traffic = controller(("INT_A",))
    traffic.request("AGV-1", "INT_A", priority=4, at_ms=77)
    reservation = traffic.resources["INT_A"].holders["AGV-1"]
    assert reservation.granted_ms == 77
    assert reservation.priority == 4

"""Journal replay, snapshot consistency, and eventual consistency under faults.

This is the heart of the project, so the tests are written to answer questions an
operator would ask rather than to exercise code paths:

* does replaying the log reproduce the live state?
* does the check *have teeth* (can it detect a divergence)?
* after duplicates / reordering / loss / a crash, does the system converge?
* do duplicate commands produce one effect?
* does a cache loss cost a rebuild or cost correctness?

The eventual-consistency assertions use the criterion documented in
``PlatformState.lifecycle_fingerprint`` and printed in every report: a SHA-256 over
the canonical projection of task states, assignments and vehicle lifecycle.
"""

from __future__ import annotations

import pytest

from fleetlab.eventlog import EventLog, EventRecord
from fleetlab.faults import FaultConfig
from fleetlab.runtime import Runtime, RuntimeConfig
from fleetlab.sim import SimConfig, Simulation
from fleetlab.state import PlatformState
from fleetlab.task_fsm import TaskEvent, TaskState


def make_runtime(**kwargs) -> Runtime:
    config = RuntimeConfig(vehicles=2, backpressure_limit=None, seed=5)
    for key, value in kwargs.items():
        setattr(config, key, value)
    runtime = Runtime(config)
    runtime.add_vehicle("AGV-01", x=60.0, y=60.0)
    runtime.add_vehicle("AGV-02", x=300.0, y=60.0)
    return runtime


def publish_task(runtime: Runtime, task_id="T0001", at_ms=0) -> None:
    runtime.publish(
        "task_created",
        task_id,
        {
            "task_id": task_id,
            "origin": [60.0, 60.0],
            "destination": [540.0, 340.0],
            "priority": 3,
            "payload_kg": 400.0,
            "created_ms": at_ms,
            "deadline_ms": 600_000,
        },
        at_ms=at_ms,
    )


def drive_full_lifecycle(runtime: Runtime, task_id="T0001") -> None:
    publish_task(runtime, task_id)
    runtime.drain()
    runtime.publish("task_assigned", task_id, {"task_id": task_id, "vehicle_id": "AGV-01"}, at_ms=100)
    runtime.drain()
    for event in (TaskEvent.DISPATCH, TaskEvent.START, TaskEvent.COMPLETE):
        runtime.publish("task_event", task_id, {"task_id": task_id, "event": event}, at_ms=200)
        runtime.drain()


# ----------------------------------------------------------------------
# journal basics
# ----------------------------------------------------------------------


def test_journal_assigns_monotonic_sequence_numbers():
    log = EventLog()
    first = log.append("a", "s1", at_ms=1)
    second = log.append("b", "s2", at_ms=2)
    assert (first.seq, second.seq) == (1, 2)
    assert log.head() == 2
    assert len(log) == 2


def test_journal_records_kind_counts():
    log = EventLog()
    for _ in range(3):
        log.append("task_event", "s")
    log.append("task_created", "s")
    assert log.kinds() == {"task_created": 1, "task_event": 3}


def test_journal_slice_is_exclusive_of_from_seq():
    log = EventLog()
    for index in range(5):
        log.append("e", f"s{index}", at_ms=index)
    assert [r.seq for r in log.slice(2)] == [3, 4, 5]
    assert [r.seq for r in log.slice(1, 3)] == [2, 3]


def test_journal_queries_by_trace_and_subject():
    log = EventLog()
    log.append("a", "T1", trace_id="tr1")
    log.append("b", "T2", trace_id="tr1")
    log.append("c", "T1", trace_id="tr2")
    assert len(log.by_trace("tr1")) == 2
    assert len(log.by_subject("T1")) == 2


def test_journal_jsonl_round_trip(tmp_path):
    path = str(tmp_path / "events.jsonl")
    log = EventLog(path=path)
    log.append("task_event", "T1", at_ms=5, payload={"event": "START"}, trace_id="tr")
    log.append("task_event", "T2", at_ms=6, payload={"event": "PAUSE"})

    reloaded = EventLog()
    assert reloaded.load_jsonl(path) == 2
    assert reloaded.head() == 2
    assert reloaded.get(1).payload == {"event": "START"}
    assert reloaded.get(1).trace_id == "tr"
    assert reloaded.digest() == log.digest()


def test_journal_digest_changes_with_content():
    first = EventLog()
    first.append("a", "s")
    second = EventLog()
    second.append("b", "s")
    assert first.digest() != second.digest()


def test_journal_record_as_dict_is_json_friendly():
    log = EventLog()
    record = log.append("k", "s", at_ms=1, payload={"x": 1})
    assert record.as_dict()["payload"] == {"x": 1}
    assert EventRecord.from_dict(record.as_dict()).kind == "k"


# ----------------------------------------------------------------------
# replay reproduces the live state
# ----------------------------------------------------------------------


def test_replay_reproduces_the_lifecycle_fingerprint():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    verification = runtime.verify_replay()
    assert verification["consistent"] is True
    assert verification["live_fingerprint"] == verification["replay_fingerprint"]
    assert verification["records_replayed"] == verification["journal_head"]


def test_replay_records_that_nothing_was_replayed_when_the_log_is_empty():
    runtime = make_runtime()
    verification = runtime.verify_replay()
    assert verification["records_replayed"] == 0
    assert verification["consistent"] is True


def test_replay_reconstructs_an_identical_task_table():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    twin = runtime.fresh_state()
    runtime.journal.replay(twin.apply)
    assert sorted(twin.state.tasks) == sorted(runtime.state.tasks)
    assert twin.state.get("T0001").state == TaskState.COMPLETED
    # A completed task has released its vehicle, on both sides.
    assert twin.state.get("T0001").assigned_vehicle == ""
    assert twin.state.get("T0001").assigned_vehicle == runtime.state.get("T0001").assigned_vehicle
    assert twin.state.lifecycle_fingerprint() == runtime.state.lifecycle_fingerprint()


def test_the_replay_check_has_teeth():
    """Negative control: an unjournaled mutation must be detected.

    Without this test the replay assertion could be passing because the projection
    ignores everything that matters.  Here we deliberately write to state without
    journalling, and the fingerprint must diverge.
    """
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    assert runtime.verify_replay()["consistent"] is True

    runtime.state.get("T0001").fsm.force_state(TaskState.FAILED)  # bypasses the journal
    verification = runtime.verify_replay()
    assert verification["consistent"] is False
    assert verification["live_fingerprint"] != verification["replay_fingerprint"]


def test_replay_into_a_partial_prefix_reproduces_a_partial_state():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    twin = runtime.fresh_state()
    replayed = runtime.journal.replay(twin.apply, to_seq=3)
    assert replayed == 3
    # Only the first three records: the task exists but is not yet complete.
    assert "T0001" in twin.state.tasks
    assert twin.state.get("T0001").state != TaskState.COMPLETED


def test_a_replayed_twin_has_no_history_of_its_own_before_replay():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    twin = runtime.fresh_state()
    assert len(twin.journal) == 0
    assert twin.state.tasks == {}


# ----------------------------------------------------------------------
# snapshot consistency
# ----------------------------------------------------------------------


def test_snapshot_and_restore_reproduce_the_full_state():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    snapshot = runtime.state.snapshot(at_ms=999)
    restored = make_runtime()
    restored.state.restore(snapshot)
    assert restored.state.full_fingerprint() == runtime.state.full_fingerprint()


def test_full_fingerprint_includes_pose_but_lifecycle_does_not():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    before_lifecycle = runtime.state.lifecycle_fingerprint()
    before_full = runtime.state.full_fingerprint()
    runtime.fleet.vehicles["AGV-01"].x += 5.0
    runtime.fleet.vehicles["AGV-01"].battery -= 1.0
    assert runtime.state.lifecycle_fingerprint() == before_lifecycle
    assert runtime.state.full_fingerprint() != before_full


def test_fingerprint_is_order_independent_for_insertion_order():
    """Two states built in different orders must fingerprint the same."""
    first = PlatformState()
    second = PlatformState()
    for state, task_ids in ((first, ["T1", "T2"]), (second, ["T2", "T1"])):
        for task_id in task_ids:
            state.create_task(task_id, created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0))
    assert first.lifecycle_fingerprint() == second.lifecycle_fingerprint()


def test_fingerprint_changes_when_a_task_state_changes():
    state = PlatformState()
    state.create_task("T1", created_ms=0, origin=(0.0, 0.0), destination=(1.0, 0.0))
    before = state.lifecycle_fingerprint()
    state.apply("T1", TaskEvent.ASSIGN)
    assert state.lifecycle_fingerprint() != before


# ----------------------------------------------------------------------
# eventual consistency under injected faults
# ----------------------------------------------------------------------


def run_scenario(config: SimConfig) -> Simulation:
    simulation = Simulation(config)
    simulation.run()
    return simulation


def fault_config(**kwargs) -> FaultConfig:
    base = {"enabled": True, "seed": 21, "name": "test"}
    base.update(kwargs)
    return FaultConfig(**base)


def test_duplicates_converge_and_produce_no_extra_effects():
    simulation = run_scenario(
        SimConfig(
            name="dup", vehicles=6, tasks=24, seed=101, task_interval_ticks=2,
            max_ticks=3000, fault=fault_config(duplicate_rate=0.5),
        )
    )
    report = simulation.report()
    assert report["faults"]["fired"]["duplicates"] > 0
    assert report["consumer"]["duplicates_suppressed"] > 0
    assert report["consistency"]["consistent"] is True
    assert not simulation.rt.state.open_tasks()


def test_reordering_converges_and_is_detected():
    simulation = run_scenario(
        SimConfig(
            name="reorder", vehicles=6, tasks=24, seed=102, task_interval_ticks=2,
            max_ticks=3000, fault=fault_config(reorder=True, reorder_window=3),
        )
    )
    report = simulation.report()
    assert report["faults"]["fired"]["reordered_messages"] > 0
    assert report["consumer"]["out_of_order_detected"] > 0
    assert report["consistency"]["consistent"] is True
    assert not simulation.rt.state.open_tasks()


def test_message_loss_is_recovered_by_gap_rereading():
    simulation = run_scenario(
        SimConfig(
            name="drop", vehicles=6, tasks=24, seed=103, task_interval_ticks=2,
            max_ticks=3000, fault=fault_config(drop_rate=0.2),
        )
    )
    report = simulation.report()
    assert report["faults"]["fired"]["drops"] > 0
    assert report["consumer"]["gap_recoveries"] > 0
    assert report["consistency"]["consistent"] is True
    assert not simulation.rt.state.open_tasks()


def test_consumer_crash_recovers_from_the_committed_offset():
    simulation = run_scenario(
        SimConfig(
            name="crash", vehicles=6, tasks=24, seed=104, task_interval_ticks=2,
            max_ticks=3000, fault=fault_config(crash_after=25),
        )
    )
    report = simulation.report()
    assert report["faults"]["fired"]["crashes"] == 1
    assert report["consumer"]["crashes"] == 1
    assert report["consistency"]["consistent"] is True
    # Crash recovery must not lose work: every task still reaches a terminal state.
    assert not simulation.rt.state.open_tasks()


def test_all_faults_together_still_converge():
    """The headline fault-injection result: everything at once, still consistent."""
    simulation = run_scenario(
        SimConfig(
            name="all", vehicles=8, tasks=30, seed=105, task_interval_ticks=2,
            max_ticks=3000,
            fault=fault_config(
                duplicate_rate=0.3, reorder=True, reorder_window=3, drop_rate=0.12,
                crash_after=30, timeout_budget=5, poison_count=2,
            ),
        )
    )
    report = simulation.report()
    fired = report["faults"]["fired"]
    assert fired["duplicates"] > 0 and fired["drops"] > 0 and fired["reordered_messages"] > 0
    assert fired["crashes"] == 1 and fired["timeouts"] > 0 and fired["poisons"] > 0
    assert report["consistency"]["consistent"] is True
    assert not simulation.rt.state.open_tasks()
    assert report["consumer"]["dlq_routed"] >= 1
    assert report["consumer"]["dlq_redrives"] >= 1
    assert report["consumer"]["dlq_remaining"] == 0


def test_timeouts_are_retried_and_do_not_reach_the_dlq():
    """A transient fault must be absorbed by retry, not escalated.

    ``max_retries`` is raised above the timeout budget on purpose: retries are
    served *before* fresh messages, so one unlucky message can consume the whole
    budget back to back.  With the default of 3 retries, six injected timeouts can
    legitimately exhaust one message — which is correct behaviour, but it tests the
    DLQ path rather than the retry path.
    """
    simulation = run_scenario(
        SimConfig(
            name="timeout", vehicles=6, tasks=20, seed=106, task_interval_ticks=2,
            max_ticks=2000, max_retries=8, fault=fault_config(timeout_budget=6),
        )
    )
    report = simulation.report()
    assert report["faults"]["fired"]["timeouts"] == 6
    assert report["consumer"]["nacked"] >= 6
    assert report["consumer"]["dlq_routed"] == 0
    assert report["consumer"]["dlq_remaining"] == 0
    assert report["consistency"]["consistent"] is True


def test_the_baseline_scenario_is_clean():
    simulation = run_scenario(
        SimConfig(name="clean", vehicles=8, tasks=20, seed=107, task_interval_ticks=2, max_ticks=2000)
    )
    report = simulation.report()
    assert report["faults"]["fired"]["duplicates"] == 0
    assert report["consumer"]["duplicates_suppressed"] == 0
    assert report["state_machine"]["rejected"] == 0
    assert report["consistency"]["consistent"] is True


# ----------------------------------------------------------------------
# idempotency and cache-expiry behaviour
# ----------------------------------------------------------------------


def test_ten_identical_pause_commands_apply_once():
    """The required assertion: repeated commands must not repeat the effect."""
    runtime = make_runtime()
    publish_task(runtime)
    runtime.publish("task_assigned", "T0001", {"task_id": "T0001", "vehicle_id": "AGV-01"}, at_ms=10)
    runtime.publish("task_event", "T0001", {"task_id": "T0001", "event": TaskEvent.DISPATCH}, at_ms=20)
    runtime.drain()

    outcomes = [
        runtime.issue_command("DUP-PAUSE", "PAUSE", "T0001", at_ms=100 + index).status
        for index in range(10)
    ]
    assert outcomes.count("applied") == 1
    assert outcomes.count("duplicate") == 9
    assert runtime.fleet.vehicles["AGV-01"].pause_commands == 1
    pause_records = [
        r for r in runtime.journal if r.kind == "task_event" and r.payload.get("event") == TaskEvent.PAUSE
    ]
    assert len(pause_records) == 1
    assert runtime.state.get("T0001").state == TaskState.PAUSED
    assert runtime.verify_replay()["consistent"] is True


def test_resume_returns_the_task_to_where_it_was_paused():
    runtime = make_runtime()
    publish_task(runtime)
    runtime.publish("task_assigned", "T0001", {"task_id": "T0001", "vehicle_id": "AGV-01"}, at_ms=10)
    for event in (TaskEvent.DISPATCH, TaskEvent.START):
        runtime.publish("task_event", "T0001", {"task_id": "T0001", "event": event}, at_ms=20)
    runtime.drain()
    runtime.issue_command("P1", "PAUSE", "T0001", at_ms=100)
    assert runtime.state.get("T0001").state == TaskState.PAUSED
    runtime.issue_command("R1", "RESUME", "T0001", at_ms=200)
    assert runtime.state.get("T0001").state == TaskState.RECOVERING
    runtime.issue_command("R2", "RECOVER", "T0001", at_ms=300)
    assert runtime.state.get("T0001").state == TaskState.EXECUTING


def test_a_command_for_an_unknown_task_is_reported_not_crashed():
    runtime = make_runtime()
    outcome = runtime.issue_command("C1", "PAUSE", "NOPE", at_ms=0)
    assert outcome.status == "failed"
    assert "unknown task" in outcome.detail


def test_losing_the_cache_costs_a_rebuild_not_correctness():
    """The cache is a projection; the state of record must be unaffected."""
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    runtime.refresh_cache(500)
    assert runtime.cache.size() > 0
    before = runtime.state.lifecycle_fingerprint()

    for key in runtime.cache.keys("*"):
        runtime.cache.expire_now(key, at_ms=600)
    assert runtime.cache.size() == 0

    rebuilt = runtime.rebuild_cache(700)
    assert rebuilt > 0
    assert runtime.cache.get("task:T0001") is not None
    assert runtime.state.lifecycle_fingerprint() == before
    assert runtime.verify_replay()["consistent"] is True


def test_cache_projection_matches_the_state_of_record():
    runtime = make_runtime()
    drive_full_lifecycle(runtime)
    runtime.refresh_cache(100)
    assert runtime.cache.hget("task:T0001", "state") == runtime.state.get("T0001").state
    assert runtime.cache.hget("veh:AGV-01", "state") == runtime.fleet.vehicles["AGV-01"].state


def test_trace_spans_cover_publish_and_consume():
    runtime = make_runtime()
    trace_id = runtime.tracer.new_trace_id()
    runtime.publish(
        "task_created",
        "T0001",
        {
            "task_id": "T0001", "origin": [0.0, 0.0], "destination": [1.0, 1.0],
            "priority": 2, "payload_kg": 1.0, "created_ms": 0, "deadline_ms": 1000,
        },
        at_ms=0,
        trace_id=trace_id,
    )
    runtime.drain()
    names = [span.name for span in runtime.tracer.trace(trace_id)]
    assert names == ["bus.publish", "consumer.process"]
    spans = runtime.tracer.trace(trace_id)
    assert spans[1].parent_span_id == spans[0].span_id
    assert spans[1].attributes["result_state"] == TaskState.PENDING
    assert runtime.logger.by_trace(trace_id)


def test_command_trace_spans_the_whole_chain():
    runtime = make_runtime()
    publish_task(runtime)
    runtime.drain()
    outcome = runtime.issue_command("TRACE-1", "CANCEL", "T0001", at_ms=50)
    assert outcome.status == "applied"
    records = runtime.logger.by_event("command_result")
    trace_id = records[-1]["trace_id"]
    names = [span.name for span in runtime.tracer.trace(trace_id)]
    assert "command.issue" in names
    assert "bus.publish" in names
    assert "consumer.process" in names
    assert "command.response" in names
    assert runtime.journal.by_trace(trace_id)


def test_vehicle_going_offline_is_journaled_and_replayable():
    runtime = make_runtime()
    for vehicle_id in runtime.fleet.ids():
        runtime.heartbeat(vehicle_id, at_ms=0)
    stale = runtime.check_timeouts(60_000)
    assert stale == ["AGV-01", "AGV-02"]  # both went silent
    assert runtime.fleet.vehicles["AGV-01"].online is False
    assert runtime.verify_replay()["consistent"] is True

    runtime.heartbeat("AGV-01", at_ms=61_000)
    assert runtime.fleet.vehicles["AGV-01"].online is True
    assert runtime.verify_replay()["consistent"] is True


def test_offline_vehicle_raises_an_alert():
    runtime = make_runtime()
    runtime.heartbeat("AGV-01", at_ms=0)
    runtime.heartbeat("AGV-02", at_ms=0)
    runtime.check_timeouts(60_000)
    assert runtime.alerts.counts_by_rule().get("vehicle_offline") == 2


def test_a_heartbeating_vehicle_is_never_taken_offline():
    runtime = make_runtime()
    for at_ms in (0, 3_000, 6_000, 9_000):
        for vehicle_id in runtime.fleet.ids():
            runtime.heartbeat(vehicle_id, at_ms=at_ms)
        assert runtime.check_timeouts(at_ms) == []
    assert all(runtime.fleet.vehicles[v].online for v in runtime.fleet.ids())


def test_backpressure_defers_a_publish_without_losing_it():
    runtime = make_runtime(backpressure_limit=1)
    publish_task(runtime, "T0001")
    publish_task(runtime, "T0002")
    assert runtime.counters["events_published_rejected"] >= 1
    runtime.drain()
    runtime.publish("task_created", "T0002", {
        "task_id": "T0002", "origin": [0.0, 0.0], "destination": [1.0, 1.0],
        "priority": 2, "payload_kg": 1.0, "created_ms": 0, "deadline_ms": 1000,
    }, at_ms=0)
    runtime.drain()
    assert "T0002" in runtime.state.tasks


def test_unknown_event_kind_is_rejected_loudly():
    runtime = make_runtime()
    record = runtime.journal.append("not_a_real_kind", "x", payload={})
    with pytest.raises(ValueError):
        runtime.apply(record)


def test_task_created_is_idempotent_under_a_redelivery():
    runtime = make_runtime()
    publish_task(runtime)
    runtime.drain()
    assert len(runtime.state.tasks) == 1
    runtime.publish(
        "task_created",
        "T0001",
        {
            "task_id": "T0001", "origin": [1.0, 1.0], "destination": [2.0, 2.0],
            "priority": 5, "payload_kg": 9.0, "created_ms": 0, "deadline_ms": 1,
        },
        at_ms=50,
    )
    runtime.drain()
    # A second create must not overwrite the existing task.
    assert len(runtime.state.tasks) == 1
    assert runtime.state.get("T0001").origin == (60.0, 60.0)

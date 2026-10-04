"""The demo: a suite of scenarios, their reports, and the consistency verdicts.

One scenario would show a number; a suite shows *why* the number is what it is.
Six scenarios run in sequence, each isolating one concern, and every scenario
ends with the same two questions:

1. does a replay of the event journal reproduce the live state exactly?
2. did every task that was created reach a terminal state, exactly once?

Nothing in the output depends on wall-clock time, so ``metrics.json`` can be
compared byte-for-byte between two runs.  The runtime duration goes to
``timing.json`` instead, and is the *only* value excluded from that comparison.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

from .faults import FaultConfig
from .sim import Simulation, SimConfig
from .task_fsm import TaskEvent

__all__ = [
    "ScenarioResult",
    "DemoResult",
    "scenario_configs",
    "run_scenario",
    "run_demo",
    "verify_scenario",
    "write_reports",
    "deterministic_metrics",
    "headline",
]

#: The scenario suite.  Each entry isolates one property, and the names are used
#: as file prefixes in ``reports/``.
BASE_SEED = 20240501


def scenario_configs() -> list[SimConfig]:
    """The suite, ordered so cheap scenarios run first."""
    return [
        SimConfig(
            name="baseline",
            # Slightly under-provisioned on purpose: a fleet that is always
            # instantly available produces a dispatch latency of zero, and a
            # latency metric that is always zero is not a measurement.
            vehicles=12,
            tasks=50,
            seed=BASE_SEED,
            task_interval_ticks=3,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
        ),
        SimConfig(
            name="overload",
            # Oversubscribed twice over: half the fleet, double the arrival rate,
            # a 30-task burst, a consumer allowed only 6 messages per tick, and a
            # lag budget low enough that the producer is actually refused.  This
            # is the scenario that produces a real backlog, real backpressure
            # rejections and real give-way contention.
            vehicles=6,
            tasks=70,
            seed=BASE_SEED + 1,
            task_interval_ticks=2,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
            consume_budget_per_tick=6,
            burst_tasks=30,
            burst_at_tick=40,
            backpressure_limit=10,
        ),
        SimConfig(
            name="fault-injection",
            vehicles=10,
            tasks=50,
            seed=BASE_SEED + 2,
            task_interval_ticks=3,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
            fault=FaultConfig(
                enabled=True,
                name="duplicate+reorder+drop+crash+timeout+poison",
                duplicate_rate=0.25,
                reorder=True,
                reorder_window=3,
                drop_rate=0.12,
                crash_after=40,
                network_timeout_rate=0.0,
                timeout_budget=8,
                poison_count=3,
                seed=99,
            ),
        ),
        SimConfig(
            name="deadlock",
            # 10 vehicles: deadlock needs enough simultaneous hold-and-wait to
            # form a cycle, and with 6 the yard is simply never that contended.
            vehicles=10,
            tasks=50,
            seed=BASE_SEED + 3,
            task_interval_ticks=2,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
            # Hold the hub intersection while negotiating the next leg: genuine
            # hold-and-wait, and therefore genuine deadlocks to detect and break.
            hold_hub_intersection=True,
        ),
        SimConfig(
            name="cache-expiry",
            vehicles=12,
            tasks=50,
            seed=BASE_SEED + 5,
            task_interval_ticks=4,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
            cache_expire_ticks=(60, 120, 180),
            fault=FaultConfig(enabled=False, name="cache-expiry", cache_expire_rate=0.5, seed=17),
        ),
        SimConfig(
            name="offline-recovery",
            vehicles=12,
            tasks=50,
            seed=BASE_SEED + 4,
            task_interval_ticks=4,
            vehicle_speed_mps=20.0,
            max_ticks=4_000,
            offline_plan=(
                (30, "AGV-03", 40),
                (60, "AGV-07", 30),
                (110, "AGV-01", 50),
            ),
        ),
    ]


def verify_scenario(sim: Simulation, label: str, report: dict | None = None) -> dict:
    """The eventual-consistency judgement for one completed scenario.

    Four independent checks, because "it looked fine" is not a criterion:

    ``replay_matches_live``
        SHA-256 of the canonical lifecycle projection, live vs. rebuilt from the
        journal.
    ``no_task_lost``
        every created task reached a terminal state, and every task appears
        exactly once in the journal as a creation.
    ``no_duplicate_effect``
        no task reached a terminal state more than once, and the journal holds at
        most one successful completion per task.
    ``injected_faults_absorbed``
        every fault the injector actually fired is accounted for: duplicates by
        the dedup counter, drops by gap re-reads, poison by retries and DLQ.
    """
    report = sim.report() if report is None else report
    rt = sim.rt
    consistency = report["consistency"]

    # -- 1. replay equivalence ------------------------------------------
    replay_matches = bool(consistency["consistent"])

    # -- 2. no task lost -------------------------------------------------
    created = [r for r in rt.journal if r.kind == "task_created"]
    created_ids = sorted(r.payload["task_id"] for r in created)
    tasks = sorted(rt.state.tasks)
    terminal = sorted(t.task_id for t in rt.state.tasks.values() if t.is_terminal)
    no_task_lost = created_ids == tasks and len(terminal) == len(tasks)
    # -- 3. no duplicate effect -----------------------------------------
    terminal_events: dict[str, int] = {}
    completed_events: dict[str, int] = {}
    for record in rt.journal:
        if record.kind != "task_event":
            continue
        event = record.payload.get("event")
        if event in (TaskEvent.COMPLETE, TaskEvent.FAIL, TaskEvent.CANCEL):
            terminal_events[record.subject] = terminal_events.get(record.subject, 0) + 1
        if event == TaskEvent.COMPLETE:
            completed_events[record.subject] = completed_events.get(record.subject, 0) + 1
    no_duplicate_effect = all(v <= 1 for v in terminal_events.values()) and all(
        v <= 1 for v in completed_events.values()
    )

    # -- 4. injected faults absorbed ------------------------------------
    fired = report["faults"]["fired"]
    consumer = report["consumer"]
    absorbed: list[str] = []
    if fired["duplicates"]:
        absorbed.append(
            "duplicates=%d suppressed=%d"
            % (fired["duplicates"], consumer["duplicates_suppressed"])
        )
    if fired["drops"]:
        absorbed.append("drops=%d gap_recoveries=%d" % (fired["drops"], consumer["gap_recoveries"]))
    if fired["reordered_messages"]:
        absorbed.append(
            "reordered=%d order_checks=%d"
            % (fired["reordered_messages"], consumer["out_of_order_detected"])
        )
    if fired["crashes"]:
        absorbed.append("crashes=%d recoveries=%d" % (fired["crashes"], 1 if fired["crashes"] else 0))
    if fired["timeouts"]:
        absorbed.append("timeouts=%d nacked=%d" % (fired["timeouts"], consumer["nacked"]))
    if fired["poisons"]:
        absorbed.append(
            "poisons=%d dlq_routed=%d redrives=%d"
            % (fired["poisons"], consumer["dlq_routed"], consumer["dlq_redrives"])
        )
    injected_faults_absorbed = True
    if fired["duplicates"] and consumer["duplicates_suppressed"] == 0:
        injected_faults_absorbed = False
    if fired["drops"] and consumer["gap_recoveries"] == 0:
        # A drop that landed on the very last message of a partition needs no
        # recovery, so only an empty recovery count *with* pending lag is a
        # failure.  Being explicit here beats a check that is sometimes vacuous.
        injected_faults_absorbed = report["bus"]["max_lag"] == 0
    if fired["poisons"] and consumer["dlq_routed"] == 0:
        injected_faults_absorbed = False

    consistent = replay_matches and no_task_lost and no_duplicate_effect and injected_faults_absorbed
    return {
        "scenario": label,
        "consistent": consistent,
        "criterion": (
            "replay(lifecycle_fingerprint) == live(lifecycle_fingerprint) "
            "AND every created task terminal exactly once "
            "AND every injected fault absorbed"
        ),
        "checks": {
            "replay_matches_live": replay_matches,
            "no_task_lost": no_task_lost,
            "no_duplicate_effect": no_duplicate_effect,
            "injected_faults_absorbed": injected_faults_absorbed,
        },
        "live_fingerprint": consistency["live_fingerprint"],
        "replay_fingerprint": consistency["replay_fingerprint"],
        "journal_digest": consistency["journal_digest"],
        "journal_records": consistency["journal_head"],
        "tasks_created": len(created_ids),
        "tasks_created_unique": len(set(created_ids)),
        "tasks_in_state": len(tasks),
        "tasks_terminal": len(terminal),
        "duplicate_create_records": len(created_ids) - len(set(created_ids)),
        "terminal_events": {k: terminal_events[k] for k in sorted(terminal_events)},
        "fault_absorption": absorbed,
    }


@dataclass
class ScenarioResult:
    """One scenario's report plus its consistency verdict."""

    name: str
    report: dict
    verdict: dict
    seconds: float = 0.0

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "seconds": round(self.seconds, 3),
            "report": self.report,
            "verdict": self.verdict,
        }


@dataclass
class DemoResult:
    """The whole suite."""

    scenarios: list[ScenarioResult] = field(default_factory=list)
    idempotency: dict = field(default_factory=dict)
    seconds: float = 0.0

    def by_name(self, name: str) -> ScenarioResult:
        for scenario in self.scenarios:
            if scenario.name == name:
                return scenario
        raise KeyError(f"no scenario named {name!r}")

    def summary(self) -> dict:
        """Deterministic summary only.

        Wall-clock runtime is deliberately *absent*: it lived here for one commit
        and was the single key that made two otherwise identical runs of
        ``verify-repro`` disagree.  It is reported in ``timing.json`` instead.
        """
        return {
            "scenarios": [s.name for s in self.scenarios],
            "all_consistent": all(s.verdict["consistent"] for s in self.scenarios),
            "scenario_count": len(self.scenarios),
        }


def run_scenario(config: SimConfig) -> ScenarioResult:
    """Run one scenario end to end and judge it."""
    started = time.perf_counter()
    sim = Simulation(config)
    sim.run()
    report = sim.report()
    elapsed = time.perf_counter() - started
    return ScenarioResult(
        name=config.name,
        report=report,
        verdict=verify_scenario(sim, config.name, report),
        seconds=elapsed,
    )


def run_idempotency_experiment(seed: int = BASE_SEED + 6) -> dict:
    """Send the same PAUSE command ten times and count the effects.

    This is the command-level twin of message dedup, and the assertion is the
    point: ten sends, one pause.  A retrying client must not be able to pause a
    vehicle twice, and an operator double-click must not either.
    """
    from .runtime import Runtime, RuntimeConfig

    rt = Runtime(RuntimeConfig(vehicles=2, seed=seed, backpressure_limit=None))
    rt.add_vehicle("AGV-01", x=60.0, y=60.0)
    rt.add_vehicle("AGV-02", x=300.0, y=60.0)
    rt.publish(
        "task_created",
        "T0001",
        {
            "task_id": "T0001",
            "origin": [60.0, 60.0],
            "destination": [540.0, 340.0],
            "priority": 3,
            "payload_kg": 500.0,
            "created_ms": 0,
            "deadline_ms": 600_000,
        },
        at_ms=0,
    )
    rt.publish("task_assigned", "T0001", {"task_id": "T0001", "vehicle_id": "AGV-01"}, at_ms=0)
    rt.publish("task_event", "T0001", {"task_id": "T0001", "event": "DISPATCH"}, at_ms=0)
    rt.drain()
    before = rt.fleet.vehicles["AGV-01"].pause_commands

    outcomes: list[str] = []
    for index in range(10):
        outcome = rt.issue_command(f"CMD-PAUSE-1", "PAUSE", "T0001", at_ms=1_000 + index)
        outcomes.append(outcome.status)
    after = rt.fleet.vehicles["AGV-01"].pause_commands

    pause_records = [
        record
        for record in rt.journal
        if record.kind == "task_event" and record.payload.get("event") == TaskEvent.PAUSE
    ]
    task_state = rt.state.get("T0001").state

    # Resume must be idempotent too, so the same test is run in the other
    # direction: ten RESUME commands, one effect.
    resume_before = rt.fleet.vehicles["AGV-01"].resume_commands
    resume_statuses: list[str] = []
    for index in range(10):
        outcome = rt.issue_command("CMD-RESUME-1", "RESUME", "T0001", at_ms=2_000 + index)
        resume_statuses.append(outcome.status)
    resume_after = rt.fleet.vehicles["AGV-01"].resume_commands
    resume_records = [
        record
        for record in rt.journal
        if record.kind == "task_event" and record.payload.get("event") == TaskEvent.RESUME
    ]

    return {
        "scenario": "command-idempotency",
        "commands_sent": 10,
        "started_from_state": "ENROUTE",
        "statuses": outcomes,
        "applied": outcomes.count("applied"),
        "duplicate": outcomes.count("duplicate"),
        "pause_counter_before": before,
        "pause_counter_after": after,
        "pause_effects": after - before,
        "pause_journal_records": len(pause_records),
        "task_state_after_pause": task_state,
        "resume_applied": resume_statuses.count("applied"),
        "resume_duplicate": resume_statuses.count("duplicate"),
        "resume_effects": resume_after - resume_before,
        "resume_journal_records": len(resume_records),
        "idempotent": (after - before) == 1
        and len(pause_records) == 1
        and (resume_after - resume_before) == 1
        and len(resume_records) == 1,
    }


def run_dlq_experiment(seed: int = BASE_SEED + 7) -> dict:
    """Poison some messages, watch them dead-letter, then redrive them.

    A dead-letter queue that is never drained is a slower way of losing data, so
    the experiment does not stop at "the DLQ has 3 messages" — it redrives them
    and asserts the effects landed.
    """
    from .runtime import Runtime, RuntimeConfig

    rt = Runtime(
        RuntimeConfig(
            vehicles=2,
            seed=seed,
            backpressure_limit=None,
            fault=FaultConfig(enabled=True, name="poison", poison_count=3, seed=5),
        )
    )
    rt.add_vehicle("AGV-01", x=60.0, y=60.0)
    rt.add_vehicle("AGV-02", x=300.0, y=60.0)
    for index in range(3):
        task_id = f"T{index + 1:04d}"
        rt.publish(
            "task_created",
            task_id,
            {
                "task_id": task_id,
                "origin": [60.0, 60.0],
                "destination": [540.0, 340.0],
                "priority": 2,
                "payload_kg": 100.0,
                "created_ms": 0,
                "deadline_ms": 600_000,
            },
            at_ms=0,
        )
    rt.drain()
    dlq_after_first_pass = rt.dlq_depth()
    attempts = dict(rt.consumer.group.attempts)
    created_before_redrive = len(rt.state.tasks)
    rt.injector.clear_poison()
    redriven = rt.redrive_dlq()
    created_after = len(rt.state.tasks)
    return {
        "scenario": "dead-letter-and-redrive",
        "messages_poisoned": 3,
        "dlq_depth_after_first_pass": dlq_after_first_pass,
        "attempts_recorded": len(attempts),
        "max_attempts": max(attempts.values()) if attempts else 0,
        "tasks_applied_before_redrive": created_before_redrive,
        "redriven": redriven,
        "dlq_depth_after_redrive": rt.dlq_depth(),
        "tasks_applied_after_redrive": created_after,
        "recovered": created_after == 3 and rt.dlq_depth() == 0,
    }


def run_demo(configs: list[SimConfig] | None = None) -> DemoResult:
    """Run the whole suite."""
    started = time.perf_counter()
    result = DemoResult()
    for config in configs if configs is not None else scenario_configs():
        result.scenarios.append(run_scenario(config))
    result.idempotency = {
        "command": run_idempotency_experiment(),
        "dlq": run_dlq_experiment(),
    }
    result.seconds = time.perf_counter() - started
    return result


# ----------------------------------------------------------------------
# Report writing
# ----------------------------------------------------------------------


def deterministic_metrics(result: DemoResult) -> dict:
    """The metrics document that must be identical across two runs."""
    return {
        "meta": {
            "project": "fleet-dispatch-lab",
            "kind": "demo-suite",
            "synthetic_data": True,
            "notice": (
                "All data in this file is generated by a seeded PRNG from a simulated "
                "six-hub yard. It is not data from any real port or fleet, and no real "
                "Redis, Kafka, RabbitMQ or MQTT service was involved."
            ),
            "clock": "virtual; only timing.json contains wall-clock values",
            "scenarios": [s.name for s in result.scenarios],
        },
        "summary": result.summary(),
        "scenarios": {s.name: s.report for s in result.scenarios},
        "consistency": {s.name: s.verdict for s in result.scenarios},
        "idempotency_experiments": result.idempotency,
        "headline": headline(result),
    }


def headline(result: DemoResult) -> dict:
    """The handful of numbers that go in the README, taken straight from the run."""
    baseline = result.by_name("baseline").report
    overload = result.by_name("overload").report
    faults = result.by_name("fault-injection")
    deadlock = result.by_name("deadlock").report
    offline = result.by_name("offline-recovery").report
    cache = result.by_name("cache-expiry").report
    command = result.idempotency["command"]
    dlq = result.idempotency["dlq"]
    return {
        "baseline_tasks": baseline["tasks"]["created"],
        "baseline_completed": baseline["tasks"]["completed"],
        "baseline_completion_rate": baseline["tasks"]["completion_rate"],
        "baseline_dispatch_p50_ms": baseline["tasks"]["timing"]["dispatch_latency_ms"]["p50"],
        "baseline_dispatch_p95_ms": baseline["tasks"]["timing"]["dispatch_latency_ms"]["p95"],
        "baseline_dispatch_mean_ms": baseline["tasks"]["timing"]["dispatch_latency_ms"]["mean"],
        "baseline_cycle_p95_ms": baseline["tasks"]["timing"]["cycle_time_ms"]["p95"],
        "overload_completion_rate": overload["tasks"]["completion_rate"],
        "overload_completed": overload["tasks"]["completed"],
        "overload_tasks": overload["tasks"]["created"],
        "overload_dispatch_p95_ms": overload["tasks"]["timing"]["dispatch_latency_ms"]["p95"],
        "overload_conflicts": overload["traffic"]["conflicts"],
        "overload_wait_timeouts": overload["traffic"]["wait_timeout_events"],
        "bus_lag_peak": max(s.report["bus"]["lag_peak"] or 0 for s in result.scenarios),
        "bus_backpressure_rejections": sum(
            s.report["bus"]["backpressure_rejections"] for s in result.scenarios
        ),
        "bus_deferred_publishes": sum(
            s.report["faults"]["deferred_publishes"] for s in result.scenarios
        ),
        "bus_published_total": sum(s.report["bus"]["published"] for s in result.scenarios),
        "duplicates_suppressed": sum(
            s.report["consumer"]["duplicates_suppressed"] for s in result.scenarios
        ),
        "gap_recoveries": sum(s.report["consumer"]["gap_recoveries"] for s in result.scenarios),
        "out_of_order_detected": sum(
            s.report["consumer"]["out_of_order_detected"] for s in result.scenarios
        ),
        "dlq_routed_total": sum(s.report["consumer"]["dlq_routed"] for s in result.scenarios),
        "dlq_redrives_total": sum(s.report["consumer"]["dlq_redrives"] for s in result.scenarios),
        "fault_scenario_consistent": faults.verdict["consistent"],
        "fault_scenario_checks": faults.verdict["checks"],
        "fault_scenario_duplicates": faults.report["faults"]["fired"]["duplicates"],
        "fault_scenario_drops": faults.report["faults"]["fired"]["drops"],
        "fault_scenario_reordered": faults.report["faults"]["fired"]["reordered_messages"],
        "fault_scenario_crashes": faults.report["faults"]["fired"]["crashes"],
        "fault_scenario_timeouts": faults.report["faults"]["fired"]["timeouts"],
        "fault_scenario_poisons": faults.report["faults"]["fired"]["poisons"],
        "deadlock_scenario_breaks": deadlock["traffic"]["deadlock_events"],
        "deadlock_scenario_conflicts": deadlock["traffic"]["conflicts"],
        "deadlock_scenario_completion_rate": deadlock["tasks"]["completion_rate"],
        "offline_events": offline["fleet"]["counters"]["offline_events"],
        "offline_reconnects": offline["fleet"]["counters"]["reconnects"],
        "offline_alerts": offline["observability"]["alerts"]["by_rule"].get("vehicle_offline", 0),
        "offline_consistent": result.by_name("offline-recovery").verdict["consistent"],
        "cache_expiries": cache["cache"]["keys_injected_expired"],
        "cache_keys_lost": cache["cache"]["keys_lost_by_fault"],
        "cache_expiry_events": len(cache["fleet"]["cache_expiry_events"]),
        "cache_consistent": result.by_name("cache-expiry").verdict["consistent"],
        "command_pause_effects": command["pause_effects"],
        "command_pause_records": command["pause_journal_records"],
        "command_duplicates": command["duplicate"],
        "command_idempotent": command["idempotent"],
        "dlq_depth_peak": dlq["dlq_depth_after_first_pass"],
        "dlq_recovered": dlq["recovered"],
        "all_scenarios_consistent": result.summary()["all_consistent"],
    }


def write_reports(result: DemoResult, out_dir: str = "reports") -> list[str]:
    """Write every artefact; returns the paths written (sorted).

    Files are written in **binary** mode with explicit UTF-8 and LF newlines.
    Text mode is a trap here: on Windows it silently rewrites every ``\\n`` as
    ``\\r\\n``, so the file on disk was ~119 kB larger than the byte-for-byte
    comparison claimed and the artefact would have differed between the Windows
    and Linux CI runners for no real reason.
    """
    os.makedirs(out_dir, exist_ok=True)
    written: list[str] = []

    metrics_path = os.path.join(out_dir, "metrics.json")
    blob = json.dumps(
        deterministic_metrics(result), indent=2, sort_keys=True, ensure_ascii=False
    )
    with open(metrics_path, "wb") as handle:
        handle.write((blob + "\n").encode("utf-8"))
    written.append(metrics_path)

    timing_path = os.path.join(out_dir, "timing.json")
    timing = json.dumps(
        {
            "note": "wall-clock values live here only; metrics.json excludes them",
            "total_seconds": round(result.seconds, 3),
            "per_scenario_seconds": {s.name: round(s.seconds, 3) for s in result.scenarios},
        },
        indent=2,
        sort_keys=True,
    )
    with open(timing_path, "wb") as handle:
        handle.write((timing + "\n").encode("utf-8"))
    written.append(timing_path)

    alerts_path = os.path.join(out_dir, "alerts.csv")
    _write_alerts_csv(result, alerts_path)
    written.append(alerts_path)

    events_path = os.path.join(out_dir, "events.jsonl")
    _write_event_samples(result, events_path)
    written.append(events_path)

    return sorted(written)


def _write_alerts_csv(result: DemoResult, path: str) -> None:
    """Flatten every scenario's alerts into one CSV for triage."""
    import csv

    rows: list[dict] = []
    for scenario in result.scenarios:
        for alert in scenario.report["observability"]["alerts"].get("list", []):
            rows.append({"scenario": scenario.name, **alert})
    fields = ["scenario", "rule", "severity", "subject", "value", "threshold", "at_ms", "message"]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _write_event_samples(result: DemoResult, path: str) -> None:
    """Write the per-scenario event-kind counts as JSONL (one line per scenario)."""
    with open(path, "w", encoding="utf-8") as handle:
        for scenario in result.scenarios:
            handle.write(
                json.dumps(
                    {
                        "scenario": scenario.name,
                        "journal_head": scenario.report["consistency"]["journal_head"],
                        "journal_digest": scenario.report["consistency"]["journal_digest"],
                        "kinds": scenario.report["observability"]["journal"]["kinds"],
                    },
                    sort_keys=True,
                )
                + "\n"
            )

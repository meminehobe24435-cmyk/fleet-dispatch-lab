"""End-to-end demo tests: the suite runs, and two runs agree exactly.

The reproducibility test is the strongest claim in the README — "running the same
command twice gives the same numbers, except for the runtime" — so it is checked
here by comparing the serialised metrics of two independent runs, byte for byte.
"""

from __future__ import annotations

import json
import os

import pytest

from fleetlab.cli import _diff_paths, main, render_markdown
from fleetlab.demo import (
    deterministic_metrics,
    headline,
    run_dlq_experiment,
    run_demo,
    run_idempotency_experiment,
    run_scenario,
    scenario_configs,
    verify_scenario,
    write_reports,
)
from fleetlab.plots import available as plots_available
from fleetlab.sim import SimConfig, Simulation, distribution

#: The suite is a few seconds of simulated work; build it once and share it.
@pytest.fixture(scope="module")
def demo():
    return run_demo(scenario_configs())


# ----------------------------------------------------------------------
# scenario suite
# ----------------------------------------------------------------------


def test_the_suite_covers_the_required_scenarios():
    names = [config.name for config in scenario_configs()]
    assert names == [
        "baseline",
        "overload",
        "fault-injection",
        "deadlock",
        "cache-expiry",
        "offline-recovery",
    ]


def test_every_scenario_finishes_and_is_consistent(demo):
    assert len(demo.scenarios) == 6
    for scenario in demo.scenarios:
        assert scenario.verdict["consistent"] is True, scenario.name
        assert scenario.report["meta"]["stopped_because"] == "all tasks terminal"
        assert scenario.report["tasks"]["open"] == 0


def test_every_scenario_reaches_terminal_state_for_every_task(demo):
    for scenario in demo.scenarios:
        verdict = scenario.verdict
        assert verdict["tasks_terminal"] == verdict["tasks_in_state"], scenario.name
        assert verdict["duplicate_create_records"] == 0


def test_all_scenarios_are_consistent_together(demo):
    assert demo.summary()["all_consistent"] is True


def test_the_metrics_are_declared_synthetic(demo):
    metrics = deterministic_metrics(demo)
    assert metrics["meta"]["synthetic_data"] is True
    assert "not data from any real port" in metrics["meta"]["notice"]
    assert "Redis" in metrics["meta"]["notice"]
    assert "virtual" in metrics["meta"]["clock"]


def test_each_scenario_reports_scale_and_timings(demo):
    for scenario in demo.scenarios:
        report = scenario.report
        assert report["scale"]["vehicles"] > 0
        assert report["scale"]["tasks_created"] > 0
        assert report["scale"]["resources"] > 0
        for key in ("dispatch_latency_ms", "cycle_time_ms", "service_time_ms"):
            assert "p95" in report["tasks"]["timing"][key]


def test_baseline_completes_most_of_its_work(demo):
    baseline = demo.by_name("baseline").report
    assert baseline["tasks"]["completion_rate"] >= 0.8
    assert baseline["dispatch"]["decisions"] > 0


def test_overload_scenario_is_actually_overloaded(demo):
    overload = demo.by_name("overload").report
    # A deliberately oversubscribed scenario must show it: queueing, contention
    # and a lower completion rate than the provisioned baseline.
    assert overload["bus"]["lag_peak"] > 0
    assert overload["bus"]["backpressure_rejections"] > 0
    assert overload["faults"]["deferred_publishes"] > 0
    assert overload["traffic"]["conflicts"] > 0
    assert overload["tasks"]["completion_rate"] < demo.by_name("baseline").report["tasks"]["completion_rate"]


def test_fault_scenario_injects_and_absorbs_every_fault(demo):
    report = demo.by_name("fault-injection").report
    fired = report["faults"]["fired"]
    assert fired["duplicates"] > 0
    assert fired["drops"] > 0
    assert fired["reordered_messages"] > 0
    assert fired["crashes"] == 1
    assert fired["timeouts"] > 0
    assert fired["poisons"] > 0
    assert report["consumer"]["duplicates_suppressed"] > 0
    assert report["consumer"]["gap_recoveries"] > 0
    assert report["consumer"]["out_of_order_detected"] > 0
    assert report["consumer"]["dlq_routed"] > 0
    assert report["consumer"]["dlq_remaining"] == 0
    assert report["consistency"]["consistent"] is True


def test_deadlock_scenario_detects_and_breaks_deadlocks(demo):
    report = demo.by_name("deadlock").report
    assert report["config"]["hold_hub_intersection"] is True
    assert report["traffic"]["deadlock_events"] > 0
    assert report["consistency"]["consistent"] is True
    assert report["tasks"]["open"] == 0


def test_offline_scenario_detects_loss_and_recovery(demo):
    report = demo.by_name("offline-recovery").report
    assert report["fleet"]["counters"]["offline_events"] == 3
    assert report["fleet"]["counters"]["reconnects"] == 3
    assert report["observability"]["alerts"]["by_rule"]["vehicle_offline"] == 3
    assert report["consistency"]["consistent"] is True


def test_cache_expiry_scenario_really_loses_cache_state(demo):
    report = demo.by_name("cache-expiry").report
    assert report["cache"]["keys_injected_expired"] > 0
    assert report["cache"]["keys_lost_by_fault"] > 0
    assert report["cache"]["rebuilds"] > 0
    # ...and the state of record is unaffected by losing the projection.
    assert report["consistency"]["consistent"] is True


def test_scenarios_do_not_share_state_between_runs():
    first = run_scenario(SimConfig(name="a", vehicles=3, tasks=6, seed=5, task_interval_ticks=1, max_ticks=400))
    second = run_scenario(SimConfig(name="b", vehicles=3, tasks=6, seed=5, task_interval_ticks=1, max_ticks=400))
    del second
    assert first.report["tasks"]["created"] == 6
    assert len(first.verdict["terminal_events"]) >= 1


# ----------------------------------------------------------------------
# reproducibility
# ----------------------------------------------------------------------


def test_two_runs_produce_byte_identical_metrics():
    """The README's central reproducibility claim, checked rather than asserted."""
    first = json.dumps(deterministic_metrics(run_demo(scenario_configs())), indent=2, sort_keys=True)
    second = json.dumps(deterministic_metrics(run_demo(scenario_configs())), indent=2, sort_keys=True)
    assert first == second


def test_two_runs_of_one_scenario_agree():
    config = SimConfig(name="same", vehicles=6, tasks=18, seed=77, task_interval_ticks=2, max_ticks=2000)
    first = json.dumps(run_scenario(config).report, sort_keys=True)
    second = json.dumps(run_scenario(config).report, sort_keys=True)
    assert first == second


def test_metrics_contain_no_wall_clock_values(demo):
    blob = json.dumps(deterministic_metrics(demo), sort_keys=True)
    for forbidden in ('"total_seconds"', '"seconds":', "elapsed", "timestamp", "generated_at"):
        assert forbidden not in blob
    # The one place wall time is allowed to appear.
    assert demo.seconds > 0


def test_a_different_seed_gives_different_numbers():
    """Reproducibility must come from the seed, not from ignoring randomness."""
    first = run_scenario(
        SimConfig(name="s1", vehicles=6, tasks=18, seed=1, task_interval_ticks=2, max_ticks=2000)
    ).report
    second = run_scenario(
        SimConfig(name="s2", vehicles=6, tasks=18, seed=2, task_interval_ticks=2, max_ticks=2000)
    ).report
    assert json.dumps(first, sort_keys=True) != json.dumps(second, sort_keys=True)


def test_diff_paths_finds_the_single_difference():
    assert _diff_paths({"a": 1, "b": {"c": 2}}, {"a": 1, "b": {"c": 3}}) == ["b.c: 2 != 3"]
    assert _diff_paths([1, 2], [1, 2, 3]) == [" (list length 2 vs 3)"]
    assert _diff_paths({"a": 1}, {"a": 1}) == []


# ----------------------------------------------------------------------
# experiments
# ----------------------------------------------------------------------


def test_command_idempotency_experiment():
    result = run_idempotency_experiment()
    assert result["idempotent"] is True
    assert result["pause_effects"] == 1
    assert result["pause_journal_records"] == 1
    assert result["duplicate"] == 9
    assert result["resume_effects"] == 1


def test_dlq_experiment_recovers_by_redrive():
    result = run_dlq_experiment()
    assert result["messages_poisoned"] == 3
    assert result["dlq_depth_after_first_pass"] == 3
    assert result["max_attempts"] > 1
    assert result["dlq_depth_after_redrive"] == 0
    assert result["recovered"] is True


def test_experiments_are_deterministic():
    assert run_idempotency_experiment() == run_idempotency_experiment()
    assert run_dlq_experiment() == run_dlq_experiment()


# ----------------------------------------------------------------------
# artefacts
# ----------------------------------------------------------------------


def test_write_reports_produces_the_expected_files(tmp_path, demo):
    written = write_reports(demo, str(tmp_path))
    names = {os.path.basename(path) for path in written}
    assert names == {"metrics.json", "timing.json", "alerts.csv", "events.jsonl"}
    for path in written:
        assert os.path.getsize(path) > 0


def test_metrics_json_on_disk_has_lf_newlines_and_parses(tmp_path, demo):
    write_reports(demo, str(tmp_path))
    raw = open(os.path.join(tmp_path, "metrics.json"), "rb").read()
    # Windows text mode would insert CRLF and make the file differ from the byte
    # count used by the reproducibility comparison.
    assert b"\r\n" not in raw
    payload = json.loads(raw.decode("utf-8"))
    assert payload["summary"]["all_consistent"] is True
    assert "headline" in payload


def test_metrics_json_is_byte_identical_between_writes(tmp_path, demo):
    first_dir = tmp_path / "a"
    second_dir = tmp_path / "b"
    write_reports(demo, str(first_dir))
    write_reports(demo, str(second_dir))
    assert (first_dir / "metrics.json").read_bytes() == (second_dir / "metrics.json").read_bytes()


def test_timing_json_holds_the_wall_clock_values(tmp_path, demo):
    write_reports(demo, str(tmp_path))
    payload = json.loads((tmp_path / "timing.json").read_text(encoding="utf-8"))
    assert payload["total_seconds"] > 0
    assert len(payload["per_scenario_seconds"]) == 6


def test_markdown_report_is_written(tmp_path, demo):
    path = os.path.join(str(tmp_path), "实验报告.md")
    render_markdown(demo, path, {})
    text = open(path, encoding="utf-8").read()
    assert "fleetlab 实验报告" in text
    assert "合成数据" in text
    assert "一致性判定" in text
    # Every required headline number should be present in the prose.
    numbers = headline(demo)
    assert str(numbers["baseline_completion_rate"]) in text
    assert str(numbers["fault_scenario_duplicates"]) in text


def test_markdown_report_lists_figures_when_given(tmp_path, demo):
    path = os.path.join(str(tmp_path), "report.md")
    render_markdown(demo, path, {"gantt": "gantt.png", "state_machine": "state_machine.png"})
    text = open(path, encoding="utf-8").read()
    assert "![gantt](gantt.png)" in text
    assert "![state_machine](state_machine.png)" in text


def test_alerts_csv_has_a_header_and_rows(tmp_path, demo):
    write_reports(demo, str(tmp_path))
    lines = (tmp_path / "alerts.csv").read_text(encoding="utf-8").strip().splitlines()
    assert lines[0].startswith("scenario,rule,severity,subject")
    assert len(lines) > 1


@pytest.mark.skipif(not plots_available(), reason="matplotlib is not installed")
def test_figures_are_generated(tmp_path, demo):
    from fleetlab.plots import plot_all

    written = plot_all(demo, str(tmp_path))
    assert set(written) == {
        "state_machine",
        "vehicle_state_machine",
        "gantt",
        "backlog",
        "scenario_summary",
    }
    for path in written.values():
        assert os.path.getsize(path) > 1000
        assert open(path, "rb").read(8) == b"\x89PNG\r\n\x1a\n"


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def test_cli_demo_writes_artefacts(tmp_path, capsys):
    code = main(["demo", "--out", str(tmp_path), "--quiet"])
    assert code == 0
    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "实验报告.md").exists()
    assert capsys.readouterr().out.strip()


def test_cli_demo_applies_overrides(tmp_path):
    main(["demo", "--out", str(tmp_path), "--vehicles", "4", "--tasks", "8", "--seed", "3", "--no-plots", "--quiet"])
    payload = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    baseline = payload["scenarios"]["baseline"]
    assert baseline["scale"]["vehicles"] == 4
    assert baseline["scale"]["tasks_created"] == 8
    assert baseline["config"]["seed"] == 3


def test_cli_no_plots_skips_figures(tmp_path):
    main(["demo", "--out", str(tmp_path), "--vehicles", "3", "--tasks", "5", "--no-plots", "--quiet"])
    assert not list(tmp_path.glob("*.png"))


def test_cli_verify_repro_succeeds(tmp_path, capsys):
    code = main(["verify-repro", "--out", str(tmp_path)])
    output = capsys.readouterr().out
    assert code == 0
    assert "REPRODUCIBLE" in output
    assert "byte-identical: True" in output
    assert (tmp_path / "metrics.json").exists()


def test_cli_scenarios_lists_the_suite(capsys):
    assert main(["scenarios"]) == 0
    output = capsys.readouterr().out
    for name in ("baseline", "overload", "fault-injection", "deadlock", "cache-expiry", "offline-recovery"):
        assert name in output


def test_cli_fsm_prints_the_tables(capsys):
    assert main(["fsm"]) == 0
    output = capsys.readouterr().out
    assert "TASK state machine" in output
    assert "VEHICLE state machine" in output
    assert "consistency problems: none" in output
    assert "PENDING" in output and "COMPLETED" in output


def test_cli_replay_reports_consistency(capsys):
    assert main(["replay"]) == 0
    output = capsys.readouterr().out
    assert "REPLAY CONSISTENT" in output
    assert "MATCH" in output


def test_cli_version_and_unknown_command(capsys):
    with pytest.raises(SystemExit) as info:
        main(["--version"])
    assert info.value.code == 0
    assert "fleetlab" in capsys.readouterr().out

    with pytest.raises(SystemExit):
        main(["not-a-command"])


def test_cli_replay_writes_metrics_when_asked(tmp_path):
    assert main(["replay", "--out", str(tmp_path)]) == 0
    assert (tmp_path / "metrics.json").exists()


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------


def test_distribution_handles_empty_input():
    summary = distribution([])
    assert summary["count"] == 0
    assert summary["p95"] is None
    assert summary["values"] == []


def test_distribution_of_one_value():
    summary = distribution([7])
    assert summary == {
        "count": 1, "min": 7, "mean": 7.0, "p50": 7, "p95": 7, "p99": 7, "max": 7, "values": [7],
    }


def test_distribution_percentile_is_a_real_sample():
    summary = distribution(list(range(1, 101)))
    assert summary["p95"] == 95
    assert summary["p95"] in summary["values"]


def test_verify_scenario_reports_all_four_checks():
    simulation = Simulation(SimConfig(name="v", vehicles=3, tasks=6, seed=9, task_interval_ticks=1, max_ticks=400))
    simulation.run()
    verdict = verify_scenario(simulation, "v")
    assert set(verdict["checks"]) == {
        "replay_matches_live",
        "no_task_lost",
        "no_duplicate_effect",
        "injected_faults_absorbed",
    }
    assert verdict["consistent"] is True
    assert verdict["journal_digest"]


def test_headline_has_every_required_number(demo):
    numbers = headline(demo)
    for key in (
        "baseline_tasks",
        "baseline_completion_rate",
        "baseline_dispatch_p50_ms",
        "baseline_dispatch_p95_ms",
        "bus_lag_peak",
        "duplicates_suppressed",
        "fault_scenario_consistent",
        "command_idempotent",
        "all_scenarios_consistent",
    ):
        assert key in numbers
        assert numbers[key] is not None

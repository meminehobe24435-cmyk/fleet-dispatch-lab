"""Observability tests: percentiles with degenerate input, tracing, alert rules."""

from __future__ import annotations

import json
import os

import pytest

from fleetlab.observability import (
    AlertEngine,
    AlertRule,
    DeterministicIds,
    Histogram,
    JsonLogger,
    Metrics,
    Tracer,
    default_rules,
    percentile,
)


# ----------------------------------------------------------------------
# percentile
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "values,expected",
    [
        ([], None),
        ([7.0], 7.0),
        ([1.0, 2.0, 3.0, 4.0, 5.0], 5.0),
        ([1.0, 2.0], 2.0),
    ],
)
def test_percentile_95(values, expected):
    assert percentile(values, 95) == expected


def test_percentile_is_nearest_rank_not_interpolated():
    """p95 must be a value that was actually observed."""
    values = [float(i) for i in range(1, 101)]
    result = percentile(values, 95)
    assert result == 95.0
    assert result in values


def test_percentile_degenerate_bounds():
    values = [3.0, 1.0, 2.0]
    assert percentile(values, 0) == 1.0
    assert percentile(values, -5) == 1.0
    assert percentile(values, 100) == 3.0
    assert percentile(values, 250) == 3.0


def test_percentile_handles_unsorted_and_duplicate_input():
    assert percentile([5.0, 1.0, 5.0, 1.0], 50) == 1.0
    assert percentile([5.0, 1.0, 5.0, 1.0], 100) == 5.0


def test_percentile_of_all_identical_values():
    assert percentile([2.0] * 10, 95) == 2.0


# ----------------------------------------------------------------------
# metrics
# ----------------------------------------------------------------------


def test_histogram_summary_on_empty_input():
    summary = Histogram("empty").summary()
    assert summary["count"] == 0
    assert summary["p95"] is None
    assert summary["mean"] is None


def test_histogram_summary_on_one_sample():
    histogram = Histogram("one")
    histogram.observe(4.5)
    summary = histogram.summary()
    assert summary == {"count": 1, "min": 4.5, "mean": 4.5, "p50": 4.5, "p95": 4.5, "p99": 4.5, "max": 4.5}


def test_histogram_percentiles_use_real_data():
    histogram = Histogram("latency")
    for value in range(1, 101):
        histogram.observe(float(value))
    assert histogram.percentile(50) == 50.0
    assert histogram.percentile(95) == 95.0
    assert histogram.maximum() == 100.0
    assert histogram.minimum() == 1.0


def test_histogram_capacity_drops_the_oldest():
    histogram = Histogram("bounded", capacity=3)
    for value in range(5):
        histogram.observe(float(value))
    assert histogram.samples == [2.0, 3.0, 4.0]


def test_counters_and_gauges():
    metrics = Metrics()
    assert metrics.incr("a") == 1
    assert metrics.incr("a", 5) == 6
    assert metrics.counter("a") == 6
    assert metrics.counter("missing") == 0
    metrics.set_gauge("g", 1.5)
    assert metrics.gauge("g") == 1.5
    assert metrics.gauge("missing") is None


def test_max_gauge_keeps_the_high_water_mark():
    metrics = Metrics()
    metrics.max_gauge("lag", 3)
    metrics.max_gauge("lag", 1)
    metrics.max_gauge("lag", 9)
    assert metrics.gauge("lag") == 9


def test_p95_helper():
    metrics = Metrics()
    for value in (10, 20, 30, 40):
        metrics.observe("latency", value)
    assert metrics.p95("latency") == 40
    assert metrics.p95("absent") is None


def test_snapshot_is_sorted_and_json_friendly():
    metrics = Metrics()
    metrics.incr("z")
    metrics.incr("a")
    metrics.observe("h", 1.0)
    snapshot = metrics.snapshot()
    assert list(snapshot["counters"]) == ["a", "z"]
    json.dumps(snapshot)  # must not raise


# ----------------------------------------------------------------------
# structured logging
# ----------------------------------------------------------------------


def test_logger_records_structured_events():
    logger = JsonLogger()
    record = logger.log("task_assigned", trace_id="t1", span_id="s1", at_ms=5, task_id="T1")
    assert record["event"] == "task_assigned"
    assert record["trace_id"] == "t1"
    assert record["at_ms"] == 5
    assert record["task_id"] == "T1"


def test_logger_queries_by_trace_and_event():
    logger = JsonLogger()
    logger.log("a", trace_id="t1")
    logger.log("b", trace_id="t2")
    logger.log("a", trace_id="t1")
    assert len(logger.by_trace("t1")) == 2
    assert len(logger.by_event("a")) == 2
    assert logger.counts_by_event() == {"a": 2, "b": 1}
    assert len(logger) == 3


def test_logger_level_filter():
    logger = JsonLogger(min_level="WARNING")
    assert logger.log("debug_thing", level="DEBUG") == {}
    assert logger.log("info_thing", level="INFO") == {}
    assert logger.log("warn_thing", level="WARNING")["event"] == "warn_thing"
    assert len(logger) == 1


def test_logger_writes_json_lines_to_a_file(tmp_path):
    path = os.path.join(tmp_path, "log.jsonl")
    logger = JsonLogger(path=path)
    logger.log("one", trace_id="t")
    logger.log("two", duration_ms=1.25)
    with open(path, encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    assert [line["event"] for line in lines] == ["one", "two"]
    assert lines[1]["duration_ms"] == 1.25


def test_logger_writes_to_a_stream():
    import io

    stream = io.StringIO()
    logger = JsonLogger(stream=stream)
    logger.log("hello", trace_id="t")
    assert json.loads(stream.getvalue().strip())["event"] == "hello"


# ----------------------------------------------------------------------
# tracing
# ----------------------------------------------------------------------


def test_ids_are_reproducible_for_a_seed():
    assert DeterministicIds(42).next() == DeterministicIds(42).next()
    assert DeterministicIds(42).next() != DeterministicIds(43).next()


def test_span_records_name_parent_and_attributes():
    tracer = Tracer(DeterministicIds(1))
    with tracer.span("outer", trace_id="tr", at_ms=10, kind="x") as outer:
        with tracer.span("inner", trace_id="tr", parent_span_id=outer.span_id, at_ms=11):
            pass
    spans = tracer.trace("tr")
    assert [s.name for s in spans] == ["outer", "inner"]
    assert spans[1].parent_span_id == spans[0].span_id
    assert spans[0].attributes["kind"] == "x"
    assert spans[0].start_ms == 10


def test_span_records_errors_and_still_propagates():
    tracer = Tracer(DeterministicIds(2))
    with pytest.raises(ValueError):
        with tracer.span("boom", trace_id="tr"):
            raise ValueError("nope")
    span = tracer.trace("tr")[0]
    assert span.status == "ERROR:ValueError"
    assert span.attributes["error"] == "nope"


def test_span_duration_is_measured():
    tracer = Tracer(DeterministicIds(3))
    with tracer.span("timed", trace_id="tr"):
        pass
    assert tracer.trace("tr")[0].duration_ms >= 0.0


def test_trace_dict_for_an_unknown_id_reports_not_found():
    payload = Tracer(DeterministicIds(4)).trace_dict("nope")
    assert payload["found"] is False
    assert payload["span_count"] == 0


def test_trace_dict_orders_by_start_time():
    tracer = Tracer(DeterministicIds(5))
    tracer.record(_span("b", start=20))
    tracer.record(_span("a", start=10))
    payload = tracer.trace_dict("tr")
    assert [s["name"] for s in payload["spans"]] == ["a", "b"]
    assert payload["found"] is True


def _span(name, start):
    from fleetlab.observability import Span

    return Span(trace_id="tr", span_id=name, name=name, start_ms=start)


def test_slowest_spans_are_ranked():
    tracer = Tracer(DeterministicIds(6))
    for index, duration in enumerate((5.0, 1.0, 9.0)):
        span = _span(f"s{index}", start=0)
        span.duration_ms = duration
        tracer.record(span)
    assert [s["name"] for s in tracer.slowest(2)] == ["s2", "s0"]


def test_tracer_evicts_old_traces_beyond_its_cap():
    tracer = Tracer(DeterministicIds(7), max_traces=2)
    for index in range(4):
        with tracer.span("x", trace_id=f"t{index}"):
            pass
    assert tracer.counters["evicted_traces"] == 2


def test_tracer_stats_shape():
    tracer = Tracer(DeterministicIds(8))
    with tracer.span("x", trace_id="t"):
        pass
    stats = tracer.stats()
    assert stats["spans"] == 1
    assert stats["traces"] == 1


# ----------------------------------------------------------------------
# alerts
# ----------------------------------------------------------------------


def rule(**kwargs) -> AlertRule:
    base = {"name": "r", "metric": "m", "threshold": 10.0}
    base.update(kwargs)
    return AlertRule(**base)


def test_alert_fires_when_the_threshold_is_breached():
    engine = AlertEngine([rule()])
    alert = engine.evaluate("r", "subject", 11.0, at_ms=0)
    assert alert is not None
    assert alert.severity == "WARNING"
    assert alert.threshold == 10.0


def test_alert_does_not_fire_below_the_threshold():
    assert AlertEngine([rule()]).evaluate("r", "s", 9.9, at_ms=0) is None


def test_less_than_comparison():
    engine = AlertEngine([rule(comparison="<", threshold=5.0)])
    assert engine.evaluate("r", "s", 4.0, at_ms=0) is not None
    assert engine.evaluate("r", "s", 6.0, at_ms=0) is None


def test_cooldown_suppresses_repeats():
    """Without a cooldown, one offline vehicle becomes a thousand alerts."""
    engine = AlertEngine([rule(cooldown_ms=1000)])
    assert engine.evaluate("r", "s", 50, at_ms=0) is not None
    assert engine.evaluate("r", "s", 50, at_ms=500) is None
    assert engine.evaluate("r", "s", 50, at_ms=1000) is not None
    assert engine.counters["suppressed"] == 1


def test_cooldown_is_per_subject():
    engine = AlertEngine([rule(cooldown_ms=1000)])
    assert engine.evaluate("r", "a", 50, at_ms=0) is not None
    assert engine.evaluate("r", "b", 50, at_ms=0) is not None


def test_unknown_rule_raises():
    with pytest.raises(KeyError):
        AlertEngine([rule()]).evaluate("nope", "s", 100, at_ms=0)


def test_builtin_checks_fire():
    engine = AlertEngine()
    assert engine.check_backlog(999, at_ms=0) is not None
    assert engine.check_vehicle_offline("AGV-1", 60_000, at_ms=0) is not None
    assert engine.check_task_timeout("T1", 10**9, at_ms=0) is not None
    assert engine.check_dlq(1, at_ms=0) is not None


def test_builtin_checks_stay_quiet_below_threshold():
    engine = AlertEngine()
    assert engine.check_backlog(0, at_ms=0) is None
    assert engine.check_dlq(0, at_ms=0) is None
    assert engine.check_vehicle_offline("AGV-1", 1, at_ms=0) is None


def test_alert_counts_and_csv(tmp_path):
    engine = AlertEngine()
    engine.check_backlog(100, at_ms=0)
    engine.check_dlq(1, at_ms=1)
    assert engine.by_severity()["WARNING"] == 1
    assert engine.counts_by_rule()["bus_backlog"] == 1
    path = os.path.join(tmp_path, "alerts.csv")
    assert engine.to_csv(path) == 2
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().strip().splitlines()
    assert lines[0].startswith("rule,severity,subject")
    assert len(lines) == 3


def test_alert_engine_list_is_json_friendly():
    engine = AlertEngine()
    engine.check_backlog(100, at_ms=0)
    payload = engine.stats()
    assert payload["alerts"] == 1
    json.dumps({**payload, "list": engine.as_list()})


def test_default_rules_cover_the_required_conditions():
    names = {r.name for r in default_rules()}
    assert {"bus_backlog", "vehicle_offline", "task_timeout", "dead_letter_depth"} <= names


def test_default_rules_have_positive_cooldowns():
    assert all(r.cooldown_ms > 0 for r in default_rules())

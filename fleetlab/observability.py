"""Observability: structured logs, metrics, distributed tracing and alert rules.

Three independent capabilities live here because they share one requirement —
*every* signal must carry the ``trace_id`` that ties it to the request that
caused it.  That is the whole point of tracing: an operator looking at a stalled
task should be able to pull one id and see the command, the bus hop, the consumer
work, the state change and the response, in order.

Reproducibility note
--------------------
Trace and span ids are generated from a **seeded** PRNG rather than
``uuid4()``/``os.urandom``.  A random id would leak wall-clock entropy into the
report and break the "run the demo twice and diff the metrics" guarantee that CI
enforces.  Ids are still collision-resistant enough for a lab (64 bits) and are
not used as a security boundary.
"""

from __future__ import annotations

import json
import math
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

__all__ = [
    "DeterministicIds",
    "JsonLogger",
    "Metrics",
    "Histogram",
    "Tracer",
    "Span",
    "Alert",
    "AlertEngine",
    "AlertRule",
    "percentile",
]


def percentile(values: list[float], p: float) -> float | None:
    """Nearest-rank percentile.

    Chosen over interpolation deliberately: with a few hundred samples, an
    interpolated p95 reports a number that no request ever observed, which is
    misleading in a latency report.  Nearest-rank always returns a real sample.

    Degenerate inputs are defined rather than accidental: empty -> ``None``,
    ``p <= 0`` -> minimum, ``p >= 100`` -> maximum, single sample -> itself.
    """
    if not values:
        return None
    ordered = sorted(values)
    if p <= 0:
        return ordered[0]
    if p >= 100:
        return ordered[-1]
    rank = math.ceil(p / 100.0 * len(ordered))
    return ordered[max(0, rank - 1)]


class DeterministicIds:
    """Seeded hex id generator (64-bit by default)."""

    def __init__(self, seed: int = 0, bits: int = 64) -> None:
        self._rng = random.Random(seed)
        self._bits = bits
        self._counter = 0

    def next(self) -> str:
        self._counter += 1
        width = self._bits // 4
        return f"{self._rng.getrandbits(self._bits):0{width}x}"

    def child(self, parent: str) -> str:
        """Derive a span id; kept short and deterministic."""
        return self._rng.getrandbits(64).to_bytes(8, "big").hex()


class JsonLogger:
    """Append-only structured log; one JSON object per event.

    Records are kept in memory *and* optionally written to a file, so tests can
    assert on the exact record set without parsing text back out of a stream.
    """

    def __init__(
        self,
        *,
        path: str | None = None,
        stream=None,
        min_level: str = "INFO",
    ) -> None:
        self.records: list[dict] = []
        self.min_level = min_level
        self._path = path
        self._stream = stream
        self._lock = threading.RLock()
        self._levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}

    def _write(self, payload: dict) -> None:
        line = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        if self._stream is not None:
            self._stream.write(line + "\n")
        if self._path is not None:
            with open(self._path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def log(
        self,
        event: str,
        *,
        level: str = "INFO",
        trace_id: str = "",
        span_id: str = "",
        duration_ms: float | None = None,
        at_ms: int | None = None,
        **fields: Any,
    ) -> dict:
        if self._levels.get(level, 20) < self._levels.get(self.min_level, 20):
            return {}
        payload: dict[str, Any] = {
            "event": event,
            "level": level,
            "trace_id": trace_id,
            "span_id": span_id,
        }
        if duration_ms is not None:
            payload["duration_ms"] = round(duration_ms, 3)
        if at_ms is not None:
            payload["at_ms"] = at_ms
        for key in sorted(fields):
            payload[key] = fields[key]
        with self._lock:
            self.records.append(payload)
            self._write(payload)
        return payload

    # -- queries ----------------------------------------------------------

    def by_trace(self, trace_id: str) -> list[dict]:
        return [r for r in self.records if r.get("trace_id") == trace_id]

    def by_event(self, event: str) -> list[dict]:
        return [r for r in self.records if r.get("event") == event]

    def counts_by_event(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records:
            key = record.get("event", "")
            counts[key] = counts.get(key, 0) + 1
        return {k: counts[k] for k in sorted(counts)}

    def __len__(self) -> int:
        return len(self.records)


@dataclass
class Span:
    """One timed operation inside a trace."""

    trace_id: str
    span_id: str
    name: str
    parent_span_id: str = ""
    start_ms: int = 0
    end_ms: int = 0
    duration_ms: float = 0.0
    status: str = "OK"
    attributes: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "duration_ms": round(self.duration_ms, 3),
            "status": self.status,
            "attributes": dict(sorted(self.attributes.items())),
        }


class Tracer:
    """Collects spans and answers "give me everything for this trace id"."""

    def __init__(self, ids: DeterministicIds | None = None, *, max_traces: int = 20_000) -> None:
        self.ids = ids if ids is not None else DeterministicIds(0)
        self.spans: list[Span] = []
        self._by_trace: dict[str, list[Span]] = {}
        self.max_traces = max_traces
        self.counters = {"traces": 0, "spans": 0, "evicted_traces": 0}
        self._lock = threading.RLock()

    def new_trace_id(self) -> str:
        with self._lock:
            self.counters["traces"] += 1
            return self.ids.next()

    @contextmanager
    def span(
        self,
        name: str,
        *,
        trace_id: str = "",
        parent_span_id: str = "",
        at_ms: int = 0,
        **attributes: Any,
    ) -> Iterator[Span]:
        """Time a block, recording real elapsed milliseconds.

        Real time is used (not simulated time) because its purpose is to explain
        *where the wall clock went*; the deterministic metrics used for
        reproducibility are computed from the virtual clock instead.
        """
        resolved_trace = trace_id or self.new_trace_id()
        span = Span(
            trace_id=resolved_trace,
            span_id=self.ids.child(parent_span_id or resolved_trace),
            name=name,
            parent_span_id=parent_span_id,
            start_ms=at_ms,
            attributes=dict(attributes),
        )
        started = time.perf_counter()
        try:
            yield span
        except Exception as exc:  # noqa: BLE001 - record, then let it propagate
            span.status = f"ERROR:{type(exc).__name__}"
            span.attributes["error"] = str(exc)
            raise
        finally:
            span.duration_ms = (time.perf_counter() - started) * 1000.0
            span.end_ms = at_ms
            with self._lock:
                self.spans.append(span)
                bucket = self._by_trace.setdefault(span.trace_id, [])
                bucket.append(span)
                self.counters["spans"] += 1
                if len(self._by_trace) > self.max_traces:
                    oldest = next(iter(self._by_trace))
                    del self._by_trace[oldest]
                    self.counters["evicted_traces"] += 1

    def record(self, span: Span) -> Span:
        """Record a span built outside the context manager."""
        with self._lock:
            self.spans.append(span)
            self._by_trace.setdefault(span.trace_id, []).append(span)
            self.counters["spans"] += 1
        return span

    def trace(self, trace_id: str) -> list[Span]:
        """All spans of a trace, ordered by start time then span id."""
        bucket = self._by_trace.get(trace_id, [])
        return sorted(bucket, key=lambda s: (s.start_ms, s.span_id))

    def trace_dict(self, trace_id: str) -> dict:
        spans = self.trace(trace_id)
        return {
            "trace_id": trace_id,
            "found": bool(spans),
            "span_count": len(spans),
            "total_duration_ms": round(sum(s.duration_ms for s in spans), 3),
            "spans": [s.as_dict() for s in spans],
        }

    def slowest(self, limit: int = 10) -> list[dict]:
        ordered = sorted(self.spans, key=lambda s: (-s.duration_ms, s.span_id))
        return [s.as_dict() for s in ordered[:limit]]

    def stats(self) -> dict:
        with self._lock:
            return {
                "spans": len(self.spans),
                "traces": len(self._by_trace),
                "counters": dict(sorted(self.counters.items())),
            }


@dataclass
class Histogram:
    """Raw observation list plus nearest-rank percentiles.

    Samples are retained rather than bucketed so percentiles are computed from
    real data; the cap exists only to bound memory on long runs, and when it
    trips the oldest samples are dropped (documented, not silent).
    """

    name: str
    samples: list[float] = field(default_factory=list)
    capacity: int = 200_000

    def observe(self, value: float) -> None:
        self.samples.append(float(value))
        if len(self.samples) > self.capacity:
            del self.samples[: len(self.samples) - self.capacity]

    def count(self) -> int:
        return len(self.samples)

    def mean(self) -> float | None:
        return None if not self.samples else sum(self.samples) / len(self.samples)

    def maximum(self) -> float | None:
        return None if not self.samples else max(self.samples)

    def minimum(self) -> float | None:
        return None if not self.samples else min(self.samples)

    def percentile(self, p: float) -> float | None:
        return percentile(self.samples, p)

    def summary(self) -> dict:
        return {
            "count": self.count(),
            "min": _round(self.minimum()),
            "mean": _round(self.mean()),
            "p50": _round(self.percentile(50)),
            "p95": _round(self.percentile(95)),
            "p99": _round(self.percentile(99)),
            "max": _round(self.maximum()),
        }


def _round(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(value, digits)


class Metrics:
    """Counters, gauges and histograms with real percentiles."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.gauges: dict[str, float] = {}
        self.histograms: dict[str, Histogram] = {}
        self._lock = threading.RLock()

    # -- writing ----------------------------------------------------------

    def incr(self, name: str, by: int = 1) -> int:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + by
            return self.counters[name]

    def set_gauge(self, name: str, value: float) -> float:
        with self._lock:
            self.gauges[name] = float(value)
            return float(value)

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            histogram = self.histograms.get(name)
            if histogram is None:
                histogram = Histogram(name)
                self.histograms[name] = histogram
            histogram.observe(value)

    def max_gauge(self, name: str, value: float) -> float:
        """Track a high-water mark; used for backlog depth."""
        with self._lock:
            current = self.gauges.get(name, float("-inf"))
            best = max(current, float(value))
            self.gauges[name] = best
            return best

    # -- reading ----------------------------------------------------------

    def counter(self, name: str) -> int:
        return self.counters.get(name, 0)

    def gauge(self, name: str) -> float | None:
        return self.gauges.get(name)

    def histogram(self, name: str) -> Histogram:
        return self.histograms.get(name, Histogram(name))

    def p95(self, name: str) -> float | None:
        return self.histogram(name).percentile(95)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": {k: self.counters[k] for k in sorted(self.counters)},
                "gauges": {k: _round(self.gauges[k]) for k in sorted(self.gauges)},
                "histograms": {
                    k: self.histograms[k].summary() for k in sorted(self.histograms)
                },
            }


@dataclass
class AlertRule:
    """A threshold and the severity it produces."""

    name: str
    metric: str
    threshold: float
    severity: str = "WARNING"
    comparison: str = ">"  # ">" or "<"
    cooldown_ms: int = 5_000
    description: str = ""

    def breached(self, value: float) -> bool:
        return value > self.threshold if self.comparison == ">" else value < self.threshold


@dataclass
class Alert:
    """One fired alert."""

    rule: str
    severity: str
    subject: str
    message: str
    value: float
    threshold: float
    at_ms: int
    trace_id: str = ""

    def as_dict(self) -> dict:
        return {
            "rule": self.rule,
            "severity": self.severity,
            "subject": self.subject,
            "message": self.message,
            "value": round(self.value, 3),
            "threshold": self.threshold,
            "at_ms": self.at_ms,
            "trace_id": self.trace_id,
        }

    CSV_FIELDS = ("rule", "severity", "subject", "value", "threshold", "at_ms", "message")


class AlertEngine:
    """Evaluates alert rules and suppresses repeats within a cooldown.

    The cooldown is not cosmetic.  Without it, a vehicle that is offline for
    thirty seconds generates one alert per tick, the alert channel becomes the
    outage, and operators learn to ignore it.
    """

    def __init__(self, rules: list[AlertRule] | None = None) -> None:
        self.rules = list(rules) if rules is not None else default_rules()
        self.alerts: list[Alert] = []
        self._last_fired: dict[tuple[str, str], int] = {}
        self.counters = {"evaluated": 0, "fired": 0, "suppressed": 0}

    def rule(self, name: str) -> AlertRule:
        for rule in self.rules:
            if rule.name == name:
                return rule
        raise KeyError(f"unknown alert rule {name!r}")

    def evaluate(
        self,
        rule_name: str,
        subject: str,
        value: float,
        *,
        at_ms: int,
        trace_id: str = "",
        message: str = "",
    ) -> Alert | None:
        """Fire ``rule_name`` for ``subject`` if breached and off cooldown."""
        self.counters["evaluated"] += 1
        rule = self.rule(rule_name)
        if not rule.breached(value):
            return None
        key = (rule.name, subject)
        last = self._last_fired.get(key)
        if last is not None and at_ms - last < rule.cooldown_ms:
            self.counters["suppressed"] += 1
            return None
        self._last_fired[key] = at_ms
        alert = Alert(
            rule=rule.name,
            severity=rule.severity,
            subject=subject,
            message=message or f"{rule.metric} breached",
            value=value,
            threshold=rule.threshold,
            at_ms=at_ms,
            trace_id=trace_id,
        )
        self.alerts.append(alert)
        self.counters["fired"] += 1
        return alert

    # -- built-in checks --------------------------------------------------

    def check_backlog(self, lag: int, *, at_ms: int, trace_id: str = "") -> Alert | None:
        return self.evaluate(
            "bus_backlog",
            subject="bus",
            value=float(lag),
            at_ms=at_ms,
            trace_id=trace_id,
            message=f"consumer lag {lag} exceeds threshold",
        )

    def check_vehicle_offline(
        self, vehicle_id: str, silent_ms: int, *, at_ms: int, trace_id: str = ""
    ) -> Alert | None:
        return self.evaluate(
            "vehicle_offline",
            subject=vehicle_id,
            value=float(silent_ms),
            at_ms=at_ms,
            trace_id=trace_id,
            message=f"{vehicle_id} silent for {silent_ms} ms",
        )

    def check_task_timeout(
        self, task_id: str, age_ms: int, *, at_ms: int, trace_id: str = ""
    ) -> Alert | None:
        return self.evaluate(
            "task_timeout",
            subject=task_id,
            value=float(age_ms),
            at_ms=at_ms,
            trace_id=trace_id,
            message=f"{task_id} unfinished after {age_ms} ms",
        )

    def check_dlq(self, depth: int, *, at_ms: int, trace_id: str = "") -> Alert | None:
        return self.evaluate(
            "dead_letter_depth",
            subject="bus",
            value=float(depth),
            at_ms=at_ms,
            trace_id=trace_id,
            message=f"{depth} message(s) in the dead-letter topic",
        )

    # -- reporting --------------------------------------------------------

    def by_severity(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for alert in self.alerts:
            counts[alert.severity] = counts.get(alert.severity, 0) + 1
        return {k: counts[k] for k in sorted(counts)}

    def counts_by_rule(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for alert in self.alerts:
            counts[alert.rule] = counts.get(alert.rule, 0) + 1
        return {k: counts[k] for k in sorted(counts)}

    def to_csv(self, path: str) -> int:
        """Write the alert list as CSV; returns the number of rows written."""
        import csv

        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(Alert.CSV_FIELDS))
            writer.writeheader()
            for alert in self.alerts:
                row = alert.as_dict()
                writer.writerow({k: row[k] for k in Alert.CSV_FIELDS})
        return len(self.alerts)

    def as_list(self) -> list[dict]:
        """All fired alerts as plain dicts, in firing order."""
        return [alert.as_dict() for alert in self.alerts]

    def stats(self) -> dict:
        return {
            "alerts": len(self.alerts),
            "by_severity": self.by_severity(),
            "by_rule": self.counts_by_rule(),
            "counters": dict(sorted(self.counters.items())),
            "rules": [
                {
                    "name": r.name,
                    "metric": r.metric,
                    "threshold": r.threshold,
                    "severity": r.severity,
                    "comparison": r.comparison,
                }
                for r in self.rules
            ],
        }


def default_rules() -> list[AlertRule]:
    """The alert policy shipped with the demo.

    Thresholds are parameters, not magic numbers buried in a comparison, so they
    can be reviewed in one place.
    """
    return [
        AlertRule(
            "bus_backlog",
            "bus_lag",
            50,
            severity="WARNING",
            cooldown_ms=2_000,
            description="consumer lag above 50 messages",
        ),
        AlertRule(
            "vehicle_offline",
            "vehicle_silence_ms",
            # Matches the heartbeat timeout so that the *first* tick past the
            # threshold alerts; a higher threshold meant detection happened and
            # nothing was ever raised, which looks identical to a broken rule.
            5_000,
            severity="CRITICAL",
            cooldown_ms=20_000,
            description="no heartbeat for more than 5 s",
        ),
        AlertRule(
            "task_timeout",
            "task_age_ms",
            120_000,
            severity="WARNING",
            # Long cooldown on purpose: a task that is merely slow must alert
            # once, not once per tick.  An alert channel that repeats itself is an
            # alert channel operators mute.
            cooldown_ms=300_000,
            description="task not finished within 120 s of creation",
        ),
        AlertRule(
            "dead_letter_depth",
            "dlq_depth",
            0,
            severity="ERROR",
            cooldown_ms=5_000,
            description="any message in the dead-letter topic",
        ),
    ]

"""Guard: every number and test name quoted in the README must be real.

This is the machine check behind the project's "no fabricated numbers" rule.  It

* collects every test name pytest knows about and verifies that each name the
  README cites actually exists — a renamed test leaves the README lying;
* verifies that every file path the README cites exists;
* re-derives the headline figures from the freshly generated
  ``reports/metrics.json`` and compares them against the literal text in the
  README.

Run it after ``fleetlab demo``.  CI runs it, so a stale README fails the build
instead of quietly misleading a reader.

    py -3.12 -m fleetlab demo --out reports
    py -3.12 scripts/check_readme.py
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _collect() -> tuple[set[str], int]:
    """Distinct test function names, and the total number of collected tests."""
    output = subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "--collect-only", "-q", "-o", "addopts="],
        capture_output=True,
        text=True,
        cwd=ROOT,
    ).stdout
    return set(re.findall(r"::(\w+)", output)), sum(1 for line in output.splitlines() if "::" in line)


#: (label, the literal text that must appear in the README, headline key, formatter)
HEADLINE_CLAIMS: list[tuple[str, str, str, str]] = [
    ("baseline completion rate", "0.960", "baseline_completion_rate", "{:.3f}"),
    ("baseline task count", "| 12 | 50 |", "baseline_tasks", "{}"),
    ("baseline completed", "48 |", "baseline_completed", "{}"),
    ("baseline p95 dispatch latency", "59 000", "baseline_dispatch_p95_ms", "{}"),
    ("overload completion rate", "0.457", "overload_completion_rate", "{:.3f}"),
    ("bus lag peak", "| **10** |", "bus_lag_peak", "{}"),
    ("duplicates suppressed", "**110**", "duplicates_suppressed", "{}"),
    ("fault duplicates", "**155**", "fault_scenario_duplicates", "{}"),
    ("fault drops", "**88**", "fault_scenario_drops", "{}"),
    ("fault reordering", "**228**", "fault_scenario_reordered", "{}"),
    ("deadlock breaks", "**4**", "deadlock_scenario_breaks", "{}"),
    ("cache expiries", "**59**", "cache_expiries", "{}"),
]


def main() -> int:
    text = open(os.path.join(ROOT, "README.md"), encoding="utf-8").read()
    problems: list[str] = []

    known, total = _collect()
    cited = set(re.findall(r"`(test_[a-z0-9_]+)`", text))
    print(f"tests collected: {total}; README cites {len(cited)} test names")
    for name in sorted(cited - known):
        problems.append(f"README cites a test that does not exist: {name}")

    claimed = set(re.findall(r"合计\s+(\d+)", text)) | set(
        re.findall(r"\*\*(\d+) 项测试\*\*", text)
    )
    print(f"test-count claim in the README: {sorted(claimed) or 'none'}")
    for value in claimed:
        if value != str(total):
            problems.append(f"README claims {value} tests but pytest collects {total}")

    paths = set(re.findall(r"`((?:fleetlab|tests|scripts|reports|\.github)/[\w./-]+)`", text))
    print(f"paths cited: {len(paths)}")
    for path in sorted(paths):
        if not os.path.exists(os.path.join(ROOT, path)):
            problems.append(f"README cites a missing path: {path}")

    metrics_path = os.path.join(ROOT, "reports", "metrics.json")
    if not os.path.exists(metrics_path):
        problems.append("reports/metrics.json is missing; run `fleetlab demo` first")
    else:
        headline = json.load(open(metrics_path, encoding="utf-8"))["headline"]
        verified = 0
        for label, literal, key, formatter in HEADLINE_CLAIMS:
            value = headline.get(key)
            rendered = formatter.format(value)
            if literal not in text:
                problems.append(f"README is missing the expected text {literal!r} for {label}")
                continue
            # Compare numerically, and against the *rendered* form: the README
            # writes 59 000 for readability and rounds 0.4571 to 0.457, and
            # neither is a different number from the one the run produced.
            tokens = re.findall(r"\d+(?:\.\d+)?", literal.replace(" ", ""))
            target = float(rendered)
            ok = any(abs(float(token) - target) < 1e-9 for token in tokens)
            print(f"  {'OK ' if ok else 'BAD'} {label}: README={literal!r} run={rendered!r}")
            verified += 1
            if not ok:
                problems.append(f"{label}: README says {literal!r}, the run produced {rendered!r}")
        print(f"headline claims verified: {verified}/{len(HEADLINE_CLAIMS)}")

    print()
    if problems:
        print("README cross-check: FAIL")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("README cross-check: OK — every cited test, path and number is real")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

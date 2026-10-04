"""Command line interface.

    py -3.12 -m fleetlab demo --out reports
    py -3.12 -m fleetlab demo --vehicles 8 --tasks 30 --seed 7
    py -3.12 -m fleetlab verify-repro --out reports
    py -3.12 -m fleetlab serve --port 8787
    py -3.12 -m fleetlab fsm
    py -3.12 -m fleetlab scenarios
    py -3.12 -m fleetlab replay --out reports

``demo`` is the one-command path: it runs the scenario suite, writes every
artefact and prints the headline numbers.  ``verify-repro`` is the guarantee:
it runs the suite twice and compares ``metrics.json`` byte for byte, so the claim
"the same command twice gives the same numbers" is checked rather than asserted.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile

from . import __version__

__all__ = ["main", "build_parser"]

#: Keys that legitimately differ between two runs.  Everything else must match.
NONDETERMINISTIC_KEYS = ("seconds", "wall", "elapsed", "timestamp", "generated_at", "duration")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fleetlab",
        description=(
            "fleetlab — event-driven fleet task dispatch and real-time state platform "
            "(synthetic simulation; no real Redis/Kafka/RabbitMQ involved)"
        ),
    )
    parser.add_argument("--version", action="version", version=f"fleetlab {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run the scenario suite and write reports/")
    demo.add_argument("--out", default="reports", help="output directory (default: reports)")
    demo.add_argument("--vehicles", type=int, default=None, help="override the baseline fleet size")
    demo.add_argument("--tasks", type=int, default=None, help="override the baseline task count")
    demo.add_argument("--seed", type=int, default=None, help="override the baseline seed")
    demo.add_argument("--no-plots", action="store_true", help="skip PNG figure generation")
    demo.add_argument("--quiet", action="store_true", help="only print the output directory")

    repro = sub.add_parser("verify-repro", help="run the suite twice and diff metrics.json")
    repro.add_argument("--out", default="reports", help="directory to keep the first run in")
    repro.add_argument("--quiet", action="store_true")

    serve = sub.add_parser("serve", help="serve the HTTP API and SSE stream")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--scenario", default="baseline", help="scenario to load first")
    serve.add_argument("--static", action="store_true", help="do not advance the simulation")

    sub.add_parser("scenarios", help="list the scenario suite")
    sub.add_parser("fsm", help="print the state transition tables")

    replay = sub.add_parser("replay", help="run the suite and verify journal replay")
    replay.add_argument("--out", default=None, help="optional directory for a metrics dump")

    return parser


# ----------------------------------------------------------------------
# demo
# ----------------------------------------------------------------------


def _apply_overrides(configs, args):
    if args.vehicles is None and args.tasks is None and args.seed is None:
        return configs
    import dataclasses

    baseline = configs[0]
    changes = {}
    if args.vehicles is not None:
        changes["vehicles"] = args.vehicles
    if args.tasks is not None:
        changes["tasks"] = args.tasks
    if args.seed is not None:
        changes["seed"] = args.seed
    configs = list(configs)
    configs[0] = dataclasses.replace(baseline, **changes)
    return configs


def cmd_demo(args) -> int:
    from .demo import headline, run_demo, scenario_configs, write_reports

    configs = _apply_overrides(scenario_configs(), args)
    result = run_demo(configs)
    written = write_reports(result, args.out)

    figures: dict[str, str] = {}
    if not args.no_plots:
        try:
            from .plots import available, plot_all

            if available():
                figures = plot_all(result, args.out)
                written = sorted(set(written) | set(figures.values()))
            else:
                print("note: matplotlib unavailable; figures skipped", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001 - figures are optional
            print(f"note: figure generation failed ({type(exc).__name__}: {exc})", file=sys.stderr)

    report_path = os.path.join(args.out, "实验报告.md")
    render_markdown(result, report_path, figures)
    written = sorted(set(written) | {report_path})

    if not args.quiet:
        _print_headline(result, headline(result))
        print()
        print(f"artefacts written to {os.path.abspath(args.out)}:")
        for path in written:
            print(f"  {path}")
    else:
        print(os.path.abspath(args.out))
    return 0


def _print_headline(result, numbers: dict) -> None:
    print("fleetlab demo — synthetic six-hub yard, virtual clock, no external services")
    print()
    print("scenario            ticks  completed  rate    conflicts  lag_peak  consistent")
    for scenario in result.scenarios:
        report = scenario.report
        print(
            "%-18s %6d %8d/%d  %5.3f %10d %9s  %s"
            % (
                scenario.name,
                report["meta"]["ticks"],
                report["tasks"]["completed"],
                report["tasks"]["created"],
                report["tasks"]["completion_rate"],
                report["traffic"]["conflicts"],
                report["bus"]["lag_peak"],
                "yes" if scenario.verdict["consistent"] else "NO",
            )
        )
    print()
    print("headline numbers")
    for key in sorted(numbers):
        print(f"  {key:36s} {numbers[key]}")


# ----------------------------------------------------------------------
# verify-repro
# ----------------------------------------------------------------------


def metrics_digest(path: str) -> str:
    """SHA-256 of a metrics file with line endings normalised to LF.

    The digest is computed over normalised bytes on purpose.  Git on Windows may
    store and check out text with CRLF even when ``.gitattributes`` asks for LF,
    which would make a naive byte comparison of the *file* fail on one CI runner
    for a reason that has nothing to do with reproducibility.  Normalising here
    means the comparison tests the thing that matters — the same numbers, in the
    same order, with the same formatting — and not the checkout's line endings.
    """
    import hashlib

    with open(path, "rb") as handle:
        blob = handle.read().replace(b"\r\n", b"\n")
    return hashlib.sha256(blob).hexdigest()


def cmd_verify_repro(args) -> int:
    """Run the suite twice and compare the deterministic metrics byte for byte."""
    from .demo import deterministic_metrics, run_demo, scenario_configs

    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    payloads = []
    with tempfile.TemporaryDirectory() as scratch:
        for index in (1, 2):
            result = run_demo(scenario_configs())
            blob = json.dumps(
                deterministic_metrics(result), indent=2, sort_keys=True, ensure_ascii=False
            ).encode("utf-8")
            payloads.append(blob)
            with open(os.path.join(scratch, f"run{index}.json"), "wb") as handle:
                handle.write(blob)

    first, second = payloads
    identical = first == second
    if identical:
        with open(os.path.join(out, "metrics.json"), "wb") as handle:
            handle.write(first + b"\n")
        # A committed reference digest lets CI check the *numbers* against a known
        # good run without depending on how any given checkout handled newlines.
        for name in ("metrics.sha256",):
            with open(os.path.join(out, name), "wb") as handle:
                handle.write(
                    f"{metrics_digest(os.path.join(out, 'metrics.json'))}  metrics.json\n".encode(
                        "ascii"
                    )
                )
        compare_path = os.path.join(out, "metrics.committed.sha256")
        if os.path.exists(compare_path):
            committed = open(compare_path, encoding="ascii").read().split()[0]
            current = metrics_digest(os.path.join(out, "metrics.json"))
            if committed != current:
                print(f"committed reference digest: {committed}")
                print(f"regenerated digest:         {current}")
                print("the metrics differ from the committed reference")
                return 1
            print(f"matches the committed reference digest ({committed[:16]}…)")

    differing: list[str] = []
    if not identical:
        left = json.loads(first.decode("utf-8"))
        right = json.loads(second.decode("utf-8"))
        differing = _diff_paths(left, right)

    if not args.quiet:
        print(f"run 1 metrics.json: {len(first)} bytes")
        print(f"run 2 metrics.json: {len(second)} bytes")
        print(f"byte-identical: {identical}")
        if differing:
            print("first differing paths:")
            for path in differing[:20]:
                print(f"  {path}")
    print("REPRODUCIBLE" if identical else "NOT REPRODUCIBLE")
    return 0 if identical else 1


def _diff_paths(left, right, prefix: str = "") -> list[str]:
    """Collect the JSON paths at which two documents disagree."""
    out: list[str] = []
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right)):
            out.extend(_diff_paths(left.get(key), right.get(key), f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            out.append(f"{prefix} (list length {len(left)} vs {len(right)})")
            return out
        for index, (a, b) in enumerate(zip(left, right)):
            out.extend(_diff_paths(a, b, f"{prefix}[{index}]"))
    elif left != right:
        out.append(f"{prefix}: {left!r} != {right!r}")
    return out


# ----------------------------------------------------------------------
# serve
# ----------------------------------------------------------------------


def cmd_serve(args) -> int:
    from .demo import scenario_configs
    from .httpd import serve
    from .sim import Simulation

    configs = {config.name: config for config in scenario_configs()}
    config = configs.get(args.scenario)
    if config is None:
        print(f"unknown scenario {args.scenario!r}; known: {sorted(configs)}", file=sys.stderr)
        return 2
    simulation = Simulation(config)
    simulation.run()
    base = f"http://{args.host}:{args.port}"
    print(f"fleetlab API on {base}")
    print(f"  scenario      {config.name} ({simulation.tick_index} ticks, "
          f"{len(simulation.rt.state.tasks)} tasks)")
    print(f"  GET  {base}/healthz")
    print(f"  GET  {base}/api/tasks")
    print(f"  GET  {base}/api/vehicles")
    print(f"  GET  {base}/api/metrics")
    print(f"  GET  {base}/api/events            (SSE)")
    print(f"  POST {base}/api/commands          {{\"action\":\"PAUSE\",\"task_id\":\"T0001\"}}")
    print("Ctrl-C to stop.")
    serve(simulation, host=args.host, port=args.port, background_ticks=not args.static)
    return 0


# ----------------------------------------------------------------------
# fsm / scenarios / replay
# ----------------------------------------------------------------------


def cmd_fsm(_args) -> int:
    from .task_fsm import (
        TRANSITIONS,
        VEHICLE_TRANSITIONS,
        check_table_consistency,
        transition_table,
    )

    for label, table in (("TASK", TRANSITIONS), ("VEHICLE", VEHICLE_TRANSITIONS)):
        print(f"### {label} state machine")
        print(f"{'from':<14}{'event':<12}{'to'}")
        for row in transition_table(table):
            print(f"{row['from']:<14}{row['event']:<12}{row['to']}")
        problems = check_table_consistency(table)
        print(f"-- consistency problems: {problems or 'none'}")
        print()
    return 0


def cmd_scenarios(_args) -> int:
    from .demo import scenario_configs

    print(f"{'name':<18}{'vehicles':>9}{'tasks':>7}{'interval':>9}  description")
    for config in scenario_configs():
        extra = []
        if config.fault.any_enabled():
            extra.append("faults")
        if config.hold_hub_intersection:
            extra.append("hold-and-wait")
        if config.offline_plan:
            extra.append("offline plan")
        if config.cache_expire_ticks:
            extra.append("cache expiry")
        if config.burst_tasks:
            extra.append(f"burst {config.burst_tasks}")
        print(
            f"{config.name:<18}{config.vehicles:>9}{config.tasks:>7}"
            f"{config.task_interval_ticks:>9}  {', '.join(extra) or 'clean'}"
        )
    return 0


def cmd_replay(args) -> int:
    from .demo import deterministic_metrics, run_demo, scenario_configs

    result = run_demo(scenario_configs())
    all_ok = True
    for scenario in result.scenarios:
        verdict = scenario.verdict
        ok = verdict["consistent"]
        all_ok = all_ok and ok
        print(
            f"{scenario.name:<18} records={verdict['journal_records']:<6} "
            f"digest={verdict['journal_digest'][:16]}  "
            f"live={verdict['live_fingerprint'][:16]}  replay={verdict['replay_fingerprint'][:16]}  "
            f"{'MATCH' if ok else 'MISMATCH'}"
        )
    if args.out:
        os.makedirs(args.out, exist_ok=True)
        blob = json.dumps(
            deterministic_metrics(result), indent=2, sort_keys=True, ensure_ascii=False
        )
        with open(os.path.join(args.out, "metrics.json"), "wb") as handle:
            handle.write((blob + "\n").encode("utf-8"))
        print(f"metrics written to {os.path.abspath(args.out)}")
    print("REPLAY CONSISTENT" if all_ok else "REPLAY MISMATCH")
    return 0 if all_ok else 1


# ----------------------------------------------------------------------
# Markdown report
# ----------------------------------------------------------------------


def render_markdown(result, path: str, figures: dict[str, str]) -> str:
    """Write the human-readable experiment report."""
    from .demo import headline

    numbers = headline(result)
    lines: list[str] = []
    add = lines.append

    add("# fleetlab 实验报告（仿真）")
    add("")
    add("> **本报告的全部数据由程序从固定种子的伪随机数发生器生成，是六枢纽仿真场地上的")
    add("> 合成数据，不是任何真实港口、码头或车队的运营数据。**")
    add("> 运行过程没有连接任何真实的 Redis / Kafka / RabbitMQ / MQTT 服务：")
    add("> 消息总线与状态缓存都是本项目自实现的，Redis 适配层用 fakeredis 验证命令语义。")
    add("")
    add(f"- 场景套件运行耗时（墙钟）：**{result.seconds:.2f} s**（唯一不参与两次运行一致性比对的值）")
    add(f"- Python：{sys.version.split()[0]}")
    add("")

    add("## 1. 场景总览")
    add("")
    add("| 场景 | tick | 任务完成 | 完成率 | 抢占 | 让行冲突 | 积压峰值 | 一致性 |")
    add("|---|---:|---:|---:|---:|---:|---:|:--:|")
    for scenario in result.scenarios:
        report = scenario.report
        add(
            "| `{name}` | {ticks} | {done}/{total} | {rate:.3f} | {pre} | {conf} | {lag} | {ok} |".format(
                name=scenario.name,
                ticks=report["meta"]["ticks"],
                done=report["tasks"]["completed"],
                total=report["tasks"]["created"],
                rate=report["tasks"]["completion_rate"],
                pre=report["dispatch"]["preemptions"],
                conf=report["traffic"]["conflicts"],
                lag=report["bus"]["lag_peak"],
                ok="✅" if scenario.verdict["consistent"] else "❌",
            )
        )
    add("")

    add("## 2. 关键结果（全部来自本次真实运行）")
    add("")
    add("| 指标 | 数值 |")
    add("|---|---:|")
    for key in sorted(numbers):
        value = numbers[key]
        if isinstance(value, dict):
            value = ", ".join(f"{k}={v}" for k, v in sorted(value.items()))
        add(f"| `{key}` | {value} |")
    add("")

    add("## 3. 各场景详情")
    add("")
    for scenario in result.scenarios:
        report = scenario.report
        verdict = scenario.verdict
        add(f"### 3.{result.scenarios.index(scenario) + 1} `{scenario.name}`")
        add("")
        add(f"- 停止原因：{report['meta']['stopped_because']}")
        add(
            "- 任务：创建 {created}，完成 {completed}，失败 {failed}，取消 {cancelled}，"
            "完成率 {rate}".format(
                created=report["tasks"]["created"],
                completed=report["tasks"]["completed"],
                failed=report["tasks"]["failed"],
                cancelled=report["tasks"]["cancelled"],
                rate=report["tasks"]["completion_rate"],
            )
        )
        timing = report["tasks"]["timing"]
        add(
            "- 调度延迟 ms：mean={m} p50={p50} p95={p95} max={mx}".format(
                m=timing["dispatch_latency_ms"]["mean"],
                p50=timing["dispatch_latency_ms"]["p50"],
                p95=timing["dispatch_latency_ms"]["p95"],
                mx=timing["dispatch_latency_ms"]["max"],
            )
        )
        add(
            "- 状态机：转移 {tr} 次，非法转移被拒 {rej} 次".format(
                tr=report["state_machine"]["total_transitions"],
                rej=report["state_machine"]["rejected"],
            )
        )
        add(
            "- 总线：发布 {pub}，背压拒绝 {bp}，积压峰值 {peak}，死信 {dlq}".format(
                pub=report["bus"]["published"],
                bp=report["bus"]["backpressure_rejections"],
                peak=report["bus"]["lag_peak"],
                dlq=report["consumer"]["dlq_routed"],
            )
        )
        add(
            "- 消费端：处理 {proc}，去重抑制 {dup}，乱序检出 {ooo}，gap 回读 {gap}，"
            "nack {nack}".format(
                proc=report["consumer"]["processed"],
                dup=report["consumer"]["duplicates_suppressed"],
                ooo=report["consumer"]["out_of_order_detected"],
                gap=report["consumer"]["gap_recoveries"],
                nack=report["consumer"]["nacked"],
            )
        )
        add(
            "- 故障注入命中：{fired}".format(
                fired=", ".join(f"{k}={v}" for k, v in sorted(report["faults"]["fired"].items()))
            )
        )
        add(
            "- 恢复动作：{rec}".format(
                rec=", ".join(f"{k}={v}" for k, v in sorted(report["recovery"].items()))
            )
        )
        add(
            "- 告警：{alerts}；链路追踪：{traces}".format(
                alerts=", ".join(
                    f"{k}={v}" for k, v in sorted(report["observability"]["alerts"]["by_rule"].items())
                )
                or "无",
                traces=", ".join(
                    f"{k}={v}" for k, v in sorted(report["observability"]["traces"]["counters"].items())
                ),
            )
        )
        add("")
        add("**一致性判定**（判据：{}）".format(verdict["criterion"]))
        add("")
        add("| 检查项 | 结果 |")
        add("|---|:--:|")
        for check, value in sorted(verdict["checks"].items()):
            add(f"| `{check}` | {'✅' if value else '❌'} |")
        add("")
        add(
            "- 生命周​期指纹 live=`{live}` replay=`{rep}`".format(
                live=verdict["live_fingerprint"][:32], rep=verdict["replay_fingerprint"][:32]
            )
        )
        add(f"- 事件日志：{verdict['journal_records']} 条，digest=`{verdict['journal_digest'][:32]}`")
        if verdict["fault_absorption"]:
            add(f"- 故障吸收：{'; '.join(verdict['fault_absorption'])}")
        add("")

    add("## 4. 幂等实验")
    add("")
    command = result.idempotency["command"]
    add("### 4.1 控制指令幂等（同一指令连发 10 次）")
    add("")
    add(f"- 起始状态：`{command['started_from_state']}`")
    add(f"- PAUSE 发送 10 次：applied={command['applied']}，duplicate={command['duplicate']}")
    add(f"- 车辆 `pause_commands` 计数变化：{command['pause_effects']}")
    add(f"- 事件日志中的 PAUSE 记录数：{command['pause_journal_records']}")
    add(f"- RESUME 发送 10 次：applied={command['resume_applied']}，duplicate={command['resume_duplicate']}")
    add(f"- 车辆 `resume_commands` 计数变化：{command['resume_effects']}")
    add(f"- 结论：**{'幂等成立' if command['idempotent'] else '幂等失败'}**")
    add("")
    dlq = result.idempotency["dlq"]
    add("### 4.2 死信队列与重投")
    add("")
    add(f"- 注入毒消息：{dlq['messages_poisoned']} 条")
    add(f"- 首轮后死信深度：{dlq['dlq_depth_after_first_pass']}")
    add(f"- 记录的重试次数：{dlq['attempts_recorded']} 条消息，最大重试 {dlq['max_attempts']}")
    add(f"- 重投条数：{dlq['redriven']}，重投后死信深度：{dlq['dlq_depth_after_redrive']}")
    add(
    f"- 恢复的任务数：{dlq['tasks_applied_before_redrive']} → {dlq['tasks_applied_after_redrive']}"
    )
    add(f"- 结论：**{'死信可恢复' if dlq['recovered'] else '死信未恢复'}**")
    add("")

    add("## 5. 图表")
    add("")
    if figures:
        captions = {
            "state_machine": "任务生命周期状态机（10 状态 / 34 迁移）",
            "vehicle_state_machine": "车辆状态机（11 状态）",
            "gantt": "baseline 场景任务甘特图",
            "backlog": "overload 场景消费积压与未完成任务",
            "scenario_summary": "场景套件对比",
        }
        for key in sorted(figures):
            relative = os.path.basename(figures[key])
            add(f"### {captions.get(key, key)}")
            add("")
            add(f"![{key}]({relative})")
            add("")
    else:
        add("_未生成图表（matplotlib 不可用或使用了 --no-plots）。_")
        add("")

    add("## 6. 一致性判据说明")
    add("")
    add("每条场景结束后执行四项独立检查：")
    add("")
    add("1. **`replay_matches_live`** — 把事件日志按序重放进一个全新的状态对象，")
    add("   比较 `PlatformState.lifecycle_fingerprint()`（任务状态/指派/车辆状态的规范投影的 SHA-256）。")
    add("2. **`no_task_lost`** — 每个 `task_created` 事件对应的任务都存在，且全部到达终态。")
    add("3. **`no_duplicate_effect`** — 没有任何任务到达终态两次，也没有任何任务被完成两次。")
    add("4. **`injected_faults_absorbed`** — 注入器实际触发的每一种故障都有对应的吸收计数：")
    add("   重复→去重抑制，丢失→gap 回读，乱序→乱序检出，崩溃→从提交位点恢复，")
    add("   超时→nack 重试，毒消息→死信 + 重投。")
    add("")
    add("四项全为真才判定该场景「最终一致」。")
    add("")

    text = "\n".join(lines) + "\n"
    # Binary write with explicit LF: text mode would insert CRLF on Windows and
    # make the report differ between the Windows and Linux CI runners.
    with open(path, "wb") as handle:
        handle.write(text.encode("utf-8"))
    return path


# ----------------------------------------------------------------------
# entry point
# ----------------------------------------------------------------------

COMMANDS = {
    "demo": cmd_demo,
    "verify-repro": cmd_verify_repro,
    "serve": cmd_serve,
    "scenarios": cmd_scenarios,
    "fsm": cmd_fsm,
    "replay": cmd_replay,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    handler = COMMANDS.get(args.command)
    if handler is None:  # pragma: no cover - argparse rejects unknown commands first
        parser.error(f"unknown command {args.command!r}")
        return 2
    return handler(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

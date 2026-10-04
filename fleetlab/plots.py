"""Figure generation for ``reports/``.

matplotlib is an *optional* dependency: the platform itself never imports it, and
:func:`available` lets the demo skip figure generation with a warning instead of
crashing.  That matters because the runtime is standard-library only, and a
reporting extra must not become a hard requirement for running the system.

All labels are ASCII on purpose.  A CJK font is present on Windows and absent on
the Ubuntu CI runner, so Chinese labels would render as tofu boxes in CI and the
figure would look broken in exactly the environment that is supposed to verify it.
The prose in the report is Unicode; the figures are not.
"""

from __future__ import annotations

import os

__all__ = [
    "available",
    "plot_state_machine",
    "plot_gantt",
    "plot_backlog",
    "plot_scenario_summary",
    "plot_all",
]

#: Task lifecycle layout: (x, y) per state.  Hand-placed because an automatic
#: layout of a graph this cyclic is unreadable, and the reader of a state diagram
#: needs the happy path to read left to right.
TASK_LAYOUT: dict[str, tuple[float, float]] = {
    "PENDING": (0.0, 2.0),
    "ASSIGNED": (1.9, 2.0),
    "ENROUTE": (3.8, 2.0),
    "QUEUED": (5.7, 2.0),
    "EXECUTING": (7.6, 2.0),
    "COMPLETED": (9.5, 2.0),
    "PAUSED": (4.75, 0.7),
    "RECOVERING": (6.65, 0.7),
    "CANCELLED": (1.9, 0.7),
    "FAILED": (8.55, 0.7),
}

VEHICLE_LAYOUT: dict[str, tuple[float, float]] = {
    "OFFLINE": (0.0, 2.0),
    "IDLE": (1.9, 2.0),
    "ASSIGNED": (3.8, 2.0),
    "ENROUTE": (5.7, 2.0),
    "QUEUED": (7.6, 2.0),
    "EXECUTING": (9.5, 2.0),
    "PAUSED": (6.65, 0.7),
    "RECOVERING": (8.55, 0.7),
    "CHARGING": (1.9, 0.7),
    "FAULT": (3.8, 0.7),
    "MAINTENANCE": (5.7, 0.7),
}

#: States whose boxes are drawn in the "terminal" style.
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}

#: Events drawn as the main flow.  Everything else (cancel, fail, requeue) leaves
#: *every* non-terminal state, so drawing those at full weight buries the happy
#: path under arrows.
PRIMARY_EVENTS = {
    "ASSIGN",
    "DISPATCH",
    "ENQUEUE",
    "PROCEED",
    "START",
    "PAUSE",
    "RESUME",
    "RECOVER",
    "COMPLETE",
    "ONLINE",
    "CHARGE",
    "CHARGE_DONE",
    "REPAIR",
    "REPAIR_DONE",
    "RELEASE",
}


def available() -> bool:
    """Is matplotlib importable?"""
    try:
        import matplotlib  # noqa: F401
    except Exception:  # noqa: BLE001 - any import failure means "not available"
        return False
    return True


def _setup():
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    plt.rcParams["figure.dpi"] = 110
    plt.rcParams["savefig.bbox"] = "tight"
    plt.rcParams["font.size"] = 9
    return plt


def plot_state_machine(
    transitions: dict[str, dict[str, str]],
    path: str,
    *,
    title: str,
    layout: dict[str, tuple[float, float]],
    dynamic: dict[str, list[str]] | None = None,
) -> str:
    """Draw a state machine as labelled boxes and arrows.

    Primary events are drawn solid; cancel / fail / requeue edges are drawn
    faintly and left unlabelled.  They leave *every* non-terminal state, and
    drawing 34 arrows at equal weight produces a hairball in which the happy path
    — the one thing a reader looks for — is invisible.  The full table is in the
    report; this figure is for orientation.

    Dynamic edges (``RESUME`` and ``PROCEED``) are dashed, because their target is
    resolved at runtime from a stack rather than fixed in the table, and a solid
    arrow would misrepresent the design.
    """
    plt = _setup()
    from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

    dynamic = dynamic or {}
    edges: list[tuple[str, str, str, str]] = []
    for source in sorted(transitions):
        for event in sorted(transitions[source]):
            target = transitions[source][event]
            kind = "primary" if event in PRIMARY_EVENTS else "faint"
            if target.startswith("__"):
                for resolved in dynamic.get(source, []):
                    if resolved in layout and resolved != source:
                        edges.append((source, resolved, event, "dynamic"))
                continue
            if target == source or target not in layout:
                continue
            edges.append((source, target, event, kind))

    figure, axes = plt.subplots(figsize=(13.5, 5.2))
    style = {
        "primary": {"color": "#37474f", "linewidth": 1.5, "linestyle": "-", "alpha": 1.0},
        "dynamic": {"color": "#a06000", "linewidth": 1.3, "linestyle": "--", "alpha": 0.95},
        "faint": {"color": "#8d8d8d", "linewidth": 0.75, "linestyle": ":", "alpha": 0.8},
    }
    for state in sorted(layout):
        x, y = layout[state]
        fill = "#ffe0e0" if state in TERMINAL else "#e3f0ff"
        edge = "#b03030" if state in TERMINAL else "#2f6fb0"
        axes.add_patch(
            FancyBboxPatch(
                (x - 0.80, y - 0.25),
                1.60,
                0.50,
                boxstyle="round,pad=0.06,rounding_size=0.12",
                linewidth=1.6,
                edgecolor=edge,
                facecolor=fill,
                zorder=3,
            )
        )
        axes.text(x, y, state, ha="center", va="center", fontsize=9, zorder=4)

    # Faint edges first, so the main flow is drawn over them rather than under.
    for source, target, event, kind in sorted(edges, key=lambda row: row[3] != "faint"):
        sx, sy = layout[source]
        tx, ty = layout[target]
        if abs(sy - ty) < 1e-9 and tx > sx:
            start, end, rad = (sx + 0.80, sy), (tx - 0.80, ty), 0.0
        elif abs(sy - ty) < 1e-9:
            start, end, rad = (sx, sy - 0.25), (tx, ty - 0.25), 0.40
        elif ty < sy:
            start, end, rad = (sx, sy - 0.25), (tx, ty + 0.25), -0.16 if tx >= sx else 0.30
        else:
            start, end, rad = (sx, sy + 0.25), (tx, ty - 0.25), 0.24

        axes.add_patch(
            FancyArrowPatch(
                start,
                end,
                connectionstyle=f"arc3,rad={rad}",
                arrowstyle="-|>",
                mutation_scale=10 if kind != "faint" else 7,
                shrinkA=1.0,
                shrinkB=1.0,
                zorder=2 if kind == "faint" else 5,
                **style[kind],
            )
        )
        if kind == "faint":
            continue
        mx, my = (start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0
        offset_y = 0.15 if abs(sy - ty) < 1e-9 else (0.13 if rad >= 0 else -0.13)
        axes.text(
            mx,
            my + offset_y,
            event,
            ha="center",
            va="center",
            fontsize=7,
            color="#333333",
            bbox={"boxstyle": "round,pad=0.12", "facecolor": "white", "edgecolor": "none"},
            zorder=6,
        )

    axes.text(
        -1.4,
        -0.42,
        "solid = main flow      dashed = target resolved from a stack (RESUME / PROCEED)      "
        "dotted (unlabelled) = cancel / fail / requeue, which leave every non-terminal state",
        fontsize=7.5,
        color="#555555",
        va="bottom",
        ha="left",
    )
    axes.set_title(title, fontsize=11.5)
    axes.set_xlim(-1.4, 10.8)
    axes.set_ylim(-0.55, 2.95)
    axes.axis("off")
    figure.savefig(path)
    plt.close(figure)
    return path
def plot_gantt(timeline: list[dict], path: str, *, title: str, limit: int = 28) -> str:
    """Draw task lifespans as a Gantt chart.

    Each row is one task; the bar starts at creation, the darker segment is the
    time a vehicle was actually occupied, and the marker shows the assignment.  A
    bar that is long and light means a task that spent most of its life waiting —
    which is what a dispatch-latency problem looks like before anyone computes a
    percentile.
    """
    plt = _setup()
    from matplotlib.patches import Patch

    rows = sorted(timeline, key=lambda r: (r["created_ms"], r["task_id"]))[:limit]
    if not rows:
        return path
    colours = {
        "COMPLETED": "#2e7d32",
        "FAILED": "#c62828",
        "CANCELLED": "#6d4c41",
    }
    figure, axes = plt.subplots(figsize=(11.0, max(3.2, 0.26 * len(rows) + 1.2)))
    labels: list[str] = []
    for index, row in enumerate(rows):
        y = len(rows) - index - 1
        labels.append(f"{row['task_id']}  P{row['priority']}")
        created = row["created_ms"]
        end = row["finished_ms"] or max(
            (r["finished_ms"] or r["created_ms"] for r in rows), default=created
        )
        axes.barh(y, (end - created) / 1000.0, left=created / 1000.0, height=0.5,
                  color="#cfd8dc", edgecolor="#90a4ae", linewidth=0.4)
        if row["assigned_ms"]:
            service_end = row["finished_ms"] or end
            axes.barh(
                y,
                (service_end - row["assigned_ms"]) / 1000.0,
                left=row["assigned_ms"] / 1000.0,
                height=0.5,
                color=colours.get(row["state"], "#546e7a"),
            )
            axes.plot(row["assigned_ms"] / 1000.0, y, marker="|", color="#0d47a1", markersize=8)
        if row["started_ms"]:
            axes.plot(row["started_ms"] / 1000.0, y, marker="o", color="#ff8f00", markersize=3)
        if row["deadline_ms"]:
            axes.plot(row["deadline_ms"] / 1000.0, y, marker="x", color="#b71c1c", markersize=4)

    axes.set_yticks(range(len(rows)))
    axes.set_yticklabels(list(reversed(labels)), fontsize=6.5)
    axes.set_xlabel("simulated time (s)")
    axes.set_title(title, fontsize=11)
    axes.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
    legend = [
        Patch(facecolor="#cfd8dc", edgecolor="#90a4ae", label="created -> terminal"),
        Patch(facecolor="#2e7d32", label="completed (vehicle occupied)"),
        Patch(facecolor="#c62828", label="failed (deadline)"),
        plt.Line2D([], [], color="#0d47a1", marker="|", linestyle="none", label="assigned"),
        plt.Line2D([], [], color="#ff8f00", marker="o", linestyle="none", label="started"),
        plt.Line2D([], [], color="#b71c1c", marker="x", linestyle="none", label="deadline"),
    ]
    axes.legend(handles=legend, fontsize=6.5, loc="lower right", framealpha=0.9)
    figure.savefig(path)
    plt.close(figure)
    return path


def plot_backlog(series: list[list[int]], path: str, *, title: str, limit: int | None = None) -> str:
    """Plot consumer lag and open-task count over simulated time."""
    plt = _setup()

    rows = series if limit is None else series[:limit]
    if not rows:
        return path
    ticks = [row[0] for row in rows]
    lag = [row[1] for row in rows]
    open_tasks = [row[2] for row in rows]
    figure, axes = plt.subplots(figsize=(11.0, 3.4))
    axes.plot(ticks, lag, color="#c62828", linewidth=1.2, label="consumer lag (messages)")
    axes.fill_between(ticks, lag, color="#c62828", alpha=0.15)
    axes.set_xlabel("tick (1 tick = 1 simulated second)")
    axes.set_ylabel("lag", color="#c62828")
    axes.tick_params(axis="y", labelcolor="#c62828")
    axes.grid(linestyle=":", linewidth=0.5, alpha=0.6)
    twin = axes.twinx()
    twin.plot(ticks, open_tasks, color="#1565c0", linewidth=1.1, linestyle="--",
              label="open tasks")
    twin.set_ylabel("open tasks", color="#1565c0")
    twin.tick_params(axis="y", labelcolor="#1565c0")
    axes.set_title(title, fontsize=11)
    lines = axes.get_lines() + twin.get_lines()
    axes.legend(lines, [line.get_label() for line in lines], fontsize=7.5, loc="upper right")
    figure.savefig(path)
    plt.close(figure)
    return path


def plot_scenario_summary(reports: dict[str, dict], path: str, *, title: str) -> str:
    """Bar chart comparing the headline numbers of every scenario."""
    plt = _setup()
    import numpy as np

    names = sorted(reports)
    rates = [reports[n]["tasks"]["completion_rate"] for n in names]
    conflicts = [reports[n]["traffic"]["conflicts"] for n in names]
    peaks = [reports[n]["bus"]["lag_peak"] or 0 for n in names]

    figure, axes = plt.subplots(1, 3, figsize=(12.5, 3.4))
    positions = np.arange(len(names))
    axes[0].bar(positions, rates, color="#2e7d32")
    axes[0].set_title("task completion rate", fontsize=9.5)
    axes[0].set_ylim(0, 1.05)

    axes[1].bar(positions, conflicts, color="#ef6c00")
    axes[1].set_title("give-way conflicts resolved", fontsize=9.5)

    axes[2].bar(positions, peaks, color="#c62828")
    axes[2].set_title("bus lag peak (messages)", fontsize=9.5)

    for axis in axes:
        axis.set_xticks(positions)
        axis.set_xticklabels(names, rotation=32, ha="right", fontsize=7.5)
        axis.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.6)
    figure.suptitle(title, fontsize=11)
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)
    return path


def plot_all(result, out_dir: str = "reports") -> dict[str, str]:
    """Render every figure for a demo run; returns ``{name: path}``.

    Skips gracefully (returning an empty mapping) when matplotlib is missing, so
    ``py -3.12 -m fleetlab demo`` still produces metrics and prose on a machine
    without the plotting extra.
    """
    if not available():
        return {}
    from .task_fsm import TRANSITIONS, VEHICLE_TRANSITIONS, transition_table

    os.makedirs(out_dir, exist_ok=True)
    written: dict[str, str] = {}
    written["state_machine"] = plot_state_machine(
        TRANSITIONS,
        os.path.join(out_dir, "state_machine.png"),
        title=(
            f"fleetlab task lifecycle ({len(set(TRANSITIONS))} states, "
            f"{len(transition_table())} transitions)"
        ),
        layout=TASK_LAYOUT,
        dynamic={
            "RECOVERING": ["ASSIGNED", "ENROUTE", "QUEUED", "EXECUTING"],
            "QUEUED": ["ASSIGNED", "ENROUTE", "EXECUTING"],
        },
    )
    written["vehicle_state_machine"] = plot_state_machine(
        VEHICLE_TRANSITIONS,
        os.path.join(out_dir, "vehicle_state_machine.png"),
        title=(
            f"fleetlab vehicle lifecycle ({len(set(VEHICLE_TRANSITIONS))} states, "
            f"{sum(len(e) for e in VEHICLE_TRANSITIONS.values())} transitions)"
        ),
        layout=VEHICLE_LAYOUT,
    )
    baseline = result.by_name("baseline").report
    overload = result.by_name("overload").report
    written["gantt"] = plot_gantt(
        baseline["timeline"],
        os.path.join(out_dir, "gantt.png"),
        title="baseline scenario: task timeline (first 28 tasks)",
    )
    written["backlog"] = plot_backlog(
        overload["series"]["lag"],
        os.path.join(out_dir, "bus_backlog.png"),
        title="overload scenario: consumer lag and open tasks under a 30-task burst",
    )
    written["scenario_summary"] = plot_scenario_summary(
        {s.name: s.report for s in result.scenarios},
        os.path.join(out_dir, "scenario_summary.png"),
        title="fleetlab scenario suite",
    )
    return written

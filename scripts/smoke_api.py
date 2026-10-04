"""Call every HTTP endpoint for real and print the responses.

Standard library only (``urllib``), so it runs anywhere the platform runs.  This
is the script the README quotes, which means the response fragments in the README
are literal output rather than something typed by hand.

    py -3.12 scripts/smoke_api.py            # ephemeral port, prints everything
    py -3.12 scripts/smoke_api.py --port 8787
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fleetlab.demo import scenario_configs  # noqa: E402
from fleetlab.httpd import make_api  # noqa: E402
from fleetlab.sim import Simulation  # noqa: E402


def call(base: str, method: str, path: str, body: dict | None = None, timeout: float = 10.0):
    """Perform one request and return ``(status, parsed_body, raw_text)``."""
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(base + path, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        status = exc.code
    try:
        parsed = json.loads(raw)
    except ValueError:
        parsed = None
    return status, parsed, raw


def show(label: str, method: str, path: str, status: int, raw: str, limit: int = 460) -> None:
    snippet = raw if len(raw) <= limit else raw[:limit] + " ...}"
    print(f"\n$ curl -s -X {method} {path}")
    print(f"# {label}  -> HTTP {status}")
    print(snippet)


def wait_for_state(base: str, task_id: str, wanted: set[str], timeout: float) -> str:
    """Poll until the task reaches one of ``wanted`` states; return what we saw."""
    deadline = time.time() + timeout
    state = "UNKNOWN"
    while time.time() < deadline:
        _status, parsed, _raw = call(base, "GET", f"/api/tasks/{task_id}")
        state = (parsed or {}).get("state", "UNKNOWN")
        if state in wanted or state in {"COMPLETED", "FAILED", "CANCELLED"}:
            return state
        time.sleep(0.1)
    return state


def read_sse(base: str, seconds: float = 2.0, max_events: int = 4) -> list[str]:
    """Read a few SSE frames and return them as raw strings."""
    request = urllib.request.Request(base + "/api/events", method="GET")
    request.add_header("Accept", "text/event-stream")
    frames: list[str] = []
    buffer = ""
    with urllib.request.urlopen(request, timeout=seconds + 5.0) as response:
        deadline = time.time() + seconds
        while time.time() < deadline and len(frames) < max_events:
            chunk = response.read(1)
            if not chunk:
                break
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n\n" in buffer:
                frame, buffer = buffer.split("\n\n", 1)
                if frame.strip():
                    frames.append(frame)
    return frames


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0, help="0 = ephemeral port")
    parser.add_argument("--scenario", default="baseline")
    args = parser.parse_args()

    configs = {c.name: c for c in scenario_configs()}
    simulation = Simulation(configs[args.scenario])
    simulation.run()

    httpd = None
    api, httpd = make_api(simulation, host="127.0.0.1", port=args.port)
    port = httpd.server_address[1]
    base = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    thread.start()
    # Give the stream something to say: advance the yard in the background.
    stop = threading.Event()

    def ticker() -> None:
        while not stop.is_set():
            time.sleep(0.05)
            with simulation.rt.lock:
                simulation.tick()
            api._broadcast("snapshot", {"ticks": simulation.tick_index})

    tick = threading.Thread(target=ticker, daemon=True)
    tick.start()

    print(f"fleetlab API smoke test against {base} (scenario={args.scenario})")
    print("=" * 78)
    try:
        for label, method, path, body in (
            ("liveness", "GET", "/healthz", None),
            ("summary of every task", "GET", "/api/tasks", None),
            ("one task", "GET", "/api/tasks/T0001", None),
            ("fleet state", "GET", "/api/vehicles", None),
            ("one vehicle", "GET", "/api/vehicles/AGV-01", None),
            ("metrics", "GET", "/api/metrics", None),
            ("alerts", "GET", "/api/alerts", None),
            ("bus state", "GET", "/api/bus", None),
            ("bad action -> 400", "POST", "/api/commands", {"action": "EXPLODE", "task_id": "T0001"}),
            ("unknown task -> 404", "GET", "/api/tasks/NOPE", None),
            ("unknown route -> 404", "GET", "/api/nothing", None),
            ("wrong method -> 405", "POST", "/api/metrics", None),
        ):
            status, _parsed, raw = call(base, method, path, body)
            show(label, method, path, status, raw)

        # Create a task and use *its* id from here on.  Hardcoding an id made the
        # script silently exercise the 404 path instead of the cancel path, which
        # is exactly the kind of "test that passes while testing nothing" this
        # project keeps finding.
        status, created, raw = call(
            base,
            "POST",
            "/api/tasks",
            {"origin": [60, 60], "destination": [540, 340], "priority": 4, "payload_kg": 800},
        )
        show("create a task", "POST", "/api/tasks", status, raw)
        new_id = (created or {}).get("task_id", "")

        # Wait for the dispatcher to actually pick it up.  Pausing a task that is
        # still PENDING is correctly rejected by the state machine (there is no
        # assignment to pause), so a demo that pauses immediately would only ever
        # show the rejection path.
        assigned = wait_for_state(base, new_id, {"ASSIGNED", "ENROUTE", "QUEUED", "EXECUTING"}, 6.0)
        print(f"\n# waited for dispatch: {new_id} state={assigned}")

        status, _parsed, raw = call(base, "POST", "/api/commands",
                                    {"action": "PAUSE", "task_id": new_id, "command_id": "SMOKE-PAUSE-1"})
        show("pause the in-flight task", "POST", "/api/commands", status, raw)
        status, _parsed, raw = call(base, "POST", "/api/commands",
                                    {"action": "PAUSE", "task_id": new_id, "command_id": "SMOKE-PAUSE-1"})
        show("pause again with the SAME command id -> one effect only", "POST", "/api/commands", status, raw)

        status, _parsed, raw = call(base, "DELETE", f"/api/tasks/{new_id}")
        show("cancel it", "DELETE", f"/api/tasks/{new_id}", status, raw)
        status, _parsed, raw = call(base, "DELETE", f"/api/tasks/{new_id}")
        show("cancel it again -> duplicate, no second effect", "DELETE", f"/api/tasks/{new_id}", status, raw)

        # And show the honest failure mode: commanding an already-terminal task.
        status, _parsed, raw = call(base, "POST", "/api/commands",
                                    {"action": "PAUSE", "task_id": "T0001", "command_id": "SMOKE-PAUSE-DONE"})
        show("pause an already-COMPLETED task -> unconfirmed (409)", "POST", "/api/commands", status, raw)

        trace_id = None
        for record in simulation.rt.journal:
            if record.trace_id and record.kind in ("task_assigned", "task_created"):
                trace_id = record.trace_id
                break
        if trace_id:
            status, _parsed, raw = call(base, "GET", f"/api/traces/{trace_id}")
            show(f"one trace id spanning the whole chain (trace_id={trace_id})", "GET",
                 f"/api/traces/{trace_id}", status, raw, limit=900)

        print("\n$ curl -sN http://127.0.0.1:%d/api/events" % port)
        print("# SSE stream, first frames:")
        frames = read_sse(base, seconds=2.0, max_events=4)
        for frame in frames:
            print("  " + frame.replace("\n", "\n  "))
        print(f"# received {len(frames)} SSE frame(s); api counters={api.counters}")
    finally:
        stop.set()
        httpd.shutdown()
        httpd.server_close()
    print("\n" + "=" * 78)
    print("all endpoints responded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

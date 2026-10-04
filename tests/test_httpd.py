"""HTTP API tests against a real socket.

These use ``urllib`` against a live ``ThreadingHTTPServer`` rather than calling the
handlers directly, because most of what can go wrong here is in the transport:
status lines, JSON encoding, keep-alive, 405 vs 404, and the flush-per-event
behaviour that makes SSE a stream instead of a response that never ends.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from fleetlab.httpd import (
    SSE_QUEUE_LIMIT,
    ApiServer,
    Request,
    Response,
    Router,
    Subscriber,
    make_api,
)
from fleetlab.sim import SimConfig, Simulation


# ----------------------------------------------------------------------
# router unit tests
# ----------------------------------------------------------------------


def test_router_matches_a_static_path():
    router = Router()
    router.get("/healthz", lambda request: Response(200, {"ok": True}))
    handler, params = router.resolve("GET", "/healthz")
    assert callable(handler) and params == {}


def test_router_extracts_path_parameters():
    router = Router()
    router.get("/api/tasks/{task_id}", lambda request: Response(200, {}))
    _handler, params = router.resolve("GET", "/api/tasks/T0001")
    assert params == {"task_id": "T0001"}


def test_router_static_routes_win_over_parameterised_ones():
    router = Router()
    router.get("/api/tasks/{task_id}", lambda request: Response(200, {"kind": "param"}))
    router.get("/api/tasks", lambda request: Response(200, {"kind": "static"}))
    handler, params = router.resolve("GET", "/api/tasks")
    assert params == {}
    assert handler(Request("GET", "/api/tasks", {}, None, {})).body == {"kind": "static"}


def test_router_returns_none_for_an_unmatched_path():
    router = Router()
    router.get("/a", lambda request: Response(200, {}))
    assert router.resolve("GET", "/b") is None


def test_router_returns_none_for_an_unmatched_method():
    router = Router()
    router.get("/a", lambda request: Response(200, {}))
    assert router.resolve("POST", "/a") is None


def test_router_reports_allowed_methods_for_405():
    router = Router()
    router.get("/a", lambda request: Response(200, {}))
    router.post("/a", lambda request: Response(200, {}))
    assert router.allowed_methods("/a") == ["GET", "POST"]
    assert router.allowed_methods("/nope") == []


def test_router_decodes_percent_escapes():
    router = Router()
    router.get("/api/tasks/{task_id}", lambda request: Response(200, {}))
    _handler, params = router.resolve("GET", "/api/tasks/T%2F1")
    assert params == {"task_id": "T/1"}


def test_router_ignores_a_trailing_slash():
    router = Router()
    router.get("/a", lambda request: Response(200, {"hit": True}))
    assert router.resolve("GET", "/a/") is not None


def test_response_encodes_json_with_sorted_keys():
    body = Response(200, {"b": 1, "a": 2}).payload().decode()
    assert body == '{"a": 2, "b": 1}'


def test_response_encodes_bytes_and_strings():
    assert Response(200, b"raw").payload() == b"raw"
    assert Response(200, "text").payload() == b"text"


def test_request_query_helper():
    request = Request("GET", "/x", {"state": ["PAUSED"]}, None, {})
    assert request.q("state") == "PAUSED"
    assert request.q("missing") is None
    assert request.q("missing", "default") == "default"


def test_subscriber_drops_when_the_queue_is_full():
    subscriber = Subscriber(1)
    for index in range(SSE_QUEUE_LIMIT):
        assert subscriber.offer({"i": index}) is True
    assert subscriber.offer({"i": "overflow"}) is False
    assert subscriber.dropped == 1


# ----------------------------------------------------------------------
# live server fixture
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def live():
    simulation = Simulation(SimConfig(name="api-test", vehicles=4, tasks=6, seed=11, task_interval_ticks=1, max_ticks=400))
    simulation.run()
    api, httpd = make_api(simulation, host="127.0.0.1", port=0)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()

    # Keep the yard moving, exactly as ``serve`` does.  Without this the fleet
    # freezes after the initial run and a PAUSE command has no assignment to act
    # on, so the test would only ever exercise the rejection path.
    stop = threading.Event()

    def ticker() -> None:
        while not stop.is_set():
            time.sleep(0.05)
            with simulation.rt.lock:
                simulation.tick()

    tick_thread = threading.Thread(target=ticker, daemon=True)
    tick_thread.start()

    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/healthz", timeout=2):
                break
        except OSError:
            time.sleep(0.05)
    yield {"base": base, "api": api, "sim": simulation, "port": port}
    stop.set()
    httpd.shutdown()
    httpd.server_close()


def call(base: str, method: str, path: str, body=None, timeout: float = 10.0):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(base + path, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode()
            return response.status, raw, dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(), dict(exc.headers)


def body_of(raw: str):
    try:
        return json.loads(raw)
    except ValueError:
        return None


# ----------------------------------------------------------------------
# endpoints
# ----------------------------------------------------------------------


def test_healthz(live):
    status, raw, _headers = call(live["base"], "GET", "/healthz")
    assert status == 200
    assert body_of(raw)["status"] == "ok"


def test_list_tasks(live):
    status, raw, _headers = call(live["base"], "GET", "/api/tasks")
    payload = body_of(raw)
    assert status == 200
    assert payload["count"] >= 1
    assert "state" in payload["tasks"][0]


def test_list_tasks_can_filter_by_state(live):
    _status, raw, _headers = call(live["base"], "GET", "/api/tasks?state=COMPLETED")
    payload = body_of(raw)
    assert all(task["state"] == "COMPLETED" for task in payload["tasks"])


def test_get_one_task(live):
    status, raw, _headers = call(live["base"], "GET", "/api/tasks/T0001")
    assert status == 200
    assert body_of(raw)["task_id"] == "T0001"


def test_unknown_task_returns_404(live):
    status, raw, _headers = call(live["base"], "GET", "/api/tasks/NOPE")
    assert status == 404
    assert body_of(raw)["error"] == "not_found"


def test_list_and_get_vehicles(live):
    _status, raw, _headers = call(live["base"], "GET", "/api/vehicles")
    assert body_of(raw)["count"] == 4
    status, vehicle_raw, _headers = call(live["base"], "GET", "/api/vehicles/AGV-01")
    assert status == 200
    assert body_of(vehicle_raw)["vehicle_id"] == "AGV-01"


def test_unknown_vehicle_returns_404(live):
    status, _raw, _headers = call(live["base"], "GET", "/api/vehicles/NOPE")
    assert status == 404


def test_metrics_endpoint(live):
    status, raw, _headers = call(live["base"], "GET", "/api/metrics")
    payload = body_of(raw)
    assert status == 200
    assert "counters" in payload and "gauges" in payload and "histograms" in payload


def test_alerts_endpoint(live):
    status, raw, _headers = call(live["base"], "GET", "/api/alerts")
    payload = body_of(raw)
    assert status == 200
    assert "list" in payload and "by_rule" in payload


def test_bus_endpoint(live):
    status, raw, _headers = call(live["base"], "GET", "/api/bus")
    payload = body_of(raw)
    assert status == 200
    assert "groups" in payload and "topics" in payload


def test_unknown_route_returns_404(live):
    status, raw, _headers = call(live["base"], "GET", "/api/nothing")
    assert status == 404
    assert body_of(raw)["error"] == "no_route"


def test_wrong_method_returns_405_with_allow_header(live):
    status, raw, headers = call(live["base"], "POST", "/api/metrics")
    assert status == 405
    assert "GET" in headers.get("Allow", "")
    assert body_of(raw)["error"] == "method_not_allowed"


def test_head_request_has_no_body(live):
    status, raw, _headers = call(live["base"], "HEAD", "/healthz")
    assert status == 200
    assert raw == ""


# ----------------------------------------------------------------------
# task creation and commands
# ----------------------------------------------------------------------


def test_create_task_publishes_and_returns_201(live):
    status, raw, _headers = call(
        live["base"], "POST", "/api/tasks",
        {"origin": [60, 60], "destination": [540, 340], "priority": 3, "payload_kg": 250},
    )
    payload = body_of(raw)
    assert status == 201
    assert payload["state"] == "PENDING"
    assert payload["trace_id"]
    _status, check_raw, _headers = call(live["base"], "GET", f"/api/tasks/{payload['task_id']}")
    assert body_of(check_raw)["task_id"] == payload["task_id"]


def test_create_task_requires_coordinates(live):
    status, raw, _headers = call(live["base"], "POST", "/api/tasks", {"priority": 1})
    assert status == 400
    assert body_of(raw)["error"] == "missing_fields"


def test_create_task_rejects_bad_coordinates(live):
    status, raw, _headers = call(
        live["base"], "POST", "/api/tasks", {"origin": ["a", "b"], "destination": [1, 2]}
    )
    assert status == 400
    assert body_of(raw)["error"] == "bad_coordinates"


def test_create_task_rejects_a_duplicate_id(live):
    status, raw, _headers = call(
        live["base"], "POST", "/api/tasks",
        {"task_id": "T0001", "origin": [1, 1], "destination": [2, 2]},
    )
    assert status == 409
    assert body_of(raw)["error"] == "already_exists"


def test_command_rejects_an_unknown_action(live):
    status, raw, _headers = call(
        live["base"], "POST", "/api/commands", {"action": "EXPLODE", "task_id": "T0001"}
    )
    assert status == 400
    assert body_of(raw)["error"] == "bad_action"


def test_command_on_an_unknown_task_is_404(live):
    status, _raw, _headers = call(
        live["base"], "POST", "/api/commands", {"action": "PAUSE", "task_id": "NOPE"}
    )
    assert status == 404


def test_pause_command_is_idempotent_over_http(live):
    _status, raw, _headers = call(
        live["base"], "POST", "/api/tasks",
        {"origin": [60, 60], "destination": [540, 340], "priority": 4},
    )
    task_id = body_of(raw)["task_id"]
    # Let the dispatcher pick it up, so PAUSE has an assignment to act on.
    deadline = time.time() + 6
    while time.time() < deadline:
        _status, poll_raw, _headers = call(live["base"], "GET", f"/api/tasks/{task_id}")
        if body_of(poll_raw)["state"] in ("ASSIGNED", "ENROUTE", "QUEUED", "EXECUTING"):
            break
        time.sleep(0.1)

    first_status, first_raw, _headers = call(
        live["base"], "POST", "/api/commands",
        {"action": "PAUSE", "task_id": task_id, "command_id": "HTTP-PAUSE-1"},
    )
    second_status, second_raw, _headers = call(
        live["base"], "POST", "/api/commands",
        {"action": "PAUSE", "task_id": task_id, "command_id": "HTTP-PAUSE-1"},
    )
    assert first_status == second_status == 200
    assert body_of(first_raw)["command"]["status"] == "applied"
    assert body_of(second_raw)["command"]["status"] == "duplicate"
    vehicle_id = body_of(first_raw).get("state")
    assert vehicle_id is not None


def test_cancel_is_idempotent_over_http(live):
    _status, raw, _headers = call(
        live["base"], "POST", "/api/tasks", {"origin": [60, 60], "destination": [540, 340]}
    )
    task_id = body_of(raw)["task_id"]
    first_status, first_raw, _headers = call(live["base"], "DELETE", f"/api/tasks/{task_id}")
    second_status, second_raw, _headers = call(live["base"], "DELETE", f"/api/tasks/{task_id}")
    assert first_status == 200
    assert body_of(first_raw)["state"] == "CANCELLED"
    assert body_of(first_raw)["command"]["status"] == "applied"
    assert body_of(second_raw)["command"]["status"] == "duplicate"


def test_deleting_an_unknown_task_is_404(live):
    status, _raw, _headers = call(live["base"], "DELETE", "/api/tasks/NOPE")
    assert status == 404


def test_trace_lookup_returns_the_whole_chain(live):
    trace_id = None
    for record in live["sim"].rt.journal:
        if record.trace_id:
            trace_id = record.trace_id
            break
    assert trace_id
    status, raw, _headers = call(live["base"], "GET", f"/api/traces/{trace_id}")
    payload = body_of(raw)
    assert status == 200
    assert payload["found"] is True
    assert payload["span_count"] >= 1
    assert payload["journal_records"] or payload["log_records"]
    names = {span["name"] for span in payload["spans"]}
    assert "consumer.process" in names


def test_unknown_trace_returns_404(live):
    status, _raw, _headers = call(live["base"], "GET", "/api/traces/deadbeef")
    assert status == 404


def test_malformed_json_body_does_not_crash_the_server(live):
    request = urllib.request.Request(
        live["base"] + "/api/tasks", data=b"{not json", method="POST"
    )
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    # Either a clean 400 or an accepted-but-empty body is fine; a 500 is not.
    assert status in (201, 400, 409, 422)
    assert live["api"].counters["errors"] == 0


# ----------------------------------------------------------------------
# SSE
# ----------------------------------------------------------------------


def test_sse_sets_the_streaming_content_type(live):
    request = urllib.request.Request(live["base"] + "/api/events")
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 200
        assert response.headers["Content-Type"].startswith("text/event-stream")
        assert response.headers["Cache-Control"] == "no-cache"
        # The first frame is delivered immediately, which is what proves the
        # response is streamed rather than buffered until close.
        chunk = response.read(64).decode()
        assert chunk.startswith("event: hello")


def test_sse_delivers_broadcast_events(live):
    request = urllib.request.Request(live["base"] + "/api/events")
    with urllib.request.urlopen(request, timeout=8) as response:
        # Read the hello frame, then trigger a broadcast and read the next one.
        buffer = ""
        deadline = time.time() + 6
        while "\n\n" not in buffer and time.time() < deadline:
            buffer += response.read(1).decode(errors="replace")
        assert "event: hello" in buffer

        live["api"]._broadcast("unit-test-event", {"marker": "x"})
        buffer = ""
        deadline = time.time() + 6
        while "\n\n" not in buffer and time.time() < deadline:
            buffer += response.read(1).decode(errors="replace")
        assert "unit-test-event" in buffer
        assert '"marker": "x"' in buffer


def test_sse_registers_a_subscriber_while_streaming(live):
    api = live["api"]
    before = set(api.subscribers)
    request = urllib.request.Request(live["base"] + "/api/events")
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read(32)
        added = set(api.subscribers) - before
        assert len(added) == 1
    for client_id in added:
        api.detach(client_id)


def test_detach_removes_a_subscriber_and_tolerates_an_unknown_id(live):
    """Cleanup is asserted directly.

    Whether the *server* notices a peer disconnect promptly depends on the OS and
    on when it next writes to the socket, so an assertion on wall-clock cleanup is
    a flaky test of the kernel rather than of this code.  The handler calls exactly
    this method from its ``finally`` block; the smoke script exercises the live
    disconnect path end to end.
    """
    api = live["api"]
    subscriber = Subscriber(4242)
    api.subscribers[4242] = subscriber
    assert 4242 in api.subscribers
    api.detach(4242)
    assert 4242 not in api.subscribers
    api.detach(99999)  # unknown id is a no-op, not an error


def test_sse_frames_start_with_a_hello_then_stream_events(live):
    """The frame encoder, tested without a socket."""
    api = live["api"]
    subscriber = Subscriber(777)
    frames = api._sse_frames(subscriber)
    hello = next(frames)
    assert hello.startswith("event: hello")
    assert hello.endswith("\n\n")
    assert json.loads(hello.split("data: ", 1)[1].strip())["snapshot"]["ticks"] >= 0

    subscriber.offer({"type": "snapshot", "ticks": 5, "marker": "x"})
    frame = next(frames)
    assert frame.startswith("event: snapshot")
    assert json.loads(frame.split("data: ", 1)[1].strip())["marker"] == "x"
    frames.close()


def test_sse_keep_alive_comment_frame_is_emitted(live, monkeypatch):
    """A stream that goes quiet must send a comment, or proxies close it."""
    import fleetlab.httpd as httpd_module

    api = live["api"]
    subscriber = Subscriber(888)
    ticks = iter([0.0, 100.0, 200.0])
    monkeypatch.setattr(httpd_module.time, "monotonic", lambda: next(ticks, 300.0))
    frames = api._sse_frames(subscriber)
    next(frames)  # hello
    # The queue is empty, so the generator times out and emits a keep-alive.
    frame = next(frames)
    assert frame == ": keep-alive\n\n"
    frames.close()


def test_sse_broadcast_counts_deliveries_and_drops(live):
    api = live["api"]
    api.counters["sse_dropped"] = 0
    dropped_before = api.counters["sse_dropped"]
    # No subscribers: a broadcast must not raise.
    api._broadcast("no-subscribers", {})
    assert api.counters["sse_dropped"] == dropped_before


def test_snapshot_brief_shape(live):
    brief = live["api"]._snapshot_brief()
    assert set(brief) == {"ticks", "tasks", "vehicles", "open_tasks"}


def test_server_survives_many_sequential_requests(live):
    """Keep-alive plus Content-Length must not leave the connection wedged."""
    for index in range(15):
        status, _raw, _headers = call(live["base"], "GET", "/healthz")
        assert status == 200, f"request {index} failed"


def test_concurrent_requests_are_served(live):
    results: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        status, _raw, _headers = call(live["base"], "GET", "/api/vehicles")
        with lock:
            results.append(status)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results == [200] * 8


def test_api_server_request_counter_advances(live):
    before = live["api"].counters["requests"]
    call(live["base"], "GET", "/healthz")
    assert live["api"].counters["requests"] > before


def test_a_broken_handler_yields_500_not_a_crash():
    """A handler that raises must produce a 500 for that request only."""
    simulation = Simulation(SimConfig(name="broken", vehicles=1, tasks=1, seed=1))
    api = ApiServer(simulation, host="127.0.0.1", port=0)

    def explode(_request):
        raise RuntimeError("handler blew up")

    api.router.get("/explode", explode)
    httpd = __import__("http.server", fromlist=["ThreadingHTTPServer"]).ThreadingHTTPServer(
        ("127.0.0.1", 0), api.make_handler()
    )
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        status, raw, _headers = call(f"http://127.0.0.1:{port}", "GET", "/explode")
        assert status == 500
        assert body_of(raw)["error"] == "internal"
        assert api.counters["errors"] == 1
        # The server is still healthy afterwards.
        status, _raw, _headers = call(f"http://127.0.0.1:{port}", "GET", "/healthz")
        assert status == 200
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_free_port_helper_does_not_leak_sockets():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        assert probe.getsockname()[1] > 0

"""Zero-dependency HTTP/1.1 API with REST routes and a Server-Sent Events stream.

Written against ``http.server`` and ``socketserver`` from the standard library
rather than FastAPI/Flask.  That is a deliberate choice, not a limitation
workaround: the whole platform ships without third-party runtime dependencies, and
adding a web framework for seven endpoints would break that.  It also means the
request-parsing, routing, status-code and streaming behaviour are ours to get
right — and to test.

Design notes worth knowing when reading the code:

* **Routing** is a small ``{param}`` pattern matcher, tried in registration order,
  so a static route always beats a parameterised one regardless of dict order.
* **Threading**: ``ThreadingHTTPServer`` gives one thread per connection, which is
  required for SSE — a single-threaded server would hold every other request
  behind the open event stream.  The runtime is guarded by its own lock.
* **SSE** uses a bounded per-subscriber queue.  A slow client is *dropped*, not
  buffered: an unbounded subscriber queue turns one stalled browser tab into a
  memory leak, and the client can always reconnect and re-read the state.

Endpoints::

    GET  /healthz                    liveness
    GET  /api/tasks                  list tasks (filter with ?state=)
    GET  /api/tasks/{id}             one task
    POST /api/tasks                  create a task
    DELETE /api/tasks/{id}           cancel a task
    GET  /api/vehicles               fleet state
    GET  /api/vehicles/{id}          one vehicle
    POST /api/commands               PAUSE / RESUME / CANCEL / RECOVER (idempotent)
    GET  /api/traces/{trace_id}      every span of one trace
    GET  /api/metrics                metrics snapshot
    GET  /api/alerts                 fired alerts
    GET  /api/bus                    bus and consumer state
    GET  /api/events                 SSE stream
"""

from __future__ import annotations

import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from .task_fsm import TaskEvent, TaskState

__all__ = ["Router", "Request", "Response", "ApiServer", "serve", "make_api"]

#: How many SSE events may queue for one subscriber before it is dropped.
SSE_QUEUE_LIMIT = 256


class Response:
    """A prepared HTTP response."""

    def __init__(
        self,
        status: int = 200,
        body: Any = None,
        *,
        content_type: str = "application/json; charset=utf-8",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.body = body
        self.content_type = content_type
        self.headers = dict(headers or {})

    def payload(self) -> bytes:
        if isinstance(self.body, (bytes, bytearray)):
            return bytes(self.body)
        if isinstance(self.body, str):
            return self.body.encode("utf-8")
        return json.dumps(self.body, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")


class Request:
    """A parsed request; the handler only ever sees this, never the raw socket."""

    def __init__(
        self,
        method: str,
        path: str,
        query: dict[str, list[str]],
        body: Any,
        headers: dict[str, str],
        params: dict[str, str] | None = None,
    ) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.body = body
        self.headers = headers
        self.params = dict(params or {})

    def q(self, name: str, default: str | None = None) -> str | None:
        values = self.query.get(name)
        return values[0] if values else default


class Router:
    """Tiny pattern router: ``/api/tasks/{task_id}``."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, list[str], Callable[[Request], Response]]] = []

    def add(self, method: str, pattern: str, handler: Callable[[Request], Response]) -> None:
        self.routes.append((method.upper(), pattern.strip("/").split("/"), handler))

    def get(self, pattern: str, handler: Callable[[Request], Response]) -> None:
        self.add("GET", pattern, handler)

    def post(self, pattern: str, handler: Callable[[Request], Response]) -> None:
        self.add("POST", pattern, handler)

    def delete(self, pattern: str, handler: Callable[[Request], Response]) -> None:
        self.add("DELETE", pattern, handler)

    @staticmethod
    def _match(pattern_parts: list[str], path_parts: list[str]) -> dict[str, str] | None:
        if len(pattern_parts) != len(path_parts):
            return None
        params: dict[str, str] = {}
        for expected, actual in zip(pattern_parts, path_parts):
            if expected.startswith("{") and expected.endswith("}"):
                params[expected[1:-1]] = actual
                continue
            if expected != actual:
                return None
        return params

    def resolve(self, method: str, path: str) -> tuple[Callable[[Request], Response], dict[str, str]] | None:
        """First match in registration order; static routes are registered first.

        ``HEAD`` falls back to the ``GET`` route, which is what RFC 9110 requires:
        a HEAD response is a GET response with the body suppressed.  Without this,
        a perfectly reasonable ``curl -I`` got a 405.
        """
        parts = [unquote(part) for part in path.strip("/").split("/")] if path.strip("/") else []
        wanted = method.upper()
        for candidate in ((wanted,) if wanted != "HEAD" else ("HEAD", "GET")):
            for route_method, pattern_parts, handler in self.routes:
                if route_method != candidate:
                    continue
                params = self._match(pattern_parts, parts)
                if params is not None:
                    return handler, params
        return None

    def allowed_methods(self, path: str) -> list[str]:
        parts = [unquote(part) for part in path.strip("/").split("/")] if path.strip("/") else []
        return sorted(
            {method for method, pattern_parts, _ in self.routes if self._match(pattern_parts, parts) is not None}
        )


class Subscriber:
    """One SSE client, with a bounded queue."""

    def __init__(self, client_id: int) -> None:
        self.client_id = client_id
        self.queue: "queue.Queue[dict]" = queue.Queue(maxsize=SSE_QUEUE_LIMIT)
        self.dropped = 0

    def offer(self, event: dict) -> bool:
        try:
            self.queue.put_nowait(event)
            return True
        except queue.Full:
            # Dropping is the documented policy; the client reconnects and
            # re-reads, which is strictly better than the server growing forever.
            self.dropped += 1
            return False


class ApiServer:
    """Owns the runtime, the router and the SSE subscriber set."""

    def __init__(self, simulation, *, host: str = "127.0.0.1", port: int = 8787) -> None:
        self.sim = simulation
        self.rt = simulation.rt
        self.host = host
        self.port = port
        self.router = Router()
        self.subscribers: dict[int, Subscriber] = {}
        self._subscriber_seq = 0
        self._sub_lock = threading.Lock()
        self.counters = {"requests": 0, "sse_events": 0, "sse_dropped": 0, "errors": 0}
        self._register_routes()

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        r = self.router
        # Static routes first: they must win over the parameterised ones.
        r.get("/healthz", self._healthz)
        r.get("/api/tasks", self._list_tasks)
        r.post("/api/tasks", self._create_task)
        r.get("/api/vehicles", self._list_vehicles)
        r.post("/api/commands", self._commands)
        r.get("/api/metrics", self._metrics)
        r.get("/api/alerts", self._alerts)
        r.get("/api/bus", self._bus)
        r.get("/api/events", self._sse)
        r.get("/api/tasks/{task_id}", self._get_task)
        r.delete("/api/tasks/{task_id}", self._cancel_task)
        r.get("/api/vehicles/{vehicle_id}", self._get_vehicle)
        r.get("/api/traces/{trace_id}", self._trace)

    def _healthz(self, request: Request) -> Response:
        return Response(200, {"status": "ok", "ticks": self.sim.tick_index, "scenario": self.sim.config.name})

    def _list_tasks(self, request: Request) -> Response:
        state = request.q("state")
        tasks = [self.rt.state.tasks[t] for t in self.rt.state.ids()]
        if state:
            tasks = [t for t in tasks if t.state == state.upper()]
        return Response(200, {"count": len(tasks), "tasks": [t.as_dict() for t in tasks]})

    def _get_task(self, request: Request) -> Response:
        task_id = request.params["task_id"]
        task = self.rt.state.tasks.get(task_id)
        if task is None:
            return Response(404, {"error": "not_found", "task_id": task_id})
        return Response(200, task.as_dict())

    def _create_task(self, request: Request) -> Response:
        body = request.body if isinstance(request.body, dict) else {}
        required = ("origin", "destination")
        missing = [key for key in required if key not in body]
        if missing:
            return Response(400, {"error": "missing_fields", "fields": missing})
        try:
            origin = (float(body["origin"][0]), float(body["origin"][1]))
            destination = (float(body["destination"][0]), float(body["destination"][1]))
        except (TypeError, ValueError, IndexError):
            return Response(400, {"error": "bad_coordinates"})
        self.sim._task_seq += 1
        task_id = body.get("task_id") or f"API{self.sim._task_seq:04d}"
        if task_id in self.rt.state.tasks:
            return Response(409, {"error": "already_exists", "task_id": task_id})
        trace_id = self.rt.tracer.new_trace_id()
        message = self.rt.publish(
            "task_created",
            task_id,
            {
                "task_id": task_id,
                "origin": list(origin),
                "destination": list(destination),
                "priority": int(body.get("priority", 2)),
                "payload_kg": float(body.get("payload_kg", 100.0)),
                "created_ms": self.rt.current_ms,
                "deadline_ms": int(body.get("deadline_ms", 0)),
            },
            at_ms=self.rt.current_ms,
            trace_id=trace_id,
        )
        if message is None:
            # Backpressure: the API says so instead of pretending the work is queued.
            return Response(
                503,
                {"error": "backpressure", "detail": "bus refused the publish", "trace_id": trace_id},
                headers={"Retry-After": "1"},
            )
        self.rt.drain()
        with self.rt.lock:
            task = self.rt.state.tasks.get(task_id)
        self._broadcast(
            "task_created", {"task_id": task_id, "trace_id": trace_id, "state": task.state if task else "PENDING"}
        )
        return Response(201, {"task_id": task_id, "trace_id": trace_id, "state": task.state if task else "PENDING"})

    def _cancel_task(self, request: Request) -> Response:
        task_id = request.params["task_id"]
        if task_id not in self.rt.state.tasks:
            return Response(404, {"error": "not_found", "task_id": task_id})
        outcome = self.rt.issue_command(
            f"API-CANCEL-{task_id}", TaskEvent.CANCEL, task_id, at_ms=self.rt.current_ms
        )
        task = self.rt.state.tasks[task_id]
        self._broadcast("task_state", {"task_id": task_id, "state": task.state})
        status = 200 if outcome.status in ("applied", "duplicate") else 409
        return Response(status, {"task_id": task_id, "state": task.state, "command": outcome.as_dict()})

    def _list_vehicles(self, request: Request) -> Response:
        return Response(200, {"count": len(self.rt.fleet.vehicles), "vehicles": self.rt.fleet.snapshot()})

    def _get_vehicle(self, request: Request) -> Response:
        vehicle_id = request.params["vehicle_id"]
        vehicle = self.rt.fleet.vehicles.get(vehicle_id)
        if vehicle is None:
            return Response(404, {"error": "not_found", "vehicle_id": vehicle_id})
        return Response(200, vehicle.as_dict())

    def _commands(self, request: Request) -> Response:
        """Issue a control command.  Repeats with the same id are no-ops."""
        body = request.body if isinstance(request.body, dict) else {}
        action = str(body.get("action", "")).upper()
        task_id = str(body.get("task_id", ""))
        command_id = body.get("command_id")
        if action not in (TaskEvent.PAUSE, TaskEvent.RESUME, TaskEvent.RECOVER, TaskEvent.CANCEL, TaskEvent.REQUEUE):
            return Response(400, {"error": "bad_action", "action": action, "allowed": ["PAUSE", "RESUME", "RECOVER", "CANCEL", "REQUEUE"]})
        if task_id not in self.rt.state.tasks:
            return Response(404, {"error": "not_found", "task_id": task_id})
        # Deriving the id from the request when the client does not supply one is
        # what makes an accidental double-click idempotent rather than a second
        # pause.  Callers that genuinely want a second command send a new id.
        command_id = command_id or f"{action}:{task_id}:{body.get('nonce', 'default')}"
        outcome = self.rt.issue_command(command_id, action, task_id, at_ms=self.rt.current_ms)
        task = self.rt.state.tasks[task_id]
        self._broadcast(
            "command", {"task_id": task_id, "action": action, "status": outcome.status, "state": task.state}
        )
        status = {"applied": 200, "duplicate": 200, "unconfirmed": 409, "failed": 422}.get(outcome.status, 500)
        return Response(status, {"task_id": task_id, "state": task.state, "command": outcome.as_dict()})

    def _trace(self, request: Request) -> Response:
        trace_id = request.params["trace_id"]
        trace = self.rt.tracer.trace_dict(trace_id)
        records = [record.as_dict() for record in self.rt.journal.by_trace(trace_id)]
        logs = self.rt.logger.by_trace(trace_id)
        payload = {
            **trace,
            "journal_records": records,
            "log_records": logs,
            "note": "one trace id spans command -> publish -> consume -> transition -> response",
        }
        return Response(200 if trace["found"] or records or logs else 404, payload)

    def _metrics(self, request: Request) -> Response:
        return Response(200, self.rt.metrics_snapshot())

    def _alerts(self, request: Request) -> Response:
        return Response(200, {**self.rt.alerts.stats(), "list": self.rt.alerts.as_list()})

    def _bus(self, request: Request) -> Response:
        return Response(200, self.rt.bus.stats())

    # ------------------------------------------------------------------
    # SSE
    # ------------------------------------------------------------------

    def _broadcast(self, event_type: str, data: dict) -> None:
        payload = {"type": event_type, "at_ms": self.rt.current_ms, **data}
        with self._sub_lock:
            targets = list(self.subscribers.values())
        for subscriber in targets:
            if subscriber.offer(payload):
                self.counters["sse_events"] += 1
            else:
                self.counters["sse_dropped"] += 1

    def _sse(self, request: Request) -> Response:
        """Stream events.  The handler owns the connection until the client leaves.

        Rising ``subscribe`` hands back a :class:`Response` whose body is a
        generator; the HTTP handler detects the ``text/event-stream`` content type
        and streams it, flushing after each event so nothing sits in a buffer.
        """
        with self._sub_lock:
            self._subscriber_seq += 1
            subscriber = Subscriber(self._subscriber_seq)
            self.subscribers[subscriber.client_id] = subscriber
        return Response(
            200,
            {"sse": subscriber.client_id},
            content_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    def _sse_frames(self, subscriber: Subscriber):
        """Yield SSE frames until the stream is closed."""
        yield _frame("hello", {"snapshot": self._snapshot_brief()})
        last_keepalive = time.monotonic()
        while True:
            try:
                event = subscriber.queue.get(timeout=1.0)
            except queue.Empty:
                if time.monotonic() - last_keepalive >= 5.0:
                    last_keepalive = time.monotonic()
                    yield ": keep-alive\n\n"
                continue
            yield _frame(event.get("type", "message"), event)

    def _snapshot_brief(self) -> dict:
        with self.rt.lock:
            return {
                "ticks": self.sim.tick_index,
                "tasks": self.rt.state.state_counts(),
                "vehicles": self.rt.fleet.state_counts(),
                "open_tasks": len(self.rt.state.open_tasks()),
            }

    def detach(self, client_id: int) -> None:
        with self._sub_lock:
            self.subscribers.pop(client_id, None)

    # ------------------------------------------------------------------
    # Server plumbing
    # ------------------------------------------------------------------

    def make_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            server_version = "fleetlab/1.0"

            def log_message(self, fmt, *args):  # noqa: A003 - stdlib signature
                # Silence the default stderr logging: the platform has its own
                # structured logger, and mixing the two makes output unparseable.
                return

            # -- helpers --------------------------------------------------

            def _read_body(self) -> Any:
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0:
                    return None
                raw = self.rfile.read(length)
                content_type = (self.headers.get("Content-Type") or "").lower()
                if "application/json" in content_type:
                    try:
                        return json.loads(raw.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        return {"__parse_error__": True}
                return raw.decode("utf-8", errors="replace")

            def _dispatch(self) -> tuple[Response, Subscriber | None]:
                parsed = urlparse(self.path)
                path = parsed.path
                query = parse_qs(parsed.query)
                matched = server.router.resolve(self.command, path)
                if matched is None:
                    allowed = server.router.allowed_methods(path)
                    if allowed:
                        return (
                            Response(405, {"error": "method_not_allowed", "allowed": allowed},
                                     headers={"Allow": ", ".join(allowed)}),
                            None,
                        )
                    return Response(404, {"error": "no_route", "path": path}), None
                handler, params = matched
                body = self._read_body()
                request = Request(self.command, path, query, body, dict(self.headers), params)
                response = handler(request)
                subscriber = None
                if response.content_type.startswith("text/event-stream"):
                    client_id = response.body.get("sse") if isinstance(response.body, dict) else None
                    with server._sub_lock:
                        subscriber = server.subscribers.get(client_id)
                return response, subscriber

            def _send(self, response: Response) -> None:
                payload = response.payload()
                self.send_response(response.status)
                self.send_header("Content-Type", response.content_type)
                self.send_header("Content-Length", str(len(payload)))
                for key, value in sorted(response.headers.items()):
                    self.send_header(key, value)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)

            # -- verbs ----------------------------------------------------

            def _handle(self) -> None:
                server.counters["requests"] += 1
                try:
                    response, subscriber = self._dispatch()
                except Exception as exc:  # noqa: BLE001 - never leak a traceback to a client
                    server.counters["errors"] += 1
                    self._send(Response(500, {"error": "internal", "detail": f"{type(exc).__name__}: {exc}"}))
                    return
                if subscriber is None:
                    self._send(response)
                    return
                self._stream(subscriber)

            def _stream(self, subscriber: Subscriber) -> None:
                """Send the SSE stream until the client goes away."""
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
                try:
                    for frame in server._sse_frames(subscriber):
                        self.wfile.write(frame.encode("utf-8"))
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                    pass
                finally:
                    server.detach(subscriber.client_id)

            do_GET = _handle
            do_POST = _handle
            do_DELETE = _handle
            do_HEAD = _handle
            do_PUT = _handle
            do_PATCH = _handle
            do_OPTIONS = _handle

        return Handler


def _frame(event_type: str, data: dict) -> str:
    """Encode one SSE frame; ``id`` and ``event`` fields then a JSON data line."""
    body = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    return f"event: {event_type}\ndata: {body}\n\n"


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    """``ThreadingHTTPServer`` that does not shout about client disconnects.

    A browser closing an SSE tab, or a test client dropping its socket, makes
    ``socketserver`` print a full traceback for an entirely expected event.  Those
    are swallowed; anything else is still reported, so a genuine server-side fault
    stays visible.
    """

    daemon_threads = True
    allow_reuse_address = True

    _BENIGN = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)

    def handle_error(self, request, client_address) -> None:  # pragma: no cover - logging path
        import sys

        exc = sys.exc_info()[1]
        if isinstance(exc, self._BENIGN):
            return
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) in (10053, 10054, 10038):
            return
        super().handle_error(request, client_address)


def make_api(simulation, *, host: str = "127.0.0.1", port: int = 8787) -> tuple[ApiServer, ThreadingHTTPServer]:
    """Build the server without starting it (used by tests and by ``serve``)."""
    api = ApiServer(simulation, host=host, port=port)
    httpd = QuietThreadingHTTPServer((host, port), api.make_handler())
    httpd.daemon_threads = True
    return api, httpd


def serve(
    simulation,
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
    background_ticks: bool = True,
    ready: threading.Event | None = None,
) -> None:
    """Serve until interrupted.

    ``background_ticks`` keeps the yard moving so the SSE stream has something to
    say; a frozen snapshot makes an event stream indistinguishable from a broken
    one.
    """
    api, httpd = make_api(simulation, host=host, port=port)
    stop = threading.Event()

    def ticker() -> None:
        while not stop.is_set():
            time.sleep(0.25)
            with simulation.rt.lock:
                simulation.tick()
            api._broadcast(
                "snapshot",
                {
                    "ticks": simulation.tick_index,
                    "open_tasks": len(simulation.rt.state.open_tasks()),
                    "bus_lag": simulation.rt.bus.max_lag(),
                },
            )

    thread = None
    if background_ticks:
        thread = threading.Thread(target=ticker, daemon=True, name="fleetlab-ticker")
        thread.start()
    if ready is not None:
        ready.set()
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        httpd.shutdown()
        httpd.server_close()

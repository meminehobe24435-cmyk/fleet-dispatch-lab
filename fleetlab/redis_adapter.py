"""Redis-backed implementation of the state-store protocol.

The in-memory :class:`~fleetlab.cache.TTLStore` keeps the platform runnable with
zero dependencies, but a real deployment wants the state shared across replicas.
This adapter exposes exactly the same method surface on top of a Redis client
(``redis-py`` command semantics), so application code is written once against
:class:`~fleetlab.cache.StateStoreProtocol`.

What the adapter normalises so both backends are genuinely interchangeable:

* ``exists`` / ``expire`` return ``bool`` (Redis returns 0/1 integers).
* ``hset`` returns the number of *new* fields (redis-py does this for ``mapping=``).
* A ``WRONGTYPE`` reply is translated into :class:`StoreTypeError`, matching the
  error the in-memory store raises for the same misuse.
* ``keys`` is sorted, so callers never depend on server iteration order.
* Missing keys are namespaced under ``prefix`` to avoid colliding with other
  tenants of the same Redis instance.

Verified against ``fakeredis`` in ``tests/test_redis_adapter.py``; see the README
for what is *not* verified (no real Redis server was available).
"""

from __future__ import annotations

import math
from typing import Any

from .cache import StoreError, StoreTypeError, TTL_MISSING, TTL_NO_EXPIRY

__all__ = ["RedisStateStore", "RedisUnavailable"]


class RedisUnavailable(StoreError):
    """Raised when the Redis client cannot serve a request."""


def _translate(exc: Exception) -> StoreError:
    """Map redis-py exceptions onto this package's error hierarchy."""
    name = type(exc).__name__
    message = str(exc)
    if "WRONGTYPE" in message or "not an integer" in message or "value is not an integer" in message:
        return StoreTypeError(message)
    if name in {"ResponseError", "DataError"}:
        return StoreTypeError(message)
    if name in {"ConnectionError", "TimeoutError", "BusyLoadingError"}:
        return RedisUnavailable(message)
    return StoreError(f"{name}: {message}")


class RedisStateStore:
    """``StateStoreProtocol`` implementation over a Redis client.

    Parameters
    ----------
    client:
        Any object implementing the redis-py client interface — a real
        ``redis.Redis`` or ``fakeredis.FakeRedis`` in tests.  Use
        ``decode_responses=True`` so values come back as ``str``.
    prefix:
        Key namespace, e.g. ``"fleetlab"``.  ``prefix`` + ``":"`` + key.
    """

    def __init__(self, client: Any, prefix: str = "fleetlab", name: str = "redis") -> None:
        self._client = client
        self._prefix = prefix.rstrip(":") if prefix else ""
        self.name = name

    # -- key mapping ------------------------------------------------------

    def _k(self, key: str) -> str:
        return f"{self._prefix}:{key}" if self._prefix else str(key)

    def _strip(self, raw: str) -> str:
        head = f"{self._prefix}:" if self._prefix else ""
        return raw[len(head):] if head and raw.startswith(head) else raw

    def _call(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - deliberately re-typed below
            raise _translate(exc) from exc

    # -- string commands --------------------------------------------------

    def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        if ttl is not None and ttl <= 0:
            raise StoreError(f"invalid TTL {ttl!r} for SET {key!r}: must be > 0")
        encoded = self._encode(value)
        if ttl is None:
            return bool(self._call(self._client.set, self._k(key), encoded))
        # Redis EX is whole seconds; round up so a 0.4 s TTL still expires "later",
        # never immediately.
        return bool(
            self._call(self._client.set, self._k(key), encoded, ex=int(math.ceil(ttl)))
        )

    def get(self, key: str) -> str | None:
        raw = self._call(self._client.get, self._k(key))
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        return int(self._call(self._client.delete, *[self._k(k) for k in keys]))

    def exists(self, key: str) -> bool:
        return bool(self._call(self._client.exists, self._k(key)))

    # -- expiry -----------------------------------------------------------

    def expire(self, key: str, ttl: float) -> bool:
        if ttl <= 0:
            return self.delete(key) > 0
        return bool(self._call(self._client.expire, self._k(key), int(math.ceil(ttl))))

    def ttl(self, key: str) -> int:
        return int(self._call(self._client.ttl, self._k(key)))

    def persist(self, key: str) -> bool:
        return bool(self._call(self._client.persist, self._k(key)))

    # -- counters ---------------------------------------------------------

    def incr(self, key: str, amount: int = 1) -> int:
        if amount == 1:
            return int(self._call(self._client.incr, self._k(key)))
        return int(self._call(self._client.incrby, self._k(key), amount))

    # -- hashes -----------------------------------------------------------

    def hset(self, key: str, mapping: dict[str, Any] | None = None, **kw: Any) -> int:
        fields: dict[str, Any] = {}
        if mapping:
            fields.update(mapping)
        fields.update(kw)
        if not fields:
            return 0
        encoded = {str(f): self._encode(v) for f, v in fields.items()}
        return int(self._call(self._client.hset, self._k(key), mapping=encoded))

    def hget(self, key: str, field: str) -> str | None:
        raw = self._call(self._client.hget, self._k(key), str(field))
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    def hgetall(self, key: str) -> dict[str, str]:
        raw = self._call(self._client.hgetall, self._k(key)) or {}
        out: dict[str, str] = {}
        for field, value in raw.items():
            f = field.decode("utf-8") if isinstance(field, bytes) else field
            v = value.decode("utf-8") if isinstance(value, bytes) else value
            out[f] = v
        return out

    def hdel(self, key: str, *fields: str) -> int:
        if not fields:
            return 0
        return int(self._call(self._client.hdel, self._k(key), *[str(f) for f in fields]))

    def hkeys(self, key: str) -> list[str]:
        raw = self._call(self._client.hkeys, self._k(key)) or []
        return sorted(f.decode("utf-8") if isinstance(f, bytes) else f for f in raw)

    def hlen(self, key: str) -> int:
        return int(self._call(self._client.hlen, self._k(key)))

    # -- keyspace ---------------------------------------------------------

    def keys(self, pattern: str = "*") -> list[str]:
        raw = self._call(self._client.keys, self._k(pattern)) or []
        decoded = [k.decode("utf-8") if isinstance(k, bytes) else k for k in raw]
        return sorted(self._strip(k) for k in decoded)

    def flush(self) -> None:
        found = self._call(self._client.keys, self._k("*")) or []
        if found:
            self._call(self._client.delete, *found)

    # -- snapshot / restore ----------------------------------------------

    def snapshot(self, at_ms: int = 0) -> dict:
        """Dump every namespaced key, distinguishing string vs hash payloads."""
        entries: list[dict] = []
        for key in self.keys("*"):
            full = self._k(key)
            kind = self._call(self._client.type, full)
            kind = kind.decode("utf-8") if isinstance(kind, bytes) else kind
            ttl = self.ttl(key)
            ttl_value = None if ttl in (TTL_MISSING, TTL_NO_EXPIRY) else ttl
            if kind == "hash":
                entries.append(
                    {
                        "key": key,
                        "kind": "hash",
                        "fields": self.hgetall(key),
                        "ttl": ttl_value,
                    }
                )
            else:
                entries.append(
                    {"key": key, "kind": "string", "value": self.get(key), "ttl": ttl_value}
                )
        return {"at_ms": at_ms, "backend": self.name, "entries": entries}

    def restore(self, snapshot: dict) -> int:
        """Replace namespaced contents with ``snapshot``; returns keys restored."""
        self.flush()
        restored = 0
        for item in snapshot.get("entries", []):
            ttl = item.get("ttl")
            if item.get("kind") == "hash":
                self.hset(item["key"], mapping=item.get("fields", {}))
            else:
                self.set(item["key"], item.get("value", ""))
            if ttl is not None and ttl > 0:
                self.expire(item["key"], ttl)
            restored += 1
        return restored

    # -- misc -------------------------------------------------------------

    @staticmethod
    def _encode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, float):
            return repr(value)
        return str(value)

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception as exc:  # noqa: BLE001
            raise _translate(exc) from exc

    def __repr__(self) -> str:  # pragma: no cover - display only
        return f"<RedisStateStore prefix={self._prefix!r} name={self.name!r}>"

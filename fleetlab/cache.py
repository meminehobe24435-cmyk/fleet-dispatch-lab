"""In-process key/value state store with Redis-compatible semantics.

The platform keeps hot state (vehicle pose, task progress, heartbeats, counters)
in a cache rather than in the event log, because the event log answers "what
happened" while the cache answers "what is true right now".  This module provides
that "right now" store with the exact semantics the production system would get
from Redis:

======================  =====================================================
Operation               Redis semantics implemented here
======================  =====================================================
``SET k v EX ttl``      value stored as a string; ``ttl`` seconds from now
``GET k``               ``None`` when missing or expired
``DEL k...``            returns the number of keys actually removed
``EXPIRE k ttl``        ``False`` when the key does not exist
``TTL k``               ``-2`` missing, ``-1`` no expiry, else seconds left
``INCR k [by]``         missing key starts at 0; non-integer value is an error
``HSET k f v ...``      returns count of *new* fields
``HGET / HGETALL``      flat string map
``KEYS pattern``        glob match, ``*`` and ``?``
======================  =====================================================

Why hand-rolled instead of just using redis-py: the deployment target has no
Redis guarantee, so the platform must run with zero third-party dependencies.
:class:`~fleetlab.redis_adapter.RedisStateStore` implements the *same* protocol
on top of a real Redis client, and the test-suite runs one shared conformance
sequence against both implementations to prove they agree.
"""

from __future__ import annotations

import fnmatch
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Protocol, runtime_checkable

__all__ = [
    "StoreError",
    "StoreTypeError",
    "TTLStore",
    "StateStoreProtocol",
    "ExpiryEvent",
    "ENTRY_TTL_SECONDS",
]

#: TTL returned by ``ttl()`` for a missing key (matches Redis).
TTL_MISSING = -2
#: TTL returned by ``ttl()`` for a key without expiry (matches Redis).
TTL_NO_EXPIRY = -1


class StoreError(Exception):
    """Base class for store failures."""


class StoreTypeError(StoreError):
    """Raised when a value cannot be used for the requested operation.

    Mirrors ``redis.exceptions.ResponseError`` for ``INCR`` on a non-integer
    value, so callers can program against one exception type regardless of the
    backend.
    """


@dataclass
class ExpiryEvent:
    """Recorded when a key is observed to have expired.

    The fault injector uses these to prove that "cache expiry" faults really
    fired, rather than assuming they did.
    """

    key: str
    at_ms: int
    source: str = "lazy"


@dataclass
class _Entry:
    value: str
    expires_at: float | None = None

    def is_expired(self, now: float) -> bool:
        return self.expires_at is not None and now >= self.expires_at


@runtime_checkable
class StateStoreProtocol(Protocol):
    """The contract both the in-memory store and the Redis adapter satisfy."""

    def set(self, key: str, value: Any, ttl: float | None = None) -> bool: ...
    def get(self, key: str) -> str | None: ...
    def delete(self, *keys: str) -> int: ...
    def exists(self, key: str) -> bool: ...
    def expire(self, key: str, ttl: float) -> bool: ...
    def ttl(self, key: str) -> int: ...
    def incr(self, key: str, amount: int = 1) -> int: ...
    def hset(self, key: str, mapping: dict[str, Any] | None = None, **kw: Any) -> int: ...
    def hget(self, key: str, field: str) -> str | None: ...
    def hgetall(self, key: str) -> dict[str, str]: ...
    def hdel(self, key: str, *fields: str) -> int: ...
    def keys(self, pattern: str = "*") -> list[str]: ...
    def flush(self) -> None: ...


#: Default seconds entities in the cache carry before they expire.
ENTRY_TTL_SECONDS = 30


class TTLStore:
    """Thread-safe in-memory store with Redis-style TTL and hash semantics.

    Expiry is *lazy* (checked on read) plus an optional active sweep, which is
    how Redis actually behaves: a key past its TTL is logically gone the moment
    it is touched, and physical reclamation happens later.  The practical
    consequence — and the one the tests pin down — is that ``ttl()`` must never
    report a positive number for an expired key even before a sweep runs.
    """

    def __init__(self, *, clock=time.monotonic, name: str = "mem") -> None:
        self._data: dict[str, _Entry] = {}
        self._lock = threading.RLock()
        self._clock = clock
        self.name = name
        self.expiries: list[ExpiryEvent] = []
        self._ops = 0

    # -- internal ---------------------------------------------------------

    def _now(self) -> float:
        return self._clock()

    def _sweep_expired_locked(self, now: float) -> list[str]:
        dead = [k for k, e in self._data.items() if e.is_expired(now)]
        for key in dead:
            del self._data[key]
        return sorted(dead)

    def sweep(self, at_ms: int = 0) -> list[str]:
        """Actively remove expired keys; returns the keys removed (sorted).

        The sorted return value matters: reproducibility requires that no code
        path ever exposes dict/set iteration order.
        """
        with self._lock:
            dead = self._sweep_expired_locked(self._now())
            for key in dead:
                self.expiries.append(ExpiryEvent(key, at_ms, source="active"))
            return dead

    def _live_locked(self, key: str, now: float) -> _Entry | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.is_expired(now):
            # Lazy expiry: logically gone, record it, drop it.
            del self._data[key]
            return None
        return entry

    @staticmethod
    def _encode(value: Any) -> str:
        """Encode any value the way redis-py would: everything becomes a string."""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, float):
            # Repr keeps round-tripping stable and avoids 0.1 -> 0.1 vs 0.1000000000000000055 drift.
            return repr(value)
        return str(value)

    # -- string commands --------------------------------------------------

    def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        if ttl is not None and ttl <= 0:
            # Redis rejects non-positive EX; emulate by refusing rather than silently
            # storing a value that is already expired.
            raise StoreError(f"invalid TTL {ttl!r} for SET {key!r}: must be > 0")
        with self._lock:
            self._ops += 1
            expires_at = None if ttl is None else self._now() + ttl
            self._data[key] = _Entry(self._encode(value), expires_at)
            return True

    def get(self, key: str) -> str | None:
        with self._lock:
            self._ops += 1
            entry = self._live_locked(key, self._now())
            return None if entry is None else entry.value

    def delete(self, *keys: str) -> int:
        removed = 0
        with self._lock:
            now = self._now()
            for key in keys:
                if self._live_locked(key, now) is None:
                    # An expired key is already gone; DEL must not count it.
                    self._data.pop(key, None)
                    continue
                del self._data[key]
                removed += 1
        return removed

    def exists(self, key: str) -> bool:
        with self._lock:
            return self._live_locked(key, self._now()) is not None

    # -- expiry -----------------------------------------------------------

    def expire(self, key: str, ttl: float) -> bool:
        if ttl <= 0:
            # Redis: EXPIRE with a non-positive TTL deletes the key and returns 1.
            return self.delete(key) > 0
        with self._lock:
            entry = self._live_locked(key, self._now())
            if entry is None:
                return False
            entry.expires_at = self._now() + ttl
            return True

    def ttl(self, key: str) -> int:
        """Seconds remaining: -2 missing, -1 no expiry, else ceil(remaining).

        Redis rounds *up* to whole seconds and never reports 0 for a live key,
        so a key with 0.4 s left reads as 1.
        """
        with self._lock:
            entry = self._live_locked(key, self._now())
            if entry is None:
                return TTL_MISSING
            if entry.expires_at is None:
                return TTL_NO_EXPIRY
            remaining = entry.expires_at - self._now()
            if remaining <= 0:
                return TTL_MISSING
            return int(math.ceil(remaining))

    def persist(self, key: str) -> bool:
        """Remove the TTL from a key (Redis ``PERSIST``)."""
        with self._lock:
            entry = self._live_locked(key, self._now())
            if entry is None or entry.expires_at is None:
                return False
            entry.expires_at = None
            return True

    def expire_now(self, key: str, *, at_ms: int = 0, reason: str = "forced") -> bool:
        """Expire a key immediately and record the event.

        A sub-millisecond TTL does *not* work for this: ``time.monotonic()`` on
        Windows has roughly 15.6 ms resolution, so ``now >= expires_at`` is still
        false a moment after a 1 µs TTL is set and the key never expires at all.
        This was a real bug — the cache-expiry fault reported 59 keys marked
        expired and removed none, while the scenario still declared itself
        consistent.  Forcing the expiry removes the clock from the equation.

        The ordinary TTL path is unaffected and is tested separately with an
        injected clock, which is the only way to test sub-tick expiry portably.
        """
        with self._lock:
            entry = self._live_locked(key, self._now())
            if entry is None:
                return False
            del self._data[key]
            self.expiries.append(ExpiryEvent(key, at_ms, source=reason))
            return True

    # -- counters ---------------------------------------------------------

    def incr(self, key: str, amount: int = 1) -> int:
        with self._lock:
            self._ops += 1
            entry = self._live_locked(key, self._now())
            if entry is None:
                # Missing key counts as 0.  Note: a new INCR key has no TTL, same as Redis.
                total = amount
                self._data[key] = _Entry(str(total), None)
                return total
            try:
                total = int(entry.value) + amount
            except (TypeError, ValueError):
                raise StoreTypeError(
                    f"value at {key!r} is not an integer: {entry.value!r}"
                ) from None
            entry.value = str(total)
            return total

    # -- hashes -----------------------------------------------------------

    def hset(self, key: str, mapping: dict[str, Any] | None = None, **kw: Any) -> int:
        fields: dict[str, Any] = {}
        if mapping:
            fields.update(mapping)
        fields.update(kw)
        with self._lock:
            now = self._now()
            entry = self._live_locked(key, now)
            if entry is None:
                # A hash is stored as a prefixed flat map inside the same keyspace so
                # that TTL/EXPIRE apply to the whole hash, which is what Redis does.
                entry = _Entry("", None)
                self._data[key] = entry
                current: dict[str, str] = {}
            else:
                current = self._decode_hash(key, entry.value)
            created = 0
            for field, value in fields.items():
                if field not in current:
                    created += 1
                current[str(field)] = self._encode(value)
            entry.value = self._encode_hash(current)
            return created

    def _hash_locked(self, key: str, now: float) -> dict[str, str] | None:
        """Return the hash at ``key``.

        ``None`` means "key absent"; a string-typed key raises ``StoreTypeError``
        the way Redis raises ``WRONGTYPE``, because silently returning an empty
        dict would hide a real type bug in application code.
        """
        entry = self._live_locked(key, now)
        if entry is None:
            return None
        if not entry.value.startswith(self.HASH_PREFIX):
            raise StoreTypeError(f"WRONGTYPE: key {key!r} holds a string, not a hash")
        return self._decode_hash(key, entry.value)

    def hget(self, key: str, field: str) -> str | None:
        with self._lock:
            current = self._hash_locked(key, self._now())
            if current is None:
                return None
            return current.get(str(field))

    def hgetall(self, key: str) -> dict[str, str]:
        with self._lock:
            current = self._hash_locked(key, self._now())
            return {} if current is None else dict(current)

    def hdel(self, key: str, *fields: str) -> int:
        with self._lock:
            entry = self._live_locked(key, self._now())
            if entry is None:
                return 0
            if not entry.value.startswith(self.HASH_PREFIX):
                raise StoreTypeError(
                    f"WRONGTYPE: key {key!r} holds a string, not a hash"
                )
            current = self._decode_hash(key, entry.value)
            removed = 0
            for field in fields:
                if str(field) in current:
                    del current[str(field)]
                    removed += 1
            if current:
                entry.value = self._encode_hash(current)
            else:
                # Redis deletes the key when the last hash field is removed.
                del self._data[key]
            return removed

    def hkeys(self, key: str) -> list[str]:
        return sorted(self.hgetall(key).keys())

    def hlen(self, key: str) -> int:
        return len(self.hgetall(key))

    # -- keyspace ---------------------------------------------------------

    def keys(self, pattern: str = "*") -> list[str]:
        with self._lock:
            self._sweep_expired_locked(self._now())
            return sorted(k for k in self._data if fnmatch.fnmatchcase(k, pattern))

    def flush(self) -> None:
        with self._lock:
            self._data.clear()

    def size(self) -> int:
        with self._lock:
            self._sweep_expired_locked(self._now())
            return len(self._data)

    def op_count(self) -> int:
        return self._ops

    # -- hash encoding ----------------------------------------------------

    HASH_PREFIX = "\x00H"

    @classmethod
    def _encode_hash(cls, mapping: dict[str, str]) -> str:
        # NUL-separated flat representation; keys/fields in this project never
        # contain NUL, and the prefix makes a hash distinguishable from a plain
        # string value occupying the same keyspace.
        items: list[str] = []
        for k in sorted(mapping):
            items.append(k)
            items.append(mapping[k])
        return cls.HASH_PREFIX + "\x1f".join(items)

    @staticmethod
    def _decode_hash(key: str, raw: str) -> dict[str, str]:
        if not raw.startswith(TTLStore.HASH_PREFIX):
            raise StoreTypeError(
                f"WRONGTYPE: key {key!r} holds a string, not a hash"
            )
        body = raw[len(TTLStore.HASH_PREFIX):]
        if not body:
            return {}
        parts = body.split("\x1f")
        return {parts[i]: parts[i + 1] for i in range(0, len(parts) - 1, 2)}

    # -- snapshot / restore ----------------------------------------------

    def snapshot(self, at_ms: int = 0) -> dict:
        """Dump live keys with their remaining TTL (``None`` == persistent).

        Hash entries are emitted as ``{"kind": "hash", "fields": {...}}`` — the
        same shape :class:`~fleetlab.redis_adapter.RedisStateStore` produces.  An
        earlier version dumped a hash as its internal encoded string with
        ``kind: "string"``, which meant a snapshot taken from the in-memory store
        could not be restored into Redis: the encoded payload would have been
        stored as a literal string value.  Snapshots are only useful if they are
        portable, so the two backends agree on the format.
        """
        with self._lock:
            now = self._now()
            self._sweep_expired_locked(now)
            entries: list[dict] = []
            for key in sorted(self._data):
                entry = self._data[key]
                ttl = None
                if entry.expires_at is not None:
                    ttl = max(0.0, entry.expires_at - now)
                if entry.value.startswith(self.HASH_PREFIX):
                    entries.append(
                        {
                            "key": key,
                            "kind": "hash",
                            "fields": self._decode_hash(key, entry.value),
                            "ttl": ttl,
                        }
                    )
                else:
                    entries.append(
                        {"key": key, "kind": "string", "value": entry.value, "ttl": ttl}
                    )
            return {"at_ms": at_ms, "backend": self.name, "entries": entries}

    def restore(self, snapshot: dict) -> int:
        """Replace current contents with ``snapshot``; returns keys restored.

        Accepts snapshots produced by either backend.
        """
        with self._lock:
            self._data.clear()
            now = self._now()
            for item in snapshot.get("entries", []):
                ttl = item.get("ttl")
                expires_at = None if ttl is None else now + ttl
                if item.get("kind") == "hash":
                    fields = {
                        str(field): str(value)
                        for field, value in (item.get("fields") or {}).items()
                    }
                    value = self._encode_hash(fields)
                else:
                    value = self._encode(item.get("value", ""))
                self._data[item["key"]] = _Entry(value, expires_at)
            return len(self._data)

    def __len__(self) -> int:
        return self.size()


#: Convenience alias used by application code that does not care which backend.
MemoryStateStore = TTLStore


def hexdump(data: Iterable[int]) -> str:  # pragma: no cover - debugging helper
    return " ".join(f"{b:02x}" for b in data)

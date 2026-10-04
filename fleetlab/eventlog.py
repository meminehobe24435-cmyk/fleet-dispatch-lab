"""Append-only event journal — the system of record, and the replay source.

Why a journal *and* a bus?  Because they answer different questions:

* the **bus** (:mod:`fleetlab.bus`) is a delivery mechanism.  It is partitioned,
  so it preserves order *per key* and deliberately not globally.  That is what
  makes it scalable, and it is also why you cannot rebuild global state from a
  partitioned log without extra work.
* the **journal** here is a single totally-ordered sequence of the events the
  system *actually applied*, in the order it applied them.

The journal is therefore the replay source, and :meth:`EventLog.replay` into a
fresh state object must reproduce the live state exactly.  That property is only
meaningful because of one architectural rule, enforced throughout the codebase:

    **All state mutation goes through the same applier that the journal
    replays.**  A component that writes state directly — instead of emitting a
    record — would make the journal a lie, and the replay test in the suite is
    what catches that.

Records are plain dicts so they serialise to JSONL without a custom encoder, and
the file is flushed line by line, so a crash mid-run still leaves a usable log.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable, Iterator

__all__ = ["EventLog", "EventRecord"]


@dataclass
class EventRecord:
    """One applied event."""

    seq: int
    kind: str
    subject: str
    at_ms: int
    payload: dict = field(default_factory=dict)
    trace_id: str = ""
    span_id: str = ""
    origin: str = ""

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "kind": self.kind,
            "subject": self.subject,
            "at_ms": self.at_ms,
            "payload": self.payload,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "origin": self.origin,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "EventRecord":
        return cls(
            seq=raw["seq"],
            kind=raw["kind"],
            subject=raw["subject"],
            at_ms=raw["at_ms"],
            payload=raw.get("payload", {}),
            trace_id=raw.get("trace_id", ""),
            span_id=raw.get("span_id", ""),
            origin=raw.get("origin", ""),
        )


class EventLog:
    """Ordered journal with replay, JSONL persistence and trace lookup."""

    def __init__(self, *, path: str | None = None) -> None:
        self.records: list[EventRecord] = []
        self._path = path
        self._seq = 0
        self.counters: dict[str, int] = {}

    # -- writing ----------------------------------------------------------

    def append(
        self,
        kind: str,
        subject: str,
        *,
        at_ms: int = 0,
        payload: dict | None = None,
        trace_id: str = "",
        span_id: str = "",
        origin: str = "",
    ) -> EventRecord:
        self._seq += 1
        record = EventRecord(
            seq=self._seq,
            kind=kind,
            subject=subject,
            at_ms=at_ms,
            payload=dict(payload or {}),
            trace_id=trace_id,
            span_id=span_id,
            origin=origin,
        )
        self.records.append(record)
        self.counters[kind] = self.counters.get(kind, 0) + 1
        if self._path is not None:
            with open(self._path, "a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(record.as_dict(), sort_keys=True, ensure_ascii=False) + "\n"
                )
        return record

    # -- reading ----------------------------------------------------------

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[EventRecord]:
        return iter(self.records)

    def head(self) -> int:
        return self._seq

    def get(self, seq: int) -> EventRecord:
        return self.records[seq - 1]

    def slice(self, from_seq: int = 0, to_seq: int | None = None) -> list[EventRecord]:
        """Records with ``from_seq < seq <= to_seq`` (``from_seq`` exclusive)."""
        stop = self._seq if to_seq is None else to_seq
        return [r for r in self.records if from_seq < r.seq <= stop]

    def by_trace(self, trace_id: str) -> list[EventRecord]:
        return [r for r in self.records if r.trace_id == trace_id]

    def by_subject(self, subject: str) -> list[EventRecord]:
        return [r for r in self.records if r.subject == subject]

    def kinds(self) -> dict[str, int]:
        return {k: self.counters[k] for k in sorted(self.counters)}

    # -- replay -----------------------------------------------------------

    def replay(
        self,
        applier: Callable[[EventRecord], None],
        *,
        from_seq: int = 0,
        to_seq: int | None = None,
    ) -> int:
        """Apply records in sequence order to whatever ``applier`` mutates.

        Returns the number of records replayed.  Replaying is intentionally
        ordinary: there is no "replay mode" flag anywhere in the applier, because
        a replay path that differs from the live path proves nothing about the
        live path.
        """
        count = 0
        for record in self.slice(from_seq, to_seq):
            applier(record)
            count += 1
        return count

    def replay_into(self, target, applier: Callable[[object, EventRecord], None]):
        """Convenience: replay into a fresh object of the caller's choosing."""
        self.replay(lambda record: applier(target, record))
        return target

    # -- persistence ------------------------------------------------------

    def load_jsonl(self, path: str) -> int:
        """Read records back from a JSONL file; replaces current contents."""
        self.records.clear()
        self._seq = 0
        self.counters.clear()
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = EventRecord.from_dict(json.loads(line))
                self.records.append(record)
                self._seq = max(self._seq, record.seq)
                self.counters[record.kind] = self.counters.get(record.kind, 0) + 1
        return len(self.records)

    def digest(self) -> str:
        """Cheap content hash of the journal, for cross-run comparison."""
        import hashlib

        hasher = hashlib.sha256()
        for record in self.records:
            hasher.update(
                json.dumps(record.as_dict(), sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
        return hasher.hexdigest()

    def stats(self) -> dict:
        return {
            "records": len(self.records),
            "head": self._seq,
            "kinds": self.kinds(),
            "digest": self.digest(),
        }

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import cast


class JournalConflictError(RuntimeError):
    """A logical record ID was reused with different immutable evidence."""


@dataclass(frozen=True, slots=True)
class JournalRecovery:
    record_count: int
    corrupted_line_numbers: tuple[int, ...]
    partial_tail: bool


class ImmutableJsonlJournal:
    """Append-only fsync journal with durable full-history logical dedup."""

    def __init__(self, path: Path, *, id_field: str) -> None:
        if not id_field.strip():
            raise ValueError("Journal ID field cannot be empty.")
        self._path = path.resolve()
        self._id_field = id_field
        self._lock = Lock()
        self._payloads: dict[str, dict[str, object]] = {}
        self._records: list[dict[str, object]] = []
        self._corrupted: tuple[int, ...] = ()
        self._partial_tail = False
        self._scan()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> JournalRecovery:
        return JournalRecovery(
            len(self._records), self._corrupted, self._partial_tail
        )

    def append(self, record_id: str, payload: dict[str, object]) -> bool:
        if not record_id.strip() or payload.get(self._id_field) != record_id:
            raise ValueError("Journal record ID is invalid.")
        normalized = _normalize(payload)
        with self._lock:
            self._finalize_valid_tail_if_present()
            existing = self._payloads.get(record_id)
            if existing is not None:
                if existing != normalized:
                    raise JournalConflictError(
                        f"Conflicting immutable record: {record_id}."
                    )
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            needs_separator = self._needs_separator()
            line = _encode(normalized)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                if needs_separator:
                    stream.write("\n")
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._scan()
            persisted = self._payloads.get(record_id)
            if persisted != normalized:
                raise OSError("Journal record was not durably recoverable.")
            return True

    def records(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(dict(item) for item in self._records)

    def _scan(self) -> None:
        payloads: dict[str, dict[str, object]] = {}
        records: list[dict[str, object]] = []
        corrupted: list[int] = []
        partial_tail = False
        if self._path.exists():
            with self._path.open("rb") as stream:
                for line_number, raw_line in enumerate(stream, start=1):
                    if not raw_line.endswith(b"\n"):
                        partial_tail = True
                        continue
                    payload = _decode(raw_line)
                    if payload is None:
                        corrupted.append(line_number)
                        continue
                    record_id = payload.get(self._id_field)
                    if not isinstance(record_id, str) or not record_id:
                        corrupted.append(line_number)
                        continue
                    existing = payloads.get(record_id)
                    if existing is not None:
                        if existing != payload:
                            raise JournalConflictError(
                                f"Conflicting journal history: {record_id}."
                            )
                        continue
                    payloads[record_id] = payload
                    records.append(payload)
        self._payloads = payloads
        self._records = records
        self._corrupted = tuple(corrupted)
        self._partial_tail = partial_tail

    def _finalize_valid_tail_if_present(self) -> None:
        if not self._partial_tail or not self._path.exists():
            return
        raw = self._path.read_bytes()
        tail = raw.rsplit(b"\n", 1)[-1]
        payload = _decode(tail)
        if payload is None or not isinstance(payload.get(self._id_field), str):
            return
        with self._path.open("ab") as stream:
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._scan()

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


def _encode(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _decode(raw: bytes) -> dict[str, object] | None:
    try:
        value = cast(object, json.loads(raw.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


def _normalize(payload: dict[str, object]) -> dict[str, object]:
    decoded = _decode(_encode(payload).encode("utf-8"))
    if decoded is None:
        raise ValueError("Journal payload is not JSON-compatible.")
    return decoded

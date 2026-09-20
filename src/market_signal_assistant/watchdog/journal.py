from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import cast


class JournalConflictError(RuntimeError):
    """A logical record ID was reused with different immutable evidence."""

    def __init__(
        self,
        *,
        journal_path: Path,
        record_id: str,
        existing_payload: Mapping[str, object],
        attempted_payload: Mapping[str, object],
    ) -> None:
        self.journal_path = journal_path
        self.record_id = record_id
        self.existing_payload = dict(existing_payload)
        self.attempted_payload = dict(attempted_payload)
        super().__init__(f"Conflicting immutable record {record_id} in {journal_path}.")


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
        self._digests: dict[str, str] = {}
        self._offsets: dict[str, int] = {}
        self._record_count = 0
        self._corrupted: tuple[int, ...] = ()
        self._partial_tail = False
        self._scan()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> JournalRecovery:
        return JournalRecovery(self._record_count, self._corrupted, self._partial_tail)

    def append(self, record_id: str, payload: dict[str, object]) -> bool:
        if not record_id.strip() or payload.get(self._id_field) != record_id:
            raise ValueError("Journal record ID is invalid.")
        normalized = _normalize(payload)
        with self._lock:
            self._finalize_valid_tail_if_present()
            digest = _digest(normalized)
            existing_digest = self._digests.get(record_id)
            if existing_digest is not None:
                if existing_digest != digest:
                    raise JournalConflictError(
                        journal_path=self._path,
                        record_id=record_id,
                        existing_payload=self._read_at(self._offsets[record_id]),
                        attempted_payload=normalized,
                    )
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            needs_separator = self._needs_separator()
            line = _encode(normalized).encode("utf-8")
            with self._path.open("ab") as stream:
                if needs_separator:
                    stream.write(b"\n")
                offset = stream.tell()
                stream.write(line)
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            persisted = self._read_at(offset)
            if persisted != normalized:
                raise OSError("Journal record was not durably recoverable.")
            self._digests[record_id] = digest
            self._offsets[record_id] = offset
            self._record_count += 1
            return True

    def records(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(self._iter_records())

    def _scan(self) -> None:
        digests: dict[str, str] = {}
        offsets: dict[str, int] = {}
        corrupted: list[int] = []
        partial_tail = False
        if self._path.exists():
            with self._path.open("rb") as stream:
                line_number = 0
                while True:
                    offset = stream.tell()
                    raw_line = stream.readline()
                    if not raw_line:
                        break
                    line_number += 1
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
                    digest = _digest(payload)
                    existing_digest = digests.get(record_id)
                    if existing_digest is not None:
                        if existing_digest != digest:
                            raise JournalConflictError(
                                journal_path=self._path,
                                record_id=record_id,
                                existing_payload=self._read_at(offsets[record_id]),
                                attempted_payload=payload,
                            )
                        continue
                    digests[record_id] = digest
                    offsets[record_id] = offset
        self._digests = digests
        self._offsets = offsets
        self._record_count = len(digests)
        self._corrupted = tuple(corrupted)
        self._partial_tail = partial_tail

    def _iter_records(self) -> list[dict[str, object]]:
        if not self._path.exists():
            return []
        records: list[dict[str, object]] = []
        seen: set[str] = set()
        with self._path.open("rb") as stream:
            for raw_line in stream:
                if not raw_line.endswith(b"\n"):
                    continue
                payload = _decode(raw_line)
                if payload is None:
                    continue
                record_id = payload.get(self._id_field)
                if not isinstance(record_id, str) or record_id in seen:
                    continue
                seen.add(record_id)
                records.append(payload)
        return records

    def _read_at(self, offset: int) -> dict[str, object]:
        with self._path.open("rb") as stream:
            stream.seek(offset)
            payload = _decode(stream.readline())
        if payload is None:
            raise OSError("Indexed journal record is not readable.")
        return payload

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


def _digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(_encode(payload).encode("utf-8")).hexdigest()

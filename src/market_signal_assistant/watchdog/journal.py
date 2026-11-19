from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.watchdog.jsonl_io import (
    MAX_RECORD_BYTES,
    read_bounded_line,
)


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
    """Append-only evidence with a rebuildable disk-backed logical-ID index.

    JSONL is authoritative. The SQLite sidecar contains only hashes and byte
    offsets and advances from the last fully scanned byte.
    """

    def __init__(self, path: Path, *, id_field: str) -> None:
        if not id_field.strip():
            raise ValueError("Journal ID field cannot be empty.")
        self._path = path.resolve()
        self._id_field = id_field
        self._index_path = self._path.with_name(f"{self._path.name}.index.sqlite3")
        self._lock = Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._prepare_index()
            self._sync_index()
        except sqlite3.DatabaseError:
            self._delete_index_files()
            self._prepare_index()
            self._sync_index()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def index_path(self) -> Path:
        return self._index_path

    @property
    def recovery(self) -> JournalRecovery:
        with self._lock, self._connection() as connection:
            self._sync_index(connection)
            count = int(
                connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
            )
            corrupted = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT line_number FROM corrupted ORDER BY line_number"
                )
            )
            partial = self._metadata(connection, "partial_tail") == "1"
        return JournalRecovery(count, corrupted, partial)

    @property
    def retained_index_entries(self) -> int:
        """Historical logical IDs retained in Python RAM (always zero)."""
        return 0

    def assert_complete(self) -> None:
        """Reject damaged critical evidence before runtime recovery uses it."""
        with self._lock, self._connection() as connection:
            self._sync_index(connection)
            if (
                connection.execute("SELECT 1 FROM corrupted LIMIT 1").fetchone()
                or self._metadata(connection, "partial_tail") == "1"
            ):
                raise ValueError(
                    f"Immutable journal is corrupt or incomplete: {self._path}"
                )

    def append(self, record_id: str, payload: dict[str, object]) -> bool:
        if not record_id.strip() or payload.get(self._id_field) != record_id:
            raise ValueError("Journal record ID is invalid.")
        normalized = _normalize(payload)
        line = _encode(normalized).encode("utf-8")
        if len(line) + 1 > MAX_RECORD_BYTES:
            raise ValueError("Journal record exceeds size limit.")
        digest = _digest(normalized)
        with self._lock, self._connection() as connection:
            self._sync_index(connection)
            self._finalize_valid_tail_if_present(connection)
            self._sync_index(connection)
            existing = self._lookup(connection, record_id)
            if existing is not None:
                if existing[0] != digest:
                    raise JournalConflictError(
                        journal_path=self._path,
                        record_id=record_id,
                        existing_payload=self._read_at(existing[1]),
                        attempted_payload=normalized,
                    )
                return False
            needs_separator = self._needs_separator()
            with self._path.open("ab") as stream:
                if needs_separator:
                    stream.write(b"\n")
                stream.write(line)
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            # JSONL commits first. A crash before the next line is harmless:
            # construction/append resumes indexing from the prior byte offset.
            self._sync_index(connection)
            indexed = self._lookup(connection, record_id)
            if indexed is None or self._read_at(indexed[1]) != normalized:
                raise OSError("Journal record was not durably recoverable.")
            return True

    def records(self) -> tuple[dict[str, object], ...]:
        return tuple(self.iter_records())

    def iter_records(self) -> Iterator[dict[str, object]]:
        if not self._path.exists():
            return
        with self._connection() as connection:
            self._sync_index(connection)
            with self._path.open("rb") as stream:
                for (offset,) in connection.execute(
                    "SELECT byte_offset FROM records ORDER BY byte_offset"
                ):
                    stream.seek(int(offset))
                    payload = _decode(read_bounded_line(stream))
                    if payload is None:
                        raise OSError("Indexed journal record is not readable.")
                    yield payload

    def contains(self, record_id: str) -> bool:
        with self._lock, self._connection() as connection:
            self._sync_index(connection)
            return self._lookup(connection, record_id) is not None

    def get(self, record_id: str) -> dict[str, object] | None:
        with self._lock, self._connection() as connection:
            self._sync_index(connection)
            indexed = self._lookup(connection, record_id)
            return None if indexed is None else self._read_at(indexed[1])

    def _prepare_index(self) -> None:
        try:
            with self._connection() as connection:
                _create_schema(connection)
        except sqlite3.DatabaseError:
            self._delete_index_files()
            with self._connection() as connection:
                _create_schema(connection)

    def _delete_index_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            Path(f"{self._index_path}{suffix}").unlink(missing_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._index_path, timeout=30.0)
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _sync_index(self, connection: sqlite3.Connection | None = None) -> None:
        if connection is None:
            with self._connection() as owned:
                self._sync_index(owned)
            return
        source_stat = self._path.stat() if self._path.exists() else None
        source_size = source_stat.st_size if source_stat is not None else 0
        scanned_offset = int(self._metadata(connection, "scanned_offset") or 0)
        line_number = int(self._metadata(connection, "line_number") or 0)
        prior_device = self._metadata(connection, "source_device")
        prior_inode = self._metadata(connection, "source_inode")
        if prior_device is not None and (
            source_stat is None
            or (str(source_stat.st_dev), str(source_stat.st_ino))
            != (prior_device, prior_inode)
        ):
            raise ValueError("Immutable journal source was replaced or removed.")
        if source_size < scanned_offset:
            raise ValueError("Immutable journal source was truncated.")
        partial_tail = False
        if self._path.exists():
            with self._path.open("rb") as stream, connection:
                stream.seek(scanned_offset)
                while True:
                    offset = stream.tell()
                    raw_line = read_bounded_line(stream)
                    if not raw_line:
                        scanned_offset = offset
                        break
                    if not raw_line.endswith(b"\n"):
                        partial_tail = True
                        scanned_offset = offset
                        break
                    line_number += 1
                    scanned_offset = stream.tell()
                    payload = _decode(raw_line)
                    if payload is None:
                        connection.execute(
                            "INSERT OR IGNORE INTO corrupted VALUES (?)", (line_number,)
                        )
                        continue
                    record_id = payload.get(self._id_field)
                    if not isinstance(record_id, str) or not record_id:
                        connection.execute(
                            "INSERT OR IGNORE INTO corrupted VALUES (?)", (line_number,)
                        )
                        continue
                    digest = _digest(payload)
                    existing = self._lookup(connection, record_id)
                    if existing is not None:
                        if existing[0] != digest:
                            raise JournalConflictError(
                                journal_path=self._path,
                                record_id=record_id,
                                existing_payload=self._read_at(existing[1]),
                                attempted_payload=payload,
                            )
                        continue
                    inserted = connection.execute(
                        "INSERT OR IGNORE INTO records VALUES (?, ?, ?)",
                        (record_id, digest, offset),
                    )
                    if inserted.rowcount == 0:
                        concurrent = self._lookup(connection, record_id)
                        if concurrent is None or concurrent[0] != digest:
                            raise JournalConflictError(
                                journal_path=self._path,
                                record_id=record_id,
                                existing_payload=(
                                    self._read_at(concurrent[1])
                                    if concurrent is not None
                                    else {}
                                ),
                                attempted_payload=payload,
                            )
        with connection:
            self._set_metadata(connection, "scanned_offset", str(scanned_offset))
            self._set_metadata(connection, "line_number", str(line_number))
            self._set_metadata(connection, "partial_tail", "1" if partial_tail else "0")
            if source_stat is not None:
                self._set_metadata(connection, "source_device", str(source_stat.st_dev))
                self._set_metadata(connection, "source_inode", str(source_stat.st_ino))

    @staticmethod
    def _metadata(connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    @staticmethod
    def _set_metadata(
        connection: sqlite3.Connection, key: str, value: str
    ) -> None:
        connection.execute(
            "INSERT INTO metadata VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    @staticmethod
    def _lookup(
        connection: sqlite3.Connection, record_id: str
    ) -> tuple[bytes, int] | None:
        row = connection.execute(
            "SELECT payload_sha256, byte_offset FROM records WHERE logical_id = ?",
            (record_id,),
        ).fetchone()
        return None if row is None else (bytes(row[0]), int(row[1]))

    def _read_at(self, offset: int) -> dict[str, object]:
        with self._path.open("rb") as stream:
            stream.seek(offset)
            payload = _decode(read_bounded_line(stream))
        if payload is None:
            raise OSError("Indexed journal record is not readable.")
        return payload

    def _finalize_valid_tail_if_present(self, connection: sqlite3.Connection) -> None:
        if self._metadata(connection, "partial_tail") != "1" or not self._path.exists():
            return
        scanned_offset = int(self._metadata(connection, "scanned_offset") or 0)
        with self._path.open("rb") as stream:
            stream.seek(scanned_offset)
            tail = read_bounded_line(stream)
        payload = _decode(tail)
        if payload is None or not isinstance(payload.get(self._id_field), str):
            return
        with self._path.open("ab") as stream:
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._sync_index(connection)

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS records (
            logical_id TEXT PRIMARY KEY,
            payload_sha256 BLOB NOT NULL,
            byte_offset INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS corrupted (line_number INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


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


def _digest(payload: dict[str, object]) -> bytes:
    return hashlib.sha256(_encode(payload).encode("utf-8")).digest()

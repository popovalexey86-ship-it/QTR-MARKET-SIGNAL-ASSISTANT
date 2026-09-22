from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class IndexRecoveryError(RuntimeError):
    """A rebuildable Watchdog index could not be recovered."""


@dataclass(frozen=True, slots=True)
class JournalIndexSource:
    kind: str
    path: Path
    id_field: str


@dataclass(frozen=True, slots=True)
class IndexRebuildResult:
    indexed_records: int
    skipped_lines: int
    database_bytes: int


class WatchdogIndexManager:
    """Disposable SQLite index incrementally advanced from JSONL evidence."""

    def __init__(self, path: Path, sources: tuple[JournalIndexSource, ...]) -> None:
        if not sources or len({item.kind for item in sources}) != len(sources):
            raise ValueError("Index sources must have unique kinds.")
        self._path = path.resolve()
        self._sources = sources

    @property
    def path(self) -> Path:
        return self._path

    def ensure(self) -> IndexRebuildResult | None:
        if not self._path.exists():
            return self.rebuild()
        try:
            needs_rebuild = False
            added = skipped = 0
            connection = sqlite3.connect(self._path)
            try:
                if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                    needs_rebuild = True
                else:
                    columns = {
                        str(row[1])
                        for row in connection.execute("PRAGMA table_info(sources)")
                    }
                    if not {"source_offset", "line_number"} <= columns:
                        needs_rebuild = True
                    else:
                        rows = connection.execute(
                            "SELECT kind, source_path, source_offset FROM sources"
                        ).fetchall()
                        indexed = {
                            str(kind): (str(source_path), int(offset))
                            for kind, source_path, offset in rows
                        }
                        needs_rebuild = any(
                            item.kind not in indexed
                            or indexed[item.kind][0] != str(item.path)
                            or _size(item.path) < indexed[item.kind][1]
                            for item in self._sources
                        )
                        if not needs_rebuild:
                            with connection:
                                for source in self._sources:
                                    source_added, source_skipped = _advance_source(
                                        connection, source
                                    )
                                    added += source_added
                                    skipped += source_skipped
            finally:
                connection.close()
            if needs_rebuild:
                return self.rebuild()
            if added == 0 and skipped == 0:
                return None
            return IndexRebuildResult(added, skipped, self._path.stat().st_size)
        except (OSError, sqlite3.DatabaseError, ValueError):
            return self.rebuild()

    def rebuild(self) -> IndexRebuildResult:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._path.parent,
            prefix=f".{self._path.name}.",
            suffix=".tmp",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        indexed = skipped = 0
        try:
            connection = sqlite3.connect(temporary)
            try:
                _create_schema(connection)
                with connection:
                    for source in self._sources:
                        source_indexed, source_skipped = _advance_source(
                            connection, source
                        )
                        indexed += source_indexed
                        skipped += source_skipped
                if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                    raise IndexRecoveryError("SQLite integrity check failed.")
            finally:
                connection.close()
            self._path.unlink(missing_ok=True)
            os.replace(temporary, self._path)
            return IndexRebuildResult(indexed, skipped, self._path.stat().st_size)
        except (OSError, sqlite3.DatabaseError, ValueError) as error:
            temporary.unlink(missing_ok=True)
            if isinstance(error, IndexRecoveryError):
                raise
            raise IndexRecoveryError("Watchdog index rebuild failed.") from error

    def counts(self) -> dict[str, int]:
        self.ensure()
        try:
            connection = sqlite3.connect(self._path)
            try:
                rows = connection.execute(
                    "SELECT kind, COUNT(*) FROM records GROUP BY kind"
                ).fetchall()
            finally:
                connection.close()
            return {str(kind): int(count) for kind, count in rows}
        except sqlite3.DatabaseError as error:
            raise IndexRecoveryError("Watchdog index query failed.") from error


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE records (
            kind TEXT NOT NULL,
            logical_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            byte_offset INTEGER NOT NULL,
            symbol TEXT,
            event_time TEXT,
            available_at TEXT,
            payload_sha256 TEXT NOT NULL,
            PRIMARY KEY (kind, logical_id)
        );
        CREATE INDEX records_symbol_kind ON records(symbol, kind);
        CREATE TABLE sources (
            kind TEXT PRIMARY KEY,
            source_path TEXT NOT NULL,
            source_size INTEGER NOT NULL,
            source_mtime_ns INTEGER NOT NULL,
            source_offset INTEGER NOT NULL,
            line_number INTEGER NOT NULL,
            record_count INTEGER NOT NULL,
            skipped_lines INTEGER NOT NULL
        );
        """
    )


def _advance_source(
    connection: sqlite3.Connection,
    source: JournalIndexSource,
) -> tuple[int, int]:
    row = connection.execute(
        "SELECT source_offset, line_number, record_count, skipped_lines "
        "FROM sources WHERE kind = ?",
        (source.kind,),
    ).fetchone()
    offset, line_number, total_indexed, total_skipped = (
        (0, 0, 0, 0) if row is None else tuple(int(item) for item in row)
    )
    indexed = skipped = 0
    if source.path.exists():
        with source.path.open("rb") as stream:
            stream.seek(offset)
            while True:
                byte_offset = stream.tell()
                raw = stream.readline()
                if not raw or not raw.endswith(b"\n"):
                    offset = byte_offset
                    break
                offset = stream.tell()
                line_number += 1
                try:
                    payload: Any = json.loads(raw.decode("utf-8"))
                    if not isinstance(payload, dict):
                        raise ValueError
                    logical_id = payload[source.id_field]
                    if not isinstance(logical_id, str):
                        raise ValueError
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO records
                        (kind, logical_id, line_number, byte_offset, symbol,
                         event_time, available_at, payload_sha256)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source.kind,
                            logical_id,
                            line_number,
                            byte_offset,
                            payload.get("symbol"),
                            payload.get("detected_at") or payload.get("observed_at"),
                            payload.get("available_at"),
                            hashlib.sha256(raw.rstrip(b"\n")).hexdigest(),
                        ),
                    )
                    indexed += int(cursor.rowcount > 0)
                except (UnicodeDecodeError, json.JSONDecodeError, KeyError, ValueError):
                    skipped += 1
    size, mtime = _signature(source.path)
    connection.execute(
        """
        INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(kind) DO UPDATE SET
          source_path=excluded.source_path,
          source_size=excluded.source_size,
          source_mtime_ns=excluded.source_mtime_ns,
          source_offset=excluded.source_offset,
          line_number=excluded.line_number,
          record_count=excluded.record_count,
          skipped_lines=excluded.skipped_lines
        """,
        (
            source.kind,
            str(source.path),
            size,
            mtime,
            offset,
            line_number,
            total_indexed + indexed,
            total_skipped + skipped,
        ),
    )
    return indexed, skipped


def _size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _signature(path: Path) -> tuple[int, int]:
    if not path.exists():
        return (0, 0)
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns

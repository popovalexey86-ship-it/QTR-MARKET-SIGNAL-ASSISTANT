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
    """Disposable SQLite lookup index rebuilt exclusively from JSONL evidence."""

    def __init__(self, path: Path, sources: tuple[JournalIndexSource, ...]) -> None:
        if not sources or len({item.kind for item in sources}) != len(sources):
            raise ValueError("Index sources must have unique kinds.")
        self._path = path.resolve()
        self._sources = sources

    @property
    def path(self) -> Path:
        return self._path

    def ensure(self) -> IndexRebuildResult | None:
        try:
            if self._valid_and_current():
                return None
        except (OSError, sqlite3.DatabaseError):
            pass
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
                        source_indexed, source_skipped = _index_source(
                            connection, source
                        )
                        indexed += source_indexed
                        skipped += source_skipped
                result = connection.execute("PRAGMA integrity_check").fetchone()
                if result != ("ok",):
                    raise IndexRecoveryError("SQLite integrity check failed.")
            finally:
                connection.close()
            # The index is disposable. Windows may reject atomic replacement of
            # a corrupt SQLite file even after every connection is closed.
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

    def _valid_and_current(self) -> bool:
        if not self._path.exists():
            return False
        connection = sqlite3.connect(self._path)
        try:
            if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                return False
            rows = connection.execute(
                "SELECT kind, source_size, source_mtime_ns FROM sources"
            ).fetchall()
        finally:
            connection.close()
        indexed = {str(kind): (int(size), int(mtime)) for kind, size, mtime in rows}
        return all(
            indexed.get(item.kind) == _signature(item.path)
            for item in self._sources
        )


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE records (
            kind TEXT NOT NULL,
            logical_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
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
            record_count INTEGER NOT NULL,
            skipped_lines INTEGER NOT NULL
        );
        """
    )


def _index_source(
    connection: sqlite3.Connection,
    source: JournalIndexSource,
) -> tuple[int, int]:
    indexed = skipped = 0
    if source.path.exists():
        with source.path.open("rb") as stream:
            for line_number, raw in enumerate(stream, start=1):
                if not raw.endswith(b"\n"):
                    skipped += 1
                    continue
                try:
                    payload: Any = json.loads(raw.decode("utf-8"))
                    if not isinstance(payload, dict):
                        raise ValueError
                    logical_id = payload[source.id_field]
                    if not isinstance(logical_id, str):
                        raise ValueError
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO records
                        (kind, logical_id, line_number, symbol, event_time,
                         available_at, payload_sha256)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source.kind,
                            logical_id,
                            line_number,
                            payload.get("symbol"),
                            payload.get("detected_at") or payload.get("observed_at"),
                            payload.get("available_at"),
                            hashlib.sha256(raw.rstrip(b"\n")).hexdigest(),
                        ),
                    )
                    indexed += 1
                except (UnicodeDecodeError, json.JSONDecodeError, KeyError, ValueError):
                    skipped += 1
    size, mtime = _signature(source.path)
    connection.execute(
        "INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?)",
        (source.kind, str(source.path), size, mtime, indexed, skipped),
    )
    return indexed, skipped


def _signature(path: Path) -> tuple[int, int]:
    if not path.exists():
        return (0, 0)
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns

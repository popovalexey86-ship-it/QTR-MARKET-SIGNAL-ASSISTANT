from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, deque
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_signal_assistant.watchdog.jsonl_io import read_bounded_line
from market_signal_assistant.watchdog.runtime.storage import StorageMonitor


def inspect(data_root: Path, *, now: datetime | None = None) -> dict[str, object]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    health = _json(data_root / "state" / "health.json")
    last_loop_raw = health.get("last_loop_at") if health else None
    last_loop = (
        datetime.fromisoformat(str(last_loop_raw))
        if last_loop_raw is not None
        else None
    )
    audit_types: Counter[str] = Counter()
    audit_count = 0
    for item in _jsonl(data_root / "operational" / "runtime.jsonl"):
        audit_count += 1
        audit_types[str(item.get("event_type"))] += 1
    gaps: deque[dict[str, Any]] = deque(maxlen=100)
    gap_count = 0
    for item in _jsonl(data_root / "operational" / "gaps.jsonl"):
        gap_count += 1
        gaps.append(item)
    states = _json(data_root / "state" / "symbols.json")
    baselines = _json(data_root / "state" / "baselines.json")
    storage = StorageMonitor(data_root).snapshot(recorded_at=current)
    return {
        "inspected_at": current.isoformat(),
        "health": health,
        "health_age_seconds": (
            max(0.0, (current - last_loop.astimezone(UTC)).total_seconds())
            if last_loop is not None
            else None
        ),
        "health_stale": (
            last_loop is None
            or current - last_loop.astimezone(UTC) > timedelta(minutes=5)
        ),
        "audit_records": audit_count,
        "audit_types": dict(audit_types),
        "state_symbols": len(states.get("symbols", [])) if states else 0,
        "baseline_observations": (
            len(baselines.get("observations", [])) if baselines else 0
        ),
        "gaps": list(gaps),
        "gaps_total": gap_count,
        "gaps_truncated": gap_count > len(gaps),
        "storage": {
            "total_bytes": storage.total_bytes,
            "jsonl_bytes": storage.jsonl_bytes,
            "sqlite_bytes": storage.sqlite_bytes,
            "disk_free_bytes": storage.disk_free_bytes,
            "pressure": storage.pressure,
            "files": dict(storage.files),
        },
        "indexes": _read_index(data_root / "state" / "evidence.sqlite3"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect Watchdog shadow storage.")
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(inspect(args.data_root), sort_keys=True, default=str))
    return 0


def _json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("rb") as stream:
        line_number = 0
        while raw := read_bounded_line(stream):
            line_number += 1
            if not raw.endswith(b"\n"):
                raise ValueError(f"Incomplete immutable journal {path}:{line_number}")
            try:
                item: Any = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"Corrupt immutable journal {path}:{line_number}"
                ) from error
            if not isinstance(item, dict):
                raise ValueError(f"Invalid immutable journal {path}:{line_number}")
            yield item


def _read_index(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"present": False}
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            counts = connection.execute(
                "SELECT kind, COUNT(*) FROM records GROUP BY kind"
            ).fetchall()
        finally:
            connection.close()
        return {
            "present": True,
            "integrity": integrity[0] if integrity else "unknown",
            "counts": {str(kind): int(count) for kind, count in counts},
        }
    except sqlite3.DatabaseError as error:
        return {"present": True, "integrity": "corrupt", "error": type(error).__name__}


if __name__ == "__main__":
    raise SystemExit(main())

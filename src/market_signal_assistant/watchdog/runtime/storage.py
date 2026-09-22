from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal


@dataclass(frozen=True, slots=True)
class StoragePolicy:
    minimum_free_bytes: int = 1_073_741_824
    minimum_free_ratio: float = 0.05


@dataclass(frozen=True, slots=True)
class StorageSnapshot:
    recorded_at: datetime
    total_bytes: int
    jsonl_bytes: int
    sqlite_bytes: int
    disk_total_bytes: int
    disk_free_bytes: int
    pressure: bool
    process_rss_bytes: int | None
    process_cpu_seconds: float
    files: tuple[tuple[str, int], ...]

    @property
    def sample_id(self) -> str:
        payload = json.dumps(
            [self.recorded_at.isoformat(), self.total_bytes, self.disk_free_bytes],
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()


class StorageMonitor:
    def __init__(self, root: Path, policy: StoragePolicy | None = None) -> None:
        self._root = root.resolve()
        self._policy = policy or StoragePolicy()

    def snapshot(self, *, recorded_at: datetime) -> StorageSnapshot:
        timestamp = recorded_at.astimezone(UTC)
        files = (
            tuple(
                sorted(
                    (
                        (str(path.relative_to(self._root)), path.stat().st_size)
                        for path in self._root.rglob("*")
                        if path.is_file()
                    ),
                    key=lambda item: item[0],
                )
            )
            if self._root.exists()
            else ()
        )
        usage_root = self._root if self._root.exists() else self._root.parent
        usage = shutil.disk_usage(usage_root)
        total = sum(size for _, size in files)
        free_ratio = usage.free / usage.total if usage.total else 0.0
        return StorageSnapshot(
            timestamp,
            total,
            sum(size for name, size in files if name.endswith(".jsonl")),
            sum(size for name, size in files if name.endswith((".sqlite", ".sqlite3"))),
            usage.total,
            usage.free,
            usage.free < self._policy.minimum_free_bytes
            or free_ratio < self._policy.minimum_free_ratio,
            process_rss_bytes(),
            time.process_time(),
            files,
        )


class StorageTelemetryJournal:
    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="sample_id")

    def append(self, snapshot: StorageSnapshot) -> bool:
        return self._journal.append(
            snapshot.sample_id,
            {
                "sample_id": snapshot.sample_id,
                "recorded_at": snapshot.recorded_at.isoformat(),
                "total_bytes": snapshot.total_bytes,
                "jsonl_bytes": snapshot.jsonl_bytes,
                "sqlite_bytes": snapshot.sqlite_bytes,
                "disk_total_bytes": snapshot.disk_total_bytes,
                "disk_free_bytes": snapshot.disk_free_bytes,
                "pressure": snapshot.pressure,
                "process_rss_bytes": snapshot.process_rss_bytes,
                "process_cpu_seconds": snapshot.process_cpu_seconds,
                "files": dict(snapshot.files),
            },
        )

    def records(self) -> tuple[dict[str, object], ...]:
        return self._journal.records()

    @property
    def retained_index_entries(self) -> int:
        return self._journal.retained_index_entries


def process_rss_bytes() -> int | None:
    if os.name == "nt":
        return _windows_rss()
    try:
        resident_pages = int(Path("/proc/self/statm").read_text().split()[1])
        sysconf: Any = vars(os)["sysconf"]
        return resident_pages * int(sysconf("SC_PAGE_SIZE"))
    except (AttributeError, IndexError, OSError, ValueError):
        pass
    try:
        resource: Any = importlib.import_module("resource")
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        platform_name = getattr(os, "uname", lambda: ("",))()[0]
        return int(rss * (1 if platform_name == "Darwin" else 1024))
    except (AttributeError, ImportError, OSError):
        return None


def _windows_rss() -> int | None:
    try:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        windll: Any = vars(ctypes)["windll"]
        get_process = windll.kernel32.GetCurrentProcess
        get_process.restype = wintypes.HANDLE
        get_memory = windll.psapi.GetProcessMemoryInfo
        get_memory.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(Counters),
            wintypes.DWORD,
        )
        get_memory.restype = wintypes.BOOL
        if not get_memory(get_process(), ctypes.byref(counters), counters.cb):
            return None
        return int(counters.WorkingSetSize)
    except (AttributeError, OSError):
        return None

"""Small, atomically replaced cross-process read model; no journal access."""

from __future__ import annotations

import os
import stat
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.control_models import (
    MAX_SNAPSHOT_BYTES,
    ScalperControlSnapshot,
    UnsupportedControlSchema,
    snapshot_from_bytes,
    snapshot_to_bytes,
)

DEFAULT_CONTROL_SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_control_snapshot.json"
)


def control_snapshot_path() -> Path:
    return Path(
        os.getenv(
            "QTR_SCALPER_V2_CONTROL_SNAPSHOT_PATH", str(DEFAULT_CONTROL_SNAPSHOT_PATH)
        )
    )


class SnapshotState(StrEnum):
    MISSING = "MISSING"
    MALFORMED = "MALFORMED"
    UNSUPPORTED = "UNSUPPORTED"
    STALE = "STALE"
    FRESH = "FRESH"
    FUTURE = "FUTURE"


@dataclass(frozen=True, slots=True)
class ControlRead:
    state: SnapshotState
    snapshot: ScalperControlSnapshot | None = None
    age_seconds: float | None = None


class ControlSnapshotStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = (path if path is not None else control_snapshot_path()).absolute()

    def _validate_path(self) -> None:
        # A snapshot path must never alias a large authoritative source journal.
        resolved = self.path.resolve()
        if resolved.suffix != ".json":
            raise ValueError("Control publication requires a .json read-model path")
        for key, value in os.environ.items():
            if (
                key.startswith("QTR_")
                and (key.endswith("JOURNAL_PATH") or key.endswith("AUDIT_PATH"))
                and Path(value).resolve() == resolved
            ):
                raise ValueError("Control snapshot aliases an evidence path")

    def publish(self, snapshot: ScalperControlSnapshot) -> None:
        self._validate_path()
        data = snapshot_to_bytes(snapshot)
        if len(data) > MAX_SNAPSHOT_BYTES:
            raise ValueError("Oversized control snapshot")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.path.parent,
                prefix=".scalper-control-",
                delete=False,
            ) as stream:
                temporary = stream.name
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
                # NamedTemporaryFile is private by default on POSIX. The
                # existing Scanner process needs a readable operator projection.
            if os.name == "posix":
                os.chmod(temporary, 0o644)
            os.replace(temporary, self.path)
            temporary = None
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)

    def read(self, *, now: datetime | None = None) -> ControlRead:
        try:
            self._validate_path()
            if not stat.S_ISREG(self.path.stat().st_mode):
                return ControlRead(SnapshotState.MALFORMED)
            with self.path.open("rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    return ControlRead(SnapshotState.MALFORMED)
                data = stream.read(MAX_SNAPSHOT_BYTES + 1)
            snapshot = snapshot_from_bytes(data)
            checked_at = now or datetime.now(UTC)
            if checked_at.tzinfo is None or checked_at.utcoffset() is None:
                return ControlRead(SnapshotState.MALFORMED)
            generated_age = (checked_at - snapshot.generated_at).total_seconds()
            observed_age = (checked_at - snapshot.observed_at).total_seconds()
            age = max(generated_age, observed_age)
            if min(generated_age, observed_age) < -5:
                return ControlRead(SnapshotState.FUTURE, snapshot, age)
            state = (
                SnapshotState.STALE
                if age > snapshot.stale_after_seconds
                else SnapshotState.FRESH
            )
            return ControlRead(state, snapshot, max(0, age))
        except FileNotFoundError:
            return ControlRead(SnapshotState.MISSING)
        except UnsupportedControlSchema:
            return ControlRead(SnapshotState.UNSUPPORTED)
        except (OSError, ValueError, TypeError, RecursionError, OverflowError):
            return ControlRead(SnapshotState.MALFORMED)

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from market_signal_assistant.watchdog.runtime.models import RuntimeHealthSnapshot


class JsonRuntimeHealthStore:
    """Atomic local health snapshot; never starts a server or background work."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def save(self, snapshot: RuntimeHealthSnapshot) -> None:
        payload = asdict(snapshot)
        for key in ("started_at", "last_loop_at", "last_successful_market_update"):
            value = payload[key]
            payload[key] = value.isoformat() if isinstance(value, datetime) else None
        payload["degraded_reasons"] = list(snapshot.degraded_reasons)
        temporary: Path | None = None
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._path)
        except OSError:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise

    def load(self) -> dict[str, object] | None:
        if not self._path.exists():
            return None
        value = json.loads(self._path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Runtime health snapshot is invalid.")
        return value

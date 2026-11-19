"""Offline detection-only pilot identity, recovery and journal tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from watchdog_evidence_helpers import evidence

from market_signal_assistant.watchdog.outcomes.models import PriceObservation
from market_signal_assistant.watchdog.runtime import observer
from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.detection_only import (
    DetectionOnlyOutcomes,
    RuntimeModeError,
)
from market_signal_assistant.watchdog.runtime.schedule import BucketCursorError

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def test_detection_only_composition_has_no_outcome_scheduler_or_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "new-root"
    first = build_bybit_shadow_runtime(root, detection_only=True).runtime
    assert isinstance(first._outcomes, DetectionOnlyOutcomes)
    mode = root / "state/runtime-mode.json"
    assert json.loads(mode.read_text(encoding="utf-8")) == {
        "version": 1,
        "mode": "DETECTION_ONLY",
    }
    assert not (root / "state/pending.json").exists()
    assert not (root / "outcomes").exists()
    with pytest.raises(ValueError, match="persisted event"):
        first._outcomes.register(evidence())
    with pytest.raises(RuntimeModeError, match="Outcome observation attempted"):
        first._outcomes.observe(PriceObservation("ABCUSDT", NOW, NOW, 100.0))
    first._events.append(evidence())
    first._outcomes.register(evidence())
    first._cursors.save("ABCUSDT", "5m", NOW)

    resumed = build_bybit_shadow_runtime(root, detection_only=True).runtime
    assert isinstance(resumed._outcomes, DetectionOnlyOutcomes)
    assert resumed._events.recovery.record_count == 1
    assert resumed._cursors.get("ABCUSDT", "5m") == NOW
    assert resumed._outcomes.retained_counts == (0, 0, 0, 0)
    assert not (root / "state/pending.json").exists()
    assert not (root / "outcomes").exists()

    with pytest.raises(RuntimeModeError, match="differs"):
        build_bybit_shadow_runtime(root)
    assert resumed._events.recovery.record_count == 1


def test_detection_only_rejects_missing_mode_for_existing_events(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    runtime = build_bybit_shadow_runtime(root, detection_only=True).runtime
    runtime._events.append(evidence())
    mode = root / "state/runtime-mode.json"
    mode.unlink()
    original = (root / "events/events.jsonl").read_bytes()
    with pytest.raises(RuntimeModeError, match="fresh dataset"):
        build_bybit_shadow_runtime(root, detection_only=True)
    assert (root / "events/events.jsonl").read_bytes() == original
    assert not mode.exists()


@pytest.mark.parametrize("corruption", ["{", '{"version":1,"mode":"FULL"}'])
def test_detection_only_rejects_corrupt_or_mismatched_mode(
    tmp_path: Path, corruption: str
) -> None:
    root = tmp_path / "root"
    build_bybit_shadow_runtime(root, detection_only=True)
    mode = root / "state/runtime-mode.json"
    mode.write_text(corruption, encoding="utf-8")
    with pytest.raises(RuntimeModeError):
        build_bybit_shadow_runtime(root, detection_only=True)
    assert mode.read_text(encoding="utf-8") == corruption


def test_detection_only_rejects_outcome_state_and_missing_cursor(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    runtime = build_bybit_shadow_runtime(root, detection_only=True).runtime
    runtime._events.append(evidence())
    with pytest.raises(BucketCursorError, match="cursor missing"):
        build_bybit_shadow_runtime(root, detection_only=True)
    runtime._cursors.save("ABCUSDT", "5m", NOW)
    outcome = root / "outcomes/outcomes.jsonl"
    outcome.parent.mkdir()
    outcome.write_bytes(b"")
    with pytest.raises(RuntimeModeError, match="outcome state"):
        build_bybit_shadow_runtime(root, detection_only=True)


def test_worker_passes_detection_mode_to_composition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requested: list[bool] = []

    def reject(*_args: object, detection_only: bool, **_kwargs: object) -> None:
        requested.append(detection_only)
        raise RuntimeModeError("injected mode conflict")

    monkeypatch.setattr(observer, "build_bybit_shadow_runtime", reject)
    assert (
        observer.worker(tmp_path, 8, 60, detection_only=True)
        == observer.INTEGRITY_EXIT
    )
    assert requested == [True]


def test_supervisor_cli_pins_mode_and_passes_worker_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "pilot"
    journal = root / "events/events.jsonl"
    journal.parent.mkdir(parents=True)
    journal.touch()
    commands: list[list[str]] = []
    monkeypatch.setattr(observer, "capacity", lambda _root: object())
    monkeypatch.setattr(observer, "check_capacity", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        observer,
        "adapter_state",
        lambda *_args, **_kwargs: observer.AdapterState(0, 0, 0, 0),
    )
    monkeypatch.setattr(observer, "check_adapter", lambda _state: None)
    monkeypatch.setattr(observer.signal, "signal", lambda *_args: None)

    def fake_popen(command: list[str]) -> object:
        commands.append(command)
        return object()

    def fake_supervise(
        _root: Path,
        _state_db: Path,
        _unit: str,
        child_factory: object,
        *,
        stop: object,
    ) -> int:
        del stop
        assert callable(child_factory)
        child_factory()
        return 0

    monkeypatch.setattr(observer.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(observer, "supervise", fake_supervise)
    assert observer.main(
        [
            "--data-root",
            str(root),
            "--adapter-state-db",
            str(tmp_path / "delivery.sqlite3"),
            "--detection-only",
        ]
    ) == 0
    assert commands and commands[0][-1] == "--detection-only"
    assert json.loads((root / "state/runtime-mode.json").read_text())["mode"] == (
        "DETECTION_ONLY"
    )

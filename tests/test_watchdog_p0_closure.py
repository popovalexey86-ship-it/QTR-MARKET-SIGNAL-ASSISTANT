from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.runtime import baseline_report, observer, soak
from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.liveness import SoakLivenessError
from market_signal_assistant.watchdog.runtime.schedule import BucketCursorError

NOW = datetime(2026, 11, 17, tzinfo=UTC)


def test_missing_legacy_checkpoint_with_prior_state_fails_closed(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "health.json").write_text("{}", encoding="utf-8")
    with pytest.raises(SoakLivenessError, match="checkpoint missing"):
        soak._load_checkpoint(state / "soak.json")
    assert not (state / "soak.json").exists()


@pytest.mark.parametrize("payload", ["{", '{"version":1}', '{"version":2}'])
def test_incomplete_or_corrupt_legacy_checkpoint_fails_closed(
    tmp_path: Path, payload: str
) -> None:
    path = tmp_path / "state" / "soak.json"
    path.parent.mkdir()
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(SoakLivenessError, match="checkpoint invalid"):
        soak._load_checkpoint(path)
    assert path.read_text(encoding="utf-8") == payload


def test_complete_legacy_checkpoint_is_recovered(tmp_path: Path) -> None:
    path = tmp_path / "state" / "soak.json"
    soak._save_checkpoint(path, 5.0, 60.0, NOW, "running")
    assert soak._load_checkpoint(path)["accumulated_seconds"] == 5.0


def test_missing_bucket_cursor_with_prior_runtime_is_rejected_before_replay(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    state = root / "state"
    state.mkdir(parents=True)
    (state / "health.json").write_text("{}", encoding="utf-8")
    journal = root / "events" / "events.jsonl"
    journal.parent.mkdir()
    journal.write_bytes(b"")
    with pytest.raises(BucketCursorError, match="cursor missing"):
        build_bybit_shadow_runtime(root)
    assert journal.read_bytes() == b""
    assert not (state / "buckets.json").exists()
    assert (
        observer._failure_exit(BucketCursorError("missing"))
        == observer.INTEGRITY_EXIT
    )


def test_corrupt_bucket_cursor_is_rejected_without_replacement(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    path = root / "state" / "buckets.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"version": 1, "cursors": "invalid"}), encoding="utf-8")
    with pytest.raises(BucketCursorError, match="invalid"):
        build_bybit_shadow_runtime(root)
    assert json.loads(path.read_text(encoding="utf-8"))["cursors"] == "invalid"


def test_history_wide_baseline_cli_requires_explicit_resource_budget(
    tmp_path: Path,
) -> None:
    with pytest.raises(SystemExit) as error:
        baseline_report.main(["--data-root", str(tmp_path)])
    assert error.value.code == 2

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.composition import (
    build_inplay_notification_service,
    build_inplay_service,
)
from market_signal_assistant.settings import InPlayTimingAuditSettings


def test_inplay_bootstrap_opens_neither_network_nor_local_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "inplay.json"
    calls = 0

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        nonlocal calls
        del url, timeout
        calls += 1
        raise AssertionError("network opened during bootstrap")

    service = build_inplay_service(
        getter=getter,
        clock=lambda: datetime(2026, 8, 2, tzinfo=UTC),
        state_path=state_path,
    )

    assert service is not None
    assert calls == 0
    assert state_path.exists() is False


def test_notification_bootstrap_does_not_create_state(tmp_path: Path) -> None:
    state_path = tmp_path / "inplay_notifications.json"

    service = build_inplay_notification_service(state_path=state_path)

    assert service is not None
    assert state_path.exists() is False


def test_enabled_timing_audit_bootstrap_is_lazy(tmp_path: Path) -> None:
    audit_path = tmp_path / "inplay_timing_audit.jsonl"
    detection_path = tmp_path / "inplay_detection_state.json"

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        raise AssertionError(f"network opened: {url}, {timeout}")

    service = build_inplay_service(
        getter=getter,
        audit_path=audit_path,
        detection_state_path=detection_path,
        audit_settings=InPlayTimingAuditSettings(enabled=True),
    )

    assert service is not None
    assert audit_path.exists() is False
    assert detection_path.exists() is False

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.composition import (
    build_news_notification_service,
    build_news_service,
)
from market_signal_assistant.settings import NewsSettings


def test_news_bootstrap_does_not_open_network() -> None:
    calls = 0

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        nonlocal calls
        del url, timeout
        calls += 1
        raise AssertionError("network opened during news bootstrap")

    service = build_news_service(
        NewsSettings(),
        getter=getter,
        clock=lambda: datetime(2026, 8, 2, tzinfo=UTC),
    )

    assert service is not None
    assert calls == 0


def test_news_notification_bootstrap_does_not_read_or_create_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "news_notifications.json"

    service = build_news_notification_service(
        NewsSettings(notification_retention_days=45),
        state_path=state_path,
    )

    assert service is not None
    assert not state_path.exists()

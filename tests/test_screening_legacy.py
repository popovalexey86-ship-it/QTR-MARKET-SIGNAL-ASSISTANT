from datetime import UTC, datetime
from typing import Any, cast

import pytest

from market_signal_assistant import screening
from market_signal_assistant.application.models import (
    MarketSummary,
    ScreeningReport,
)
from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.models import AssetClass, Instrument
from market_signal_assistant.providers import MarketDataProvider


def test_legacy_screener_delegates_to_canonical_application_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = Instrument("BTCUSDT", AssetClass.CRYPTO)
    report = ScreeningReport(
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
        successful_results=(),
        failed_instruments=(),
        ranked_signals=(),
        market_summary=MarketSummary(1, 1, 0, 0, 0, 1),
    )
    calls: list[object] = []

    class RecordingService:
        def __init__(self, **kwargs: Any) -> None:
            calls.append(kwargs)

        def screen(self, request: object) -> ScreeningReport:
            calls.append(request)
            return report

    monkeypatch.setattr(screening, "MarketScreeningService", RecordingService)
    result = screening.MarketScreener(
        provider=cast(MarketDataProvider, object()),
        engine=cast(SignalEngine, object()),
    ).screen((instrument,), interval="1h")

    assert result.generated_at == report.generated_at
    assert result.signals == ()
    assert result.no_signal == ()
    assert result.failures == ()
    assert len(calls) == 2

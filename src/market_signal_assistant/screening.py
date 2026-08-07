from __future__ import annotations

from collections.abc import Iterable

from market_signal_assistant.application.models import ScreeningRequest
from market_signal_assistant.application.service import MarketScreeningService
from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.models import (
    Instrument,
    ScreeningFailure,
    ScreeningResult,
)
from market_signal_assistant.providers import MarketDataProvider


class MarketScreener:
    def __init__(
        self,
        *,
        provider: MarketDataProvider,
        engine: SignalEngine,
    ) -> None:
        self._provider = provider
        self._engine = engine

    def screen(
        self,
        instruments: Iterable[Instrument],
        *,
        interval: str,
        limit: int = 250,
    ) -> ScreeningResult:
        if limit <= 0:
            raise ValueError("Candle limit must be positive.")
        selected = tuple(instruments)
        if not selected:
            from datetime import UTC, datetime

            return ScreeningResult(datetime.now(UTC), (), (), ())
        report = MarketScreeningService(
            provider=self._provider,
            analyzer=self._engine,
            candle_limit=limit,
        ).screen(
            ScreeningRequest(
                instruments=selected,
                interval=interval,
                minimum_score=0.0,
                minimum_confidence=0.0,
                maximum_results=len(selected),
            )
        )
        return ScreeningResult(
            generated_at=report.generated_at,
            signals=tuple(
                item.technical_signal
                for item in report.ranked_signals
                if item.technical_signal is not None
            ),
            no_signal=tuple(
                item.instrument
                for item in report.successful_results
                if item.technical_signal is None
            ),
            failures=tuple(
                ScreeningFailure(
                    instrument=item.instrument,
                    error_type=item.error_type,
                    message=item.message,
                )
                for item in report.failed_instruments
            ),
        )

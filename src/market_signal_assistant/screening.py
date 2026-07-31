from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

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
        signals = []
        no_signal = []
        failures = []
        for instrument in instruments:
            try:
                series = self._provider.load(instrument, interval, limit)
                signal = self._engine.analyze(series)
            except Exception as error:
                failures.append(
                    ScreeningFailure(
                        instrument=instrument,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
                continue
            if signal is None:
                no_signal.append(instrument)
            else:
                signals.append(signal)
        signals.sort(
            key=lambda signal: (signal.confidence, signal.score),
            reverse=True,
        )
        return ScreeningResult(
            generated_at=datetime.now(UTC),
            signals=tuple(signals),
            no_signal=tuple(no_signal),
            failures=tuple(failures),
        )

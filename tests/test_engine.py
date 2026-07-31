from datetime import UTC, datetime, timedelta

from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
    SignalDirection,
)

START = datetime(2026, 1, 1, tzinfo=UTC)


def series(
    *,
    bullish: bool,
    breakout: bool = True,
    volume_spike: bool = True,
) -> MarketSeries:
    candles = []
    for index in range(60):
        base = 100.0 + index * (0.5 if bullish else -0.5)
        close = base + (0.3 if bullish else -0.3)
        volume = 100.0
        if index == 59:
            if breakout:
                close += 4.0 if bullish else -4.0
            if volume_spike:
                volume = 300.0
        candles.append(
            Candle(
                timestamp=START + timedelta(hours=index),
                open=base,
                high=max(base, close) + 0.5,
                low=min(base, close) - 0.5,
                close=close,
                volume=volume,
            )
        )
    return MarketSeries(
        Instrument("TEST", AssetClass.CRYPTO),
        "1h",
        tuple(candles),
    )


def test_bullish_signal_is_explainable() -> None:
    signal = SignalEngine().analyze(series(bullish=True))

    assert signal is not None
    assert signal.direction is SignalDirection.BULLISH
    assert signal.confirmations >= 2
    assert signal.score >= 45
    assert {item.name for item in signal.evidence} >= {
        "trend",
        "momentum",
    }


def test_bearish_signal_is_explainable() -> None:
    signal = SignalEngine().analyze(series(bullish=False))

    assert signal is not None
    assert signal.direction is SignalDirection.BEARISH
    assert signal.confirmations >= 2


def test_noise_filter_removes_weak_observation() -> None:
    signal = SignalEngine(
        min_score=90,
        min_confirmations=4,
    ).analyze(
        series(
            bullish=True,
            breakout=False,
            volume_spike=False,
        )
    )

    assert signal is None


def test_insufficient_history_is_not_a_signal() -> None:
    complete = series(bullish=True)
    short = MarketSeries(
        complete.instrument,
        complete.interval,
        complete.candles[:20],
    )

    assert SignalEngine().analyze(short) is None


def test_analysis_is_deterministic() -> None:
    market = series(bullish=True)
    engine = SignalEngine()

    assert engine.analyze(market) == engine.analyze(market)

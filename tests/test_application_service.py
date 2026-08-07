from datetime import UTC, datetime

import pytest

from market_signal_assistant.application.models import (
    ScreeningDirection,
    ScreeningReport,
    ScreeningRequest,
)
from market_signal_assistant.application.service import MarketScreeningService
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)
from market_signal_assistant.providers import MarketDataError

NOW = datetime(2026, 7, 31, tzinfo=UTC)


def instrument(symbol: str) -> Instrument:
    return Instrument(symbol, AssetClass.CRYPTO)


def series(item: Instrument) -> MarketSeries:
    return MarketSeries(
        item,
        "1h",
        (Candle(NOW, 100, 101, 99, 100, 10),),
    )


def signal(
    item: Instrument,
    *,
    direction: SignalDirection,
    score: float,
    confidence: float,
    conflicts: int = 0,
) -> MarketSignal:
    return MarketSignal(
        instrument=item,
        interval="1h",
        timestamp=NOW,
        direction=direction,
        score=score,
        confidence=confidence,
        confirmations=2,
        conflicts=conflicts,
        price=100,
        evidence=(SignalEvidence("trend", direction, 25, "trend"),),
    )


class FakeProvider:
    def __init__(self, failures: set[str] | None = None) -> None:
        self.failures = failures or set()

    def load(
        self, item: Instrument, interval: str, limit: int
    ) -> MarketSeries:
        del interval, limit
        if item.symbol in self.failures:
            raise MarketDataError("provider unavailable: secret=hidden")
        return series(item)


class FakeAnalyzer:
    def __init__(self, signals: dict[str, MarketSignal | None]) -> None:
        self.signals = signals

    def analyze(self, market: MarketSeries) -> MarketSignal | None:
        return self.signals[market.instrument.symbol]


def run(
    items: tuple[Instrument, ...],
    signals: dict[str, MarketSignal | None],
    *,
    failures: set[str] | None = None,
    minimum_score: float = 0,
    minimum_confidence: float = 0,
    maximum_results: int = 10,
) -> ScreeningReport:
    service = MarketScreeningService(
        provider=FakeProvider(failures),
        analyzer=FakeAnalyzer(signals),
        clock=lambda: NOW,
    )
    return service.screen(
        ScreeningRequest(
            instruments=items,
            interval="1h",
            minimum_score=minimum_score,
            minimum_confidence=minimum_confidence,
            maximum_results=maximum_results,
        )
    )


def test_successful_long_short_and_neutral_results() -> None:
    btc, eth, sol = instrument("BTC"), instrument("ETH"), instrument("SOL")
    report = run(
        (btc, eth, sol),
        {
            "BTC": signal(
                btc, direction=SignalDirection.BULLISH, score=80, confidence=70
            ),
            "ETH": signal(
                eth, direction=SignalDirection.BEARISH, score=75, confidence=65
            ),
            "SOL": None,
        },
    )
    assert tuple(item.direction for item in report.successful_results) == (
        ScreeningDirection.LONG,
        ScreeningDirection.SHORT,
        ScreeningDirection.NEUTRAL,
    )
    assert report.market_summary.long == 1
    assert report.market_summary.short == 1
    assert report.market_summary.neutral == 1


def test_failure_is_isolated_from_other_instruments() -> None:
    btc, eth = instrument("BTC"), instrument("ETH")
    report = run(
        (btc, eth),
        {"BTC": None, "ETH": None},
        failures={"ETH"},
    )
    assert tuple(item.instrument for item in report.successful_results) == (btc,)
    assert report.failed_instruments[0].instrument == eth
    assert report.failed_instruments[0].stage == "technical"
    assert report.failed_instruments[0].message == "Market data is unavailable."
    assert "secret" not in report.failed_instruments[0].message
    assert report.market_summary.failed == 1


def test_unexpected_analyzer_error_is_not_masked_as_provider_failure() -> None:
    btc = instrument("BTC")

    class BrokenAnalyzer:
        def analyze(self, market: MarketSeries) -> MarketSignal | None:
            del market
            raise RuntimeError("programming defect")

    service = MarketScreeningService(
        provider=FakeProvider(),
        analyzer=BrokenAnalyzer(),
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="programming defect"):
        service.screen(ScreeningRequest((btc,), minimum_score=0))


def test_score_and_confidence_filters_are_inclusive() -> None:
    btc, eth, sol = instrument("BTC"), instrument("ETH"), instrument("SOL")
    report = run(
        (btc, eth, sol),
        {
            "BTC": signal(
                btc, direction=SignalDirection.BULLISH, score=60, confidence=70
            ),
            "ETH": signal(
                eth, direction=SignalDirection.BEARISH, score=59, confidence=90
            ),
            "SOL": signal(
                sol, direction=SignalDirection.BULLISH, score=90, confidence=69
            ),
        },
        minimum_score=60,
        minimum_confidence=70,
    )
    assert tuple(item.instrument.symbol for item in report.ranked_signals) == (
        "BTC",
    )
    assert len(report.successful_results) == 3


def test_sorting_uses_score_magnitude_then_confidence_and_applies_limit() -> None:
    btc, eth, sol = instrument("BTC"), instrument("ETH"), instrument("SOL")
    report = run(
        (btc, eth, sol),
        {
            "BTC": signal(
                btc, direction=SignalDirection.BEARISH, score=80, confidence=70
            ),
            "ETH": signal(
                eth, direction=SignalDirection.BULLISH, score=90, confidence=60
            ),
            "SOL": signal(
                sol, direction=SignalDirection.BULLISH, score=80, confidence=90
            ),
        },
        maximum_results=2,
    )
    assert tuple(item.instrument.symbol for item in report.ranked_signals) == (
        "ETH",
        "SOL",
    )


def test_summary_counts_all_requested_outcomes() -> None:
    btc, eth = instrument("BTC"), instrument("ETH")
    report = run(
        (btc, eth),
        {"BTC": None, "ETH": None},
        failures={"ETH"},
    )
    assert report.generated_at == NOW
    assert report.market_summary.total_instruments == 2
    assert report.market_summary.successful == 1
    assert report.market_summary.failed == 1


def test_equal_scores_and_confidence_keep_request_order() -> None:
    btc, eth, sol = instrument("BTC"), instrument("ETH"), instrument("SOL")
    report = run(
        (btc, eth, sol),
        {
            item.symbol: signal(
                item,
                direction=SignalDirection.BULLISH,
                score=70,
                confidence=70,
            )
            for item in (btc, eth, sol)
        },
    )
    assert tuple(item.instrument for item in report.ranked_signals) == (
        btc,
        eth,
        sol,
    )


def test_all_neutral_report_has_empty_ranked_signals() -> None:
    btc, eth = instrument("BTC"), instrument("ETH")
    report = run((btc, eth), {"BTC": None, "ETH": None})
    assert report.ranked_signals == ()
    assert report.market_summary.successful == 2
    assert report.market_summary.neutral == 2
    assert report.market_summary.long == 0
    assert report.market_summary.short == 0


def test_all_failed_report_has_no_successful_or_ranked_results() -> None:
    btc, eth = instrument("BTC"), instrument("ETH")
    report = run(
        (btc, eth),
        {"BTC": None, "ETH": None},
        failures={"BTC", "ETH"},
    )
    assert report.successful_results == ()
    assert report.ranked_signals == ()
    assert len(report.failed_instruments) == 2
    assert report.market_summary.failed == 2
    assert report.generated_at.utcoffset() is not None

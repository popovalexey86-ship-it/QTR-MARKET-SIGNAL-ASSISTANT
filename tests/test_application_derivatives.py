from datetime import UTC, datetime

from market_signal_assistant.application.models import (
    ScreeningRequest,
    ScreeningWarningCode,
)
from market_signal_assistant.application.presentation import present_report
from market_signal_assistant.application.service import MarketScreeningService
from market_signal_assistant.derivatives.intelligence import DerivativesIntelligence
from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.derivatives.provider import DerivativesDataError
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)
from market_signal_assistant.signals.fusion import FusionEffect, SignalFusion

NOW = datetime(2026, 7, 31, tzinfo=UTC)
BTC = Instrument("BTCUSDT", AssetClass.CRYPTO)


class Provider:
    def load(
        self, instrument: Instrument, interval: str, limit: int
    ) -> MarketSeries:
        del interval, limit
        return MarketSeries(
            instrument, "1h", (Candle(NOW, 100, 101, 99, 100, 10),)
        )


class Analyzer:
    def __init__(self, direction: SignalDirection) -> None:
        self.direction = direction

    def analyze(self, series: MarketSeries) -> MarketSignal:
        return MarketSignal(
            instrument=series.instrument,
            interval="1h",
            timestamp=NOW,
            direction=self.direction,
            score=80,
            confidence=75,
            confirmations=2,
            conflicts=0,
            price=100,
            evidence=(SignalEvidence("trend", self.direction, 25, "trend"),),
        )


class DerivativesProvider:
    def __init__(self, value: DerivativesSnapshot | Exception) -> None:
        self.value = value

    @property
    def name(self) -> str:
        return "test"

    def collect(self, symbol: str) -> DerivativesSnapshot:
        assert symbol == "BTCUSDT"
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def snapshot(**changes: float) -> DerivativesSnapshot:
    values = {
        "funding_rate": 0.0,
        "open_interest": 100.0,
        "open_interest_change": 0.03,
        "price_change": 0.02,
        "volume_change": 0.3,
        "long_liquidations": 0.0,
        "short_liquidations": 0.0,
    }
    values.update(changes)
    return DerivativesSnapshot("test", "BTCUSDT", NOW, **values)


def service(
    value: DerivativesSnapshot | Exception,
    *,
    direction: SignalDirection = SignalDirection.BULLISH,
    live: bool = False,
) -> MarketScreeningService:
    return MarketScreeningService(
        provider=Provider(),
        analyzer=Analyzer(direction),
        derivatives_provider=DerivativesProvider(value),
        derivatives_intelligence=DerivativesIntelligence(),
        fusion=SignalFusion(),
        liquidations_active=lambda: live,
        clock=lambda: NOW,
    )


def request(*, minimum_score: float = 0) -> ScreeningRequest:
    return ScreeningRequest(
        instruments=(BTC,),
        interval="1h",
        minimum_score=minimum_score,
        include_derivatives=True,
        maximum_results=10,
    )


def test_successful_derivatives_fusion() -> None:
    report = service(snapshot()).screen(request())
    result = report.successful_results[0]
    assert result.derivatives_signal is not None
    assert result.fused_signal is not None
    assert result.fused_signal.effect is FusionEffect.STRENGTHENED


def test_derivatives_failure_preserves_technical_result() -> None:
    report = service(
        DerivativesDataError("context unavailable: secret=hidden")
    ).screen(request())
    result = report.successful_results[0]
    assert result.technical_signal is not None
    assert result.derivatives_signal is None
    assert report.failed_instruments[0].stage == "derivatives"
    assert report.failed_instruments[0].message == "Derivatives context is unavailable."
    assert "secret" not in report.failed_instruments[0].message
    assert ScreeningWarningCode.DERIVATIVES_UNAVAILABLE in {
        warning.code for warning in result.warnings
    }
    assert report.market_summary.total_instruments == 1
    assert report.market_summary.successful == 1
    assert report.market_summary.failed == 0
    view = present_report(report).successful_results[0]
    assert view.combined_score == view.technical_score == 80
    assert view.derivatives_context == (
        "Данные деривативов недоступны. "
        "Итог основан на техническом анализе."
    )


def test_missing_derivatives_configuration_is_a_warning_not_total_failure() -> None:
    service_without_context = MarketScreeningService(
        provider=Provider(), analyzer=Analyzer(SignalDirection.BULLISH)
    )
    report = service_without_context.screen(request())
    assert report.failed_instruments == ()
    assert report.successful_results[0].warnings[0].code is (
        ScreeningWarningCode.DERIVATIVES_UNAVAILABLE
    )


def test_unconfirmed_oi_and_inactive_live_context_are_visible() -> None:
    report = service(
        snapshot(open_interest_change=0.0, volume_change=0.0)
    ).screen(request())
    codes = {warning.code for warning in report.successful_results[0].warnings}
    assert ScreeningWarningCode.OI_UNCONFIRMED in codes
    assert ScreeningWarningCode.LIVE_LIQUIDATIONS_INACTIVE in codes


def test_conflicting_derivatives_are_not_hidden() -> None:
    report = service(
        snapshot(
            funding_rate=0.001,
            open_interest_change=0.03,
            price_change=0.02,
            volume_change=0.0,
        )
    ).screen(request())
    result = report.successful_results[0]
    assert result.fused_signal is not None
    assert result.fused_signal.effect is FusionEffect.WEAKENED
    codes = {warning.code for warning in result.warnings}
    assert ScreeningWarningCode.DERIVATIVES_WEAKENED in codes
    assert ScreeningWarningCode.OVERHEATED_LONG in codes


def test_negative_short_combined_score_filters_by_magnitude() -> None:
    report = service(
        snapshot(
            open_interest_change=-0.03,
            price_change=-0.02,
            long_liquidations=300,
            short_liquidations=100,
        ),
        direction=SignalDirection.BEARISH,
        live=True,
    ).screen(request(minimum_score=60))
    assert report.ranked_signals
    fused = report.ranked_signals[0].fused_signal
    assert fused is not None
    assert fused.combined_score < 0
    assert abs(fused.combined_score) >= 60


def test_neutral_derivatives_reduce_magnitude_but_keep_short_direction() -> None:
    report = service(
        snapshot(
            open_interest_change=0.0,
            price_change=0.0,
            volume_change=0.0,
        ),
        direction=SignalDirection.BEARISH,
        live=True,
    ).screen(request())
    view = present_report(report).successful_results[0]
    assert view.direction == "ШОРТ"
    assert view.derivatives_score == 0.0
    assert view.combined_score < 0
    assert abs(view.combined_score) < view.technical_score
    assert view.derivatives_context.startswith("Деривативы нейтральны")

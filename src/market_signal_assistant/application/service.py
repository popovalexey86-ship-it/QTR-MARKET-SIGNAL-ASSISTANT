from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.application.models import (
    InstrumentFailure,
    MarketSummary,
    ScreeningDirection,
    ScreeningReport,
    ScreeningRequest,
    ScreeningSignalResult,
    ScreeningWarning,
    ScreeningWarningCode,
)
from market_signal_assistant.derivatives.intelligence import DerivativesIntelligence
from market_signal_assistant.derivatives.models import MarketPositioning
from market_signal_assistant.derivatives.provider import (
    DerivativesDataError,
    DerivativesProvider,
)
from market_signal_assistant.models import (
    AssetClass,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
)
from market_signal_assistant.providers import MarketDataError, MarketDataProvider
from market_signal_assistant.signals.fusion import FusionEffect, SignalFusion


class SignalAnalyzer(Protocol):
    def analyze(self, series: MarketSeries) -> MarketSignal | None: ...


class MarketScreeningService:
    """Canonical synchronous screening use case for every interface."""

    def __init__(
        self,
        *,
        provider: MarketDataProvider,
        analyzer: SignalAnalyzer,
        derivatives_provider: DerivativesProvider | None = None,
        derivatives_intelligence: DerivativesIntelligence | None = None,
        fusion: SignalFusion | None = None,
        liquidations_active: Callable[[], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
        candle_limit: int = 250,
    ) -> None:
        if candle_limit <= 0:
            raise ValueError("Candle limit must be positive.")
        self._provider = provider
        self._analyzer = analyzer
        self._derivatives_provider = derivatives_provider
        self._derivatives_intelligence = derivatives_intelligence
        self._fusion = fusion
        self._liquidations_active = liquidations_active or (lambda: False)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._candle_limit = candle_limit

    def screen(self, request: ScreeningRequest) -> ScreeningReport:
        successful: list[ScreeningSignalResult] = []
        failures: list[InstrumentFailure] = []
        for instrument in request.instruments:
            try:
                market = self._provider.load(
                    instrument,
                    request.interval,
                    self._candle_limit,
                )
            except MarketDataError as error:
                failures.append(
                    InstrumentFailure(
                        instrument=instrument,
                        stage="technical",
                        error_type=type(error).__name__,
                        message="Market data is unavailable.",
                    )
                )
                continue
            technical = self._analyzer.analyze(market)
            result = self._technical_result(instrument, technical)
            if (
                request.include_derivatives
                and instrument.asset_class is AssetClass.CRYPTO
            ):
                result, failure = self._with_derivatives(result)
                if failure is not None:
                    failures.append(failure)
            successful.append(result)

        ranked = tuple(
            sorted(
                (
                    item
                    for item in successful
                    if self._passes_filters(item, request)
                ),
                key=self._ranking_key,
                reverse=True,
            )[: request.maximum_results]
        )
        summary = MarketSummary(
            total_instruments=len(request.instruments),
            successful=len(successful),
            failed=len(request.instruments) - len(successful),
            long=sum(
                item.direction is ScreeningDirection.LONG for item in successful
            ),
            short=sum(
                item.direction is ScreeningDirection.SHORT for item in successful
            ),
            neutral=sum(
                item.direction is ScreeningDirection.NEUTRAL for item in successful
            ),
        )
        return ScreeningReport(
            generated_at=self._clock(),
            successful_results=tuple(successful),
            failed_instruments=tuple(failures),
            ranked_signals=ranked,
            market_summary=summary,
        )

    def _with_derivatives(
        self,
        result: ScreeningSignalResult,
    ) -> tuple[ScreeningSignalResult, InstrumentFailure | None]:
        if (
            self._derivatives_provider is None
            or self._derivatives_intelligence is None
            or self._fusion is None
        ):
            return (
                self._add_warning(
                    result,
                    ScreeningWarningCode.DERIVATIVES_UNAVAILABLE,
                    "Derivatives context is not configured.",
                ),
                None,
            )
        try:
            snapshot = self._derivatives_provider.collect(result.instrument.symbol)
            derivatives = self._derivatives_intelligence.analyze(snapshot)
        except DerivativesDataError as error:
            return (
                self._add_warning(
                    result,
                    ScreeningWarningCode.DERIVATIVES_UNAVAILABLE,
                    "Derivatives context is unavailable.",
                ),
                InstrumentFailure(
                    instrument=result.instrument,
                    stage="derivatives",
                    error_type=type(error).__name__,
                    message="Derivatives context is unavailable.",
                ),
            )
        fused = (
            self._fusion.combine(result.technical_signal, derivatives)
            if result.technical_signal is not None
            else None
        )
        warnings = list(result.warnings)
        if derivatives.regime is MarketPositioning.UNCONFIRMED_MOVE:
            warnings.append(
                ScreeningWarning(
                    ScreeningWarningCode.OI_UNCONFIRMED,
                    "Price movement is not confirmed by open interest.",
                )
            )
        if derivatives.regime is MarketPositioning.OVERHEATED_LONG:
            warnings.append(
                ScreeningWarning(
                    ScreeningWarningCode.OVERHEATED_LONG,
                    "Long positioning appears overheated.",
                )
            )
        if fused is not None and fused.effect is FusionEffect.WEAKENED:
            warnings.append(
                ScreeningWarning(
                    ScreeningWarningCode.DERIVATIVES_WEAKENED,
                    "Derivatives context conflicts with the technical signal.",
                )
            )
        if not self._liquidations_active():
            warnings.append(
                ScreeningWarning(
                    ScreeningWarningCode.LIVE_LIQUIDATIONS_INACTIVE,
                    "Live liquidation context is not active.",
                )
            )
        return (
            ScreeningSignalResult(
                instrument=result.instrument,
                direction=result.direction,
                technical_signal=result.technical_signal,
                derivatives_signal=derivatives,
                fused_signal=fused,
                warnings=tuple(warnings),
            ),
            None,
        )

    @staticmethod
    def _add_warning(
        result: ScreeningSignalResult,
        code: ScreeningWarningCode,
        message: str,
    ) -> ScreeningSignalResult:
        return ScreeningSignalResult(
            instrument=result.instrument,
            direction=result.direction,
            technical_signal=result.technical_signal,
            derivatives_signal=result.derivatives_signal,
            fused_signal=result.fused_signal,
            warnings=(*result.warnings, ScreeningWarning(code, message)),
        )

    @staticmethod
    def _technical_result(
        instrument: Instrument,
        technical: MarketSignal | None,
    ) -> ScreeningSignalResult:
        if technical is None:
            return ScreeningSignalResult(
                instrument=instrument,
                direction=ScreeningDirection.NEUTRAL,
                technical_signal=None,
            )
        direction = (
            ScreeningDirection.LONG
            if technical.direction is SignalDirection.BULLISH
            else ScreeningDirection.SHORT
        )
        warnings = (
            (
                ScreeningWarning(
                    ScreeningWarningCode.TECHNICAL_CONFLICT,
                    f"{technical.conflicts} conflicting technical factor(s).",
                ),
            )
            if technical.conflicts
            else ()
        )
        return ScreeningSignalResult(
            instrument=instrument,
            direction=direction,
            technical_signal=technical,
            warnings=warnings,
        )

    @staticmethod
    def _passes_filters(
        result: ScreeningSignalResult,
        request: ScreeningRequest,
    ) -> bool:
        signal = result.technical_signal
        effective_score = MarketScreeningService._effective_score(result)
        return (
            signal is not None
            and effective_score >= request.minimum_score
            and signal.confidence >= request.minimum_confidence
        )

    @staticmethod
    def _ranking_key(result: ScreeningSignalResult) -> tuple[float, float]:
        signal = result.technical_signal
        if signal is None:
            return (0.0, 0.0)
        return (MarketScreeningService._effective_score(result), signal.confidence)

    @staticmethod
    def _effective_score(result: ScreeningSignalResult) -> float:
        if result.fused_signal is not None:
            return abs(result.fused_signal.combined_score)
        if result.technical_signal is not None:
            return result.technical_signal.score
        return 0.0

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.derivatives.intelligence import DerivativesIntelligence
from market_signal_assistant.derivatives.models import MarketPositioningSignal
from market_signal_assistant.derivatives.provider import (
    DerivativesDataError,
    DerivativesProvider,
)
from market_signal_assistant.indicators import mean, true_ranges
from market_signal_assistant.inplay.audit import InPlayAuditCandidate, ScanSource
from market_signal_assistant.inplay.models import (
    CatalogInstrument,
    InPlayDirection,
    InPlayReport,
    InPlayResult,
    ListingStatus,
)
from market_signal_assistant.models import (
    AssetClass,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
)
from market_signal_assistant.providers import MarketDataError, MarketDataProvider
from market_signal_assistant.signals.fusion import SignalFusion

INPLAY_MIN_SCORE = 50.0
SIGNIFICANT_MOVE_WARNING = (
    "Движение уже значительно реализовано; повышен риск отката."
)
EXTREME_MOVE_WARNING = (
    "Резкое движение уже состоялось; высокий риск позднего входа "
    "и сильного отката."
)

_LOGGER = logging.getLogger(__name__)


class CatalogProvider(Protocol):
    def list_instruments(self) -> tuple[CatalogInstrument, ...]: ...


class SignalAnalyzer(Protocol):
    def analyze(self, series: MarketSeries) -> MarketSignal | None: ...


class ListingObserver(Protocol):
    def observe(
        self, symbols: tuple[str, ...], observed_at: datetime
    ) -> tuple[ListingStatus, ...]: ...


class TimingAuditor(Protocol):
    def record(
        self,
        candidates: tuple[InPlayAuditCandidate, ...],
        scanned_at: datetime,
        *,
        scan_source: ScanSource,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class _Activity:
    catalog: CatalogInstrument
    listing: ListingStatus
    series: MarketSeries
    technical: MarketSignal | None
    base_score: float
    score: float
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]


class InPlayService:
    MINIMUM_TURNOVER_24H = 5_000_000.0
    MAXIMUM_SPREAD_RATIO = 0.005
    MINIMUM_OBSERVATION_CANDLES = 5
    TECHNICAL_HISTORY_CANDLES = 55
    MINIMUM_BASE_ACTIVITY_SCORE = 20.0
    MAXIMUM_RESULTS = 10

    def __init__(
        self,
        *,
        catalog_provider: CatalogProvider,
        market_provider: MarketDataProvider,
        analyzer: SignalAnalyzer,
        listing_tracker: ListingObserver,
        derivatives_provider: DerivativesProvider | None = None,
        derivatives_intelligence: DerivativesIntelligence | None = None,
        fusion: SignalFusion | None = None,
        clock: Callable[[], datetime] | None = None,
        candidate_limit: int = 50,
        timing_auditor: TimingAuditor | None = None,
    ) -> None:
        if candidate_limit <= 0:
            raise ValueError("IN PLAY candidate limit must be positive.")
        self._catalog = catalog_provider
        self._market = market_provider
        self._analyzer = analyzer
        self._listings = listing_tracker
        self._derivatives = derivatives_provider
        self._intelligence = derivatives_intelligence
        self._fusion = fusion or SignalFusion()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._candidate_limit = candidate_limit
        self._timing_auditor = timing_auditor
        self._scan_lock = threading.Lock()

    def scan(
        self,
        maximum_results: int = MAXIMUM_RESULTS,
        *,
        scan_source: ScanSource = "manual",
    ) -> InPlayReport:
        with self._scan_lock:
            return self._scan_unlocked(maximum_results, scan_source)

    def _scan_unlocked(
        self,
        maximum_results: int,
        scan_source: ScanSource,
    ) -> InPlayReport:
        if maximum_results <= 0:
            raise ValueError("IN PLAY maximum results must be positive.")
        now = self._clock().astimezone(UTC)
        catalog = _deduplicate_usdt(self._catalog.list_instruments())
        listings = {
            item.symbol: item
            for item in self._listings.observe(
                tuple(item.symbol for item in catalog),
                now,
            )
        }
        eligible = tuple(
            item
            for item in catalog
            if item.status == "Trading"
            and item.turnover_24h >= self.MINIMUM_TURNOVER_24H
            and item.bid > 0
            and item.ask > 0
            and item.spread_ratio <= self.MAXIMUM_SPREAD_RATIO
        )
        selected = sorted(
            eligible,
            key=lambda item: item.turnover_24h,
            reverse=True,
        )[: self._candidate_limit]
        activities: list[_Activity] = []
        for item in selected:
            listing = listings[item.symbol]
            activity = self._activity(item, listing)
            if activity is not None:
                activities.append(activity)
        preliminary = sorted(
            activities,
            key=lambda item: item.score,
            reverse=True,
        )[: max(self.MAXIMUM_RESULTS * 2, maximum_results)]
        results = tuple(self._result(item) for item in preliminary)
        self._record_timing_audit(preliminary, results, now, scan_source)
        ranked = tuple(
            sorted(
                (
                    item
                    for item in results
                    if item.inplay_score >= INPLAY_MIN_SCORE
                ),
                key=lambda item: item.inplay_score,
                reverse=True,
            )[: min(maximum_results, self.MAXIMUM_RESULTS)]
        )
        return InPlayReport(now, ranked)

    def _record_timing_audit(
        self,
        activities: list[_Activity],
        results: tuple[InPlayResult, ...],
        scanned_at: datetime,
        scan_source: ScanSource,
    ) -> None:
        if self._timing_auditor is None:
            return
        try:
            candidates = tuple(
                InPlayAuditCandidate(
                    catalog=activity.catalog,
                    listing=activity.listing,
                    series=activity.series,
                    intraday_series=self._audit_intraday_series(
                        activity.catalog.symbol
                    ),
                    technical=activity.technical,
                    result=result,
                )
                for activity, result in zip(activities, results, strict=True)
            )
            self._timing_auditor.record(
                candidates,
                scanned_at,
                scan_source=scan_source,
            )
        except Exception as error:
            _LOGGER.warning(
                "Диагностический IN PLAY timing audit завершился ошибкой %s; "
                "результат сканирования сохранён.",
                type(error).__name__,
            )

    def _audit_intraday_series(self, symbol: str) -> MarketSeries | None:
        try:
            return self._market.load(
                Instrument(symbol, AssetClass.CRYPTO),
                "5m",
                5,
            )
        except (MarketDataError, ValueError):
            return None

    def _activity(
        self,
        catalog: CatalogInstrument,
        listing: ListingStatus,
    ) -> _Activity | None:
        try:
            series = self._market.load(
                Instrument(catalog.symbol, AssetClass.CRYPTO),
                "1h",
                100,
            )
        except (MarketDataError, ValueError):
            return None
        return self._activity_from_series(catalog, listing, series)

    def evaluate_existing_series(
        self,
        catalog: CatalogInstrument,
        series: MarketSeries,
        observed_at: datetime,
    ) -> InPlayResult | None:
        """Evaluate already loaded 1h data for diagnostic comparison only."""
        listing = ListingStatus(
            symbol=catalog.symbol,
            first_seen=observed_at.astimezone(UTC),
            is_new_listing=False,
            listing_bonus=0.0,
        )
        activity = self._activity_from_series(catalog, listing, series)
        if activity is None:
            return None
        direction, directional_score = self._direction(activity.technical, None)
        return InPlayResult(
            symbol=activity.catalog.symbol,
            direction=direction,
            inplay_score=activity.score,
            directional_score=directional_score,
            reasons=activity.reasons,
            warnings=activity.warnings,
            first_seen=listing.first_seen,
        )

    def _activity_from_series(
        self,
        catalog: CatalogInstrument,
        listing: ListingStatus,
        series: MarketSeries,
    ) -> _Activity | None:
        count = len(series.candles)
        if count < self.MINIMUM_OBSERVATION_CANDLES:
            return None
        if count < self.TECHNICAL_HISTORY_CANDLES and not listing.is_new_listing:
            return None
        technical = (
            self._analyzer.analyze(series)
            if count >= self.TECHNICAL_HISTORY_CANDLES
            else None
        )
        base_score, reasons, warnings = _score_activity(
            series,
            catalog,
            technical,
            short_history=count < self.TECHNICAL_HISTORY_CANDLES,
        )
        if base_score < self.MINIMUM_BASE_ACTIVITY_SCORE:
            return None
        if listing.is_new_listing:
            age = _age_text(listing.first_seen, self._clock())
            reasons = (
                f"Новый листинг обнаружен {age} назад.",
                *reasons,
            )
        return _Activity(
            catalog=catalog,
            listing=listing,
            series=series,
            technical=technical,
            base_score=base_score,
            score=min(100.0, base_score + listing.listing_bonus),
            reasons=reasons,
            warnings=warnings,
        )

    def _result(self, activity: _Activity) -> InPlayResult:
        score = activity.score
        reasons = activity.reasons
        warnings = activity.warnings
        positioning: MarketPositioningSignal | None = None
        if self._derivatives is not None and self._intelligence is not None:
            try:
                positioning = self._intelligence.analyze(
                    self._derivatives.collect(activity.catalog.symbol)
                )
            except DerivativesDataError:
                warnings = (*warnings, "Деривативный контекст недоступен.")
        if positioning is not None:
            derivative_points = (
                abs(positioning.directional_score) * positioning.confidence * 0.1
            )
            score = min(100.0, score + derivative_points)
            if derivative_points > 0:
                reasons = (
                    *reasons,
                    "Деривативы подтверждают активное позиционирование.",
                )
        direction, directional_score = self._direction(
            activity.technical,
            positioning,
        )
        return InPlayResult(
            symbol=activity.catalog.symbol,
            direction=direction,
            inplay_score=score,
            directional_score=directional_score,
            reasons=tuple(dict.fromkeys(reasons)),
            warnings=tuple(dict.fromkeys(warnings)),
            first_seen=activity.listing.first_seen,
            is_new_listing=activity.listing.is_new_listing,
            listing_bonus=activity.listing.listing_bonus,
        )

    def _direction(
        self,
        technical: MarketSignal | None,
        positioning: MarketPositioningSignal | None,
    ) -> tuple[InPlayDirection, float | None]:
        if (
            technical is None
            or technical.score < 70
            or technical.confidence < 70
            or technical.confirmations < 3
        ):
            return InPlayDirection.WATCH, None
        signed = (
            technical.score
            if technical.direction is SignalDirection.BULLISH
            else -technical.score
        )
        if positioning is not None:
            signed = self._fusion.combine(technical, positioning).combined_score
        if abs(signed) < 60:
            return InPlayDirection.WATCH, signed
        direction = (
            InPlayDirection.LONG
            if technical.direction is SignalDirection.BULLISH
            else InPlayDirection.SHORT
        )
        return direction, signed


def _deduplicate_usdt(
    instruments: tuple[CatalogInstrument, ...],
) -> tuple[CatalogInstrument, ...]:
    selected: dict[str, CatalogInstrument] = {}
    for item in instruments:
        symbol = item.symbol.strip().upper()
        if not item.is_crypto_linear_usdt or not symbol.endswith("USDT"):
            continue
        existing = selected.get(symbol)
        if existing is None or item.turnover_24h > existing.turnover_24h:
            selected[symbol] = item
    return tuple(selected.values())


def _score_activity(
    series: MarketSeries,
    catalog: CatalogInstrument,
    technical: MarketSignal | None,
    *,
    short_history: bool,
) -> tuple[float, tuple[str, ...], tuple[str, ...]]:
    candles = series.candles
    last = candles[-1]
    prior_volumes = tuple(item.volume for item in candles[-21:-1])
    average_volume = mean(prior_volumes)
    relative_volume = last.volume / average_volume if average_volume > 0 else 0.0
    volume_points = min(25.0, max(0.0, (relative_volume - 1.0) * 20.0))

    highs = tuple(item.high for item in candles)
    lows = tuple(item.low for item in candles)
    closes = tuple(item.close for item in candles)
    ranges = true_ranges(highs, lows, closes)
    atr = mean(ranges[-min(14, len(ranges)) :])
    volatility_pct = atr / last.close * 100.0
    volatility_points = min(20.0, volatility_pct * 5.0)

    reference = closes[-min(25, len(closes))]
    price_change = (last.close / reference - 1.0) if reference > 0 else 0.0
    price_points = min(15.0, abs(price_change) * 150.0)

    breakout = False
    if len(candles) >= 21:
        breakout = (
            last.close > max(highs[-21:-1])
            or last.close < min(lows[-21:-1])
        )
    breakout_points = 15.0 if breakout else 0.0
    technical_points = technical.score * 0.15 if technical is not None else 0.0

    reasons: list[str] = []
    if volume_points > 0:
        reasons.append(f"Относительный объём {_decimal(relative_volume)}×")
    if volatility_points > 0:
        reasons.append(f"Волатильность ATR {_decimal(volatility_pct)}%")
    if price_points > 0:
        reasons.append(
            f"Изменение цены {_signed_decimal(price_change * 100)}%"
        )
    if breakout:
        reasons.append("Цена вышла из локального диапазона")
    if technical is not None:
        reasons.append(f"Техническая сила {technical.score:.0f}")

    warnings: list[str] = []
    absolute_price_change = abs(price_change)
    if absolute_price_change >= 0.30 - 1e-12:
        warnings.append(EXTREME_MOVE_WARNING)
    elif absolute_price_change >= 0.15 - 1e-12:
        warnings.append(SIGNIFICANT_MOVE_WARNING)
    if short_history:
        warnings.append("Короткая история: направление не подтверждено.")
    elif technical is None:
        warnings.append("Направленный технический сигнал не подтверждён.")
    if catalog.spread_ratio >= 0.002:
        warnings.append("Спред повышен относительно более ликвидных инструментов.")
    if relative_volume < 1.2:
        warnings.append("Относительный объём пока слабый.")

    score = (
        volume_points
        + volatility_points
        + price_points
        + breakout_points
        + technical_points
    )
    return score, tuple(reasons), tuple(warnings)


def _age_text(first_seen: datetime, observed_at: datetime) -> str:
    elapsed = observed_at.astimezone(UTC) - first_seen
    hours = max(0, int(elapsed.total_seconds() // 3600))
    if hours < 24:
        return f"{hours} ч"
    return f"{hours // 24} дн"


def _decimal(value: float) -> str:
    return f"{value:.1f}".replace(".", ",")


def _signed_decimal(value: float) -> str:
    return f"{value:+.1f}".replace(".", ",")

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    LiquidityIntelligence,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketStateAssessment,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics
from market_signal_assistant.qtr_micro_scalper.scoring import ScalperScore
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowOpportunity

_COMPONENT_NAMES = (
    "trade_flow",
    "orderbook",
    "liquidity",
    "market_state",
    "setup_context",
    "scalper_score",
)
_MAX_TIMESTAMP_SKEW = timedelta(seconds=1)


class SnapshotReadiness(StrEnum):
    READY = "READY"
    PARTIAL = "PARTIAL"
    NOT_READY = "NOT_READY"


@dataclass(frozen=True, slots=True)
class MicrostructureSnapshotBundle:
    """Immutable point-in-time view of the complete Scalper V2 analysis chain."""

    symbol: str
    generated_at: datetime
    readiness: SnapshotReadiness
    trade_flow: TradeFlowMetrics | None
    orderbook: OrderBookMetrics | None
    liquidity: LiquidityIntelligence | None
    market_state: MarketStateAssessment | None
    setup_context: ShadowOpportunity | None
    scalper_score: ScalperScore | None
    missing_components: tuple[str, ...]
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        normalized_symbol = self.symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("Snapshot bundle symbol cannot be empty.")
        object.__setattr__(self, "symbol", normalized_symbol)
        normalized_at = _utc("generated_at", self.generated_at)
        object.__setattr__(
            self,
            "generated_at",
            normalized_at,
        )
        if not isinstance(self.readiness, SnapshotReadiness):
            raise ValueError("Snapshot bundle readiness is invalid.")
        _validate_symbols(
            normalized_symbol,
            self.trade_flow,
            self.orderbook,
            self.liquidity,
            self.market_state,
            self.setup_context,
        )
        _validate_timestamps(
            normalized_at,
            self.trade_flow,
            self.orderbook,
            self.market_state,
            self.setup_context,
        )
        expected_missing = _missing_components(
            self.trade_flow,
            self.orderbook,
            self.liquidity,
            self.market_state,
            self.setup_context,
            self.scalper_score,
        )
        if self.missing_components != expected_missing:
            raise ValueError("Snapshot bundle missing component list is inconsistent.")
        expected_readiness = _readiness(
            self.trade_flow,
            self.orderbook,
            self.market_state,
            self.setup_context,
            expected_missing,
        )
        if self.readiness is not expected_readiness:
            raise ValueError("Snapshot bundle readiness is inconsistent.")
        if not self.reasons:
            raise ValueError("Snapshot bundle requires explainable reasons.")
        _validate_texts("reason", self.reasons)
        _validate_texts("warning", self.warnings)


class MicrostructureSnapshotBuilder:
    """Combine prepared offline V2 results without collecting or executing."""

    def build(
        self,
        *,
        symbol: str,
        generated_at: datetime,
        trade_flow: TradeFlowMetrics | None = None,
        orderbook: OrderBookMetrics | None = None,
        liquidity: LiquidityIntelligence | None = None,
        market_state: MarketStateAssessment | None = None,
        setup_context: ShadowOpportunity | None = None,
        scalper_score: ScalperScore | None = None,
    ) -> MicrostructureSnapshotBundle:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("Snapshot bundle symbol cannot be empty.")
        normalized_at = _utc("generated_at", generated_at)
        _validate_symbols(
            normalized_symbol,
            trade_flow,
            orderbook,
            liquidity,
            market_state,
            setup_context,
        )
        _validate_timestamps(
            normalized_at,
            trade_flow,
            orderbook,
            market_state,
            setup_context,
        )
        missing = _missing_components(
            trade_flow,
            orderbook,
            liquidity,
            market_state,
            setup_context,
            scalper_score,
        )
        readiness = _readiness(
            trade_flow,
            orderbook,
            market_state,
            setup_context,
            missing,
        )
        warnings = _warnings(
            orderbook,
            market_state,
            setup_context,
            scalper_score,
            missing,
        )
        return MicrostructureSnapshotBundle(
            symbol=normalized_symbol,
            generated_at=normalized_at,
            readiness=readiness,
            trade_flow=trade_flow,
            orderbook=orderbook,
            liquidity=liquidity,
            market_state=market_state,
            setup_context=setup_context,
            scalper_score=scalper_score,
            missing_components=missing,
            reasons=_reasons(readiness, missing),
            warnings=warnings,
        )


def simulate_microstructure_snapshot(
    *,
    symbol: str,
    generated_at: datetime,
    trade_flow: TradeFlowMetrics | None = None,
    orderbook: OrderBookMetrics | None = None,
    liquidity: LiquidityIntelligence | None = None,
    market_state: MarketStateAssessment | None = None,
    setup_context: ShadowOpportunity | None = None,
    scalper_score: ScalperScore | None = None,
) -> MicrostructureSnapshotBundle:
    """Deterministic offline entry point over already prepared V2 components."""

    return MicrostructureSnapshotBuilder().build(
        symbol=symbol,
        generated_at=generated_at,
        trade_flow=trade_flow,
        orderbook=orderbook,
        liquidity=liquidity,
        market_state=market_state,
        setup_context=setup_context,
        scalper_score=scalper_score,
    )


def _missing_components(
    trade_flow: TradeFlowMetrics | None,
    orderbook: OrderBookMetrics | None,
    liquidity: LiquidityIntelligence | None,
    market_state: MarketStateAssessment | None,
    setup_context: ShadowOpportunity | None,
    scalper_score: ScalperScore | None,
) -> tuple[str, ...]:
    values = (
        trade_flow,
        orderbook,
        liquidity,
        market_state,
        setup_context,
        scalper_score,
    )
    return tuple(
        name
        for name, value in zip(_COMPONENT_NAMES, values, strict=True)
        if value is None
    )


def _readiness(
    trade_flow: TradeFlowMetrics | None,
    orderbook: OrderBookMetrics | None,
    market_state: MarketStateAssessment | None,
    setup_context: ShadowOpportunity | None,
    missing: tuple[str, ...],
) -> SnapshotReadiness:
    if trade_flow is None or orderbook is None:
        return SnapshotReadiness.NOT_READY
    if not orderbook.ready or (market_state is not None and not market_state.ready):
        return SnapshotReadiness.NOT_READY
    if setup_context is not None and not setup_context.price_context.ready:
        return SnapshotReadiness.NOT_READY
    if missing:
        return SnapshotReadiness.PARTIAL
    return SnapshotReadiness.READY


def _validate_symbols(
    symbol: str,
    trade_flow: TradeFlowMetrics | None,
    orderbook: OrderBookMetrics | None,
    liquidity: LiquidityIntelligence | None,
    market_state: MarketStateAssessment | None,
    setup_context: ShadowOpportunity | None,
) -> None:
    component_symbols = (
        ("trade_flow", trade_flow.symbol if trade_flow is not None else None),
        ("orderbook", orderbook.symbol if orderbook is not None else None),
        ("liquidity", liquidity.symbol if liquidity is not None else None),
        ("market_state", market_state.symbol if market_state is not None else None),
        ("setup_context", setup_context.symbol if setup_context is not None else None),
    )
    mismatches = tuple(
        name
        for name, component_symbol in component_symbols
        if component_symbol is not None and component_symbol != symbol
    )
    if mismatches:
        names = ", ".join(mismatches)
        raise ValueError(f"Snapshot bundle symbol mismatch: {names}.")


def _validate_timestamps(
    generated_at: datetime,
    trade_flow: TradeFlowMetrics | None,
    orderbook: OrderBookMetrics | None,
    market_state: MarketStateAssessment | None,
    setup_context: ShadowOpportunity | None,
) -> None:
    timestamps = (
        ("trade_flow", trade_flow.as_of if trade_flow is not None else None),
        ("orderbook", orderbook.as_of if orderbook is not None else None),
        (
            "market_state",
            market_state.assessed_at if market_state is not None else None,
        ),
        (
            "setup_context",
            setup_context.assessed_at if setup_context is not None else None,
        ),
    )
    normalized: list[datetime] = []
    for name, value in timestamps:
        if value is None:
            continue
        timestamp = _utc(f"{name} timestamp", value)
        if timestamp > generated_at:
            raise ValueError(f"Snapshot {name} timestamp cannot be in the future.")
        normalized.append(timestamp)
    if normalized and max(normalized) - min(normalized) > _MAX_TIMESTAMP_SKEW:
        raise ValueError(
            "Snapshot component timestamps differ by more than one second."
        )


def _warnings(
    orderbook: OrderBookMetrics | None,
    market_state: MarketStateAssessment | None,
    setup_context: ShadowOpportunity | None,
    scalper_score: ScalperScore | None,
    missing: tuple[str, ...],
) -> tuple[str, ...]:
    warnings = [f"Missing component: {name}." for name in missing]
    if orderbook is not None and not orderbook.ready:
        warnings.extend(f"OrderBook: {reason}." for reason in orderbook.health_reasons)
    if market_state is not None:
        if not market_state.ready:
            warnings.append("Market State is not ready.")
        warnings.extend(market_state.warnings)
    if setup_context is not None:
        if not setup_context.price_context.ready:
            warnings.extend(setup_context.price_context.health_reasons)
        warnings.extend(setup_context.warnings)
    if scalper_score is not None:
        warnings.extend(scalper_score.warnings)
    return _unique(tuple(warnings))


def _reasons(
    readiness: SnapshotReadiness,
    missing: tuple[str, ...],
) -> tuple[str, ...]:
    if readiness is SnapshotReadiness.READY:
        return ("All six Scalper V2 components are present and data-ready.",)
    if readiness is SnapshotReadiness.PARTIAL:
        return (
            "Core market data are ready, but the analysis chain is incomplete.",
            f"Missing: {', '.join(missing)}.",
        )
    if missing:
        return (
            "Snapshot does not contain enough healthy core data.",
            f"Missing: {', '.join(missing)}.",
        )
    return ("Snapshot contains an unhealthy core data component.",)


def _validate_texts(name: str, values: tuple[str, ...]) -> None:
    if any(not value.strip() for value in values):
        raise ValueError(f"Snapshot bundle {name} cannot be empty.")
    if len(values) != len(set(values)):
        raise ValueError(f"Snapshot bundle {name}s must be unique.")


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value.strip() for value in values if value.strip()))


def _utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"Snapshot bundle {name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"Snapshot bundle {name} must be timezone-aware.")
    return value.astimezone(UTC)

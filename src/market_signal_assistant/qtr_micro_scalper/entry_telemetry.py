from __future__ import annotations

import json
import math
import os
from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    FlowSide,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketBias,
    MarketState,
)
from market_signal_assistant.qtr_micro_scalper.scoring import ScalperScore
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    ShadowDirection,
    ShadowOpportunityDecision,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTrade

if TYPE_CHECKING:
    from market_signal_assistant.qtr_micro_scalper.orchestrator import (
        ShadowAnalysisInput,
    )

DEFAULT_ENTRY_FEATURE_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_entry_features.jsonl"
)
ENTRY_FEATURE_SCHEMA_VERSION = 1
_DEFAULT_DEDUP_CAPACITY = 10_000
_MAX_RECOVERY_WARNINGS = 100


@dataclass(frozen=True, slots=True)
class EntryFeatureTelemetrySettings:
    enabled: bool = False
    journal_path: Path = DEFAULT_ENTRY_FEATURE_JOURNAL_PATH

    @classmethod
    def from_environment(cls) -> EntryFeatureTelemetrySettings:
        return cls(
            enabled=_environment_bool(
                "QTR_SCALPER_V2_ENTRY_TELEMETRY_ENABLED",
                default=False,
            ),
            journal_path=Path(
                os.getenv(
                    "QTR_SCALPER_V2_ENTRY_TELEMETRY_JOURNAL_PATH",
                    str(DEFAULT_ENTRY_FEATURE_JOURNAL_PATH),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class EntryFeatureSnapshot:
    """Immutable causal inputs captured once when an entry plan is created."""

    trade_id: str
    symbol: str
    direction: ShadowDirection
    decision_timestamp: datetime

    total_score: float
    score_confidence: float
    trade_flow_component: float
    liquidity_component: float
    order_book_component: float
    market_state_component: float
    setup_context_component: float
    risk_penalty: float
    score_reasons: tuple[str, ...]
    score_warnings: tuple[str, ...]

    delta_1s: float
    delta_5s: float
    delta_15s: float
    delta_60s: float
    cvd_episode: float | None
    cvd_process: float
    cvd_day: float
    block_delta: float
    rpi_delta: float
    trade_count_5s: int
    largest_trade_5s: float

    imbalance_l1: float | None
    imbalance_l5: float | None
    imbalance_l10: float | None
    spread_bps: float | None
    bid_depth: float | None
    ask_depth: float | None
    book_age_ms: float | None
    trade_age_ms: float | None
    liquidity_pressure: float

    sweep_detected: bool
    sweep_direction: SweepDirection
    sweep_score: float
    sweep_aggressive_notional: float
    sweep_aggressive_flow_ratio: float
    sweep_delta_acceleration: float | None
    sweep_displacement_bps: float | None
    sweep_consumed_levels: int
    sweep_swept_notional: float
    sweep_opposing_depth_depletion: float | None

    absorption_detected: bool
    absorption_aggressive_side: FlowSide
    absorption_score: float
    absorption_aggressive_notional: float
    absorption_aggressive_flow_ratio: float
    absorption_favorable_price_move_bps: float | None
    absorption_opposing_depth_retention: float | None

    market_state: MarketState
    market_bias: MarketBias
    market_confidence: float

    entry_price: float
    initial_stop: float
    initial_risk_pct: float
    atr: float
    trigger_price: float
    structural_invalidation: float
    local_range_low: float | None
    local_range_high: float | None
    trigger_progress_atr: float | None
    structure_valid: bool
    setup_decision: ShadowOpportunityDecision
    setup_opportunity_score: float
    setup_confidence: float

    verified_setup_state: str | None
    verified_setup_confidence: float | None
    volume_confirmation: bool | None
    volatility_confirmation: bool | None
    liquidity_confirmation: bool | None
    source_observed_at: datetime | None
    source_age_seconds: float | None
    schema_version: int = ENTRY_FEATURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        trade_id = self.trade_id.strip()
        symbol = self.symbol.strip().upper()
        if not trade_id or not symbol:
            raise ValueError("Entry telemetry identity cannot be empty.")
        object.__setattr__(self, "trade_id", trade_id)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(
            self,
            "decision_timestamp",
            _utc("decision timestamp", self.decision_timestamp),
        )
        if self.source_observed_at is not None:
            object.__setattr__(
                self,
                "source_observed_at",
                _utc("source observed timestamp", self.source_observed_at),
            )
        for name in ("total_score", "score_confidence", "market_confidence"):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 100.0 or not math.isfinite(value):
                raise ValueError(f"Entry telemetry {name} must be within 0..100.")
        for name in ("entry_price", "initial_stop", "initial_risk_pct", "atr"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"Entry telemetry {name} must be positive.")
        if self.schema_version != ENTRY_FEATURE_SCHEMA_VERSION:
            raise ValueError("Entry telemetry schema version is unsupported.")


@dataclass(frozen=True, slots=True)
class EntryFeatureJournalMetrics:
    bootstrap_scans: int
    records_seen: int
    records_written: int
    duplicates_suppressed: int
    malformed_lines: int
    incomplete_trailing_lines: int
    retained_trade_ids: int


class EntryFeatureJournal:
    """Append-only JSONL journal with bounded recent-ID recovery state."""

    def __init__(
        self,
        path: Path = DEFAULT_ENTRY_FEATURE_JOURNAL_PATH,
        *,
        dedup_capacity: int = _DEFAULT_DEDUP_CAPACITY,
    ) -> None:
        if isinstance(dedup_capacity, bool) or dedup_capacity < 1:
            raise ValueError("Entry telemetry dedup capacity must be positive.")
        self._path = path.resolve()
        self._dedup_capacity = dedup_capacity
        self._recent_ids: OrderedDict[str, None] = OrderedDict()
        self._warnings: list[str] = []
        self._bootstrap_scans = 0
        self._records_seen = 0
        self._records_written = 0
        self._duplicates_suppressed = 0
        self._malformed_lines = 0
        self._incomplete_trailing_lines = 0
        self._needs_separator = False
        self._lock = Lock()
        self._recover()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def warnings(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._warnings)

    @property
    def metrics(self) -> EntryFeatureJournalMetrics:
        with self._lock:
            return EntryFeatureJournalMetrics(
                bootstrap_scans=self._bootstrap_scans,
                records_seen=self._records_seen,
                records_written=self._records_written,
                duplicates_suppressed=self._duplicates_suppressed,
                malformed_lines=self._malformed_lines,
                incomplete_trailing_lines=self._incomplete_trailing_lines,
                retained_trade_ids=len(self._recent_ids),
            )

    def append(self, snapshot: EntryFeatureSnapshot) -> bool:
        payload = (serialize_entry_feature_snapshot(snapshot) + "\n").encode("utf-8")
        with self._lock:
            if snapshot.trade_id in self._recent_ids:
                self._duplicates_suppressed += 1
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("ab") as stream:
                if self._needs_separator:
                    stream.write(b"\n")
                    self._needs_separator = False
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self._remember(snapshot.trade_id)
            self._records_written += 1
            return True

    def _recover(self) -> None:
        if not self._path.exists():
            return
        self._bootstrap_scans += 1
        try:
            with self._path.open("rb") as stream:
                for line_number, raw in enumerate(stream, 1):
                    if not raw.strip():
                        continue
                    if not raw.endswith(b"\n"):
                        self._incomplete_trailing_lines += 1
                        self._needs_separator = True
                        self._warn(
                            "Ignored incomplete entry telemetry trailing line "
                            f"{line_number}."
                        )
                        continue
                    try:
                        payload = json.loads(raw.decode("utf-8"))
                        trade_id = _recovered_trade_id(payload)
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                        self._malformed_lines += 1
                        self._warn(
                            f"Ignored malformed entry telemetry line {line_number}."
                        )
                        continue
                    self._records_seen += 1
                    self._remember(trade_id)
        except OSError as exc:
            self._warn(f"Entry telemetry recovery failed: {type(exc).__name__}: {exc}")

    def _remember(self, trade_id: str) -> None:
        self._recent_ids.pop(trade_id, None)
        self._recent_ids[trade_id] = None
        while len(self._recent_ids) > self._dedup_capacity:
            self._recent_ids.popitem(last=False)

    def _warn(self, message: str) -> None:
        if len(self._warnings) < _MAX_RECOVERY_WARNINGS:
            self._warnings.append(message)


class EntryFeatureTelemetry:
    """Build and persist causal entry snapshots without decision authority."""

    def __init__(self, journal: EntryFeatureJournal) -> None:
        self._journal = journal

    @property
    def journal(self) -> EntryFeatureJournal:
        return self._journal

    def capture(
        self,
        analysis: ShadowAnalysisInput,
        score: ScalperScore,
        trade: ShadowTrade,
    ) -> bool:
        return self._journal.append(
            build_entry_feature_snapshot(analysis, score, trade)
        )


def build_entry_feature_snapshot(
    analysis: ShadowAnalysisInput,
    score: ScalperScore,
    trade: ShadowTrade,
) -> EntryFeatureSnapshot:
    if (
        analysis.symbol != trade.symbol
        or score.direction.value != trade.direction.value
    ):
        raise ValueError("Entry telemetry inputs describe different trades.")
    flow = analysis.trade_flow
    book = analysis.orderbook
    liquidity = analysis.liquidity
    market = analysis.market_state
    opportunity = analysis.setup_context
    price = opportunity.price_context
    components = score.component_scores
    source_age = (
        (trade.planned_at - price.source_observed_at).total_seconds()
        if price.source_observed_at is not None
        else None
    )
    if source_age is not None and source_age < 0.0:
        source_age = None
    return EntryFeatureSnapshot(
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        direction=trade.direction,
        decision_timestamp=trade.planned_at,
        total_score=score.total_score,
        score_confidence=score.confidence,
        trade_flow_component=components.trade_flow_score,
        liquidity_component=components.liquidity_score,
        order_book_component=components.orderbook_score,
        market_state_component=components.market_state_score,
        setup_context_component=components.setup_score,
        risk_penalty=components.risk_score,
        score_reasons=score.reasons,
        score_warnings=score.warnings,
        delta_1s=flow.delta_1s,
        delta_5s=flow.delta_5s,
        delta_15s=flow.delta_15s,
        delta_60s=flow.delta_60s,
        cvd_episode=flow.cvd_episode,
        cvd_process=flow.cvd_process,
        cvd_day=flow.cvd_utc_day,
        block_delta=flow.block_delta_60s,
        rpi_delta=flow.rpi_delta_60s,
        trade_count_5s=flow.trade_count_5s,
        largest_trade_5s=flow.largest_trade_5s,
        imbalance_l1=book.imbalance_l1,
        imbalance_l5=book.imbalance_l5,
        imbalance_l10=book.imbalance_l10,
        spread_bps=book.spread_bps,
        # The decision pipeline exposes causal aggregate depth within 10 bps.
        bid_depth=book.bid_depth_10bps,
        ask_depth=book.ask_depth_10bps,
        book_age_ms=book.book_age_ms,
        trade_age_ms=market.metrics.trade_age_ms,
        liquidity_pressure=liquidity.pressure.combined_pressure,
        sweep_detected=liquidity.sweep.detected,
        sweep_direction=liquidity.sweep.direction,
        sweep_score=liquidity.sweep.score,
        sweep_aggressive_notional=liquidity.sweep.aggressive_notional,
        sweep_aggressive_flow_ratio=liquidity.sweep.aggressive_flow_ratio,
        sweep_delta_acceleration=liquidity.sweep.delta_acceleration,
        sweep_displacement_bps=liquidity.sweep.price_displacement_bps,
        sweep_consumed_levels=liquidity.sweep.levels_consumed,
        sweep_swept_notional=liquidity.sweep.swept_notional,
        sweep_opposing_depth_depletion=liquidity.sweep.depth_depletion,
        absorption_detected=liquidity.absorption.detected,
        absorption_aggressive_side=liquidity.absorption.aggressive_side,
        absorption_score=liquidity.absorption.score,
        absorption_aggressive_notional=liquidity.absorption.aggressive_notional,
        absorption_aggressive_flow_ratio=(
            liquidity.absorption.aggressive_flow_ratio
        ),
        absorption_favorable_price_move_bps=(
            liquidity.absorption.favorable_price_move_bps
        ),
        absorption_opposing_depth_retention=(
            liquidity.absorption.opposing_depth_retention
        ),
        market_state=market.state,
        market_bias=market.bias,
        market_confidence=market.confidence,
        entry_price=trade.entry_price,
        initial_stop=trade.initial_stop,
        initial_risk_pct=trade.risk_per_unit / trade.entry_price * 100.0,
        atr=price.atr,
        trigger_price=price.trigger_price,
        structural_invalidation=price.invalidation_price,
        local_range_low=price.local_range_low,
        local_range_high=price.local_range_high,
        trigger_progress_atr=price.trigger_progress_atr,
        structure_valid=price.structure_valid,
        setup_decision=opportunity.decision,
        setup_opportunity_score=opportunity.opportunity_score,
        setup_confidence=opportunity.confidence,
        verified_setup_state=price.verified_setup_state,
        verified_setup_confidence=price.verified_setup_confidence,
        volume_confirmation=price.volume_confirmation,
        volatility_confirmation=price.volatility_confirmation,
        liquidity_confirmation=price.liquidity_confirmation,
        source_observed_at=price.source_observed_at,
        source_age_seconds=source_age,
    )


def serialize_entry_feature_snapshot(snapshot: EntryFeatureSnapshot) -> str:
    payload = asdict(snapshot)
    payload["decision_timestamp"] = snapshot.decision_timestamp.isoformat()
    payload["source_observed_at"] = (
        snapshot.source_observed_at.isoformat()
        if snapshot.source_observed_at is not None
        else None
    )
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _recovered_trade_id(payload: object) -> str:
    if not isinstance(payload, dict):
        raise ValueError("Entry telemetry line must contain an object.")
    if payload.get("schema_version") != ENTRY_FEATURE_SCHEMA_VERSION:
        raise ValueError("Entry telemetry schema version is unsupported.")
    value = payload.get("trade_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Entry telemetry trade_id is missing.")
    return value.strip()


def _utc(name: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"Entry telemetry {name} must be timezone-aware.")
    return value.astimezone(UTC)


def _environment_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")

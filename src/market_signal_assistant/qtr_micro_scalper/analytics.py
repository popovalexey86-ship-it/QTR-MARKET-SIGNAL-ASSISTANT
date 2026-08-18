from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
    ShadowDecisionJournal,
    ShadowDecisionRecord,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)

_SCORE_BUCKETS = (
    ("0-49", 0.0, 50.0),
    ("50-64", 50.0, 65.0),
    ("65-79", 65.0, 80.0),
    ("80-100", 80.0, 101.0),
)
_DECISION_EVENTS = {
    ShadowDecisionEventType.DECISION_BLOCKED,
    ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
}
_TERMINAL_STAGES = {ShadowTradeStage.CLOSED, ShadowTradeStage.EXPIRED}
_ACTIVE_STAGES = {
    ShadowTradeStage.WAITING_ENTRY,
    ShadowTradeStage.OPEN,
    ShadowTradeStage.TP1_HIT,
}


@dataclass(frozen=True, slots=True)
class AnalyticsMetrics:
    total_decisions: int
    blocked_decisions: int
    shadow_entries: int
    completed_trades: int
    active_trades: int
    wins: int
    win_rate: float
    average_r: float
    total_r: float


@dataclass(frozen=True, slots=True)
class AnalyticsSlice:
    key: str
    metrics: AnalyticsMetrics

    def __post_init__(self) -> None:
        normalized = self.key.strip().upper()
        if not normalized:
            raise ValueError("Analytics slice key cannot be empty.")
        object.__setattr__(self, "key", normalized)


@dataclass(frozen=True, slots=True)
class AnalyticsReason:
    reason: str
    count: int

    def __post_init__(self) -> None:
        normalized = self.reason.strip()
        if not normalized:
            raise ValueError("Analytics reason cannot be empty.")
        if isinstance(self.count, bool) or self.count < 1:
            raise ValueError("Analytics reason count must be positive.")
        object.__setattr__(self, "reason", normalized)


@dataclass(frozen=True, slots=True)
class AnalyticsSnapshot:
    generated_at: datetime
    decision_journal_records: int
    trade_journal_records: int
    overall: AnalyticsMetrics
    by_score_bucket: tuple[AnalyticsSlice, ...]
    by_symbol: tuple[AnalyticsSlice, ...]
    by_direction: tuple[AnalyticsSlice, ...]
    by_market_state: tuple[AnalyticsSlice, ...]
    by_setup_type: tuple[AnalyticsSlice, ...]
    top_blocked_reasons: tuple[AnalyticsReason, ...]
    top_loss_reasons: tuple[AnalyticsReason, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "generated_at", _utc(self.generated_at))


@dataclass(frozen=True, slots=True)
class DecisionAnalyticsCacheMetrics:
    records_processed: int
    retained_entry_records: int
    aggregate_state_size: int


@dataclass(slots=True)
class _DecisionCounts:
    total_decisions: int = 0
    blocked_decisions: int = 0
    shadow_entries: int = 0

    def add(self, event_type: ShadowDecisionEventType) -> None:
        self.total_decisions += 1
        self.blocked_decisions += int(
            event_type is ShadowDecisionEventType.DECISION_BLOCKED
        )
        self.shadow_entries += int(
            event_type is ShadowDecisionEventType.SHADOW_ENTRY_CREATED
        )


class IncrementalDecisionAnalytics:
    """Bounded aggregate of decision history plus rare entry records."""

    def __init__(self) -> None:
        self._record_count = 0
        self._overall = _DecisionCounts()
        self._by_score: dict[str, _DecisionCounts] = {}
        self._by_symbol: dict[str, _DecisionCounts] = {}
        self._by_market: dict[str, _DecisionCounts] = {}
        self._by_setup: dict[str, _DecisionCounts] = {}
        self._blocked_reasons: dict[str, int] = {}
        self._entries: list[ShadowDecisionRecord] = []
        self._lock = Lock()

    def consume(self, record: ShadowDecisionRecord) -> None:
        with self._lock:
            self._record_count += 1
            if record.event_type not in _DECISION_EVENTS:
                return
            self._overall.add(record.event_type)
            self._add(self._by_score, _score_bucket(record.score), record)
            self._add(self._by_symbol, record.symbol, record)
            self._add(
                self._by_market,
                record.market_state or "UNKNOWN",
                record,
            )
            self._add(
                self._by_setup,
                record.setup_context or "UNSPECIFIED",
                record,
            )
            if record.event_type is ShadowDecisionEventType.SHADOW_ENTRY_CREATED:
                self._entries.append(record)
            else:
                self._add_blocked_reasons(record)

    def reset(self) -> None:
        with self._lock:
            self._record_count = 0
            self._overall = _DecisionCounts()
            self._by_score.clear()
            self._by_symbol.clear()
            self._by_market.clear()
            self._by_setup.clear()
            self._blocked_reasons.clear()
            self._entries.clear()

    def metrics(self) -> DecisionAnalyticsCacheMetrics:
        with self._lock:
            aggregate_size = (
                len(self._by_score)
                + len(self._by_symbol)
                + len(self._by_market)
                + len(self._by_setup)
                + len(self._blocked_reasons)
                + len(self._entries)
                + 1
            )
            return DecisionAnalyticsCacheMetrics(
                records_processed=self._record_count,
                retained_entry_records=len(self._entries),
                aggregate_state_size=aggregate_size,
            )

    def snapshot(
        self,
        trade_records: Iterable[ShadowTradeRecord],
        *,
        generated_at: datetime,
        recovery_warnings: tuple[str, ...] = (),
    ) -> AnalyticsSnapshot:
        with self._lock:
            return self._snapshot_locked(
                trade_records,
                generated_at=generated_at,
                recovery_warnings=recovery_warnings,
            )

    def _snapshot_locked(
        self,
        trade_records: Iterable[ShadowTradeRecord],
        *,
        generated_at: datetime,
        recovery_warnings: tuple[str, ...],
    ) -> AnalyticsSnapshot:
        normalized_at = _utc(generated_at)
        all_trades = _unique_trades(trade_records)
        lifecycles = _lifecycles(all_trades)
        entries = tuple(
            sorted(
                self._entries,
                key=lambda item: (item.timestamp, item.event_id),
            )
        )
        matched = _match_entries(entries, lifecycles)
        market_by_trade = {
            trade_id: decision.market_state or "UNKNOWN"
            for trade_id, decision in matched.items()
        }
        setup_by_trade = {
            trade_id: decision.setup_context or "UNSPECIFIED"
            for trade_id, decision in matched.items()
        }
        direction_counts: dict[str, _DecisionCounts] = {}
        if self._overall.blocked_decisions:
            direction_counts["UNKNOWN"] = _DecisionCounts(
                total_decisions=self._overall.blocked_decisions,
                blocked_decisions=self._overall.blocked_decisions,
            )
        direction_by_entry = {
            decision.event_id: lifecycle.latest.direction.value
            for lifecycle in lifecycles
            if (decision := matched.get(lifecycle.trade_id)) is not None
        }
        for entry in entries:
            key = direction_by_entry.get(entry.event_id, "UNKNOWN")
            self._add(direction_counts, key, entry)

        warnings = list(dict.fromkeys(recovery_warnings))
        warnings.append(
            "Setup type breakdown uses durable setup_context because the journals "
            "do not store a separate setup_type field."
        )
        if self._overall.blocked_decisions:
            warnings.append(
                "Blocked decision direction is UNKNOWN because the durable decision "
                "record does not store direction."
            )
        if any(item.trade_id not in matched for item in lifecycles):
            warnings.append(
                "Market state and setup type are UNKNOWN for legacy trades without a "
                "matching SHADOW_ENTRY_CREATED decision."
            )

        return AnalyticsSnapshot(
            generated_at=normalized_at,
            decision_journal_records=self._record_count,
            trade_journal_records=len(all_trades),
            overall=_metrics_from_counts(self._overall, lifecycles),
            by_score_bucket=_aggregate_breakdown(
                self._by_score,
                lifecycles,
                trade_key=lambda item: _score_bucket(item.latest.score),
                ordered_keys=tuple(item[0] for item in _SCORE_BUCKETS),
            ),
            by_symbol=_aggregate_breakdown(
                self._by_symbol,
                lifecycles,
                trade_key=lambda item: item.latest.symbol,
            ),
            by_direction=_aggregate_breakdown(
                direction_counts,
                lifecycles,
                trade_key=lambda item: item.latest.direction.value,
            ),
            by_market_state=_aggregate_breakdown(
                self._by_market,
                lifecycles,
                trade_key=lambda item: market_by_trade.get(
                    item.trade_id,
                    "UNKNOWN",
                ),
            ),
            by_setup_type=_aggregate_breakdown(
                self._by_setup,
                lifecycles,
                trade_key=lambda item: setup_by_trade.get(
                    item.trade_id,
                    "UNSPECIFIED",
                ),
            ),
            top_blocked_reasons=_ranked_reasons(self._blocked_reasons),
            top_loss_reasons=_loss_reasons(lifecycles),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    @staticmethod
    def _add(
        target: dict[str, _DecisionCounts],
        key: str,
        record: ShadowDecisionRecord,
    ) -> None:
        target.setdefault(key, _DecisionCounts()).add(record.event_type)

    def _add_blocked_reasons(self, record: ShadowDecisionRecord) -> None:
        text = " ".join((*record.reasons, *record.warnings)).casefold()
        matched = False
        for category, predicate in (
            ("spread", _contains_spread),
            ("liquidity conflict", _contains_liquidity_conflict),
            ("stale data", _contains_stale),
            ("low score", _contains_low_score),
            ("risk", _contains_risk),
        ):
            if predicate(text):
                self._blocked_reasons[category] = (
                    self._blocked_reasons.get(category, 0) + 1
                )
                matched = True
        if not matched:
            self._blocked_reasons["other"] = (
                self._blocked_reasons.get("other", 0) + 1
            )



@dataclass(frozen=True, slots=True)
class _TradeLifecycle:
    trade_id: str
    records: tuple[ShadowTradeRecord, ...]
    latest: ShadowTradeRecord
    entered: bool
    completed: bool
    active: bool
    won: bool


class ShadowAnalyticsEngine:
    """Operational, deterministic analytics over both durable shadow journals."""

    def __init__(
        self,
        decision_journal: ShadowDecisionJournal,
        trade_journal: ShadowTradeJournal,
    ) -> None:
        self._decision_journal = decision_journal
        self._trade_journal = trade_journal

    def snapshot(self, *, generated_at: datetime) -> AnalyticsSnapshot:
        return analyze_shadow_journals(
            self._decision_journal.records(),
            self._trade_journal.records(),
            generated_at=generated_at,
            recovery_warnings=(
                *self._decision_journal.recovery.warnings,
                *self._trade_journal.recovery.warnings,
            ),
        )


def analyze_shadow_journals(
    decision_records: Iterable[ShadowDecisionRecord],
    trade_records: Iterable[ShadowTradeRecord],
    *,
    generated_at: datetime,
    recovery_warnings: tuple[str, ...] = (),
) -> AnalyticsSnapshot:
    """Build an offline snapshot independent of source record ordering."""

    normalized_at = _utc(generated_at)
    all_decisions = _unique_decisions(decision_records)
    all_trades = _unique_trades(trade_records)
    decisions = tuple(
        record for record in all_decisions if record.event_type in _DECISION_EVENTS
    )
    lifecycles = _lifecycles(all_trades)
    matched = _match_entries(decisions, lifecycles)
    market_by_trade = {
        trade_id: decision.market_state or "UNKNOWN"
        for trade_id, decision in matched.items()
    }
    setup_by_trade = {
        trade_id: decision.setup_context or "UNSPECIFIED"
        for trade_id, decision in matched.items()
    }
    direction_by_decision = {
        decision.event_id: lifecycle.latest.direction.value
        for lifecycle in lifecycles
        if (decision := matched.get(lifecycle.trade_id)) is not None
    }
    warnings = list(dict.fromkeys(recovery_warnings))
    warnings.append(
        "Setup type breakdown uses durable setup_context because the journals "
        "do not store a separate setup_type field."
    )
    if any(
        item.event_type is ShadowDecisionEventType.DECISION_BLOCKED
        for item in decisions
    ):
        warnings.append(
            "Blocked decision direction is UNKNOWN because the durable decision "
            "record does not store direction."
        )
    if any(item.trade_id not in matched for item in lifecycles):
        warnings.append(
            "Market state and setup type are UNKNOWN for legacy trades without a "
            "matching SHADOW_ENTRY_CREATED decision."
        )

    return AnalyticsSnapshot(
        generated_at=normalized_at,
        decision_journal_records=len(all_decisions),
        trade_journal_records=len(all_trades),
        overall=_metrics(decisions, lifecycles),
        by_score_bucket=_breakdown(
            decisions,
            lifecycles,
            decision_key=lambda item: _score_bucket(item.score),
            trade_key=lambda item: _score_bucket(item.latest.score),
            ordered_keys=tuple(item[0] for item in _SCORE_BUCKETS),
        ),
        by_symbol=_breakdown(
            decisions,
            lifecycles,
            decision_key=lambda item: item.symbol,
            trade_key=lambda item: item.latest.symbol,
        ),
        by_direction=_breakdown(
            decisions,
            lifecycles,
            decision_key=lambda item: direction_by_decision.get(
                item.event_id,
                "UNKNOWN",
            ),
            trade_key=lambda item: item.latest.direction.value,
        ),
        by_market_state=_breakdown(
            decisions,
            lifecycles,
            decision_key=lambda item: item.market_state or "UNKNOWN",
            trade_key=lambda item: market_by_trade.get(item.trade_id, "UNKNOWN"),
        ),
        by_setup_type=_breakdown(
            decisions,
            lifecycles,
            decision_key=lambda item: item.setup_context or "UNSPECIFIED",
            trade_key=lambda item: setup_by_trade.get(
                item.trade_id,
                "UNSPECIFIED",
            ),
        ),
        top_blocked_reasons=_blocked_reasons(decisions),
        top_loss_reasons=_loss_reasons(lifecycles),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _unique_decisions(
    records: Iterable[ShadowDecisionRecord],
) -> tuple[ShadowDecisionRecord, ...]:
    unique = {record.event_id: record for record in records}
    return tuple(
        sorted(unique.values(), key=lambda item: (item.timestamp, item.event_id))
    )


def _unique_trades(
    records: Iterable[ShadowTradeRecord],
) -> tuple[ShadowTradeRecord, ...]:
    unique = {record.record_id: record for record in records}
    return tuple(
        sorted(unique.values(), key=lambda item: (item.recorded_at, item.record_id))
    )


def _lifecycles(records: tuple[ShadowTradeRecord, ...]) -> tuple[_TradeLifecycle, ...]:
    grouped: dict[str, list[ShadowTradeRecord]] = defaultdict(list)
    for record in records:
        grouped[record.trade_id].append(record)
    result: list[_TradeLifecycle] = []
    for trade_id in sorted(grouped):
        ordered = tuple(sorted(grouped[trade_id], key=_trade_record_order))
        latest = ordered[-1]
        entered = any(
            item.entry_time is not None
            or item.stage
            in {
                ShadowTradeStage.OPEN,
                ShadowTradeStage.TP1_HIT,
                ShadowTradeStage.CLOSED,
            }
            for item in ordered
        )
        result.append(
            _TradeLifecycle(
                trade_id=trade_id,
                records=ordered,
                latest=latest,
                entered=entered,
                completed=latest.stage in _TERMINAL_STAGES,
                active=latest.stage in _ACTIVE_STAGES,
                won=latest.outcome is ShadowOutcomeStatus.WIN,
            )
        )
    return tuple(result)


def _match_entries(
    decisions: tuple[ShadowDecisionRecord, ...],
    lifecycles: tuple[_TradeLifecycle, ...],
) -> dict[str, ShadowDecisionRecord]:
    entries: dict[tuple[str, float], list[ShadowDecisionRecord]] = defaultdict(list)
    trades: dict[tuple[str, float], list[_TradeLifecycle]] = defaultdict(list)
    for decision in decisions:
        if (
            decision.event_type is ShadowDecisionEventType.SHADOW_ENTRY_CREATED
            and decision.score is not None
        ):
            entries[(decision.symbol, decision.score)].append(decision)
    for lifecycle in lifecycles:
        trades[(lifecycle.latest.symbol, lifecycle.latest.score)].append(lifecycle)
    matched: dict[str, ShadowDecisionRecord] = {}
    for key in sorted(set(entries) & set(trades)):
        ordered_entries = sorted(
            entries[key],
            key=lambda item: (item.timestamp, item.event_id),
        )
        ordered_trades = sorted(
            trades[key],
            key=lambda item: (
                item.records[0].recorded_at,
                item.trade_id,
            ),
        )
        for decision, lifecycle in zip(ordered_entries, ordered_trades, strict=False):
            matched[lifecycle.trade_id] = decision
    return matched


def _metrics(
    decisions: tuple[ShadowDecisionRecord, ...],
    lifecycles: tuple[_TradeLifecycle, ...],
) -> AnalyticsMetrics:
    completed = tuple(item for item in lifecycles if item.completed)
    resolved = tuple(
        item
        for item in completed
        if item.latest.outcome
        in {
            ShadowOutcomeStatus.WIN,
            ShadowOutcomeStatus.LOSS,
            ShadowOutcomeStatus.BREAKEVEN,
        }
    )
    result_values = tuple(item.latest.result_r for item in resolved)
    wins = sum(item.won for item in resolved)
    return AnalyticsMetrics(
        total_decisions=len(decisions),
        blocked_decisions=sum(
            item.event_type is ShadowDecisionEventType.DECISION_BLOCKED
            for item in decisions
        ),
        shadow_entries=sum(
            item.event_type is ShadowDecisionEventType.SHADOW_ENTRY_CREATED
            for item in decisions
        ),
        completed_trades=len(completed),
        active_trades=sum(item.active for item in lifecycles),
        wins=wins,
        win_rate=_rate(wins, len(resolved)),
        average_r=_average(result_values),
        total_r=sum(result_values),
    )


def _metrics_from_counts(
    counts: _DecisionCounts,
    lifecycles: tuple[_TradeLifecycle, ...],
) -> AnalyticsMetrics:
    completed = tuple(item for item in lifecycles if item.completed)
    resolved = tuple(
        item
        for item in completed
        if item.latest.outcome
        in {
            ShadowOutcomeStatus.WIN,
            ShadowOutcomeStatus.LOSS,
            ShadowOutcomeStatus.BREAKEVEN,
        }
    )
    result_values = tuple(item.latest.result_r for item in resolved)
    wins = sum(item.won for item in resolved)
    return AnalyticsMetrics(
        total_decisions=counts.total_decisions,
        blocked_decisions=counts.blocked_decisions,
        shadow_entries=counts.shadow_entries,
        completed_trades=len(completed),
        active_trades=sum(item.active for item in lifecycles),
        wins=wins,
        win_rate=_rate(wins, len(resolved)),
        average_r=_average(result_values),
        total_r=sum(result_values),
    )


def _aggregate_breakdown(
    decision_groups: dict[str, _DecisionCounts],
    lifecycles: tuple[_TradeLifecycle, ...],
    *,
    trade_key: Callable[[_TradeLifecycle], str],
    ordered_keys: tuple[str, ...] | None = None,
) -> tuple[AnalyticsSlice, ...]:
    trade_groups: dict[str, list[_TradeLifecycle]] = defaultdict(list)
    for lifecycle in lifecycles:
        trade_groups[trade_key(lifecycle)].append(lifecycle)
    present = set(decision_groups) | set(trade_groups)
    keys = (
        tuple(key for key in ordered_keys if key in present)
        if ordered_keys is not None
        else tuple(sorted(present))
    )
    return tuple(
        AnalyticsSlice(
            key,
            _metrics_from_counts(
                decision_groups.get(key, _DecisionCounts()),
                tuple(trade_groups.get(key, ())),
            ),
        )
        for key in keys
    )



def _breakdown(
    decisions: tuple[ShadowDecisionRecord, ...],
    lifecycles: tuple[_TradeLifecycle, ...],
    *,
    decision_key: Callable[[ShadowDecisionRecord], str],
    trade_key: Callable[[_TradeLifecycle], str],
    ordered_keys: tuple[str, ...] | None = None,
) -> tuple[AnalyticsSlice, ...]:
    decision_groups: dict[str, list[ShadowDecisionRecord]] = defaultdict(list)
    trade_groups: dict[str, list[_TradeLifecycle]] = defaultdict(list)
    for decision in decisions:
        decision_groups[decision_key(decision)].append(decision)
    for lifecycle in lifecycles:
        trade_groups[trade_key(lifecycle)].append(lifecycle)
    present = set(decision_groups) | set(trade_groups)
    keys = (
        tuple(key for key in ordered_keys if key in present)
        if ordered_keys is not None
        else tuple(sorted(present))
    )
    return tuple(
        AnalyticsSlice(
            key,
            _metrics(
                tuple(decision_groups.get(key, ())),
                tuple(trade_groups.get(key, ())),
            ),
        )
        for key in keys
    )


def _blocked_reasons(
    decisions: tuple[ShadowDecisionRecord, ...],
) -> tuple[AnalyticsReason, ...]:
    counts: dict[str, int] = defaultdict(int)
    for decision in decisions:
        if decision.event_type is not ShadowDecisionEventType.DECISION_BLOCKED:
            continue
        text = " ".join((*decision.reasons, *decision.warnings)).casefold()
        matched = False
        for category, predicate in (
            ("spread", _contains_spread),
            ("liquidity conflict", _contains_liquidity_conflict),
            ("stale data", _contains_stale),
            ("low score", _contains_low_score),
            ("risk", _contains_risk),
        ):
            if predicate(text):
                counts[category] += 1
                matched = True
        if not matched:
            counts["other"] += 1
    return _ranked_reasons(counts)


def _loss_reasons(
    lifecycles: tuple[_TradeLifecycle, ...],
) -> tuple[AnalyticsReason, ...]:
    counts: dict[str, int] = defaultdict(int)
    for lifecycle in lifecycles:
        events = {
            event.event_type
            for record in lifecycle.records
            for event in record.events
        }
        if (
            lifecycle.latest.stage is ShadowTradeStage.EXPIRED
            or ShadowRuntimeEventType.EXPIRED in events
            or lifecycle.latest.outcome is ShadowOutcomeStatus.NOT_TRIGGERED
        ):
            counts["expired"] += 1
        elif (
            ShadowRuntimeEventType.STOPPED in events
            or any(
                "stop" in text.casefold() or "стоп" in text.casefold()
                for record in lifecycle.records
                for text in (*record.reasons, *record.warnings)
            )
        ):
            counts["stop"] += 1
        elif lifecycle.completed and lifecycle.latest.result_r < 0.0:
            counts["failed setup"] += 1
    return _ranked_reasons(counts)


def _ranked_reasons(counts: dict[str, int]) -> tuple[AnalyticsReason, ...]:
    return tuple(
        AnalyticsReason(reason, count)
        for reason, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )


def _score_bucket(score: float | None) -> str:
    if score is None:
        return "UNKNOWN"
    return next(
        label
        for label, minimum, maximum in _SCORE_BUCKETS
        if minimum <= score < maximum
    )


def _trade_record_order(record: ShadowTradeRecord) -> tuple[datetime, int, str]:
    order = {
        ShadowTradeStage.WAITING_ENTRY: 0,
        ShadowTradeStage.OPEN: 1,
        ShadowTradeStage.TP1_HIT: 2,
        ShadowTradeStage.CLOSED: 3,
        ShadowTradeStage.EXPIRED: 3,
    }
    return record.recorded_at, order[record.stage], record.record_id


def _contains_spread(text: str) -> bool:
    return "spread" in text or "спред" in text


def _contains_liquidity_conflict(text: str) -> bool:
    return (
        "liquidity conflict" in text
        or "opposing liquidity" in text
        or "opposite liquidity" in text
        or "конфликт ликвидност" in text
        or "противоположн" in text
        and "ликвидност" in text
    )


def _contains_stale(text: str) -> bool:
    return "stale" in text or "outdated" in text or "устар" in text


def _contains_low_score(text: str) -> bool:
    return (
        "low score" in text
        or "score below" in text
        or "score is below" in text
        or "insufficient score" in text
        or "низк" in text
        and "балл" in text
    )


def _contains_risk(text: str) -> bool:
    return "risk" in text or "риск" in text


def _rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator * 100.0


def _average(values: tuple[float, ...]) -> float:
    return 0.0 if not values else sum(values) / len(values)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Analytics timestamp must be timezone-aware.")
    return value.astimezone(UTC)

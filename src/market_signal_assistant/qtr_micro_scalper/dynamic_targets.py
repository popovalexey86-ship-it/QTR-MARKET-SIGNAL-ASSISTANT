from __future__ import annotations

import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    VerifiedSetupRecord,
)

_ELIGIBLE_STATES = frozenset({"FORMING", "CONFIRMING", "READY_TO_CONSIDER"})
_STATE_PRIORITY = {
    "FORMING": 1,
    "CONFIRMING": 2,
    "READY_TO_CONSIDER": 3,
}


class VerifiedUniverseProvider(Protocol):
    def latest_records(self) -> tuple[VerifiedSetupRecord, ...]: ...


class TargetExclusionReason(StrEnum):
    CANCELLED = "CANCELLED"
    LATE = "LATE"
    STALE = "stale"
    INCOMPLETE = "incomplete"
    OUTSIDE_TOP_N = "outside_top_n"


@dataclass(frozen=True, slots=True)
class DynamicTargetSettings:
    enabled: bool = False
    max_active_symbols: int = 5
    refresh_seconds: float = 30.0
    maximum_age: timedelta = timedelta(minutes=15)

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_active_symbols, bool)
            or not 1 <= self.max_active_symbols <= 100
        ):
            raise ValueError(
                "QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS must be between 1 and 100."
            )
        if not 1.0 <= self.refresh_seconds <= 3_600.0:
            raise ValueError(
                "QTR_SCALPER_V2_TARGET_REFRESH_SECONDS must be between 1 and 3600."
            )
        if self.maximum_age <= timedelta(0):
            raise ValueError("Dynamic target freshness must be positive.")

    @classmethod
    def from_environment(cls) -> DynamicTargetSettings:
        enabled = (
            os.getenv("QTR_SCALPER_V2_DYNAMIC_TARGETS_ENABLED", "false").strip().lower()
            == "true"
        )
        try:
            maximum = int(os.getenv("QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS", "5").strip())
            refresh = float(
                os.getenv("QTR_SCALPER_V2_TARGET_REFRESH_SECONDS", "30").strip()
            )
        except ValueError:
            raise ValueError(
                "Dynamic Scalper V2 target settings must be numeric."
            ) from None
        return cls(
            enabled=enabled,
            max_active_symbols=maximum,
            refresh_seconds=refresh,
        )


@dataclass(frozen=True, slots=True)
class RankedVerifiedTarget:
    record: VerifiedSetupRecord
    state_priority: int

    @property
    def symbol(self) -> str:
        return self.record.symbol


@dataclass(frozen=True, slots=True)
class TargetExclusion:
    symbol: str
    reason: TargetExclusionReason


@dataclass(frozen=True, slots=True)
class DynamicUniverseSnapshot:
    refreshed_at: datetime
    eligible: tuple[RankedVerifiedTarget, ...]
    desired_symbols: tuple[str, ...]
    active_symbols: tuple[str, ...]
    protected_trade_symbols: tuple[str, ...]
    added: tuple[str, ...]
    retained: tuple[str, ...]
    removed: tuple[str, ...]
    replaced: tuple[tuple[str, str], ...]
    exclusions: tuple[TargetExclusion, ...]


@dataclass(frozen=True, slots=True)
class DynamicTargetMetrics:
    target_refreshes: int
    eligible_verified_symbols: int
    desired_symbols: int
    active_symbols: int
    protected_trade_symbols: int
    symbols_added: int
    symbols_removed: int
    last_target_refresh_at: datetime | None
    aggregate_state_size: int


class DynamicVerifiedTargetManager:
    """Select a bounded verified universe without using Scalper scores."""

    def __init__(
        self,
        provider: VerifiedUniverseProvider,
        settings: DynamicTargetSettings | None = None,
    ) -> None:
        self._provider = provider
        self._settings = settings or DynamicTargetSettings()
        self._active: tuple[str, ...] = ()
        self._refreshes = 0
        self._symbols_added = 0
        self._symbols_removed = 0
        self._last: DynamicUniverseSnapshot | None = None

    @property
    def settings(self) -> DynamicTargetSettings:
        return self._settings

    def refresh(
        self,
        *,
        at: datetime,
        protected_symbols: Iterable[str] = (),
    ) -> DynamicUniverseSnapshot:
        now = _utc(at)
        protected = tuple(
            sorted(
                dict.fromkeys(
                    value.strip().upper()
                    for value in protected_symbols
                    if value.strip()
                )
            )
        )
        eligible: list[RankedVerifiedTarget] = []
        excluded: list[TargetExclusion] = []
        for record in self._provider.latest_records():
            reason = _exclusion(record, now, self._settings.maximum_age)
            if reason is not None:
                excluded.append(TargetExclusion(record.symbol, reason))
                continue
            eligible.append(
                RankedVerifiedTarget(
                    record=record,
                    state_priority=_STATE_PRIORITY[record.setup_state],
                )
            )
        ranked = tuple(sorted(eligible, key=_rank_key))
        selected = ranked[: self._settings.max_active_symbols]
        desired = tuple(item.symbol for item in selected)
        selected_set = set(desired)
        excluded.extend(
            TargetExclusion(item.symbol, TargetExclusionReason.OUTSIDE_TOP_N)
            for item in ranked[self._settings.max_active_symbols :]
        )
        active = tuple(sorted(selected_set | set(protected)))
        previous = set(self._active)
        current = set(active)
        added = tuple(sorted(current - previous))
        retained = tuple(sorted(current & previous))
        removed = tuple(sorted(previous - current))
        replaced = tuple(zip(removed, added, strict=False))
        snapshot = DynamicUniverseSnapshot(
            refreshed_at=now,
            eligible=ranked,
            desired_symbols=desired,
            active_symbols=active,
            protected_trade_symbols=protected,
            added=added,
            retained=retained,
            removed=removed,
            replaced=replaced,
            exclusions=tuple(
                sorted(excluded, key=lambda item: (item.reason.value, item.symbol))
            ),
        )
        self._active = active
        self._refreshes += 1
        self._symbols_added += len(added)
        self._symbols_removed += len(removed)
        self._last = snapshot
        return snapshot

    def snapshot(self) -> DynamicUniverseSnapshot | None:
        return self._last

    def metrics(self) -> DynamicTargetMetrics:
        last = self._last
        return DynamicTargetMetrics(
            target_refreshes=self._refreshes,
            eligible_verified_symbols=len(last.eligible) if last else 0,
            desired_symbols=len(last.desired_symbols) if last else 0,
            active_symbols=len(last.active_symbols) if last else 0,
            protected_trade_symbols=(len(last.protected_trade_symbols) if last else 0),
            symbols_added=self._symbols_added,
            symbols_removed=self._symbols_removed,
            last_target_refresh_at=last.refreshed_at if last else None,
            aggregate_state_size=(
                len(self._active) + (len(last.exclusions) if last else 0)
            ),
        )


def format_dynamic_universe(snapshot: DynamicUniverseSnapshot) -> str:
    lines = [
        "🧠 Dynamic Universe",
        f"Eligible: {len(snapshot.eligible)}",
        f"Desired TOP: {len(snapshot.desired_symbols)}",
        f"Active subscriptions: {len(snapshot.active_symbols)}",
        f"Protected by active trades: {len(snapshot.protected_trade_symbols)}",
        "",
        "Symbols:",
    ]
    if not snapshot.eligible:
        lines.append("Нет verified targets.")
    else:
        for index, item in enumerate(snapshot.eligible, start=1):
            lines.append(
                f"{index}. {item.symbol} — {item.record.setup_state} — "
                f"{item.record.setup_confidence:.0f}"
            )
    lines.extend(("", "Причины исключения:"))
    if not snapshot.exclusions:
        lines.append("Нет.")
    else:
        counts: dict[TargetExclusionReason, int] = {}
        for exclusion in snapshot.exclusions:
            counts[exclusion.reason] = counts.get(exclusion.reason, 0) + 1
        for reason in TargetExclusionReason:
            if reason in counts:
                lines.append(f"- {reason.value}: {counts[reason]}")
    return "\n".join(lines)


def _rank_key(item: RankedVerifiedTarget) -> tuple[object, ...]:
    record = item.record
    return (
        -item.state_priority,
        -record.setup_confidence,
        -int(record.liquidity_ok),
        -int(record.volume_confirmation),
        -int(record.volatility_confirmation),
        -record.observed_at.timestamp(),
        record.symbol,
    )


def _exclusion(
    record: VerifiedSetupRecord,
    at: datetime,
    maximum_age: timedelta,
) -> TargetExclusionReason | None:
    if record.setup_state == "CANCELLED":
        return TargetExclusionReason.CANCELLED
    if record.setup_state == "LATE":
        return TargetExclusionReason.LATE
    age = at - record.observed_at
    if age < timedelta(0) or age > maximum_age:
        return TargetExclusionReason.STALE
    if record.setup_state not in _ELIGIBLE_STATES:
        return TargetExclusionReason.INCOMPLETE
    values = (
        record.market_price,
        record.atr,
        record.trigger_price,
        record.invalidation_price,
        record.local_range_low,
        record.local_range_high,
    )
    if (
        record.source_direction != record.setup_direction
        or record.setup_direction not in {"UP", "DOWN"}
        or any(not _positive(value) for value in values)
        or record.local_range_low >= record.local_range_high
        or not record.local_range_low <= record.trigger_price <= record.local_range_high
        or not 0.0 <= record.setup_confidence <= 100.0
    ):
        return TargetExclusionReason.INCOMPLETE
    return None


def _positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Dynamic target timestamps must be timezone-aware.")
    return value.astimezone(UTC)

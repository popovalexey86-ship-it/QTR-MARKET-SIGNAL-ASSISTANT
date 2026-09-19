from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from market_signal_assistant.inplay.models import CatalogInstrument


class UniverseTier(StrEnum):
    COLD_START = "COLD_START"
    STANDARD = "STANDARD"
    ACTIVE = "ACTIVE"
    CORE = "CORE"


@dataclass(frozen=True, slots=True)
class UniversePolicy:
    minimum_turnover_24h: float = 5_000_000.0
    maximum_spread_ratio: float = 0.005
    active_turnover_24h: float = 25_000_000.0
    core_turnover_24h: float = 100_000_000.0
    minimum_baseline_samples: int = 20
    new_market_window: timedelta = timedelta(days=7)

    def __post_init__(self) -> None:
        if not (
            0 < self.minimum_turnover_24h
            <= self.active_turnover_24h
            <= self.core_turnover_24h
        ):
            raise ValueError("Universe turnover thresholds are inconsistent.")
        if not 0 < self.maximum_spread_ratio < 1:
            raise ValueError("Universe spread threshold must be between 0 and 1.")
        if self.minimum_baseline_samples <= 0:
            raise ValueError("Minimum baseline samples must be positive.")
        if self.new_market_window <= timedelta(0):
            raise ValueError("New-market window must be positive.")


@dataclass(frozen=True, slots=True)
class UniverseEntry:
    instrument: CatalogInstrument
    tier: UniverseTier
    baseline_samples: int
    is_new_market: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    observed_at: datetime
    eligible: tuple[UniverseEntry, ...]
    rejected: tuple[tuple[str, tuple[str, ...]], ...]

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("Universe timestamp must be timezone-aware.")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))


class DynamicUniverse:
    """Build all eligible instruments; it deliberately has no rank cap."""

    def __init__(self, policy: UniversePolicy | None = None) -> None:
        self._policy = policy or UniversePolicy()

    def build(
        self,
        instruments: tuple[CatalogInstrument, ...],
        *,
        observed_at: datetime,
        baseline_samples: Mapping[str, int] | None = None,
    ) -> UniverseSnapshot:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("Universe clock must be timezone-aware.")
        now = observed_at.astimezone(UTC)
        counts = baseline_samples or {}
        selected: dict[str, CatalogInstrument] = {}
        for item in instruments:
            symbol = item.symbol.strip().upper()
            existing = selected.get(symbol)
            if existing is None or item.turnover_24h > existing.turnover_24h:
                selected[symbol] = item

        eligible: list[UniverseEntry] = []
        rejected: list[tuple[str, tuple[str, ...]]] = []
        for symbol, item in sorted(selected.items()):
            failures = self._rejection_reasons(item, now)
            if failures:
                rejected.append((symbol, failures))
                continue
            count = counts.get(symbol, 0)
            if count < 0:
                raise ValueError("Baseline sample count cannot be negative.")
            is_new = (
                item.launch_time is not None
                and timedelta()
                <= now - item.launch_time
                <= self._policy.new_market_window
            )
            tier = self._tier(item, count, is_new)
            reasons = (
                f"turnover_24h={item.turnover_24h:.6g}",
                f"spread_ratio={item.spread_ratio:.6g}",
                f"baseline_samples={count}",
            )
            eligible.append(UniverseEntry(item, tier, count, is_new, reasons))
        return UniverseSnapshot(now, tuple(eligible), tuple(rejected))

    def _rejection_reasons(
        self,
        item: CatalogInstrument,
        observed_at: datetime,
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        if item.status != "Trading":
            reasons.append("not_trading")
        if not item.is_crypto_linear_usdt:
            reasons.append("not_linear_usdt_perpetual")
        if item.turnover_24h < self._policy.minimum_turnover_24h:
            reasons.append("turnover_below_minimum")
        if item.bid <= 0 or item.ask <= 0:
            reasons.append("missing_top_of_book")
        elif item.spread_ratio > self._policy.maximum_spread_ratio:
            reasons.append("spread_above_maximum")
        if item.launch_time is not None and item.launch_time > observed_at:
            reasons.append("not_launched_yet")
        return tuple(reasons)

    def _tier(
        self,
        item: CatalogInstrument,
        samples: int,
        is_new: bool,
    ) -> UniverseTier:
        if is_new or samples < self._policy.minimum_baseline_samples:
            return UniverseTier.COLD_START
        if item.turnover_24h >= self._policy.core_turnover_24h:
            return UniverseTier.CORE
        if item.turnover_24h >= self._policy.active_turnover_24h:
            return UniverseTier.ACTIVE
        return UniverseTier.STANDARD

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.universe import UniverseTier


class RuntimeInterestTier(StrEnum):
    COLD_START = "COLD_START"
    STANDARD = "STANDARD"
    ACTIVE = "ACTIVE"
    CORE = "CORE"
    WATCH = "WATCH"
    IN_PLAY = "IN_PLAY"
    HIGH_ATTENTION = "HIGH_ATTENTION"


@dataclass(frozen=True, slots=True)
class PollingRule:
    interval: str
    check_every: timedelta
    derivatives: bool

    def __post_init__(self) -> None:
        if self.interval not in {"1m", "5m", "15m"}:
            raise ValueError("Runtime polling interval must be 1m, 5m, or 15m.")
        if self.check_every <= timedelta(0):
            raise ValueError("Runtime polling frequency must be positive.")


@dataclass(frozen=True, slots=True)
class PollingPolicy:
    cold_start: PollingRule = PollingRule("15m", timedelta(minutes=15), False)
    standard: PollingRule = PollingRule("15m", timedelta(minutes=10), False)
    active: PollingRule = PollingRule("5m", timedelta(minutes=5), True)
    core: PollingRule = PollingRule("5m", timedelta(minutes=2), True)
    watch: PollingRule = PollingRule("1m", timedelta(minutes=1), True)
    in_play: PollingRule = PollingRule("1m", timedelta(seconds=30), True)
    high_attention: PollingRule = PollingRule("1m", timedelta(seconds=15), True)

    def rule(self, tier: RuntimeInterestTier) -> PollingRule:
        return {
            RuntimeInterestTier.COLD_START: self.cold_start,
            RuntimeInterestTier.STANDARD: self.standard,
            RuntimeInterestTier.ACTIVE: self.active,
            RuntimeInterestTier.CORE: self.core,
            RuntimeInterestTier.WATCH: self.watch,
            RuntimeInterestTier.IN_PLAY: self.in_play,
            RuntimeInterestTier.HIGH_ATTENTION: self.high_attention,
        }[tier]


@dataclass(frozen=True, slots=True)
class ShadowRuntimeConfig:
    universe_refresh: timedelta = timedelta(minutes=15)
    loop_interval: timedelta = timedelta(seconds=10)
    maximum_workers: int = 4
    maximum_symbols_per_loop: int = 24
    candle_limit: int = 240
    maximum_catchup_buckets: int = 64
    api_calls_per_minute: int = 90
    retry_attempts: int = 3
    retry_base_delay: float = 0.5
    retry_max_delay: float = 8.0
    retry_jitter: float = 0.25
    provider_timeout: float = 10.0

    def __post_init__(self) -> None:
        if self.universe_refresh <= timedelta(0) or self.loop_interval <= timedelta(0):
            raise ValueError("Runtime intervals must be positive.")
        integers = (
            self.maximum_workers,
            self.maximum_symbols_per_loop,
            self.candle_limit,
            self.maximum_catchup_buckets,
            self.api_calls_per_minute,
            self.retry_attempts,
        )
        if any(value <= 0 for value in integers):
            raise ValueError("Runtime limits must be positive.")
        floats = (
            self.retry_base_delay,
            self.retry_max_delay,
            self.provider_timeout,
        )
        if any(not math.isfinite(value) or value <= 0 for value in floats):
            raise ValueError("Runtime timing values must be positive and finite.")
        if not math.isfinite(self.retry_jitter) or not 0 <= self.retry_jitter <= 1:
            raise ValueError("Retry jitter must be between zero and one.")


@dataclass(frozen=True, slots=True)
class RuntimeHealthSnapshot:
    started_at: datetime | None
    last_loop_at: datetime | None
    last_successful_market_update: datetime | None
    symbols_in_universe: int
    symbols_processed: int
    symbols_failed: int
    events_today: int
    pending_outcomes: int
    provider_errors: int
    rate_limit_events: int
    loop_duration_seconds: float
    max_symbol_latency_seconds: float
    stale_data_count: int
    missing_data_count: int
    api_calls: int
    scheduling_gaps: int
    storage_bytes: int
    disk_free_bytes: int
    disk_pressure: bool
    process_rss_bytes: int | None
    baseline_ready_symbols: int
    cold_start_symbols: int
    readiness_by_tier: tuple[tuple[str, int, int], ...]
    degraded: bool
    degraded_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        for field in ("started_at", "last_loop_at", "last_successful_market_update"):
            value = getattr(self, field)
            if value is not None:
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("Health timestamps must be timezone-aware.")
                object.__setattr__(self, field, value.astimezone(UTC))


def interest_tier(
    universe_tier: UniverseTier,
    state: WatchdogState,
) -> RuntimeInterestTier:
    elevated = {
        WatchdogState.WATCH: RuntimeInterestTier.WATCH,
        WatchdogState.IN_PLAY: RuntimeInterestTier.IN_PLAY,
        WatchdogState.HIGH_ATTENTION: RuntimeInterestTier.HIGH_ATTENTION,
    }.get(state)
    if elevated is not None:
        return elevated
    return RuntimeInterestTier(universe_tier.value)

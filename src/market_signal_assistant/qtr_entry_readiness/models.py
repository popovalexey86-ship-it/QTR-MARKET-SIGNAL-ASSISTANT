from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType

ENTRY_READINESS_SCHEMA_VERSION = 2


class UserReadiness(StrEnum):
    WAIT = "WAIT"
    NOW = "NOW"


class WaitReason(StrEnum):
    CONFIRMATION_PENDING = "CONFIRMATION_PENDING"
    PRICE_NOT_IN_ENTRY_ZONE = "PRICE_NOT_IN_ENTRY_ZONE"


class InternalDisposition(StrEnum):
    EVALUATED = "EVALUATED"
    SUPPRESSED = "SUPPRESSED"


class InternalReason(StrEnum):
    ATR_MISSING = "ATR_MISSING"
    TRIGGER_MISSING = "TRIGGER_MISSING"
    INVALIDATION_MISSING = "INVALIDATION_MISSING"
    FRESH_PRICE_MISSING = "FRESH_PRICE_MISSING"
    WRONG_SIDE = "WRONG_SIDE"
    STRUCTURE_INVALID = "STRUCTURE_INVALID"
    CURRENT_FAILURE = "CURRENT_FAILURE"
    LATE = "LATE"
    SPREAD_BAD = "SPREAD_BAD"
    LIQUIDITY_BAD = "LIQUIDITY_BAD"
    INVALID_DIRECTION = "INVALID_DIRECTION"
    UNSUPPORTED_SETUP = "UNSUPPORTED_SETUP"
    TECHNICAL_DATA_INCOMPLETE = "TECHNICAL_DATA_INCOMPLETE"
    STALE_CONFIRMATION = "STALE_CONFIRMATION"


class DistanceBucket(StrEnum):
    ZERO_TO_015 = "0.00-0.15_ATR"
    FROM_015_TO_025 = "0.15-0.25_ATR"
    FROM_025_TO_040 = "0.25-0.40_ATR"
    FROM_040_TO_060 = "0.40-0.60_ATR"
    FROM_060_TO_080 = "0.60-0.80_ATR"
    FROM_080_TO_120 = "0.80-1.20_ATR"
    ABOVE_120 = ">1.20_ATR"


class RiskBucket(StrEnum):
    ZERO_TO_050 = "0.00-0.50_ATR"
    FROM_050_TO_100 = "0.50-1.00_ATR"
    FROM_100_TO_150 = "1.00-1.50_ATR"
    FROM_150_TO_200 = "1.50-2.00_ATR"
    ABOVE_200 = ">2.00_ATR"


class AgeBucket(StrEnum):
    ZERO_TO_30 = "0-30_SEC"
    FROM_30_TO_60 = "30-60_SEC"
    FROM_60_TO_120 = "60-120_SEC"
    FROM_120_TO_300 = "120-300_SEC"
    ABOVE_300 = ">300_SEC"


@dataclass(frozen=True, slots=True)
class EntryReadinessConfig:
    max_entry_distance_atr: float = 0.25
    max_confirmation_age_seconds: float = 60.0
    protective_buffer_atr: float = 0.15

    def __post_init__(self) -> None:
        for name, value in (
            ("max_entry_distance_atr", self.max_entry_distance_atr),
            ("max_confirmation_age_seconds", self.max_confirmation_age_seconds),
            ("protective_buffer_atr", self.protective_buffer_atr),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite.")


@dataclass(frozen=True, slots=True)
class EntryReadinessEvaluation:
    schema_version: int
    recorded_at: datetime
    evaluation_id: str
    candidate_id: str
    setup_episode_id: str
    setup_episode_key: str
    symbol: str
    direction: str
    setup_type: str
    quality_score: float
    quality_components: Mapping[str, float]
    user_readiness: UserReadiness | None
    wait_reason: WaitReason | None
    internal_disposition: InternalDisposition
    internal_reason: InternalReason | None
    signal_time: datetime | None
    first_confirmation_observed_at: datetime | None
    evaluation_time: datetime
    fresh_price_time: datetime | None
    signal_age_seconds: float | None
    confirmation_age_seconds: float | None
    signal_price: float | None
    fresh_price: float | None
    trigger: float | None
    atr: float | None
    entry_zone_low: float | None
    entry_zone_high: float | None
    structural_invalidation: float | None
    protective_level: float | None
    signal_distance_atr: float | None
    fresh_distance_atr: float | None
    distance_bucket: DistanceBucket | None
    risk_distance: float | None
    risk_distance_atr: float | None
    risk_bucket: RiskBucket | None
    age_bucket: AgeBucket | None
    structure_ok: bool
    confirmation_ok: bool
    retest_held: bool | None
    breakout_confirmed: bool | None
    volume_confirmed: bool | None
    correct_side: bool | None
    spread_ok: bool
    liquidity_ok: bool
    current_failure: bool
    late: bool
    context_ids: tuple[str, ...]
    previous_user_readiness: UserReadiness | None = None
    transition: str | None = None
    first_wait_at: datetime | None = None
    first_now_at: datetime | None = None
    wait_to_now_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.schema_version != ENTRY_READINESS_SCHEMA_VERSION:
            raise ValueError("Unsupported entry-readiness schema version.")
        for value in (self.recorded_at, self.evaluation_time):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("Entry-readiness timestamps must be timezone-aware.")
        object.__setattr__(
            self,
            "quality_components",
            MappingProxyType(dict(sorted(self.quality_components.items()))),
        )
        object.__setattr__(self, "context_ids", tuple(self.context_ids))
        object.__setattr__(self, "recorded_at", self.recorded_at.astimezone(UTC))
        object.__setattr__(
            self, "evaluation_time", self.evaluation_time.astimezone(UTC)
        )
        if self.first_confirmation_observed_at is not None:
            if (
                self.first_confirmation_observed_at.tzinfo is None
                or self.first_confirmation_observed_at.utcoffset() is None
            ):
                raise ValueError("Confirmation observation must be timezone-aware.")
            object.__setattr__(
                self,
                "first_confirmation_observed_at",
                self.first_confirmation_observed_at.astimezone(UTC),
            )


@dataclass(frozen=True, slots=True)
class EntryReadinessEpisodeState:
    """Bounded state reconstructable from the append-only shadow audit."""

    setup_episode_key: str
    latest_readiness: UserReadiness | None
    first_wait_at: datetime | None
    first_now_at: datetime | None
    first_confirmation_observed_at: datetime | None

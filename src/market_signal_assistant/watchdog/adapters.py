from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.watchdog.models import FeatureObservation


@dataclass(frozen=True, slots=True)
class PITAdapterResult:
    features: tuple[FeatureObservation, ...]
    missing_data: tuple[str, ...] = ()
    stale_data: tuple[str, ...] = ()


class DerivativesSnapshotAdapter:
    """Expose existing derivatives data only while it is PIT-valid and fresh."""

    def __init__(self, *, maximum_age: timedelta = timedelta(minutes=10)) -> None:
        if maximum_age <= timedelta(0):
            raise ValueError("Derivatives maximum age must be positive.")
        self._maximum_age = maximum_age

    def adapt(
        self,
        snapshot: DerivativesSnapshot | None,
        *,
        symbol: str,
        detected_at: datetime,
    ) -> PITAdapterResult:
        decision_time = _utc(detected_at)
        normalized_symbol = symbol.strip().upper()
        if snapshot is None:
            return PITAdapterResult((), ("open_interest", "funding"))
        if snapshot.symbol.strip().upper() != normalized_symbol:
            raise ValueError("Derivatives symbol does not match market series.")
        if snapshot.as_of > decision_time:
            raise ValueError("Future derivatives snapshot is not PIT-valid.")
        if decision_time - snapshot.as_of > self._maximum_age:
            return PITAdapterResult(
                (),
                ("open_interest", "funding"),
                ("open_interest", "funding"),
            )
        return PITAdapterResult(
            (
                FeatureObservation(
                    name="open_interest_change",
                    value=snapshot.open_interest_change,
                    observed_at=(
                        snapshot.open_interest_observed_at or snapshot.as_of
                    ),
                    available_at=(
                        snapshot.open_interest_available_at or snapshot.as_of
                    ),
                    unit="ratio",
                ),
                FeatureObservation(
                    name="funding_rate",
                    value=snapshot.funding_rate,
                    observed_at=snapshot.funding_observed_at or snapshot.as_of,
                    available_at=snapshot.funding_available_at or snapshot.as_of,
                    unit="ratio",
                ),
            )
        )


class LiquidationFeatureAdapter(Protocol):
    """Boundary for existing liquidation infrastructure; no new source is created."""

    def adapt(self, *, symbol: str, detected_at: datetime) -> PITAdapterResult: ...


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Adapter time must be timezone-aware.")
    return value.astimezone(UTC)

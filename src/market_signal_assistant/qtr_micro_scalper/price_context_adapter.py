from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper.inplay_bridge import ScalperTarget
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    ShadowDirection,
)

DEFAULT_VERIFIED_SETUP_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_setup_telegram_pilot_audit.jsonl"
)


@dataclass(frozen=True, slots=True)
class VerifiedSetupRecord:
    symbol: str
    observed_at: datetime
    source_direction: str
    setup_direction: str
    market_price: float
    atr: float
    trigger_price: float
    invalidation_price: float
    local_range_low: float
    local_range_high: float
    setup_state: str
    setup_confidence: float
    volume_confirmation: bool
    volatility_confirmation: bool
    liquidity_ok: bool
    confirmations: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", _utc(self.observed_at))


class VerifiedSetupProvider(Protocol):
    def latest(self, symbol: str) -> VerifiedSetupRecord | None: ...


class JsonlVerifiedSetupProvider:
    """Read the latest complete Setup Pilot record without network access."""

    def __init__(self, path: Path = DEFAULT_VERIFIED_SETUP_PATH) -> None:
        self._path = path.resolve()

    @property
    def path(self) -> Path:
        return self._path

    def latest(self, symbol: str) -> VerifiedSetupRecord | None:
        normalized = symbol.strip().upper()
        if not normalized or not self._path.is_file():
            return None
        latest: VerifiedSetupRecord | None = None
        try:
            with self._path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    record = _record_from_line(line, normalized)
                    if record is not None and (
                        latest is None or record.observed_at >= latest.observed_at
                    ):
                        latest = record
        except OSError:
            return None
        return latest


class VerifiedPriceContextAdapter:
    """Map only complete, fresh Setup Pilot evidence into PriceContext."""

    def __init__(
        self,
        provider: VerifiedSetupProvider,
        *,
        maximum_age: timedelta = timedelta(minutes=15),
    ) -> None:
        if maximum_age <= timedelta(0):
            raise ValueError("Verified price-context maximum age must be positive.")
        self._provider = provider
        self._maximum_age = maximum_age

    def __call__(
        self,
        symbol: str,
        assessed_at: datetime,
        market_price: float,
    ) -> PriceContext | None:
        normalized = symbol.strip().upper()
        now = _utc(assessed_at)
        source = self._provider.latest(normalized)
        if not _valid_source(source, normalized, now, market_price, self._maximum_age):
            return None
        assert source is not None
        direction = _direction(source.setup_direction)
        if direction is None:
            return None
        context = PriceContext(
            symbol=normalized,
            assessed_at=now,
            direction=direction,
            market_price=market_price,
            atr=source.atr,
            trigger_price=source.trigger_price,
            invalidation_price=source.invalidation_price,
            local_range_low=source.local_range_low,
            local_range_high=source.local_range_high,
            confirmations=source.confirmations,
            warnings=source.warnings,
        )
        return context if context.structure_valid else None

    def target(self, symbol: str, observed_at: datetime) -> ScalperTarget | None:
        normalized = symbol.strip().upper()
        now = _utc(observed_at)
        source = self._provider.latest(normalized)
        if not _valid_source(source, normalized, now, None, self._maximum_age):
            return None
        assert source is not None
        return ScalperTarget(
            symbol=source.symbol,
            discovered_at=source.observed_at,
            source="qtr_setup_pilot_verified_state",
            reason=f"Verified Setup Engine state: {source.setup_state}.",
            priority=source.setup_confidence,
            volatility_score=_confirmation_score(source.volatility_confirmation),
            volume_score=_confirmation_score(source.volume_confirmation),
            liquidity_score=_confirmation_score(source.liquidity_ok),
        )


def _record_from_line(line: str, symbol: str) -> VerifiedSetupRecord | None:
    try:
        payload = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    if (
        not isinstance(payload, Mapping)
        or str(payload.get("symbol", "")).upper() != symbol
    ):
        return None
    context = payload.get("price_context")
    if not isinstance(context, Mapping):
        return None
    try:
        return VerifiedSetupRecord(
            symbol=symbol,
            observed_at=datetime.fromisoformat(str(context["observed_at"])),
            source_direction=str(context["source_direction"]),
            setup_direction=str(context["setup_direction"]),
            market_price=_number(context["market_price"]),
            atr=_number(context["atr"]),
            trigger_price=_number(context["trigger_price"]),
            invalidation_price=_number(context["invalidation_price"]),
            local_range_low=_number(context["local_range_low"]),
            local_range_high=_number(context["local_range_high"]),
            setup_state=str(context["setup_state"]),
            setup_confidence=_number(context["setup_confidence"]),
            volume_confirmation=_boolean(context["volume_confirmation"]),
            volatility_confirmation=_boolean(context["volatility_confirmation"]),
            liquidity_ok=_boolean(context["liquidity_ok"]),
            confirmations=_strings(context.get("confirmations", ())),
            warnings=_strings(context.get("warnings", ())),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _valid_source(
    source: VerifiedSetupRecord | None,
    symbol: str,
    assessed_at: datetime,
    market_price: float | None,
    maximum_age: timedelta,
) -> bool:
    if source is None or source.symbol != symbol:
        return False
    age = assessed_at - source.observed_at
    if age < timedelta(0) or age > maximum_age:
        return False
    if source.source_direction != source.setup_direction:
        return False
    if _direction(source.setup_direction) is None:
        return False
    if source.setup_state in {"CANCELLED", "LATE"}:
        return False
    values = (
        source.market_price,
        source.atr,
        source.trigger_price,
        source.invalidation_price,
        source.local_range_low,
        source.local_range_high,
    )
    if any(not _positive(value) for value in values):
        return False
    if source.local_range_low >= source.local_range_high:
        return False
    if not source.local_range_low <= source.trigger_price <= source.local_range_high:
        return False
    if not 0.0 <= source.setup_confidence <= 100.0:
        return False
    return market_price is None or _positive(market_price)


def _direction(value: str) -> ShadowDirection | None:
    return {"UP": ShadowDirection.LONG, "DOWN": ShadowDirection.SHORT}.get(value)


def _confirmation_score(value: bool) -> float:
    return 100.0 if value else 0.0


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("Expected a JSON number.")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Expected a finite JSON number.")
    return number


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("Expected a JSON boolean.")
    return value


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    strings = tuple(
        item.strip()
        for item in value
        if isinstance(item, str) and item.strip()
    )
    return tuple(dict.fromkeys(strings))


def _positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Verified price-context timestamp must be timezone-aware.")
    return value.astimezone(UTC)

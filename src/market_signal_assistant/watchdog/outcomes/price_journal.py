from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from market_signal_assistant.watchdog.journal import (
    ImmutableJsonlJournal,
    JournalRecovery,
)
from market_signal_assistant.watchdog.outcomes.models import PriceObservation


class WatchdogPriceJournal:
    """Immutable raw market observations used to calculate forward outcomes."""

    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="observation_id")

    @property
    def path(self) -> Path:
        return self._journal.path

    @property
    def recovery(self) -> JournalRecovery:
        return self._journal.recovery

    def append(self, point: PriceObservation) -> bool:
        return self._journal.append(point.observation_id, _payload(point))

    def records(self) -> tuple[PriceObservation, ...]:
        return tuple(_from_payload(item) for item in self._journal.records())


def _payload(item: PriceObservation) -> dict[str, object]:
    return {
        "observation_id": item.observation_id,
        "symbol": item.symbol,
        "observed_at": item.observed_at.isoformat(),
        "available_at": item.available_at.isoformat(),
        "price": item.price,
        "high": item.high,
        "low": item.low,
        "source": item.source,
    }


def _from_payload(value: Mapping[str, object]) -> PriceObservation:
    point = PriceObservation(
        symbol=_string(value, "symbol"),
        observed_at=datetime.fromisoformat(_string(value, "observed_at")),
        available_at=datetime.fromisoformat(_string(value, "available_at")),
        price=_number(value, "price"),
        high=_number(value, "high"),
        low=_number(value, "low"),
        source=_string(value, "source"),
    )
    if point.observation_id != _string(value, "observation_id"):
        raise ValueError("Price observation ID does not match immutable payload.")
    return point


def _string(value: Mapping[str, object], field: str) -> str:
    raw = value.get(field)
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"Price observation field {field} must be a string.")
    return raw


def _number(value: Mapping[str, object], field: str) -> float:
    raw = value.get(field)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"Price observation field {field} must be numeric.")
    return float(raw)

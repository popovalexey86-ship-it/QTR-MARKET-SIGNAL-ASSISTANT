from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from market_signal_assistant.watchdog.journal import (
    ImmutableJsonlJournal,
    JournalRecovery,
)
from market_signal_assistant.watchdog.outcomes.models import (
    BreakoutSide,
    ForwardOutcome,
    OutcomeDataQuality,
)


class WatchdogOutcomeJournal:
    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="outcome_id")

    @property
    def path(self) -> Path:
        return self._journal.path

    @property
    def recovery(self) -> JournalRecovery:
        return self._journal.recovery

    def append(self, outcome: ForwardOutcome) -> bool:
        return self._journal.append(outcome.outcome_id, _payload(outcome))

    def records(self) -> tuple[ForwardOutcome, ...]:
        return tuple(_from_payload(item) for item in self._journal.records())


def _payload(item: ForwardOutcome) -> dict[str, object]:
    return {
        "schema_version": item.schema_version,
        "outcome_id": item.outcome_id,
        "event_id": item.event_id,
        "symbol": item.symbol,
        "horizon": f"{item.horizon_minutes}m",
        "horizon_minutes": item.horizon_minutes,
        "target_time": item.target_time.isoformat(),
        "observed_at": item.observed_at.isoformat(),
        "available_at": item.available_at.isoformat(),
        "reference_price": item.reference_price,
        "horizon_price": item.horizon_price,
        "signed_return": item.signed_return,
        "return": item.signed_return,
        "abs_return": item.abs_return,
        "mfe": item.mfe,
        "mae": item.mae,
        "mfe_up": item.mfe_up,
        "mfe_down": item.mfe_down,
        "max_abs_move": item.max_abs_move,
        "max_abs_excursion": item.max_abs_excursion,
        "realized_range_after_event": item.realized_range_after_event,
        "time_to_mfe_up_seconds": item.time_to_mfe_up_seconds,
        "time_to_mfe_down_seconds": item.time_to_mfe_down_seconds,
        "lateness_seconds": item.lateness_seconds,
        "data_quality": item.data_quality.value,
        "did_expansion_occur": item.did_expansion_occur,
        "time_to_expansion_seconds": item.time_to_expansion_seconds,
        "expansion_magnitude": item.expansion_magnitude,
        "breakout_side": (
            item.breakout_side.value if item.breakout_side is not None else None
        ),
        "false_breakout": item.false_breakout,
        "subsequent_abs_move": item.subsequent_abs_move,
        "persistence": item.persistence,
        "reversal": item.reversal,
        "volatility_persistence": item.volatility_persistence,
    }


def _from_payload(value: Mapping[str, object]) -> ForwardOutcome:
    breakout = value.get("breakout_side")
    return ForwardOutcome(
        outcome_id=_string(value, "outcome_id"),
        event_id=_string(value, "event_id"),
        symbol=_string(value, "symbol"),
        horizon_minutes=_integer(value, "horizon_minutes"),
        target_time=_datetime(value, "target_time"),
        observed_at=_datetime(value, "observed_at"),
        available_at=_datetime(value, "available_at"),
        reference_price=_number(value, "reference_price"),
        horizon_price=_optional_number(value, "horizon_price"),
        signed_return=_optional_number(value, "signed_return"),
        abs_return=_optional_number(value, "abs_return"),
        mfe_up=_optional_number(value, "mfe_up"),
        mfe_down=_optional_number(value, "mfe_down"),
        max_abs_excursion=_optional_number(value, "max_abs_excursion"),
        realized_range_after_event=_optional_number(
            value, "realized_range_after_event"
        ),
        time_to_mfe_up_seconds=_optional_number(
            value, "time_to_mfe_up_seconds"
        ),
        time_to_mfe_down_seconds=_optional_number(
            value, "time_to_mfe_down_seconds"
        ),
        lateness_seconds=_number(value, "lateness_seconds"),
        data_quality=OutcomeDataQuality(_string(value, "data_quality")),
        did_expansion_occur=_optional_bool(value, "did_expansion_occur"),
        time_to_expansion_seconds=_optional_number(
            value, "time_to_expansion_seconds"
        ),
        expansion_magnitude=_optional_number(value, "expansion_magnitude"),
        breakout_side=BreakoutSide(str(breakout)) if breakout is not None else None,
        false_breakout=_optional_bool(value, "false_breakout"),
        subsequent_abs_move=_optional_number(value, "subsequent_abs_move"),
        persistence=_optional_number(value, "persistence"),
        reversal=_optional_bool(value, "reversal"),
        volatility_persistence=_optional_number(
            value, "volatility_persistence"
        ),
        schema_version=_integer(value, "schema_version"),
    )


def _string(value: Mapping[str, object], field: str) -> str:
    raw = value.get(field)
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"Outcome field {field} must be a string.")
    return raw


def _number(value: Mapping[str, object], field: str) -> float:
    raw = value.get(field)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"Outcome field {field} must be numeric.")
    return float(raw)


def _optional_number(value: Mapping[str, object], field: str) -> float | None:
    return None if value.get(field) is None else _number(value, field)


def _integer(value: Mapping[str, object], field: str) -> int:
    raw = value.get(field)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"Outcome field {field} must be an integer.")
    return raw


def _optional_bool(value: Mapping[str, object], field: str) -> bool | None:
    raw = value.get(field)
    if raw is not None and not isinstance(raw, bool):
        raise ValueError(f"Outcome field {field} must be boolean or null.")
    return raw


def _datetime(value: Mapping[str, object], field: str) -> datetime:
    return datetime.fromisoformat(_string(value, field))

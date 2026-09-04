from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.qtr_signal_outcome.models import (
    BarrierHit,
    BarrierOrder,
    BarrierPairOutcome,
    Direction,
    HorizonOutcome,
    OutcomeStatus,
    SignalOutcome,
    SignalSnapshot,
)

DEFAULT_OUTCOME_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_signal_outcomes.jsonl"
)
_LOGGER = logging.getLogger(__name__)


class JsonlOutcomeJournal:
    """Append-only journal with streaming recovery and logical latest revisions."""

    def __init__(self, path: Path = DEFAULT_OUTCOME_JOURNAL_PATH) -> None:
        self._path = path.resolve()
        self._latest: dict[str, tuple[OutcomeStatus, datetime | None, int]] = {}
        self._recovery_malformed_lines = 0
        self._recover()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery_malformed_lines(self) -> int:
        return self._recovery_malformed_lines

    def is_complete(self, signal_id: str, required_horizon: int = 240) -> bool:
        current = self._latest.get(signal_id)
        return (
            current is not None
            and current[0] is OutcomeStatus.COMPLETE
            and current[2] >= required_horizon
        )

    def append(self, outcome: SignalOutcome) -> bool:
        key = outcome.signal.signal_id
        revision = (
            outcome.status,
            outcome.analyzed_through,
            outcome.maximum_horizon_minutes,
        )
        if self._latest.get(key) == revision or self.is_complete(
            key, outcome.maximum_horizon_minutes
        ):
            return False
        line = json.dumps(
            outcome_to_dict(outcome),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(line + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._latest[key] = revision
        return True

    def iter_latest(self) -> Iterator[SignalOutcome]:
        latest: dict[str, SignalOutcome] = {}
        for outcome in self.iter_all():
            latest[outcome.signal.signal_id] = outcome
        yield from (latest[key] for key in sorted(latest))

    def iter_all(self) -> Iterator[SignalOutcome]:
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    if not isinstance(raw, Mapping):
                        raise ValueError
                    yield outcome_from_dict(raw)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    continue

    def _recover(self) -> None:
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    if not isinstance(raw, Mapping):
                        raise ValueError
                    signal_id = str(raw["signal"]["signal_id"])
                    status = OutcomeStatus(str(raw["status"]))
                    through = _optional_time(raw.get("analyzed_through"))
                    horizon = int(raw["maximum_horizon_minutes"])
                    self._latest[signal_id] = (status, through, horizon)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    self._recovery_malformed_lines += 1
        if self._recovery_malformed_lines:
            _LOGGER.warning(
                "Outcome journal recovery skipped %d malformed lines.",
                self._recovery_malformed_lines,
            )


def outcome_to_dict(outcome: SignalOutcome) -> dict[str, Any]:
    signal = outcome.signal
    return {
        "schema_version": 1,
        "signal": {
            "signal_id": signal.signal_id,
            "symbol": signal.symbol,
            "direction": signal.direction.value,
            "setup_type": signal.setup_type,
            "signal_timestamp": signal.signal_timestamp.isoformat(),
            "source_observed_at": signal.source_observed_at.isoformat(),
            "semantic_fingerprint": signal.semantic_fingerprint,
            "signal_price": signal.signal_price,
            "trigger_price": signal.trigger_price,
            "invalidation_price": signal.invalidation_price,
            "atr": signal.atr,
            "setup_confidence": signal.setup_confidence,
            "telegram_quality_score": signal.telegram_quality_score,
            "quality_components": dict(signal.quality_components),
            "volume_confirmation": signal.volume_confirmation,
            "volatility_confirmation": signal.volatility_confirmation,
            "liquidity_ok": signal.liquidity_ok,
            "confirmations": list(signal.confirmations),
            "warnings": list(signal.warnings),
        },
        "status": outcome.status.value,
        "analyzed_at": outcome.analyzed_at.isoformat(),
        "analyzed_through": _time_json(outcome.analyzed_through),
        "maximum_horizon_minutes": outcome.maximum_horizon_minutes,
        "horizons": [
            {
                "horizon_minutes": item.horizon_minutes,
                "close_price": item.close_price,
                "directional_close_return_pct": item.directional_close_return_pct,
                "directional_close_return_atr": item.directional_close_return_atr,
                "mfe_price": item.mfe_price,
                "mae_price": item.mae_price,
                "mfe_atr": item.mfe_atr,
                "mae_atr": item.mae_atr,
            }
            for item in outcome.horizons
        ],
        "favorable_barriers": [_hit_dict(item) for item in outcome.favorable_barriers],
        "adverse_barriers": [_hit_dict(item) for item in outcome.adverse_barriers],
        "barrier_orders": [
            {
                "favorable_atr": item.favorable_atr,
                "adverse_atr": item.adverse_atr,
                "order": item.order.value,
            }
            for item in outcome.barrier_orders
        ],
        "invalidation_hit": outcome.invalidation_hit,
        "invalidation_first_hit_timestamp": _time_json(
            outcome.invalidation_first_hit_timestamp
        ),
        "invalidation_minutes": outcome.invalidation_minutes,
        "market_data_error": outcome.market_data_error,
    }


def outcome_from_dict(raw: Mapping[str, Any]) -> SignalOutcome:
    value = raw["signal"]
    if not isinstance(value, Mapping):
        raise ValueError("Invalid signal.")
    signal = SignalSnapshot(
        signal_id=str(value["signal_id"]),
        symbol=str(value["symbol"]),
        direction=Direction(str(value["direction"])),
        setup_type=str(value["setup_type"]),
        signal_timestamp=_time(value["signal_timestamp"]),
        source_observed_at=_time(value["source_observed_at"]),
        semantic_fingerprint=str(value.get("semantic_fingerprint", "")),
        signal_price=float(value["signal_price"]),
        trigger_price=_optional_float(value.get("trigger_price")),
        invalidation_price=_optional_float(value.get("invalidation_price")),
        atr=float(value["atr"]),
        setup_confidence=_optional_float(value.get("setup_confidence")),
        telegram_quality_score=_optional_float(value.get("telegram_quality_score")),
        quality_components=_pairs(value.get("quality_components", {})),
        volume_confirmation=_optional_bool(value.get("volume_confirmation")),
        volatility_confirmation=_optional_bool(value.get("volatility_confirmation")),
        liquidity_ok=_optional_bool(value.get("liquidity_ok")),
        confirmations=_tuple_strings(value.get("confirmations", [])),
        warnings=_tuple_strings(value.get("warnings", [])),
    )
    return SignalOutcome(
        signal=signal,
        status=OutcomeStatus(str(raw["status"])),
        analyzed_at=_time(raw["analyzed_at"]),
        analyzed_through=_optional_time(raw.get("analyzed_through")),
        maximum_horizon_minutes=int(raw["maximum_horizon_minutes"]),
        horizons=tuple(_horizon(item) for item in _list(raw.get("horizons", []))),
        favorable_barriers=tuple(
            _hit(item) for item in _list(raw.get("favorable_barriers", []))
        ),
        adverse_barriers=tuple(
            _hit(item) for item in _list(raw.get("adverse_barriers", []))
        ),
        barrier_orders=tuple(
            _pair(item) for item in _list(raw.get("barrier_orders", []))
        ),
        invalidation_hit=bool(raw.get("invalidation_hit", False)),
        invalidation_first_hit_timestamp=_optional_time(
            raw.get("invalidation_first_hit_timestamp")
        ),
        invalidation_minutes=_optional_float(raw.get("invalidation_minutes")),
        market_data_error=(
            str(raw["market_data_error"])
            if raw.get("market_data_error") is not None
            else None
        ),
    )


def _hit_dict(item: BarrierHit) -> dict[str, Any]:
    return {
        "threshold_atr": item.threshold_atr,
        "hit": item.hit,
        "first_hit_timestamp": _time_json(item.first_hit_timestamp),
        "first_hit_minutes_from_signal": item.first_hit_minutes_from_signal,
    }


def _horizon(value: Any) -> HorizonOutcome:
    item = _mapping(value)
    return HorizonOutcome(
        int(item["horizon_minutes"]),
        _optional_float(item.get("close_price")),
        _optional_float(item.get("directional_close_return_pct")),
        _optional_float(item.get("directional_close_return_atr")),
        _optional_float(item.get("mfe_price")),
        _optional_float(item.get("mae_price")),
        _optional_float(item.get("mfe_atr")),
        _optional_float(item.get("mae_atr")),
    )


def _hit(value: Any) -> BarrierHit:
    item = _mapping(value)
    return BarrierHit(
        float(item["threshold_atr"]),
        bool(item["hit"]),
        _optional_time(item.get("first_hit_timestamp")),
        _optional_float(item.get("first_hit_minutes_from_signal")),
    )


def _pair(value: Any) -> BarrierPairOutcome:
    item = _mapping(value)
    return BarrierPairOutcome(
        float(item["favorable_atr"]),
        float(item["adverse_atr"]),
        BarrierOrder(str(item["order"])),
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("Expected mapping.")
    return value


def _list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("Expected list.")
    return value


def _pairs(value: Any) -> tuple[tuple[str, float], ...]:
    item = _mapping(value)
    return tuple(sorted((str(key), float(score)) for key, score in item.items()))


def _tuple_strings(value: Any) -> tuple[str, ...]:
    items = _list(value)
    if not all(isinstance(item, str) for item in items):
        raise ValueError("Expected string list.")
    return tuple(items)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError("Expected boolean.")
    return value


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Invalid timestamp.")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _optional_time(value: Any) -> datetime | None:
    return None if value is None else _time(value)


def _time_json(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None

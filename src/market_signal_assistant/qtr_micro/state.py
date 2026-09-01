from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from market_signal_assistant.qtr_micro.models import (
    MicroDirection,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    MicroState,
)
from market_signal_assistant.setup_engine.models import SetupType

DEFAULT_QTR_MICRO_STATE_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_state.json"
)
QTR_MICRO_STATE_VERSION = 2
_LOGGER = logging.getLogger(__name__)


class QtrMicroStateError(RuntimeError):
    pass


class JsonQtrMicroStateStore:
    def __init__(self, path: Path = DEFAULT_QTR_MICRO_STATE_PATH) -> None:
        self._path = path.resolve()

    @property
    def path(self) -> Path:
        return self._path

    def load(self, *, today: date, trading_enabled: bool) -> MicroState:
        if not self._path.exists():
            return empty_state(today=today, trading_enabled=trading_enabled)
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return _state_from_json(raw)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            _LOGGER.warning("QTR Micro state повреждён; требуется Demo reconciliation.")
            self._backup_corrupt()
            return empty_state(
                today=today,
                trading_enabled=False,
                blocked_reason="Повреждён local state; требуется reconciliation.",
            )

    def save(self, state: MicroState) -> None:
        payload = {
            "schema_version": QTR_MICRO_STATE_VERSION,
            **_encode(asdict(state)),
        }
        temporary = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temporary.replace(self._path)
        except OSError:
            raise QtrMicroStateError(
                "Не удалось атомарно сохранить QTR Micro state."
            ) from None

    def _backup_corrupt(self) -> None:
        try:
            self._path.replace(self._path.with_suffix(self._path.suffix + ".corrupt"))
        except OSError:
            _LOGGER.warning("Не удалось сохранить backup повреждённого Micro state.")


def empty_state(
    *,
    today: date,
    trading_enabled: bool,
    equity: float = 0.0,
    blocked_reason: str | None = None,
) -> MicroState:
    return MicroState(
        updated_at=None,
        trading_enabled=trading_enabled,
        trading_day=today,
        day_start_equity=equity,
        realised_daily_pnl=0.0,
        consecutive_losses=0,
        loss_pause_until=None,
        positions={},
        blocked_reason=blocked_reason,
    )


def _encode(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    return value


def _state_from_json(raw: Any) -> MicroState:
    if not isinstance(raw, dict) or raw.get("schema_version") not in {
        1,
        QTR_MICRO_STATE_VERSION,
    }:
        raise ValueError("Unsupported QTR Micro state.")
    positions_raw = raw.get("positions")
    if not isinstance(positions_raw, dict):
        raise ValueError("Invalid Micro positions.")
    positions: dict[str, MicroPosition] = {}
    for key, value in positions_raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            raise ValueError("Invalid Micro position.")
        positions[key] = _position_from_json(value)
    updated_at = _optional_datetime(raw.get("updated_at"))
    pause = _optional_datetime(raw.get("loss_pause_until"))
    trading_day = date.fromisoformat(str(raw["trading_day"]))
    return MicroState(
        updated_at=updated_at,
        trading_enabled=bool(raw["trading_enabled"]),
        trading_day=trading_day,
        day_start_equity=float(raw["day_start_equity"]),
        realised_daily_pnl=float(raw["realised_daily_pnl"]),
        consecutive_losses=int(raw["consecutive_losses"]),
        loss_pause_until=pause,
        positions=positions,
        blocked_reason=(
            str(raw["blocked_reason"])
            if raw.get("blocked_reason") is not None
            else None
        ),
    )


def _position_from_json(raw: dict[str, Any]) -> MicroPosition:
    return MicroPosition(
        trade_id=str(raw["trade_id"]),
        setup_episode_id=str(raw["setup_episode_id"]),
        symbol=str(raw["symbol"]),
        direction=MicroDirection(str(raw["direction"])),
        setup_type=SetupType(str(raw["setup_type"])),
        setup_confidence=float(raw["setup_confidence"]),
        entry_order_link_id=str(raw["entry_order_link_id"]),
        entry_order_id=(
            str(raw["entry_order_id"])
            if raw.get("entry_order_id") is not None
            else None
        ),
        average_fill=(
            float(raw["average_fill"]) if raw.get("average_fill") is not None else None
        ),
        filled_qty=float(raw["filled_qty"]),
        initial_qty=float(raw["initial_qty"]),
        current_qty=float(raw["current_qty"]),
        leverage=int(raw["leverage"]),
        risk_pct=float(raw["risk_pct"]),
        risk_amount=float(raw["risk_amount"]),
        structural_stop=float(raw["structural_stop"]),
        current_stop=float(raw["current_stop"]),
        initial_r=float(raw["initial_r"]),
        tp1_price=float(raw["tp1_price"]),
        tp1_qty=float(raw["tp1_qty"]),
        tp2_price=float(raw["tp2_price"]),
        tp2_qty=float(raw["tp2_qty"]),
        runner_target_price=float(raw["runner_target_price"]),
        runner_qty=float(raw["runner_qty"]),
        realised_partial_pnl=float(raw["realised_partial_pnl"]),
        fees=float(raw["fees"]),
        opened_at=_optional_datetime(raw.get("opened_at")),
        last_updated=_required_datetime(raw["last_updated"]),
        stage=MicroStage(str(raw["stage"])),
        signal_at=_required_datetime(raw["signal_at"]),
        signal_price=float(raw["signal_price"]),
        max_favorable_price=(
            float(raw["max_favorable_price"])
            if raw.get("max_favorable_price") is not None
            else None
        ),
        max_adverse_price=(
            float(raw["max_adverse_price"])
            if raw.get("max_adverse_price") is not None
            else None
        ),
        order_submitted_at=_optional_datetime(raw.get("order_submitted_at")),
        pending_exit_order_id=(
            str(raw["pending_exit_order_id"])
            if raw.get("pending_exit_order_id") is not None
            else None
        ),
        pending_exit_order_link_id=(
            str(raw["pending_exit_order_link_id"])
            if raw.get("pending_exit_order_link_id") is not None
            else None
        ),
        pending_exit_reason=(
            MicroExitReason(str(raw["pending_exit_reason"]))
            if raw.get("pending_exit_reason") is not None
            else None
        ),
        pending_exit_qty=float(raw.get("pending_exit_qty", 0.0)),
        pending_new_stop=(
            float(raw["pending_new_stop"])
            if raw.get("pending_new_stop") is not None
            else None
        ),
        tp1_fill_price=(
            float(raw["tp1_fill_price"])
            if raw.get("tp1_fill_price") is not None
            else None
        ),
        tp2_fill_price=(
            float(raw["tp2_fill_price"])
            if raw.get("tp2_fill_price") is not None
            else None
        ),
        runner_exit_price=(
            float(raw["runner_exit_price"])
            if raw.get("runner_exit_price") is not None
            else None
        ),
        pre_submit_price=(
            float(raw["pre_submit_price"])
            if raw.get("pre_submit_price") is not None
            else None
        ),
        planned_notional=float(raw.get("planned_notional", 0.0)),
        actual_risk_at_fill=(
            float(raw["actual_risk_at_fill"])
            if raw.get("actual_risk_at_fill") is not None
            else None
        ),
        actual_risk_pct=(
            float(raw["actual_risk_pct"])
            if raw.get("actual_risk_pct") is not None
            else None
        ),
        entry_fees=float(raw.get("entry_fees", raw.get("fees", 0.0))),
        exit_fees=float(raw.get("exit_fees", 0.0)),
        funding=(float(raw["funding"]) if raw.get("funding") is not None else None),
        journaled=bool(raw.get("journaled", False)),
    )


def _required_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Micro state timestamp is naive.")
    return parsed.astimezone(UTC)


def _optional_datetime(value: object) -> datetime | None:
    return None if value is None else _required_datetime(value)

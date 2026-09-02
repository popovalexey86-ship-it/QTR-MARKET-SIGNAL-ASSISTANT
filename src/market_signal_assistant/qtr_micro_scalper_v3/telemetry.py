from __future__ import annotations

import hashlib
import json
import os
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper_v3.models import (
    CashCostEstimate,
    ImpulseDirection,
    ImpulseSnapshot,
    V3EntryTelemetry,
    V3ForwardOutcome,
    V3ShadowTrade,
    V3TradeRecord,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENTRY_TELEMETRY_PATH = (
    _PROJECT_ROOT / "data" / "qtr_micro_scalper_v3_entries.jsonl"
)
DEFAULT_TRADE_JOURNAL_PATH = (
    _PROJECT_ROOT / "data" / "qtr_micro_scalper_v3_trades.jsonl"
)
DEFAULT_FORWARD_OUTCOME_PATH = (
    _PROJECT_ROOT / "data" / "qtr_micro_scalper_v3_forward_outcomes.jsonl"
)
FORWARD_WINDOWS_SECONDS = (30, 60, 180, 300, 600)


class JournalRecord(Protocol):
    @property
    def record_id(self) -> str: ...


@dataclass(frozen=True, slots=True)
class _PricePoint:
    observed_at: datetime
    price: float


class JsonlTelemetryJournal:
    """Append-only, deterministic JSONL with bounded duplicate memory."""

    def __init__(self, path: Path, *, dedup_capacity: int = 10_000) -> None:
        if dedup_capacity < 1:
            raise ValueError("V3 journal dedup capacity must be positive.")
        self._path = path.resolve()
        self._capacity = dedup_capacity
        self._ids: OrderedDict[str, None] = OrderedDict()
        self._lock = Lock()
        self._recover_ids()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: JournalRecord) -> bool:
        payload = record_payload(record)
        line = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock:
            if record.record_id in self._ids:
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._remember(record.record_id)
            return True

    def flush(self) -> None:
        with self._lock:
            if not self._path.exists():
                return
            with self._path.open("a", encoding="utf-8") as stream:
                stream.flush()
                os.fsync(stream.fileno())

    def _recover_ids(self) -> None:
        if not self._path.exists():
            return
        with self._path.open("rb") as stream:
            for raw_line in stream:
                if not raw_line.endswith(b"\n"):
                    continue
                try:
                    payload = json.loads(raw_line.decode("utf-8"))
                    record_id = payload["record_id"]
                except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                    continue
                if isinstance(record_id, str) and record_id:
                    self._remember(record_id)

    def _remember(self, record_id: str) -> None:
        self._ids.pop(record_id, None)
        self._ids[record_id] = None
        while len(self._ids) > self._capacity:
            self._ids.popitem(last=False)


class ForwardOutcomeTracker:
    """Observe fixed forward windows independently from the V3 exit lifecycle."""

    def __init__(
        self,
        *,
        entry_id: str,
        symbol: str,
        direction: ImpulseDirection,
        entry_at: datetime,
        entry_price: float,
        round_trip_cost_pct: float,
    ) -> None:
        self._entry_id = entry_id
        self._symbol = symbol
        self._direction = direction
        self._entry_at = entry_at
        self._entry_price = entry_price
        self._cost_pct = round_trip_cost_pct
        self._points = [_PricePoint(entry_at, entry_price)]
        self._completed: set[int] = set()
        self._time_to_025: float | None = None
        self._time_to_050: float | None = None

    @property
    def complete(self) -> bool:
        return len(self._completed) == len(FORWARD_WINDOWS_SECONDS)

    @property
    def symbol(self) -> str:
        return self._symbol

    def observe(
        self,
        observed_at: datetime,
        price: float,
    ) -> tuple[V3ForwardOutcome, ...]:
        if observed_at < self._entry_at or price <= 0:
            return ()
        if observed_at <= self._points[-1].observed_at:
            return ()
        point = _PricePoint(observed_at, price)
        self._points.append(point)
        elapsed = (observed_at - self._entry_at).total_seconds()
        move = _directional_pct(self._direction, self._entry_price, price)
        if move >= 0.25 and self._time_to_025 is None:
            self._time_to_025 = elapsed
        if move >= 0.50 and self._time_to_050 is None:
            self._time_to_050 = elapsed

        results: list[V3ForwardOutcome] = []
        for window in FORWARD_WINDOWS_SECONDS:
            if window in self._completed or elapsed < window:
                continue
            self._completed.add(window)
            results.append(self._outcome(window))
        return tuple(results)

    def _outcome(self, window: int) -> V3ForwardOutcome:
        deadline = self._entry_at + timedelta(seconds=window)
        eligible = tuple(item for item in self._points if item.observed_at <= deadline)
        point = eligible[-1]
        moves = tuple(
            _directional_pct(self._direction, self._entry_price, item.price)
            for item in eligible
        )
        gross = _directional_pct(self._direction, self._entry_price, point.price)
        payload_id = f"{self._entry_id}|{window}"
        return V3ForwardOutcome(
            record_id=_hash(payload_id),
            entry_id=self._entry_id,
            symbol=self._symbol,
            direction=self._direction,
            entry_at=self._entry_at,
            measured_at=deadline,
            window_seconds=window,
            mfe_pct=_round(max(0.0, max(moves))),
            mae_pct=_round(max(0.0, -min(moves))),
            gross_hypothetical_pct=_round(gross),
            transaction_cost_pct=_round(self._cost_pct),
            net_hypothetical_pct=_round(gross - self._cost_pct),
            reached_025=(
                self._time_to_025 is not None and self._time_to_025 <= window
            ),
            reached_050=(
                self._time_to_050 is not None and self._time_to_050 <= window
            ),
            time_to_025_seconds=(
                self._time_to_025
                if self._time_to_025 is not None and self._time_to_025 <= window
                else None
            ),
            time_to_050_seconds=(
                self._time_to_050
                if self._time_to_050 is not None and self._time_to_050 <= window
                else None
            ),
        )


def build_entry_record(
    snapshot: ImpulseSnapshot,
    *,
    recorded_at: datetime,
    notional: float,
    entry_price: float,
    target_price: float,
    stop_price: float,
    cost: CashCostEstimate,
) -> V3EntryTelemetry:
    identity = "|".join((snapshot.symbol, snapshot.impulse_id, recorded_at.isoformat()))
    return V3EntryTelemetry(
        record_id=_hash(identity),
        recorded_at=recorded_at,
        symbol=snapshot.symbol,
        direction=snapshot.direction,
        impulse_id=snapshot.impulse_id,
        snapshot=snapshot,
        notional=notional,
        entry_price=entry_price,
        target_price=target_price,
        stop_price=stop_price,
        estimated_round_trip_cost_bps=cost.total_round_trip_bps,
        estimated_round_trip_cost_pct=cost.total_round_trip_pct,
        estimated_round_trip_cost_cash=cost.expected_cash,
    )


def build_trade_record(
    trade: V3ShadowTrade,
    *,
    recorded_at: datetime,
) -> V3TradeRecord:
    identity = "|".join(
        (
            trade.trade_id,
            trade.stage.value,
            recorded_at.isoformat(),
            str(trade.exit_reason),
        )
    )
    return V3TradeRecord(
        record_id=_hash(identity),
        recorded_at=recorded_at,
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        impulse_id=trade.impulse_id,
        direction=trade.direction,
        entry_at=trade.entry_at,
        exit_at=trade.exit_at,
        entry_price=trade.entry_price,
        exit_price=trade.exit_price,
        exit_reason=trade.exit_reason,
        gross_return_pct=trade.gross_return_pct,
        transaction_cost_pct=trade.transaction_cost_pct,
        net_return_pct=trade.net_return_pct,
        mfe_pct=trade.mfe_pct,
        mae_pct=trade.mae_pct,
    )


def record_payload(record: JournalRecord) -> dict[str, object]:
    if isinstance(record, V3EntryTelemetry):
        snapshot = record.snapshot
        return {
            "record_id": record.record_id,
            "recorded_at": record.recorded_at.isoformat(),
            "symbol": record.symbol,
            "direction": record.direction.value,
            "impulse_id": record.impulse_id,
            "notional": record.notional,
            "entry_price": record.entry_price,
            "target_price": record.target_price,
            "stop_price": record.stop_price,
            "estimated_round_trip_cost_bps": (
                record.estimated_round_trip_cost_bps
            ),
            "estimated_round_trip_cost_pct": (
                record.estimated_round_trip_cost_pct
            ),
            "estimated_round_trip_cost_cash": (
                record.estimated_round_trip_cost_cash
            ),
            **_snapshot_payload(snapshot),
        }
    if isinstance(record, V3ForwardOutcome):
        return {
            "record_id": record.record_id,
            "entry_id": record.entry_id,
            "symbol": record.symbol,
            "direction": record.direction.value,
            "entry_at": record.entry_at.isoformat(),
            "measured_at": record.measured_at.isoformat(),
            "window_seconds": record.window_seconds,
            "mfe_pct": record.mfe_pct,
            "mae_pct": record.mae_pct,
            "gross_hypothetical_pct": record.gross_hypothetical_pct,
            "transaction_cost_pct": record.transaction_cost_pct,
            "net_hypothetical_pct": record.net_hypothetical_pct,
            "reached_025": record.reached_025,
            "reached_050": record.reached_050,
            "time_to_025_seconds": record.time_to_025_seconds,
            "time_to_050_seconds": record.time_to_050_seconds,
        }
    if isinstance(record, V3TradeRecord):
        return {
            "record_id": record.record_id,
            "recorded_at": record.recorded_at.isoformat(),
            "trade_id": record.trade_id,
            "symbol": record.symbol,
            "impulse_id": record.impulse_id,
            "direction": record.direction.value,
            "entry_at": record.entry_at.isoformat(),
            "exit_at": record.exit_at.isoformat() if record.exit_at else None,
            "entry_price": record.entry_price,
            "exit_price": record.exit_price,
            "exit_reason": record.exit_reason.value if record.exit_reason else None,
            "gross_return_pct": record.gross_return_pct,
            "transaction_cost_pct": record.transaction_cost_pct,
            "net_return_pct": record.net_return_pct,
            "mfe_pct": record.mfe_pct,
            "mae_pct": record.mae_pct,
        }
    raise TypeError("Unsupported V3 telemetry record.")


def _snapshot_payload(snapshot: ImpulseSnapshot) -> dict[str, object]:
    return {
        "observed_at": snapshot.observed_at.isoformat(),
        "source_at": snapshot.source_at.isoformat(),
        "source_age_ms": snapshot.source_age_ms,
        "impulse_started_at": snapshot.impulse_started_at.isoformat(),
        "impulse_age_seconds": snapshot.impulse_age_seconds,
        "market_price": snapshot.market_price,
        "best_bid": snapshot.best_bid,
        "best_ask": snapshot.best_ask,
        "spread_bps": snapshot.spread_bps,
        "bid_depth_10bps": snapshot.bid_depth_10bps,
        "ask_depth_10bps": snapshot.ask_depth_10bps,
        "delta_1s": snapshot.delta_1s,
        "delta_5s": snapshot.delta_5s,
        "delta_15s": snapshot.delta_15s,
        "flow_imbalance_5s": snapshot.flow_imbalance_5s,
        "flow_acceleration": snapshot.flow_acceleration,
        "price_displacement_1s_bps": snapshot.price_displacement_1s_bps,
        "price_displacement_5s_bps": snapshot.price_displacement_5s_bps,
        "price_displacement_15s_bps": snapshot.price_displacement_15s_bps,
        "impulse_displacement_bps": snapshot.impulse_displacement_bps,
        "price_response_bps_per_10k": snapshot.price_response_bps_per_10k,
        "estimated_potential_bps": snapshot.estimated_potential_bps,
        "local_volatility_bps": snapshot.local_volatility_bps,
        "orderbook_imbalance": snapshot.orderbook_imbalance,
        "sweep_direction": snapshot.sweep_direction.value,
        "absorption_detected": snapshot.absorption_detected,
        "trigger_progress_atr": snapshot.trigger_progress_atr,
    }


def _directional_pct(direction: ImpulseDirection, entry: float, price: float) -> float:
    sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
    return sign * (price - entry) / entry * 100.0


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _round(value: float) -> float:
    return round(value, 12)

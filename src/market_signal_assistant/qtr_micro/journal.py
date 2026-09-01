from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.qtr_micro.models import (
    EntryDecision,
    MicroDirection,
)

DEFAULT_QTR_MICRO_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_trades.jsonl"
)
DEFAULT_QTR_MICRO_DECISION_AUDIT_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_decisions.jsonl"
)
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TradeJournalEntry:
    trade_id: str
    setup_episode: str
    symbol: str
    direction: MicroDirection
    setup_type: str
    setup_confidence: float
    entry_signal_timestamp: datetime
    order_submit_timestamp: datetime
    fill_timestamp: datetime
    signal_price: float
    average_fill: float
    slippage: float
    initial_stop: float
    risk_pct: float
    risk_usdt: float
    leverage: int
    qty: float
    tp1_fill: float | None
    tp2_fill: float | None
    runner_exit: float | None
    structure_time_exits: tuple[str, ...]
    fees: float
    funding: float | None
    realised_gross_pnl: float
    realised_net_pnl: float
    result_r: float
    max_favorable_excursion: float | None
    max_adverse_excursion: float | None
    hold_duration_seconds: int
    exit_reason: str
    outcome: str
    gross_pnl: float | None = None
    entry_fees: float = 0.0
    exit_fees: float = 0.0
    total_fees: float = 0.0
    net_pnl: float | None = None
    gross_r: float | None = None
    net_r: float | None = None
    actual_risk_at_fill: float | None = None
    actual_risk_pct: float | None = None
    planned_risk_usdt: float | None = None
    planned_notional: float | None = None
    effective_leverage: float | None = None
    pre_submit_price: float | None = None
    signal_to_submit_slippage: float | None = None
    submit_to_fill_slippage: float | None = None


class JsonlQtrMicroTradeJournal:
    def __init__(self, path: Path = DEFAULT_QTR_MICRO_JOURNAL_PATH) -> None:
        self._path = path.resolve()
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, entry: TradeJournalEntry) -> None:
        self.append_once(entry)

    def contains(self, trade_id: str) -> bool:
        if not self._path.exists():
            return False
        try:
            with self._path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (
                        isinstance(payload, dict)
                        and payload.get("trade_id") == trade_id
                    ):
                        return True
        except OSError:
            return False
        return False

    def append_once(self, entry: TradeJournalEntry) -> bool:
        payload = asdict(entry)
        payload["direction"] = entry.direction.value
        for field in (
            "entry_signal_timestamp",
            "order_submit_timestamp",
            "fill_timestamp",
        ):
            value = payload[field]
            assert isinstance(value, datetime)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("Journal timestamps must be timezone-aware.")
            payload[field] = value.astimezone(UTC).isoformat()
        try:
            with self._lock:
                if self.contains(entry.trade_id):
                    return False
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as stream:
                    stream.write(
                        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
                    )
        except OSError:
            _LOGGER.error("Не удалось записать QTR Micro trade journal.")
            raise
        return True


class JsonlQtrMicroDecisionAudit:
    """Best-effort append-only audit for skipped dynamic-symbol decisions."""

    def __init__(
        self,
        path: Path = DEFAULT_QTR_MICRO_DECISION_AUDIT_PATH,
    ) -> None:
        self._path = path.resolve()

    @property
    def path(self) -> Path:
        return self._path

    def append_skip(
        self,
        *,
        decided_at: datetime,
        symbol: str,
        episode_id: str,
        decision: EntryDecision,
    ) -> None:
        timestamp = decided_at
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("Decision audit timestamp must be timezone-aware.")
        payload = {
            "decided_at": timestamp.astimezone(UTC).isoformat(),
            "symbol": symbol,
            "episode_id": episode_id,
            "skip_reason": (
                decision.skip_reason.value
                if decision.skip_reason is not None
                else "unknown"
            ),
            "skip_detail": decision.skip_detail,
            "instrument_status": (
                decision.instrument_status.value
                if decision.instrument_status is not None
                else None
            ),
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
                )
        except OSError:
            _LOGGER.warning("Не удалось записать QTR Micro decision audit.")

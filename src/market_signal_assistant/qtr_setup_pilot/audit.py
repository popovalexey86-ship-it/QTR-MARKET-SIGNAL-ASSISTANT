from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.qtr_setup_pilot.notifications import QtrSetupDecision

DEFAULT_QTR_SETUP_TELEGRAM_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_setup_telegram_pilot_audit.jsonl"
)
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class QtrSetupAuditOutcome:
    decision: QtrSetupDecision
    sent: bool
    delivery_committed: bool


class JsonlQtrSetupTelegramAuditStore:
    def __init__(self, path: Path = DEFAULT_QTR_SETUP_TELEGRAM_AUDIT_PATH) -> None:
        self._path = path.resolve()

    @property
    def path(self) -> Path:
        return self._path

    def append(
        self,
        outcomes: tuple[QtrSetupAuditOutcome, ...],
        timestamp: datetime,
    ) -> None:
        if not outcomes:
            return
        observed_at = timestamp.astimezone(UTC)
        lines: list[str] = []
        for outcome in outcomes:
            decision = outcome.decision
            event = decision.event
            result = decision.candidate.result
            payload = {
                "timestamp": observed_at.isoformat(),
                "symbol": result.symbol,
                "direction": (
                    event.direction.value if event else result.direction.value
                ),
                "type": (event.setup_type.value if event else result.setup_type.value),
                "state": (event.state.value if event else result.setup_state.value),
                "decision": "send" if decision.should_notify else "suppress",
                "suppression_reason": (
                    None if decision.should_notify else decision.reason.value
                ),
                "sent": outcome.sent,
                "semantic_fingerprint": (
                    event.semantic_fingerprint if event is not None else None
                ),
                "delivery_committed": outcome.delivery_committed,
                "telegram_quality_score": (
                    event.quality_score if event is not None else None
                ),
                "price_context": {
                    "observed_at": result.analyzed_at.isoformat(),
                    "source_direction": decision.candidate.source_input.direction.value,
                    "setup_direction": result.direction.value,
                    "market_price": result.current_price,
                    "atr": decision.candidate.atr_value,
                    "trigger_price": result.trigger_level,
                    "invalidation_price": result.invalidation_level,
                    "local_range_low": decision.candidate.local_range_low,
                    "local_range_high": decision.candidate.local_range_high,
                    "setup_state": result.setup_state.value,
                    "setup_confidence": result.confidence,
                    "volume_confirmation": result.volume_confirmation,
                    "volatility_confirmation": result.volatility_confirmation,
                    "liquidity_ok": result.liquidity_ok,
                    "confirmations": list(result.reasons),
                    "warnings": list(result.warnings),
                },
            }
            lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as stream:
                stream.write("\n".join(lines) + "\n")
        except OSError:
            _LOGGER.warning("Не удалось записать аудит QTR Setup Pilot.")

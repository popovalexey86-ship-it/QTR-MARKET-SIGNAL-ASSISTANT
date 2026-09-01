from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupAnalysisResult,
)

DEFAULT_SETUP_ENGINE_AUDIT_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_setup_engine_audit.jsonl"
)
SETUP_ENGINE_AUDIT_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class JsonlSetupAuditStore:
    path: Path = DEFAULT_SETUP_ENGINE_AUDIT_PATH
    _lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
        compare=False,
    )

    def append(
        self,
        data: SetupAnalysisInput,
        result: SetupAnalysisResult,
    ) -> None:
        payload = {
            "schema_version": SETUP_ENGINE_AUDIT_SCHEMA_VERSION,
            "recorded_at": result.analyzed_at.isoformat(),
            "input": {
                "snapshot_ids": list(data.snapshot_ids),
                "source": data.source,
                "symbol": data.symbol,
                "analyzed_at": data.analyzed_at.isoformat(),
                "current_breakout_failure": data.current_breakout_failure,
                "historical_breakout_failure": data.historical_breakout_failure,
                "structure_recovered": data.structure_recovered,
                "technical_gap": data.technical_gap,
            },
            "result": result_to_dict(result),
            "reasons": list(result.reasons),
            "warnings": list(result.warnings),
            "confirmations": {
                "structure": result.structure_confirmation,
                "volume": result.volume_confirmation,
                "volatility": result.volatility_confirmation,
                "correct_side": data.correct_side_of_level is True,
                "hold_candles": result.hold_candles,
                "retest_detected": result.retest_detected,
                "retest_held": result.retest_held,
                "freshness": result.freshness_confirmation,
                "liquidity": result.liquidity_ok,
                "spread": result.spread_ok,
            },
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock, self.path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(line)
            stream.write("\n")


def result_to_dict(result: SetupAnalysisResult) -> dict[str, Any]:
    return {
        "symbol": result.symbol,
        "analyzed_at": result.analyzed_at.isoformat(),
        "direction": result.direction.value,
        "direction_ru": result.direction_ru,
        "setup_type": result.setup_type.value,
        "setup_type_ru": result.setup_type_ru,
        "setup_state": result.setup_state.value,
        "setup_state_ru": result.setup_state_ru,
        "confidence": result.confidence,
        "trigger_level": result.trigger_level,
        "invalidation_level": result.invalidation_level,
        "current_price": result.current_price,
        "distance_to_trigger_pct": result.distance_to_trigger_pct,
        "distance_to_trigger_atr": result.distance_to_trigger_atr,
        "breakout_age_bars": result.breakout_age_bars,
        "hold_candles": result.hold_candles,
        "retest_detected": result.retest_detected,
        "retest_held": result.retest_held,
        "breakout_failed": result.breakout_failed,
        "volume_confirmation": result.volume_confirmation,
        "volatility_confirmation": result.volatility_confirmation,
        "structure_confirmation": result.structure_confirmation,
        "freshness_confirmation": result.freshness_confirmation,
        "liquidity_ok": result.liquidity_ok,
        "spread_ok": result.spread_ok,
        "is_late": result.is_late,
        "reasons": list(result.reasons),
        "warnings": list(result.warnings),
        "missing_data": list(result.missing_data),
        "current_breakout_failure": result.current_breakout_failure,
        "historical_breakout_failure": result.historical_breakout_failure,
        "structure_recovered": result.structure_recovered,
        "trade_eligible": result.trade_eligible,
        "trade_eligibility": result.trade_eligibility.value,
        "trade_eligibility_ru": result.trade_eligibility_ru,
        "no_trade_reasons": list(result.no_trade_reasons),
        "data_quality": result.data_quality,
        "technical_gap": result.technical_gap,
        "classification_candidates": [
            item.value for item in result.classification_candidates
        ],
        "classification_winner_reason": result.classification_winner_reason,
    }

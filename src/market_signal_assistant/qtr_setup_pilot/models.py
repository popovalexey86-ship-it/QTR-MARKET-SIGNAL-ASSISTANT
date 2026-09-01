from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupAnalysisResult,
)


@dataclass(frozen=True, slots=True)
class QtrSetupCandidate:
    """One pure Setup Engine result with a stable V2 episode identity."""

    episode_id: str
    source_input: SetupAnalysisInput
    result: SetupAnalysisResult
    atr_value: float | None = None
    local_range_low: float | None = None
    local_range_high: float | None = None

    def __post_init__(self) -> None:
        if not self.episode_id.strip():
            raise ValueError("QTR setup episode id cannot be empty.")
        if self.source_input.symbol != self.result.symbol:
            raise ValueError("QTR setup input and result symbols must match.")
        if self.atr_value is not None and self.atr_value <= 0:
            raise ValueError("QTR setup ATR must be positive when supplied.")
        if (self.local_range_low is None) != (self.local_range_high is None):
            raise ValueError("QTR setup local range requires both boundaries.")
        if (
            self.local_range_low is not None
            and self.local_range_high is not None
            and (
                self.local_range_low <= 0
                or self.local_range_low >= self.local_range_high
            )
        ):
            raise ValueError("QTR setup local range is invalid.")

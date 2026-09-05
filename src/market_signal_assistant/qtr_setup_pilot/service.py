from __future__ import annotations

from typing import Protocol

from market_signal_assistant.inplay.early_discovery import MarketDirection
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2Result,
    EarlyDiscoveryV2ScanReport,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.adapters import (
    input_from_early_discovery_v2,
)
from market_signal_assistant.setup_engine.analyzer import analyze_setup


class EarlyDiscoveryV2Scanner(Protocol):
    def scan(self) -> EarlyDiscoveryV2ScanReport: ...


class QtrSetupScanService:
    """Reuse one V2 scan and classify its snapshots without further I/O."""

    def __init__(self, scanner: EarlyDiscoveryV2Scanner) -> None:
        self._scanner = scanner

    def scan(self) -> tuple[QtrSetupCandidate, ...]:
        report = self._scanner.scan()
        candidates: list[QtrSetupCandidate] = []
        for result in report.results:
            source_input = input_from_early_discovery_v2(
                result,
                invalidation_level=_structural_invalidation(result),
            )
            episode_id = (
                result.first_detected_at.isoformat()
                if result.first_detected_at is not None
                else "unassigned"
            )
            candidates.append(
                QtrSetupCandidate(
                    episode_id=episode_id,
                    source_input=source_input,
                    result=analyze_setup(source_input),
                    atr_value=result.atr,
                    local_range_low=result.local_range_low,
                    local_range_high=result.local_range_high,
                )
            )
        return tuple(candidates)


def _structural_invalidation(result: EarlyDiscoveryV2Result) -> float | None:
    if result.local_range_low is None or result.local_range_high is None:
        return None
    if result.market_direction is MarketDirection.UP:
        return result.local_range_low
    if result.market_direction is MarketDirection.DOWN:
        return result.local_range_high
    return None

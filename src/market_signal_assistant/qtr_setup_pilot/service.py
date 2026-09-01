from __future__ import annotations

from typing import Protocol

from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2ScanReport,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine import (
    analyze_setup,
    input_from_early_discovery_v2,
)


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
            source_input = input_from_early_discovery_v2(result)
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
                    atr_value=_atr_value(
                        result.absolute_distance, result.absolute_distance_atr
                    ),
                    local_range_low=result.local_range_low,
                    local_range_high=result.local_range_high,
                )
            )
        return tuple(candidates)


def _atr_value(distance: float | None, distance_atr: float | None) -> float | None:
    if distance is None or distance_atr is None or distance_atr <= 0:
        return None
    value = distance / distance_atr
    return value if value > 0 else None

"""Historical rows are evidence, not runtime checkpoints.

Schema 1 cannot reconstruct a ShadowTrade (expiry, remaining fraction, current
stop, processing cursor and score/context are absent). Preserve its unfinished
rows without creating executable plans or synthetic terminal transitions.
New checkpoint formats require an explicit validator/hydrator before startup;
they must never fall through this schema-1 quarantine policy.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
)


@dataclass(frozen=True, slots=True)
class LegacyShadowQuarantine:
    legacy_unresumable_records: tuple[ShadowTradeRecord, ...] = ()
    reason: str = "LEGACY_SCHEMA_1_WITHOUT_RUNTIME_CHECKPOINT"

    @property
    def trade_ids_and_stages(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (record.trade_id, record.stage.value)
            for record in self.legacy_unresumable_records
        )

    @property
    def warning(self) -> str | None:
        if not self.legacy_unresumable_records:
            return None
        stages = dict(
            sorted(Counter(stage for _, stage in self.trade_ids_and_stages).items())
        )
        return (
            f"{len(self.legacy_unresumable_records)} legacy unfinished Shadow trades "
            "preserved in historical journal and quarantined from runtime recovery. "
            f"reason={self.reason}; stages={stages}; "
            f"trades={self.trade_ids_and_stages}; runtime_restored_legacy_trades=0"
        )


def classify_legacy_shadow_recovery(
    journal: ShadowTradeJournal,
) -> LegacyShadowQuarantine:
    """Read compact state only; do not write, hydrate, close or expire trades."""
    snapshot = journal.snapshot()
    metrics = journal.index_metrics
    size = journal.path.stat().st_size if journal.path.exists() else 0
    if metrics.malformed_lines or metrics.file_offset != size:
        raise RuntimeError(
            "Shadow recovery requires review: malformed/unsupported records or "
            "an incomplete journal tail cannot be classified as legacy quarantine."
        )
    unfinished = snapshot.historical_unfinished_records
    if any(record.schema_version != 1 for record in unfinished):
        raise RuntimeError("Shadow checkpoint requires a dedicated validated hydrator.")
    return LegacyShadowQuarantine(unfinished)

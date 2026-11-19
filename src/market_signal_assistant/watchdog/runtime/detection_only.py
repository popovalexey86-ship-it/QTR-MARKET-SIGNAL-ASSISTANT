"""Detection-only outcome boundary and durable dataset mode identity."""

from __future__ import annotations

import json
import os
from pathlib import Path

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.outcomes.models import (
    ForwardOutcome,
    PriceObservation,
)
from market_signal_assistant.watchdog.outcomes.scheduler import PendingHorizon


class RuntimeModeError(ValueError):
    """Dataset mode is missing, damaged, or incompatible with the requested run."""


class DetectionOnlyOutcomes:
    """No outcome graph, price observations, or outcome writes."""

    def __init__(self, events: WatchdogEventJournal) -> None:
        self._events = events

    def register(self, event: WatchdogEventEvidence) -> None:
        if self._events.get(event.event_id) != event:
            raise ValueError(
                "Detection-only scheduling requires persisted event evidence."
            )

    def pending_horizons(self) -> tuple[PendingHorizon, ...]:
        return ()

    def pending_horizon_count(self) -> int:
        return 0

    def observe(self, point: PriceObservation) -> tuple[ForwardOutcome, ...]:
        del point
        raise RuntimeModeError(
            "Outcome observation attempted in detection-only mode."
        )

    def recover_pending(self) -> tuple[ForwardOutcome, ...]:
        return ()

    @property
    def retained_counts(self) -> tuple[int, int, int, int]:
        return (0, 0, 0, 0)

    @property
    def journal_retained_counts(self) -> tuple[int, int]:
        return (0, 0)


def ensure_dataset_mode(root: Path, *, detection_only: bool) -> None:
    """Pin a detection-only root; refuse mode changes or legacy data imports."""
    path = root / "state" / "runtime-mode.json"
    requested = "DETECTION_ONLY" if detection_only else "FULL"
    if path.exists():
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise RuntimeModeError("Runtime mode manifest is invalid.") from error
        if value != {"version": 1, "mode": requested}:
            raise RuntimeModeError("Runtime mode differs from dataset identity.")
        if detection_only:
            _reject_outcome_state(root)
        return
    if not detection_only:
        return  # Legacy full-outcome datasets retain their existing contract.
    if any(
        (root / relative).exists()
        for relative in (
            "state/pending.json",
            "state/health.json",
            "state/buckets.json",
            "state/symbols.json",
            "state/baselines.json",
            "state/observer.json",
        )
    ) or (root / "events/events.jsonl").exists() and (
        root / "events/events.jsonl"
    ).stat().st_size > 0:
        raise RuntimeModeError("Detection-only mode requires a fresh dataset.")
    _reject_outcome_state(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump({"version": 1, "mode": requested}, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        raise RuntimeModeError("Runtime mode manifest cannot be created.") from error


def _reject_outcome_state(root: Path) -> None:
    if any(
        (root / relative).exists()
        for relative in (
            "state/pending.json",
            "outcomes/outcomes.jsonl",
            "outcomes/price_observations.jsonl",
        )
    ):
        raise RuntimeModeError("Detection-only dataset contains outcome state.")

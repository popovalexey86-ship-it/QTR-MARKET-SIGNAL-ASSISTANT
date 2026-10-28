from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from copy import deepcopy
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from market_signal_assistant.qtr_micro.distance_shadow_accounting import (
    ShadowAccounting,
    drain_thread_task,
    read_shadow_accounting,
)
from market_signal_assistant.qtr_micro.engine import QtrMicroEntryEngine
from market_signal_assistant.qtr_micro.models import (
    EntryDecision,
    InstrumentRules,
    MicroDirection,
    MicroState,
    PreflightResult,
    direction_from_setup,
)
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrNotificationReason,
    QtrSetupDecision,
    QtrSetupNotificationPlan,
)

EXPERIMENT_NAME = "qtr_micro_distance_funnel_v1"
EXPERIMENT_VERSION = 3
CONTROL_THRESHOLD = 0.25
DISTANCE_THRESHOLDS = (CONTROL_THRESHOLD, 0.40, 0.60)
MINIMUM_REVIEW_DELTA_EPISODE_RATE_PP = 10.0
MINIMUM_REVIEW_DELTA_ELIGIBLE_EPISODES = 5
DEFAULT_DISTANCE_SHADOW_EVIDENCE_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_distance_shadow_v2"
)
_QUALIFIED_REASONS = frozenset(
    {
        QtrNotificationReason.NEW_EPISODE,
        QtrNotificationReason.DIRECTION_CHANGED,
        QtrNotificationReason.TYPE_CHANGED,
        QtrNotificationReason.QUALITY_IMPROVED,
        QtrNotificationReason.DUPLICATE,
    }
)
_FORBIDDEN_EVIDENCE_KEYS = frozenset({"trade_id", "order_link_id"})
_LOGGER = logging.getLogger(__name__)


class ShadowStatus(StrEnum):
    PRE_SUBMIT_ELIGIBLE = "PRE_SUBMIT_ELIGIBLE"
    REJECT = "REJECT"
    EVIDENCE_MISSING = "EVIDENCE_MISSING"


class ExperimentCategory(StrEnum):
    NO_MATERIAL_FUNNEL_CHANGE = "NO_MATERIAL_FUNNEL_CHANGE"
    DISTANCE_INCREASES_ELIGIBILITY = "DISTANCE_INCREASES_ELIGIBILITY"
    INCONCLUSIVE_SAMPLE = "INCONCLUSIVE_SAMPLE"
    EXPERIMENT_INVALID = "EXPERIMENT_INVALID"


def _meets_review_threshold(
    delta_episode_rate_pp_vs_control: float,
    delta_eligible_episodes_vs_control: int,
) -> bool:
    rate_threshold_met = (
        delta_episode_rate_pp_vs_control >= MINIMUM_REVIEW_DELTA_EPISODE_RATE_PP
        or math.isclose(
            delta_episode_rate_pp_vs_control,
            MINIMUM_REVIEW_DELTA_EPISODE_RATE_PP,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
    )
    return (
        rate_threshold_met
        and delta_eligible_episodes_vs_control >= MINIMUM_REVIEW_DELTA_ELIGIBLE_EPISODES
    )


class FreshPriceReader(Protocol):
    """Narrow read-only boundary: order methods are deliberately inaccessible."""

    def current_market_price(self, symbol: str) -> float: ...


@dataclass(frozen=True, slots=True)
class CallableFreshPriceReader:
    """Expose only the read-only quote method to the Shadow observer."""

    getter: Callable[[str], float]

    def current_market_price(self, symbol: str) -> float:
        return self.getter(symbol)


@dataclass(frozen=True, slots=True)
class FrozenRuntimeContext:
    now: datetime
    equity: float
    rules: InstrumentRules
    state: MicroState
    preflight: PreflightResult

    @classmethod
    def capture(
        cls,
        *,
        now: datetime,
        equity: float,
        rules: InstrumentRules,
        state: MicroState,
        preflight: PreflightResult,
    ) -> FrozenRuntimeContext:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Shadow observation time must be timezone-aware.")
        if not math.isfinite(equity) or equity <= 0:
            raise ValueError("Shadow equity snapshot must be positive.")
        return cls(
            now.astimezone(UTC),
            equity,
            deepcopy(rules),
            deepcopy(state),
            deepcopy(preflight),
        )


@dataclass(frozen=True, slots=True)
class QualifiedScannerSnapshot:
    decision: QtrSetupDecision
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("Scanner observation time must be timezone-aware.")
        if (
            self.decision.event is None
            or self.decision.reason not in _QUALIFIED_REASONS
        ):
            raise ValueError("Snapshot is not a qualified Scanner A+ observation.")


@dataclass(frozen=True, slots=True)
class TemporalEvidence:
    qualified_at: datetime
    enqueued_at: datetime
    production_handler_completed_at: datetime
    worker_started_at: datetime
    production_cycle_id: str
    context_captured_at: datetime | None
    context_source: str = "PUBLISHED_RUNTIME_CONTEXT"

    def __post_init__(self) -> None:
        for value in (
            self.qualified_at,
            self.enqueued_at,
            self.production_handler_completed_at,
            self.worker_started_at,
        ):
            _utc(value)
        if self.context_captured_at is not None:
            _utc(self.context_captured_at)
        if not self.production_cycle_id.strip():
            raise ValueError("Production cycle identity cannot be empty.")
        if self.enqueued_at < self.qualified_at:
            raise ValueError("Shadow enqueue precedes qualification.")
        if self.production_handler_completed_at < self.enqueued_at:
            raise ValueError("Production completion precedes Shadow enqueue.")
        if self.worker_started_at < self.production_handler_completed_at:
            raise ValueError("Shadow worker precedes production completion.")
        if self.context_source not in {
            "PUBLISHED_RUNTIME_CONTEXT",
            "SHADOW_READ_ONLY_FALLBACK",
        }:
            raise ValueError("Unknown Shadow context source.")
        if self.context_captured_at is not None:
            if self.context_source == "PUBLISHED_RUNTIME_CONTEXT":
                if self.context_captured_at > self.production_handler_completed_at:
                    raise ValueError(
                        "Runtime context was captured after cycle completion."
                    )
            elif self.context_captured_at < self.worker_started_at:
                raise ValueError("Fallback context precedes the Shadow worker.")


class JsonlDistanceShadowEvidence:
    """Append-only evidence sink; it has no access to factual Micro state."""

    def __init__(self, path: Path | None = None, *, run_id: str | None = None) -> None:
        self.run_id = run_id or uuid4().hex
        self._path = (
            path
            if path is not None
            else DEFAULT_DISTANCE_SHADOW_EVIDENCE_PATH / f"{self.run_id}.jsonl"
        ).resolve()
        if self._path.name == "qtr_micro_distance_shadow_v1.jsonl":
            raise ValueError("The invalid V1 evidence path is immutable.")
        if self._path.exists() and self._path.stat().st_size:
            first = json.loads(self._path.read_bytes().splitlines()[0])
            if (
                run_id is None
                or first.get("experiment_run_id") != run_id
                or first.get("schema_version") != EXPERIMENT_VERSION
            ):
                raise ValueError("A new Shadow run requires a fresh evidence path.")
        if self._path.with_suffix(".accounting.seal.json").exists():
            raise ValueError("A sealed Shadow run cannot be reopened.")
        self._sealed = False
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: Mapping[str, Any]) -> None:
        _assert_no_forbidden_evidence_keys(record)
        payload = json.dumps(
            record, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        with self._lock:
            if self._sealed:
                raise ValueError("Shadow evidence is sealed.")
            self._path.parent.mkdir(parents=True, exist_ok=True)
            _ensure_line_boundary(self._path)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(payload + "\n")
                stream.flush()
                os.fsync(stream.fileno())

    def finalized_snapshot(self) -> tuple[int, str]:
        with self._lock:
            self._sealed = True
            raw = self._path.read_bytes() if self._path.exists() else b""
            if raw and not raw.endswith(b"\n"):
                raise ValueError("Incomplete Shadow evidence cannot be sealed.")
            records = [json.loads(line) for line in raw.splitlines() if line.strip()]
            identities = set()
            for record in records:
                _validate_record(record)
                identity = _observation_key(record)
                if identity in identities:
                    raise ValueError("Duplicate Shadow observation identity.")
                identities.add(identity)
                if record["experiment_run_id"] != self.run_id:
                    raise ValueError("Mixed evidence runs cannot be sealed.")
            return len(records), hashlib.sha256(raw).hexdigest()


class ShadowEvidenceJournalFailure(RuntimeError):
    """Writing or serializing Shadow evidence failed; never a production failure."""


class QtrMicroDistanceShadowObserver:
    """Pure pre-submit comparison. It cannot submit, persist, or route an order."""

    def __init__(
        self,
        *,
        production_settings: QtrMicroSettings,
        price_reader: FreshPriceReader,
        evidence: JsonlDistanceShadowEvidence,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if production_settings.max_entry_distance_atr != CONTROL_THRESHOLD:
            raise ValueError(
                "Production control threshold must remain exactly 0.25 ATR."
            )
        self._settings = deepcopy(production_settings)
        self._price_reader = price_reader
        self._evidence = evidence
        self._clock = clock

    @property
    def run_id(self) -> str:
        return self._evidence.run_id

    @property
    def accounting_path(self) -> Path:
        return self._evidence.path.with_suffix(".accounting.jsonl")

    def finalized_evidence_snapshot(self) -> tuple[int, str]:
        return self._evidence.finalized_snapshot()

    def observe(
        self,
        snapshot: QualifiedScannerSnapshot,
        context: FrozenRuntimeContext,
        *,
        temporal: TemporalEvidence | None = None,
    ) -> dict[str, Any]:
        if temporal is None:
            direct_at = _utc(self._clock())
            temporal = TemporalEvidence(
                qualified_at=snapshot.observed_at,
                enqueued_at=direct_at,
                production_handler_completed_at=direct_at,
                worker_started_at=direct_at,
                production_cycle_id="DIRECT-UNIT-OBSERVATION",
                context_captured_at=context.now,
            )
        candidate = deepcopy(snapshot.decision.candidate)
        event = snapshot.decision.event
        assert event is not None
        context = FrozenRuntimeContext.capture(
            now=context.now,
            equity=context.equity,
            rules=context.rules,
            state=context.state,
            preflight=context.preflight,
        )
        candidate_id = _candidate_id(snapshot)
        context_digest = _context_digest(context)
        initial: list[tuple[float, QtrMicroSettings, EntryDecision]] = []
        for threshold in DISTANCE_THRESHOLDS:
            branch_settings = replace(self._settings, max_entry_distance_atr=threshold)
            initial.append(
                (
                    threshold,
                    branch_settings,
                    QtrMicroEntryEngine(branch_settings).prepare_entry(
                        candidate,
                        now=context.now,
                        equity=context.equity,
                        rules=context.rules,
                        state=context.state,
                        preflight=context.preflight,
                    ),
                )
            )

        requested_at = _utc(self._clock())
        price: float | None = None
        price_error: str | None = None
        try:
            value = float(
                self._price_reader.current_market_price(candidate.result.symbol)
            )
            if not math.isfinite(value) or value <= 0:
                raise ValueError("fresh price is non-positive or non-finite")
            price = value
        except Exception as exc:  # fail closed; evidence records class, never secrets
            price_error = type(exc).__name__
        received_at = _utc(self._clock())

        trigger = candidate.result.trigger_level
        atr = candidate.atr_value
        direction = direction_from_setup(candidate.result.direction)
        fresh_distance = (
            abs(price - trigger) / atr
            if price is not None and trigger is not None and atr is not None
            else None
        )
        correct_side = (
            _correct_side(direction, price, trigger)
            if price is not None and trigger is not None
            else None
        )
        quote_id = (
            _digest(
                {
                    "symbol": candidate.result.symbol,
                    "requested_at": requested_at,
                    "received_at": received_at,
                    "price": price,
                }
            )
            if price is not None
            else None
        )

        branches = []
        for threshold, branch_settings, prepared in initial:
            revalidated: EntryDecision | None = None
            if price is not None and prepared.accepted:
                assert prepared.plan is not None
                revalidated = QtrMicroEntryEngine(branch_settings).revalidate_entry(
                    candidate,
                    prepared.plan,
                    current_price=price,
                    now=context.now,
                    equity=context.equity,
                    rules=context.rules,
                    state=context.state,
                    preflight=context.preflight,
                )
            final_status = _final_status(price, prepared, revalidated)
            initial_result = _decision_result(prepared)
            revalidation_result = (
                _decision_result(revalidated) if revalidated is not None else None
            )
            branches.append(
                {
                    "max_distance_atr": threshold,
                    "threshold_atr": threshold,
                    "role": "CONTROL" if threshold == CONTROL_THRESHOLD else "SHADOW",
                    "candidate_id": candidate_id,
                    "context_digest": context_digest,
                    "settings_digest": _settings_digest(branch_settings),
                    "settings_override_fields": _settings_override_fields(
                        self._settings, branch_settings
                    ),
                    "initial": initial_result,
                    "initial_accepted": initial_result["accepted"],
                    "initial_skip_reason": initial_result["reason"],
                    "initial_skip_detail": initial_result["detail"],
                    "revalidation_executed": revalidated is not None,
                    "revalidation": revalidation_result,
                    "revalidation_accepted": (
                        revalidation_result["accepted"]
                        if revalidation_result is not None
                        else None
                    ),
                    "revalidation_skip_reason": (
                        revalidation_result["reason"]
                        if revalidation_result is not None
                        else None
                    ),
                    "revalidation_skip_detail": (
                        revalidation_result["detail"]
                        if revalidation_result is not None
                        else None
                    ),
                    "initial_distance_atr": candidate.result.distance_to_trigger_atr,
                    "fresh_price": price,
                    "fresh_distance_atr": fresh_distance,
                    "fresh_correct_side": correct_side,
                    "correct_side_of_level": correct_side,
                    "freshness": {
                        "signal_at": _time(candidate.result.analyzed_at),
                        "evaluated_at": _time(context.now),
                        "signal_age_seconds": (
                            context.now - _utc(candidate.result.analyzed_at)
                        ).total_seconds(),
                        "max_signal_age_seconds": (
                            branch_settings.max_signal_age_seconds
                        ),
                    },
                    "quote_id": quote_id,
                    "final_status": final_status.value,
                    "plan_metadata": (
                        _safe_plan_metadata(revalidated)
                        if final_status is ShadowStatus.PRE_SUBMIT_ELIGIBLE
                        else None
                    ),
                }
            )

        record: dict[str, Any] = {
            "experiment": EXPERIMENT_NAME,
            "experiment_run_id": self.run_id,
            "schema_version": EXPERIMENT_VERSION,
            "version": EXPERIMENT_VERSION,
            "candidate_id": candidate_id,
            "episode_id": candidate.episode_id,
            "symbol": candidate.result.symbol,
            "direction": candidate.result.direction.value,
            "setup_type": candidate.result.setup_type.value,
            "snapshot_ids": list(candidate.source_input.snapshot_ids),
            "scanner_timestamps": {
                "source_analyzed_at": _time(candidate.source_input.analyzed_at),
                "result_analyzed_at": _time(candidate.result.analyzed_at),
                "observed_at": _time(snapshot.observed_at),
            },
            "scanner": {
                "state": event.state.value,
                "eligibility": event.trade_eligibility.value,
                "quality": event.quality_score,
                "qualification_reason": snapshot.decision.reason.value,
                "telegram_notification_independent": True,
            },
            "candidate_price": candidate.result.current_price,
            "trigger_price": trigger,
            "atr": atr,
            "initial_distance_atr": candidate.result.distance_to_trigger_atr,
            "fresh_price": price,
            "fresh_distance_atr": fresh_distance,
            "correct_side_of_level": correct_side,
            "fresh_price_requested_at": _time(requested_at),
            "fresh_price_received_at": _time(received_at),
            "context_source": temporal.context_source,
            "production_completed_at": _time(temporal.production_handler_completed_at),
            "context_acquired_at": (
                _time(temporal.context_captured_at)
                if temporal.context_captured_at is not None
                else None
            ),
            "qualified_at": _time(temporal.qualified_at),
            "enqueued_at": _time(temporal.enqueued_at),
            "production_handler_completed_at": _time(
                temporal.production_handler_completed_at
            ),
            "worker_started_at": _time(temporal.worker_started_at),
            "fresh_quote_at": _time(received_at),
            "quote_requested_at": _time(requested_at),
            "quote_received_at": _time(received_at),
            "queue_delay_ms": _milliseconds(
                temporal.worker_started_at - temporal.enqueued_at
            ),
            "post_production_delay_ms": _milliseconds(
                temporal.worker_started_at - temporal.production_handler_completed_at
            ),
            "qualification_to_quote_ms": _milliseconds(
                requested_at - temporal.qualified_at
            ),
            "quote_latency_ms": _milliseconds(received_at - requested_at),
            "fresh_price_error": price_error,
            "quote_id": quote_id,
            "production_cycle_id": temporal.production_cycle_id,
            "context_captured_at": (
                _time(temporal.context_captured_at)
                if temporal.context_captured_at is not None
                else None
            ),
            "settings_digest": _settings_digest(self._settings),
            "context_digest": context_digest,
            "frozen_runtime_context": _safe_context(context),
            "branches": branches,
            "control_025": branches[0],
            "shadow_040": branches[1],
            "shadow_060": branches[2],
        }
        self._append_evidence(record)
        return record

    def _append_evidence(self, record: Mapping[str, Any]) -> None:
        try:
            self._evidence.append(record)
        except Exception as error:
            raise ShadowEvidenceJournalFailure(type(error).__name__) from None

    def observe_missing_context(
        self,
        snapshot: QualifiedScannerSnapshot,
        *,
        temporal: TemporalEvidence,
        reason: str = "MATCHING_CONTEXT_MISSING",
        detail: str | None = None,
    ) -> dict[str, Any]:
        """Record a qualified observation without inventing runtime context."""
        candidate = snapshot.decision.candidate
        event = snapshot.decision.event
        assert event is not None
        candidate_id = _candidate_id(snapshot)
        branches = []
        for threshold in DISTANCE_THRESHOLDS:
            branch_settings = replace(self._settings, max_entry_distance_atr=threshold)
            branches.append(
                {
                    "max_distance_atr": threshold,
                    "threshold_atr": threshold,
                    "role": "CONTROL" if threshold == CONTROL_THRESHOLD else "SHADOW",
                    "candidate_id": candidate_id,
                    "context_digest": None,
                    "settings_digest": _settings_digest(branch_settings),
                    "settings_override_fields": _settings_override_fields(
                        self._settings, branch_settings
                    ),
                    "initial": None,
                    "initial_accepted": None,
                    "initial_skip_reason": reason,
                    "initial_skip_detail": detail,
                    "revalidation_executed": False,
                    "revalidation": None,
                    "revalidation_accepted": None,
                    "revalidation_skip_reason": reason,
                    "revalidation_skip_detail": detail,
                    "initial_distance_atr": candidate.result.distance_to_trigger_atr,
                    "fresh_price": None,
                    "fresh_distance_atr": None,
                    "fresh_correct_side": None,
                    "correct_side_of_level": None,
                    "freshness": None,
                    "quote_id": None,
                    "final_status": ShadowStatus.EVIDENCE_MISSING.value,
                    "plan_metadata": None,
                }
            )
        record: dict[str, Any] = {
            "experiment": EXPERIMENT_NAME,
            "experiment_run_id": self.run_id,
            "schema_version": EXPERIMENT_VERSION,
            "version": EXPERIMENT_VERSION,
            "candidate_id": candidate_id,
            "episode_id": candidate.episode_id,
            "symbol": candidate.result.symbol,
            "direction": candidate.result.direction.value,
            "setup_type": candidate.result.setup_type.value,
            "snapshot_ids": list(candidate.source_input.snapshot_ids),
            "scanner_timestamps": {
                "source_analyzed_at": _time(candidate.source_input.analyzed_at),
                "result_analyzed_at": _time(candidate.result.analyzed_at),
                "observed_at": _time(snapshot.observed_at),
            },
            "scanner": {
                "state": event.state.value,
                "eligibility": event.trade_eligibility.value,
                "quality": event.quality_score,
                "qualification_reason": snapshot.decision.reason.value,
                "telegram_notification_independent": True,
            },
            "candidate_price": candidate.result.current_price,
            "trigger_price": candidate.result.trigger_level,
            "atr": candidate.atr_value,
            "initial_distance_atr": candidate.result.distance_to_trigger_atr,
            "fresh_price": None,
            "fresh_distance_atr": None,
            "correct_side_of_level": None,
            "fresh_price_requested_at": None,
            "fresh_price_received_at": None,
            "context_source": temporal.context_source,
            "production_completed_at": _time(temporal.production_handler_completed_at),
            "context_acquired_at": (
                _time(temporal.context_captured_at)
                if temporal.context_captured_at is not None
                else None
            ),
            "qualified_at": _time(temporal.qualified_at),
            "enqueued_at": _time(temporal.enqueued_at),
            "production_handler_completed_at": _time(
                temporal.production_handler_completed_at
            ),
            "worker_started_at": _time(temporal.worker_started_at),
            "fresh_quote_at": None,
            "quote_requested_at": None,
            "quote_received_at": None,
            "queue_delay_ms": _milliseconds(
                temporal.worker_started_at - temporal.enqueued_at
            ),
            "post_production_delay_ms": _milliseconds(
                temporal.worker_started_at - temporal.production_handler_completed_at
            ),
            "qualification_to_quote_ms": None,
            "quote_latency_ms": None,
            "fresh_price_error": reason,
            "context_failure_detail": detail,
            "quote_id": None,
            "production_cycle_id": temporal.production_cycle_id,
            "context_captured_at": None,
            "settings_digest": _settings_digest(self._settings),
            "context_digest": None,
            "frozen_runtime_context": None,
            "branches": branches,
            "control_025": branches[0],
            "shadow_040": branches[1],
            "shadow_060": branches[2],
        }
        self._append_evidence(record)
        return record


@dataclass(frozen=True, slots=True)
class _CapturedContext:
    context: FrozenRuntimeContext
    captured_at: datetime


class _PendingCycle:
    def __init__(
        self,
        cycle_id: str,
        qualified_at: datetime,
        snapshots: tuple[QualifiedScannerSnapshot, ...],
    ) -> None:
        self.cycle_id = cycle_id
        self.qualified_at = qualified_at
        self.snapshots = snapshots
        self.completed_at: datetime | None = None
        self.completed = asyncio.Event()
        self.contexts: dict[str, _CapturedContext] = {}
        self.remaining = len(snapshots)


@dataclass(frozen=True, slots=True)
class _QueuedObservation:
    cycle: _PendingCycle
    snapshot: QualifiedScannerSnapshot
    enqueued_at: datetime


class DistanceShadowDispatcher:
    """Non-blocking join between qualified snapshots and exact cycle context."""

    def __init__(
        self,
        observer: QtrMicroDistanceShadowObserver,
        *,
        capacity: int = 128,
        fallback_provider: Callable[[QtrSetupCandidate], FrozenRuntimeContext]
        | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if capacity < 1:
            raise ValueError("Distance Shadow queue capacity must be positive.")
        self._observer = observer
        self._queue: asyncio.Queue[_QueuedObservation] = asyncio.Queue(capacity)
        self._clock = clock
        self._cycles: dict[str, _PendingCycle] = {}
        self._worker: asyncio.Task[None] | None = None
        self._fallback_provider = fallback_provider
        self.accounting = ShadowAccounting(observer.accounting_path, observer.run_id)
        self._admission_closed_at: str | None = None
        self._finalization: asyncio.Task[dict[str, Any]] | None = None

    def set_fallback_provider(
        self, provider: Callable[[QtrSetupCandidate], FrozenRuntimeContext]
    ) -> None:
        self._fallback_provider = provider

    @property
    def invalid_observations(self) -> int:
        return int(self.accounting.snapshot()["invalid_observations"])

    def enqueue(
        self, plan: QtrSetupNotificationPlan, qualified_at: datetime
    ) -> str | None:
        if self._admission_closed_at is not None:
            _LOGGER.warning("QTR distance Shadow admission closed; outside run cutoff.")
            return None
        qualified = []
        for decision in plan.decisions:
            try:
                qualified.append(QualifiedScannerSnapshot(decision, plan.observed_at))
            except ValueError:
                continue
        snapshots = tuple(qualified)
        if not snapshots:
            return None
        accepted_before = self.accounting.counts["accepted_count"]
        dropped_before = (
            self.accounting.counts["queue_dropped_count"]
            + self.accounting.counts["enqueue_dropped_count"]
        )
        try:
            return self._enqueue(plan, qualified_at, snapshots)
        except Exception:
            self.accounting.admission_failed(
                accepted_before=accepted_before,
                dropped_before=dropped_before,
                dropped=len(snapshots),
            )
            raise

    def _enqueue(
        self,
        plan: QtrSetupNotificationPlan,
        qualified_at: datetime,
        snapshots: tuple[QualifiedScannerSnapshot, ...],
    ) -> str | None:
        if self._queue.qsize() + len(snapshots) > self._queue.maxsize:
            self.accounting.change(
                queue_overflow_count=1, queue_dropped_count=len(snapshots)
            )
            _LOGGER.warning(
                "QTR distance Shadow queue full; observations invalidated: %d.",
                len(snapshots),
            )
            return None
        qualified_at = _utc(qualified_at)
        cycle_id = (
            "QTRDS-"
            + _digest(
                {
                    "scanner_observed_at": plan.observed_at,
                    "qualified_at": qualified_at,
                    "candidate_ids": [_candidate_id(item) for item in snapshots],
                }
            )[:24]
        )
        loop = asyncio.get_running_loop()
        cycle = _PendingCycle(cycle_id, qualified_at, snapshots)
        # All fallible clock/identity/preparation work precedes queue mutation.
        queued = tuple(
            _QueuedObservation(cycle, snapshot, _utc(self._clock()))
            for snapshot in snapshots
        )
        if cycle_id in self._cycles:
            raise ValueError("Shadow cycle already has completion ownership.")
        # Accounting is established before anything becomes consumable. No await
        # occurs until the caller receives its cycle/completion handle.
        self.accounting.change(accepted_count=len(snapshots))
        self._cycles[cycle_id] = cycle
        try:
            if self._worker is None or self._worker.done():
                self._worker = loop.create_task(self._run())
            for item in queued:
                self._queue.put_nowait(item)
        except Exception:
            # Synchronous rollback: preserve other cycles and queue task counts.
            retained = []
            while not self._queue.empty():
                pending = self._queue.get_nowait()
                self._queue.task_done()
                if pending.cycle is not cycle:
                    retained.append(pending)
            for pending in retained:
                asyncio.Queue.put_nowait(self._queue, pending)
            self._cycles.pop(cycle_id, None)
            raise
        return cycle_id

    def capture_context(
        self,
        production_cycle_id: str,
        candidate: QtrSetupCandidate,
        context: FrozenRuntimeContext,
        captured_at: datetime,
    ) -> None:
        cycle = self._cycles.get(production_cycle_id)
        if cycle is None or cycle.completed.is_set():
            return
        for snapshot in cycle.snapshots:
            if snapshot.decision.candidate == candidate:
                cycle.contexts[_candidate_id(snapshot)] = _CapturedContext(
                    context=FrozenRuntimeContext.capture(
                        now=context.now,
                        equity=context.equity,
                        rules=context.rules,
                        state=context.state,
                        preflight=context.preflight,
                    ),
                    captured_at=_utc(captured_at),
                )
                return

    def production_completed(
        self, production_cycle_id: str | None, completed_at: datetime
    ) -> None:
        if production_cycle_id is None:
            return
        cycle = self._cycles.get(production_cycle_id)
        if cycle is None:
            return
        cycle.completed_at = _utc(completed_at)
        cycle.completed.set()

    async def wait(self) -> None:
        joined = asyncio.create_task(self._queue.join())
        try:
            while not joined.done():
                # No await between checking and replacing: concurrent drainers
                # share one worker, including after admission has closed.
                if self._queue.qsize() and (
                    self._worker is None or self._worker.done()
                ):
                    self._worker = asyncio.create_task(self._run())
                worker = self._worker
                if worker is None or worker.done():
                    await joined
                    break
                await asyncio.wait(
                    (joined, worker), return_when=asyncio.FIRST_COMPLETED
                )
            await joined
        finally:
            if not joined.done():
                joined.cancel()
            # Cancelling a drainer must not cancel the shared worker.
            with suppress(asyncio.CancelledError):
                await joined
        await self.accounting.wait()

    async def finalize(self) -> dict[str, Any]:
        """Close admission, drain every accepted operation, then publish the seal."""
        if self._finalization is None:
            self._admission_closed_at = _time(self._clock())
            self._finalization = asyncio.create_task(self._finish_run())
        try:
            return await asyncio.shield(self._finalization)
        except asyncio.CancelledError:
            await drain_thread_task(self._finalization)
            raise

    async def _finish_run(self) -> dict[str, Any]:
        await self.wait()
        io = asyncio.create_task(
            asyncio.to_thread(self._observer.finalized_evidence_snapshot)
        )
        try:
            count, digest = await asyncio.shield(io)
        except asyncio.CancelledError:
            await drain_thread_task(io)
            raise
        assert self._admission_closed_at is not None
        return await self.accounting.seal(
            evidence_record_count=count,
            evidence_sha256=digest,
            admission_closed_at=self._admission_closed_at,
            unique_observation_count=count,
        )

    async def _thread_call(
        self, call: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        io = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
        try:
            return await asyncio.shield(io)
        except asyncio.CancelledError:
            try:
                await drain_thread_task(io)
            except Exception as error:
                if isinstance(error, (OSError, ShadowEvidenceJournalFailure)):
                    self.accounting.change(journal_failure_count=1)
            raise

    async def _run(self) -> None:
        while True:
            queued = await self._queue.get()
            try:
                await queued.cycle.completed.wait()
                worker_started_at = _utc(self._clock())
                completed_at = queued.cycle.completed_at
                assert completed_at is not None
                captured = queued.cycle.contexts.get(_candidate_id(queued.snapshot))
                fallback_error: Exception | None = None
                source = "PUBLISHED_RUNTIME_CONTEXT"
                if captured is None and self._fallback_provider is not None:
                    source = "SHADOW_READ_ONLY_FALLBACK"
                    try:
                        context = await self._thread_call(
                            self._fallback_provider, queued.snapshot.decision.candidate
                        )
                        captured = _CapturedContext(
                            FrozenRuntimeContext.capture(
                                now=context.now,
                                equity=context.equity,
                                rules=context.rules,
                                state=context.state,
                                preflight=context.preflight,
                            ),
                            _utc(self._clock()),
                        )
                    except Exception as error:
                        fallback_error = error
                        self.accounting.change(fallback_failure_count=1)
                temporal = TemporalEvidence(
                    qualified_at=queued.cycle.qualified_at,
                    enqueued_at=queued.enqueued_at,
                    production_handler_completed_at=completed_at,
                    worker_started_at=worker_started_at,
                    production_cycle_id=queued.cycle.cycle_id,
                    context_source=source,
                    context_captured_at=(
                        captured.captured_at if captured is not None else None
                    ),
                )
                if captured is None:
                    await self._thread_call(
                        self._observer.observe_missing_context,
                        queued.snapshot,
                        temporal=temporal,
                        reason=(
                            "CONTEXT_FALLBACK_FAILED"
                            if fallback_error is not None
                            else "MATCHING_CONTEXT_MISSING"
                        ),
                        detail=(
                            str(fallback_error)
                            if type(fallback_error).__name__ == "ContextFallbackFailed"
                            else type(fallback_error).__name__
                            if fallback_error is not None
                            else None
                        ),
                    )
                else:
                    await self._thread_call(
                        self._observer.observe,
                        queued.snapshot,
                        captured.context,
                        temporal=temporal,
                    )
            except asyncio.CancelledError:
                self.accounting.change(cancelled_invalid_count=1)
                raise
            except Exception as error:
                self.accounting.change(
                    failed_invalid_count=1,
                    worker_exception_count=1,
                    journal_failure_count=int(
                        isinstance(error, (OSError, ShadowEvidenceJournalFailure))
                    ),
                )
                _LOGGER.warning(
                    "QTR distance Shadow observation failed (%s).",
                    type(error).__name__,
                )
            else:
                self.accounting.change(completed_count=1)
            finally:
                queued.cycle.remaining -= 1
                if queued.cycle.remaining == 0:
                    self._cycles.pop(queued.cycle.cycle_id, None)
                self._queue.task_done()


def summarize_distance_shadow(
    path: Path,
    *,
    minimum_conclusive_snapshots: int = 100,
    minimum_conclusive_episodes: int = 30,
    invalid_observations: int | None = None,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    invalid = 0
    raw = path.read_bytes() if path.exists() else b""
    if path.exists():
        for line in raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                _validate_record(record)
                records.append(record)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                invalid += 1
    raw_record_count = len(records)
    unique = {_observation_key(record): record for record in records}
    duplicate_records = len(records) - len(unique)
    records = list(unique.values())
    runs = {record.get("experiment_run_id") for record in records}
    mixed_runs = len(runs) > 1
    supplied_invalid_observations = invalid_observations
    invalid_observations = None
    finalized = False
    if len(runs) == 1 and None not in runs:
        try:
            checkpoint = read_shadow_accounting(
                path.with_suffix(".accounting.jsonl"), run_id=str(next(iter(runs)))
            )
            if (
                checkpoint["completed_count"] != raw_record_count + invalid
                or checkpoint["evidence_record_count"] != raw_record_count + invalid
                or duplicate_records
                or checkpoint["unique_observation_count"] != len(records)
                or checkpoint["evidence_sha256"] != hashlib.sha256(raw).hexdigest()
            ):
                raise ValueError("Sealed accounting does not match evidence.")
            created = datetime.fromisoformat(checkpoint["run_created_at"])
            cutoff = datetime.fromisoformat(checkpoint["admission_closed_at"])
            finalized_at = datetime.fromisoformat(checkpoint["finalized_at"])
            for record in records:
                if (
                    not created
                    <= datetime.fromisoformat(record["qualified_at"])
                    <= cutoff
                ):
                    raise ValueError("Observation falls outside Shadow admission.")
                for key in (
                    "production_completed_at",
                    "worker_started_at",
                    "context_acquired_at",
                    "fresh_quote_at",
                ):
                    if (
                        record[key] is not None
                        and datetime.fromisoformat(record[key]) > finalized_at
                    ):
                        raise ValueError("Observation outlives finalization.")
            invalid_observations = checkpoint["invalid_observations"]
            if (
                supplied_invalid_observations is not None
                and supplied_invalid_observations != invalid_observations
            ):
                raise ValueError("Supplied accounting differs from final seal.")
            finalized = True
        except (OSError, KeyError, TypeError, ValueError):
            invalid_observations = (
                supplied_invalid_observations
                if supplied_invalid_observations is not None
                and supplied_invalid_observations > 0
                else None
            )
    episodes = {_episode_key(record) for record in records}
    rows = []
    control_count = 0
    for threshold in DISTANCE_THRESHOLDS:
        eligible_episodes = {
            _episode_key(record)
            for record in records
            if _branch(record, threshold)["final_status"]
            == ShadowStatus.PRE_SUBMIT_ELIGIBLE.value
        }
        eligible_snapshots = sum(
            _branch(record, threshold)["final_status"]
            == ShadowStatus.PRE_SUBMIT_ELIGIBLE.value
            for record in records
        )
        if threshold == CONTROL_THRESHOLD:
            control_count = len(eligible_episodes)
        denominator = len(episodes)
        rate = len(eligible_episodes) / denominator if denominator else 0.0
        snapshot_rate = eligible_snapshots / len(records) if records else 0.0
        rows.append(
            {
                "threshold_atr": threshold,
                "eligible_unique_episodes": len(eligible_episodes),
                "total_unique_episodes": denominator,
                "episode_eligibility_rate": rate,
                "delta_eligible_episodes_vs_control": len(eligible_episodes)
                - control_count,
                "eligible_snapshots": eligible_snapshots,
                "total_snapshots": len(records),
                "snapshot_eligibility_rate": snapshot_rate,
            }
        )
    control_rate = rows[0]["episode_eligibility_rate"] if rows else 0.0
    for row in rows:
        row["delta_episode_rate_pp_vs_control"] = (
            row["episode_eligibility_rate"] - control_rate
        ) * 100
    evidence_missing = sum(
        any(
            branch["final_status"] == ShadowStatus.EVIDENCE_MISSING.value
            for branch in record["branches"]
        )
        for record in records
    )
    snapshots_met = len(records) >= minimum_conclusive_snapshots
    episodes_met = len(episodes) >= minimum_conclusive_episodes
    invalid_observations_requirement_met = invalid_observations == 0
    if (
        not finalized
        or mixed_runs
        or invalid
        or evidence_missing
        or not invalid_observations_requirement_met
    ):
        category = ExperimentCategory.EXPERIMENT_INVALID
    elif not snapshots_met or not episodes_met:
        category = ExperimentCategory.INCONCLUSIVE_SAMPLE
    elif any(
        _meets_review_threshold(
            row["delta_episode_rate_pp_vs_control"],
            row["delta_eligible_episodes_vs_control"],
        )
        for row in rows[1:]
    ):
        category = ExperimentCategory.DISTANCE_INCREASES_ELIGIBILITY
    else:
        category = ExperimentCategory.NO_MATERIAL_FUNNEL_CHANGE
    request_delays = [
        float(record["qualification_to_quote_ms"])
        for record in records
        if record.get("qualification_to_quote_ms") is not None
    ]
    received_delays = [
        _milliseconds(
            datetime.fromisoformat(record["quote_received_at"])
            - datetime.fromisoformat(record["qualified_at"])
        )
        for record in records
        if record.get("quote_received_at") is not None
    ]
    return {
        "category": category.value,
        "finalized": finalized,
        "mixed_experiment_runs": mixed_runs,
        "valid_records": len(records),
        "raw_valid_records": raw_record_count,
        "duplicate_records": duplicate_records,
        "unique_observations": len(records),
        "observation_identity": "experiment_run_id,production_cycle_id,candidate_id",
        "invalid_records": invalid,
        "evidence_missing_records": evidence_missing,
        "invalid_observations": invalid_observations,
        "unique_episodes": len(episodes),
        "episode_aggregation": "experiment_run_id,symbol,raw_episode_id",
        "sample_gate": {
            "minimum_valid_snapshots": minimum_conclusive_snapshots,
            "minimum_unique_episodes": minimum_conclusive_episodes,
            "valid_snapshots_met": snapshots_met,
            "unique_episodes_met": episodes_met,
            "invalid_observations_must_equal": 0,
            "invalid_observations_requirement_met": (
                invalid_observations_requirement_met
            ),
        },
        "thresholds": rows,
        "temporal_evidence": {
            "qualification_to_quote_request_ms": _distribution(request_delays),
            "qualification_to_quote_received_ms": _distribution(received_delays),
        },
    }


def _final_status(
    price: float | None,
    prepared: EntryDecision,
    revalidated: EntryDecision | None,
) -> ShadowStatus:
    if price is None:
        return ShadowStatus.EVIDENCE_MISSING
    if not prepared.accepted or revalidated is None or not revalidated.accepted:
        return ShadowStatus.REJECT
    return ShadowStatus.PRE_SUBMIT_ELIGIBLE


def _decision_result(decision: EntryDecision) -> dict[str, Any]:
    return {
        "accepted": decision.accepted,
        "reason": decision.skip_reason.value if decision.skip_reason else None,
        "detail": decision.skip_detail,
    }


def _safe_plan_metadata(decision: EntryDecision | None) -> dict[str, Any] | None:
    if decision is None or decision.plan is None:
        return None
    plan = decision.plan
    return {
        "direction": plan.direction.value,
        "setup_type": plan.setup_type.value,
        "entry_price": plan.entry_price,
        "stop_price": plan.stop_price,
        "risk_pct": plan.risk_pct,
        "risk_amount": plan.risk_amount,
        "qty": plan.qty,
        "leverage": plan.leverage,
        "notional": plan.notional,
        "estimated_round_trip_fees": plan.estimated_round_trip_fees,
        "estimated_fees_r_pct": plan.estimated_fees_r_pct,
        "tp1_price": plan.tp1_price,
        "tp1_qty": plan.tp1_qty,
        "tp2_price": plan.tp2_price,
        "tp2_qty": plan.tp2_qty,
        "runner_target_price": plan.runner_target_price,
        "runner_qty": plan.runner_qty,
        "initial_r": plan.initial_r,
    }


def _settings_override_fields(
    base: QtrMicroSettings, branch: QtrMicroSettings
) -> list[str]:
    return [
        field.name
        for field in fields(base)
        if getattr(base, field.name) != getattr(branch, field.name)
    ]


def _settings_digest(settings: QtrMicroSettings) -> str:
    safe = asdict(settings)
    safe.pop("api_key", None)
    safe.pop("api_secret", None)
    return _digest(safe)


def _candidate_id(snapshot: QualifiedScannerSnapshot) -> str:
    candidate = snapshot.decision.candidate
    return _digest(
        {
            "episode_id": candidate.episode_id,
            "symbol": candidate.result.symbol,
            "snapshot_ids": candidate.source_input.snapshot_ids,
            "source_analyzed_at": candidate.source_input.analyzed_at,
            "result_analyzed_at": candidate.result.analyzed_at,
            "scanner_observed_at": snapshot.observed_at,
        }
    )


def _context_digest(context: FrozenRuntimeContext) -> str:
    return _digest(_safe_context(context))


def _safe_context(context: FrozenRuntimeContext) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        _jsonable(
            {
                "now": context.now,
                "equity": context.equity,
                "rules": context.rules,
                "state": {
                    "updated_at": context.state.updated_at,
                    "trading_enabled": context.state.trading_enabled,
                    "trading_day": context.state.trading_day,
                    "day_start_equity": context.state.day_start_equity,
                    "realised_daily_pnl": context.state.realised_daily_pnl,
                    "consecutive_losses": context.state.consecutive_losses,
                    "loss_pause_until": context.state.loss_pause_until,
                    "blocked_reason": context.state.blocked_reason,
                    "positions": sorted(
                        (
                            item.symbol,
                            item.setup_episode_id,
                            item.stage.value,
                        )
                        for item in context.state.positions.values()
                    ),
                },
                "preflight": context.preflight,
            }
        ),
    )


def _digest(value: Any) -> str:
    payload = json.dumps(
        _jsonable(value), sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return _time(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(item) for item in value)
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _correct_side(
    direction: MicroDirection | None, price: float, trigger: float
) -> bool:
    if direction is MicroDirection.LONG:
        return price >= trigger
    if direction is MicroDirection.SHORT:
        return price <= trigger
    return False


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return _utc(value).isoformat()


def _milliseconds(value: timedelta) -> float:
    return float(value.total_seconds() * 1000)


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    return {
        "count": len(ordered),
        "p50": _percentile(ordered, 0.50),
        "p95": _percentile(ordered, 0.95),
        "max": ordered[-1],
    }


def _percentile(ordered: Sequence[float], fraction: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _ensure_line_boundary(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("rb") as stream:
        stream.seek(-1, 2)
        last = stream.read(1)
    if last != b"\n":
        with path.open("ab") as stream:
            stream.write(b"\n")


def _assert_no_forbidden_evidence_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        forbidden = _FORBIDDEN_EVIDENCE_KEYS.intersection(value)
        if forbidden:
            raise ValueError(f"Forbidden evidence keys: {sorted(forbidden)}")
        for item in value.values():
            _assert_no_forbidden_evidence_keys(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            _assert_no_forbidden_evidence_keys(item)


def _branch(record: Mapping[str, Any], threshold: float) -> Mapping[str, Any]:
    matches = [
        branch
        for branch in record["branches"]
        if float(branch["threshold_atr"]) == threshold
    ]
    if len(matches) != 1:
        raise ValueError("Record does not contain exactly one branch per threshold.")
    return cast(Mapping[str, Any], matches[0])


def _validate_record(record: Mapping[str, Any]) -> None:
    _observation_key(record)
    if record.get("experiment") != EXPERIMENT_NAME:
        raise ValueError("Unexpected experiment name.")
    if record.get("schema_version") != EXPERIMENT_VERSION:
        raise ValueError("Evidence version requires a separate historical report.")
    if not record.get("experiment_run_id"):
        raise ValueError("Missing Shadow run identity.")
    if record["production_completed_at"] != record["production_handler_completed_at"]:
        raise ValueError("Production completion aliases differ.")
    if record["context_acquired_at"] != record["context_captured_at"]:
        raise ValueError("Context timestamp aliases differ.")
    if record["fresh_quote_at"] != record["quote_received_at"]:
        raise ValueError("Quote timestamp aliases differ.")
    TemporalEvidence(
        qualified_at=datetime.fromisoformat(record["qualified_at"]),
        enqueued_at=datetime.fromisoformat(record["enqueued_at"]),
        production_handler_completed_at=datetime.fromisoformat(
            record["production_completed_at"]
        ),
        worker_started_at=datetime.fromisoformat(record["worker_started_at"]),
        production_cycle_id=record["production_cycle_id"],
        context_captured_at=(
            datetime.fromisoformat(record["context_acquired_at"])
            if record["context_acquired_at"] is not None
            else None
        ),
        context_source=record["context_source"],
    )
    if record["quote_requested_at"] is not None:
        request_at = datetime.fromisoformat(record["quote_requested_at"])
        receive_at = datetime.fromisoformat(record["quote_received_at"])
        if receive_at < request_at or (
            record["context_acquired_at"] is not None
            and request_at < datetime.fromisoformat(record["context_acquired_at"])
        ):
            raise ValueError("Quote precedes its context/request.")
    if len(record["branches"]) != 3:
        raise ValueError("Expected exactly three branches.")
    if not record.get("symbol") or record.get("episode_id") in {None, "", "unassigned"}:
        raise ValueError("Missing symbol-scoped episode identity.")
    branches = [_branch(record, threshold) for threshold in DISTANCE_THRESHOLDS]
    aliases = ("control_025", "shadow_040", "shadow_060")
    if any(
        record.get(alias) != branch
        for alias, branch in zip(aliases, branches, strict=True)
    ):
        raise ValueError("Named branch evidence differs from canonical branches.")
    for branch in branches:
        if branch["candidate_id"] != record["candidate_id"]:
            raise ValueError("Candidate identity differs between branches.")
        if branch["context_digest"] != record["context_digest"]:
            raise ValueError("Runtime context differs between branches.")
        if branch["fresh_price"] != record["fresh_price"]:
            raise ValueError("Fresh price differs between branches.")
        if branch["quote_id"] != record["quote_id"]:
            raise ValueError("Quote identity differs between branches.")
        allowed = (
            []
            if branch["threshold_atr"] == CONTROL_THRESHOLD
            else ["max_entry_distance_atr"]
        )
        if branch["settings_override_fields"] != allowed:
            raise ValueError("Branch settings differ outside the threshold.")
        if branch["final_status"] not in {item.value for item in ShadowStatus}:
            raise ValueError("Invalid shadow status.")
    _assert_no_forbidden_evidence_keys(record)


def _observation_key(record: Mapping[str, Any]) -> tuple[str, str, str]:
    values = tuple(
        record.get(key)
        for key in (
            "experiment_run_id",
            "production_cycle_id",
            "candidate_id",
        )
    )
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError("Missing Shadow observation identity.")
    return cast(tuple[str, str, str], values)


def _episode_key(record: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("experiment_run_id", "LEGACY")),
        str(record["symbol"]),
        str(record["episode_id"]),
    )

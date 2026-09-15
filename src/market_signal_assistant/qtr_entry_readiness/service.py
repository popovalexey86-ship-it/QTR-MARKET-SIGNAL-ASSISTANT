from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.providers import PublicPriceQuote
from market_signal_assistant.qtr_entry_readiness.audit import (
    JsonlEntryReadinessAuditStore,
    append_safely,
)
from market_signal_assistant.qtr_entry_readiness.engine import (
    EntryReadinessEngine,
    confirmation_complete,
    setup_episode_key,
)
from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessEpisodeState,
    EntryReadinessEvaluation,
    EntryReadinessRunStatus,
    EntryReadinessRunTelemetry,
    InternalDisposition,
    UserReadiness,
)
from market_signal_assistant.qtr_entry_readiness.run_audit import (
    EntryReadinessRunAuditWriter,
    append_run_safely,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate

_LOGGER = logging.getLogger(__name__)


class PublicPriceProvider(Protocol):
    def latest_prices(
        self, symbols: tuple[str, ...]
    ) -> Mapping[str, PublicPriceQuote]: ...


class EntryReadinessShadowService:
    """Fetch public prices, evaluate candidates, and append shadow observations."""

    def __init__(
        self,
        engine: EntryReadinessEngine,
        price_provider: PublicPriceProvider,
        audit_store: JsonlEntryReadinessAuditStore,
        *,
        run_audit_store: EntryReadinessRunAuditWriter | None = None,
        state_capacity: int = 10_000,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        if state_capacity <= 0:
            raise ValueError("Entry-readiness state capacity must be positive.")
        self._engine = engine
        self._prices = price_provider
        self._audit = audit_store
        self._run_audit = run_audit_store
        self._state_capacity = state_capacity
        self._monotonic = monotonic
        recovered = audit_store.recover_episode_states(capacity=state_capacity)
        self._episodes: OrderedDict[str, EntryReadinessEpisodeState] = OrderedDict(
            (state.setup_episode_key, state) for state in recovered
        )

    @property
    def tracked_episode_count(self) -> int:
        return len(self._episodes)

    def evaluate(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        evaluated_at: datetime,
    ) -> tuple[EntryReadinessEvaluation, ...]:
        observation_time = _utc(evaluated_at)
        run_id = _run_id(candidates, observation_time)
        started_at = self._monotonic()
        started_recorded = self._append_run(
            _run_record(
                observation_time,
                run_id,
                EntryReadinessRunStatus.STARTED,
                candidates_received=len(candidates),
            )
        )
        symbols = tuple(sorted({item.result.symbol for item in candidates}))
        quotes, provider_error, batch_latency = self._load_quotes(symbols)
        prices_received = sum(
            1 for candidate in candidates if candidate.result.symbol in quotes
        )
        evaluations: list[EntryReadinessEvaluation] = []
        evaluation_error: str | None = None
        for candidate in candidates:
            quote = quotes.get(candidate.result.symbol)
            effective_time = (
                max(observation_time, quote.observed_at)
                if quote is not None
                else observation_time
            )
            try:
                key = setup_episode_key(candidate)
                previous = self._episodes.get(key)
                first_confirmation = (
                    previous.first_confirmation_observed_at
                    if previous is not None
                    else None
                )
                if confirmation_complete(candidate) and first_confirmation is None:
                    first_confirmation = observation_time
                evaluation = self._engine.evaluate(
                    candidate,
                    quote,
                    effective_time,
                    first_confirmation_observed_at=first_confirmation,
                )
                evaluations.append(
                    self._track_transition(evaluation, first_confirmation)
                )
            except Exception as error:
                if evaluation_error is None:
                    evaluation_error = type(error).__name__
                _LOGGER.warning(
                    "QTR Entry Readiness candidate failed: symbol=%s error=%s",
                    candidate.result.symbol,
                    type(error).__name__,
                )
        result = tuple(evaluations)
        candidate_audit_recorded = append_safely(self._audit, result)
        error_type = evaluation_error or provider_error
        status = (
            EntryReadinessRunStatus.FAILED
            if evaluation_error is not None
            or not candidate_audit_recorded
            or not started_recorded
            else EntryReadinessRunStatus.PROVIDER_FAILED
            if provider_error is not None
            else EntryReadinessRunStatus.COMPLETED
        )
        if not started_recorded:
            error_type = "RunAuditError"
        elif not candidate_audit_recorded:
            error_type = "CandidateAuditError"
        self._append_run(
            _run_record(
                observation_time,
                run_id,
                status,
                candidates_received=len(candidates),
                candidates_evaluated=len(result),
                candidates_suppressed=sum(
                    evaluation.internal_disposition
                    is InternalDisposition.SUPPRESSED
                    for evaluation in result
                ),
                prices_received=prices_received,
                prices_missing=len(candidates) - prices_received,
                batch_price_latency_ms=batch_latency,
                total_run_latency_ms=_elapsed_ms(
                    started_at, self._monotonic()
                ),
                error_type=error_type,
            )
        )
        return result

    def record_skipped_busy(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        observed_at: datetime,
    ) -> None:
        """Record a skipped scan without starting another provider request."""
        observation_time = _utc(observed_at)
        self._append_run(
            _run_record(
                observation_time,
                _run_id(candidates, observation_time),
                EntryReadinessRunStatus.SKIPPED_BUSY,
                candidates_received=len(candidates),
            )
        )

    def _load_quotes(
        self, symbols: tuple[str, ...]
    ) -> tuple[Mapping[str, PublicPriceQuote], str | None, float]:
        started_at = self._monotonic()
        if not symbols:
            return {}, None, _elapsed_ms(started_at, self._monotonic())
        try:
            quotes = self._prices.latest_prices(symbols)
        except Exception as error:
            # Shadow provider boundary: primary Scanner flow must always continue.
            _LOGGER.warning(
                "QTR Entry Readiness public prices unavailable: symbols=%d error=%s",
                len(symbols),
                type(error).__name__,
            )
            return (
                {},
                type(error).__name__,
                _elapsed_ms(started_at, self._monotonic()),
            )
        if not isinstance(quotes, Mapping):
            return (
                {},
                "MalformedProviderResponse",
                _elapsed_ms(started_at, self._monotonic()),
            )
        return quotes, None, _elapsed_ms(started_at, self._monotonic())

    def _append_run(self, record: EntryReadinessRunTelemetry) -> bool:
        if self._run_audit is None:
            return True
        return append_run_safely(self._run_audit, record)

    def _track_transition(
        self,
        evaluation: EntryReadinessEvaluation,
        first_confirmation_observed_at: datetime | None,
    ) -> EntryReadinessEvaluation:
        previous = self._episodes.get(evaluation.setup_episode_key)
        readiness = evaluation.user_readiness
        first_wait = previous.first_wait_at if previous is not None else None
        first_now = previous.first_now_at if previous is not None else None
        if readiness is UserReadiness.WAIT and first_wait is None:
            first_wait = evaluation.evaluation_time
        if readiness is UserReadiness.NOW and first_now is None:
            first_now = evaluation.evaluation_time
        transition = (
            "WAIT_TO_NOW"
            if previous is not None
            and previous.latest_readiness is UserReadiness.WAIT
            and readiness is UserReadiness.NOW
            else None
        )
        elapsed = (
            (evaluation.evaluation_time - first_wait).total_seconds()
            if transition is not None and first_wait is not None
            else None
        )
        latest_readiness = (
            readiness
            if readiness is not None
            else previous.latest_readiness
            if previous is not None
            else None
        )
        self._episodes[evaluation.setup_episode_key] = EntryReadinessEpisodeState(
            setup_episode_key=evaluation.setup_episode_key,
            latest_readiness=latest_readiness,
            first_wait_at=first_wait,
            first_now_at=first_now,
            first_confirmation_observed_at=first_confirmation_observed_at,
        )
        self._episodes.move_to_end(evaluation.setup_episode_key)
        while len(self._episodes) > self._state_capacity:
            self._episodes.popitem(last=False)
        return replace(
            evaluation,
            previous_user_readiness=(
                previous.latest_readiness if previous is not None else None
            ),
            transition=transition,
            first_wait_at=first_wait,
            first_now_at=first_now,
            wait_to_now_seconds=elapsed,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Entry-readiness observation time must be timezone-aware.")
    return value.astimezone(UTC)


def _run_id(
    candidates: tuple[QtrSetupCandidate, ...], observed_at: datetime
) -> str:
    payload = {
        "observed_at": observed_at.isoformat(),
        "candidates": sorted(
            (
                candidate.result.symbol,
                candidate.episode_id,
                candidate.result.direction.value,
                candidate.result.setup_type.value,
            )
            for candidate in candidates
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _run_record(
    recorded_at: datetime,
    run_id: str,
    status: EntryReadinessRunStatus,
    *,
    candidates_received: int,
    candidates_evaluated: int = 0,
    candidates_suppressed: int = 0,
    prices_received: int = 0,
    prices_missing: int = 0,
    batch_price_latency_ms: float | None = None,
    total_run_latency_ms: float | None = None,
    error_type: str | None = None,
) -> EntryReadinessRunTelemetry:
    return EntryReadinessRunTelemetry(
        recorded_at=recorded_at,
        run_id=run_id,
        status=status,
        candidates_received=candidates_received,
        candidates_evaluated=candidates_evaluated,
        candidates_suppressed=candidates_suppressed,
        prices_received=prices_received,
        prices_missing=prices_missing,
        batch_price_latency_ms=batch_price_latency_ms,
        total_run_latency_ms=total_run_latency_ms,
        error_type=error_type,
    )


def _elapsed_ms(started_at: float, completed_at: float) -> float:
    return max(0.0, (completed_at - started_at) * 1_000.0)

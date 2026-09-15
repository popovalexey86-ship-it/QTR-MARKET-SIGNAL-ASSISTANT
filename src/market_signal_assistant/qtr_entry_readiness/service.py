from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol

from market_signal_assistant.providers import PublicPriceQuote
from market_signal_assistant.qtr_entry_readiness.audit import (
    JsonlEntryReadinessAuditStore,
    append_safely,
)
from market_signal_assistant.qtr_entry_readiness.engine import EntryReadinessEngine
from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessEvaluation,
    UserReadiness,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate

_LOGGER = logging.getLogger(__name__)


class PublicPriceProvider(Protocol):
    def latest_price(self, symbol: str) -> PublicPriceQuote: ...


@dataclass(frozen=True, slots=True)
class _EpisodeState:
    latest_readiness: UserReadiness
    first_wait_at: datetime | None
    first_now_at: datetime | None


class EntryReadinessShadowService:
    """Fetch public prices, evaluate candidates, and append shadow observations."""

    def __init__(
        self,
        engine: EntryReadinessEngine,
        price_provider: PublicPriceProvider,
        audit_store: JsonlEntryReadinessAuditStore,
        *,
        state_capacity: int = 10_000,
    ) -> None:
        if state_capacity <= 0:
            raise ValueError("Entry-readiness state capacity must be positive.")
        self._engine = engine
        self._prices = price_provider
        self._audit = audit_store
        self._state_capacity = state_capacity
        self._episodes: OrderedDict[str, _EpisodeState] = OrderedDict()

    @property
    def tracked_episode_count(self) -> int:
        return len(self._episodes)

    def evaluate(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        evaluated_at: datetime,
    ) -> tuple[EntryReadinessEvaluation, ...]:
        evaluations: list[EntryReadinessEvaluation] = []
        for candidate in candidates:
            quote = self._load_quote(candidate.result.symbol)
            effective_time = (
                max(evaluated_at, quote.observed_at)
                if quote is not None
                else evaluated_at
            )
            try:
                evaluation = self._engine.evaluate(
                    candidate,
                    quote,
                    effective_time,
                )
                evaluations.append(self._track_transition(evaluation))
            except (TypeError, ValueError):
                _LOGGER.warning(
                    "QTR Entry Readiness candidate failed: symbol=%s",
                    candidate.result.symbol,
                )
        result = tuple(evaluations)
        append_safely(self._audit, result)
        return result

    def _load_quote(self, symbol: str) -> PublicPriceQuote | None:
        try:
            return self._prices.latest_price(symbol)
        except Exception as error:
            # Provider boundary: one symbol must not abort a complete scan.
            _LOGGER.warning(
                "QTR Entry Readiness public price unavailable: symbol=%s error=%s",
                symbol,
                type(error).__name__,
            )
            return None

    def _track_transition(
        self, evaluation: EntryReadinessEvaluation
    ) -> EntryReadinessEvaluation:
        previous = self._episodes.get(evaluation.setup_episode_key)
        readiness = evaluation.user_readiness
        if readiness is None:
            if previous is None:
                return evaluation
            return replace(
                evaluation,
                previous_user_readiness=previous.latest_readiness,
                first_wait_at=previous.first_wait_at,
                first_now_at=previous.first_now_at,
            )
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
        self._episodes[evaluation.setup_episode_key] = _EpisodeState(
            readiness,
            first_wait,
            first_now,
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

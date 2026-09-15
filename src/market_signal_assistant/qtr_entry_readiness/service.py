from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Mapping
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
    UserReadiness,
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
        state_capacity: int = 10_000,
    ) -> None:
        if state_capacity <= 0:
            raise ValueError("Entry-readiness state capacity must be positive.")
        self._engine = engine
        self._prices = price_provider
        self._audit = audit_store
        self._state_capacity = state_capacity
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
        symbols = tuple(sorted({item.result.symbol for item in candidates}))
        quotes = self._load_quotes(symbols)
        evaluations: list[EntryReadinessEvaluation] = []
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
            except (TypeError, ValueError):
                _LOGGER.warning(
                    "QTR Entry Readiness candidate failed: symbol=%s",
                    candidate.result.symbol,
                )
        result = tuple(evaluations)
        append_safely(self._audit, result)
        return result

    def _load_quotes(
        self, symbols: tuple[str, ...]
    ) -> Mapping[str, PublicPriceQuote]:
        if not symbols:
            return {}
        try:
            return self._prices.latest_prices(symbols)
        except Exception as error:
            # Shadow provider boundary: primary Scanner flow must always continue.
            _LOGGER.warning(
                "QTR Entry Readiness public prices unavailable: symbols=%d error=%s",
                len(symbols),
                type(error).__name__,
            )
            return {}

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

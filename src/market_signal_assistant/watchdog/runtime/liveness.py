from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


class SoakLivenessError(RuntimeError):
    """The runtime can no longer provide a valid acceptance interval."""


@dataclass(frozen=True, slots=True)
class RuntimeLiveness:
    observed_at: datetime
    running: bool
    fatal_error: str | None
    last_loop_at: datetime | None
    last_market_progress_at: datetime | None
    progress_marker: tuple[object, ...]
    degraded: bool = False
    market_progress_due_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in (
            "observed_at",
            "last_loop_at",
            "last_market_progress_at",
            "market_progress_due_at",
        ):
            value = getattr(self, name)
            if value is not None:
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("Liveness timestamps must be timezone-aware.")
                object.__setattr__(self, name, value.astimezone(UTC))


class ConfirmedHealthyClock:
    """Accumulate only intervals closed by fresh, healthy market progress."""

    def __init__(
        self,
        accumulated_seconds: float,
        *,
        stale_after_seconds: float,
    ) -> None:
        if (
            isinstance(accumulated_seconds, bool)
            or not math.isfinite(accumulated_seconds)
            or accumulated_seconds < 0
        ):
            raise ValueError("Accumulated soak time must be finite and non-negative.")
        if not math.isfinite(stale_after_seconds) or stale_after_seconds <= 0:
            raise ValueError("Liveness timeout must be positive and finite.")
        self._accumulated = accumulated_seconds
        self._stale_after = stale_after_seconds
        self._last_observed_monotonic: float | None = None
        self._confirmed_anchor: float | None = None
        self._progress_marker: tuple[object, ...] | None = None
        self._unhealthy_since: float | None = None

    @property
    def accumulated_seconds(self) -> float:
        return self._accumulated

    def observe(self, probe: RuntimeLiveness, *, monotonic_now: float) -> bool:
        if not math.isfinite(monotonic_now):
            raise ValueError("Monotonic time must be finite.")
        previous = self._last_observed_monotonic
        if previous is not None and monotonic_now < previous:
            raise ValueError("Monotonic time cannot move backwards.")
        self._last_observed_monotonic = monotonic_now

        if probe.fatal_error is not None:
            raise SoakLivenessError(f"runtime failure: {probe.fatal_error}")
        if not probe.running:
            raise SoakLivenessError("runtime thread is not alive")
        if probe.last_loop_at is None or probe.last_market_progress_at is None:
            return self._reject(monotonic_now, "runtime progress was not initialized")
        if _age(probe.observed_at, probe.last_loop_at) > self._stale_after:
            raise SoakLivenessError("runtime heartbeat is stale")
        past_expected_progress = (
            probe.market_progress_due_at is None
            or probe.observed_at
            > probe.market_progress_due_at + timedelta(seconds=self._stale_after)
        )
        if (
            _age(probe.observed_at, probe.last_market_progress_at) > self._stale_after
            and past_expected_progress
        ):
            raise SoakLivenessError("market progress is stale")
        if probe.degraded:
            return self._reject(monotonic_now, "runtime remained degraded")

        self._unhealthy_since = None
        if self._progress_marker is None or self._confirmed_anchor is None:
            self._progress_marker = probe.progress_marker
            self._confirmed_anchor = monotonic_now
            return False
        if probe.progress_marker == self._progress_marker:
            if (
                monotonic_now - self._confirmed_anchor > self._stale_after
                and past_expected_progress
            ):
                raise SoakLivenessError("market progress did not advance")
            return False

        elapsed = monotonic_now - self._confirmed_anchor
        self._accumulated += max(0.0, elapsed)
        self._confirmed_anchor = monotonic_now
        self._progress_marker = probe.progress_marker
        return elapsed > 0

    def _reject(self, monotonic_now: float, reason: str) -> bool:
        self._confirmed_anchor = None
        self._progress_marker = None
        if self._unhealthy_since is None:
            self._unhealthy_since = monotonic_now
            return False
        if monotonic_now - self._unhealthy_since > self._stale_after:
            raise SoakLivenessError(reason)
        return False


def _age(now: datetime, then: datetime) -> float:
    return max(0.0, (now - then).total_seconds())

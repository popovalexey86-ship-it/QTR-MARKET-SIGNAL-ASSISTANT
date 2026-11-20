"""Notification-only selection; Watchdog events remain immutable and complete."""

from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence

WINDOW_SECONDS = 24 * 60 * 60
DIGEST_SECONDS = 6 * 60 * 60
FRESH_SECONDS = 10 * 60
SYMBOL_COOLDOWN_SECONDS = 12 * 60 * 60
ESCALATION_COOLDOWN_SECONDS = 30 * 60
MAX_IMMEDIATE_PER_WINDOW = 36


@dataclass(frozen=True, slots=True)
class AlertDecision:
    send: bool
    reason: str


def decide_alert(
    event: WatchdogEventEvidence,
    *,
    now: float,
    previous: tuple[float, float] | None,
    sent_last_day: int,
) -> AlertDecision:
    """Choose an immediate alert without changing detection or event persistence."""
    age = now - event.detected_at.timestamp()
    if age > FRESH_SECONDS or age < -60:
        return AlertDecision(False, "EXPIRED_OR_FUTURE")
    score = event.anomaly_score
    watch_transition = (
        event.state_before != event.state_after
        and event.state_after.value == "WATCH"
        and len(event.anomaly_types) >= 2
        and score >= 50
    )
    if score < 55 and not watch_transition:
        return AlertDecision(False, "SECONDARY")
    if previous is not None:
        last_at, last_score = previous
        elapsed = event.detected_at.timestamp() - last_at
        escalation = (
            score >= 60
            and score >= last_score + 15
            and elapsed >= ESCALATION_COOLDOWN_SECONDS
        )
        if elapsed < SYMBOL_COOLDOWN_SECONDS and not escalation:
            return AlertDecision(False, "SYMBOL_COOLDOWN")
    if sent_last_day >= MAX_IMMEDIATE_PER_WINDOW:
        return AlertDecision(False, "GLOBAL_CAP")
    return AlertDecision(True, "MATERIAL_ESCALATION" if previous else "NEW_STRONG")

from __future__ import annotations

from dataclasses import replace

from market_signal_assistant.qtr_signal_outcome.engine import OutcomeEngine
from market_signal_assistant.qtr_signal_outcome.metrics import (
    build_summary,
    format_summary,
)
from market_signal_assistant.qtr_signal_outcome.models import Direction
from qtr_signal_outcome.helpers import candle, signal


def test_summary_segments_direction_setup_and_quality() -> None:
    engine = OutcomeEngine()
    long = engine.analyze(signal(), tuple(candle(i) for i in range(1, 241)))
    short_signal = replace(
        signal(Direction.SHORT),
        signal_id="signal-2",
        setup_type="RETEST",
        telegram_quality_score=None,
    )
    short = engine.analyze(short_signal, tuple(candle(i) for i in range(1, 241)))
    summary = build_summary((long, short), invalid_source_records=3)
    assert summary.total_delivered_signals == 2
    assert summary.complete == 2
    assert summary.invalid_source_records == 3
    assert summary.segments["direction:LONG"].n == 1
    assert summary.segments["direction:SHORT"].n == 1
    assert summary.segments["setup:BREAKOUT"].n == 1
    assert summary.segments["setup:RETEST"].n == 1
    assert summary.segments["quality:90-99"].n == 1
    assert summary.segments["quality:UNKNOWN"].n == 1
    overall = summary.segments["all"]
    assert "+1_before_-1" in overall.favorable_first_rates
    assert overall.invalidation_hit_rate is not None
    rendered = format_summary(summary)
    assert "+1_before_-1=" in rendered
    assert "invalidation hit rate=" in rendered

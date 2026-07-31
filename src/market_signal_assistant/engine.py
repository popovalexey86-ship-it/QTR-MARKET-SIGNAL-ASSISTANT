from __future__ import annotations

import math
from dataclasses import dataclass

from market_signal_assistant.indicators import ema, mean, rsi, true_ranges
from market_signal_assistant.models import (
    MarketSeries,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)


@dataclass(frozen=True, slots=True)
class SignalEngine:
    min_score: float = 45.0
    min_confirmations: int = 2

    def __post_init__(self) -> None:
        if not math.isfinite(self.min_score) or not 0 < self.min_score <= 100:
            raise ValueError("Minimum score must be in the interval (0, 100].")
        if self.min_confirmations <= 0:
            raise ValueError("Minimum confirmations must be positive.")

    def analyze(self, series: MarketSeries) -> MarketSignal | None:
        if len(series.candles) < 55:
            return None
        candles = series.candles
        closes = tuple(candle.close for candle in candles)
        highs = tuple(candle.high for candle in candles)
        lows = tuple(candle.low for candle in candles)
        volumes = tuple(candle.volume for candle in candles)
        last = candles[-1]
        ema20 = ema(closes, 20)[-1]
        ema50 = ema(closes, 50)[-1]
        current_rsi = rsi(closes, 14)
        ranges = true_ranges(highs, lows, closes)
        current_atr = mean(ranges[-14:])
        baseline_atr = mean(ranges[-34:-14])
        prior_high = max(highs[-21:-1])
        prior_low = min(lows[-21:-1])
        average_volume = mean(volumes[-21:-1])

        evidence: list[SignalEvidence] = []
        if ema20 > ema50 and last.close > ema20:
            evidence.append(
                SignalEvidence(
                    "trend",
                    SignalDirection.BULLISH,
                    25.0,
                    f"EMA20 {ema20:.6g} is above EMA50 {ema50:.6g}",
                )
            )
        elif ema20 < ema50 and last.close < ema20:
            evidence.append(
                SignalEvidence(
                    "trend",
                    SignalDirection.BEARISH,
                    25.0,
                    f"EMA20 {ema20:.6g} is below EMA50 {ema50:.6g}",
                )
            )

        if current_rsi is not None and current_rsi >= 55:
            evidence.append(
                SignalEvidence(
                    "momentum",
                    SignalDirection.BULLISH,
                    15.0,
                    f"RSI14 is {current_rsi:.2f}",
                )
            )
        elif current_rsi is not None and current_rsi <= 45:
            evidence.append(
                SignalEvidence(
                    "momentum",
                    SignalDirection.BEARISH,
                    15.0,
                    f"RSI14 is {current_rsi:.2f}",
                )
            )

        if last.close > prior_high:
            evidence.append(
                SignalEvidence(
                    "range_breakout",
                    SignalDirection.BULLISH,
                    35.0,
                    f"Close broke the prior 20-bar high {prior_high:.6g}",
                )
            )
        elif last.close < prior_low:
            evidence.append(
                SignalEvidence(
                    "range_breakout",
                    SignalDirection.BEARISH,
                    35.0,
                    f"Close broke the prior 20-bar low {prior_low:.6g}",
                )
            )

        candle_direction = (
            SignalDirection.BULLISH
            if last.close > last.open
            else SignalDirection.BEARISH
        )
        if baseline_atr > 0 and current_atr >= baseline_atr * 1.25:
            evidence.append(
                SignalEvidence(
                    "volatility_expansion",
                    candle_direction,
                    15.0,
                    "ATR14 expanded at least 25% above its prior baseline",
                )
            )
        if average_volume > 0 and last.volume >= average_volume * 1.5:
            evidence.append(
                SignalEvidence(
                    "volume_expansion",
                    candle_direction,
                    10.0,
                    "Volume is at least 1.5x its 20-bar average",
                )
            )

        bullish = sum(
            item.weight
            for item in evidence
            if item.direction is SignalDirection.BULLISH
        )
        bearish = sum(
            item.weight
            for item in evidence
            if item.direction is SignalDirection.BEARISH
        )
        if bullish == bearish:
            return None
        direction = (
            SignalDirection.BULLISH
            if bullish > bearish
            else SignalDirection.BEARISH
        )
        aligned = tuple(item for item in evidence if item.direction is direction)
        conflicts = len(evidence) - len(aligned)
        raw_score = abs(bullish - bearish)
        if raw_score < self.min_score or len(aligned) < self.min_confirmations:
            return None
        score = min(100.0, raw_score)
        confidence = min(
            100.0,
            max(0.0, score + len(aligned) * 5.0 - conflicts * 10.0),
        )
        return MarketSignal(
            instrument=series.instrument,
            interval=series.interval,
            timestamp=last.timestamp,
            direction=direction,
            score=score,
            confidence=confidence,
            confirmations=len(aligned),
            conflicts=conflicts,
            price=last.close,
            evidence=tuple(evidence),
        )

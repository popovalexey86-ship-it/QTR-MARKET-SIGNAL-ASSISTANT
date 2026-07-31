from __future__ import annotations

import math
from collections.abc import Sequence


def ema(values: Sequence[float], period: int) -> tuple[float, ...]:
    if period <= 0:
        raise ValueError("EMA period must be positive.")
    if not values:
        return ()
    multiplier = 2.0 / (period + 1.0)
    output = [float(values[0])]
    for value in values[1:]:
        output.append((value - output[-1]) * multiplier + output[-1])
    return tuple(output)


def rsi(values: Sequence[float], period: int = 14) -> float | None:
    if period <= 0:
        raise ValueError("RSI period must be positive.")
    if len(values) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for previous, current in zip(
        values[-period - 1 : -1],
        values[-period:],
        strict=True,
    ):
        change = current - previous
        gains += max(change, 0.0)
        losses += max(-change, 0.0)
    average_gain = gains / period
    average_loss = losses / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100.0 - 100.0 / (1.0 + relative_strength)


def true_ranges(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
) -> tuple[float, ...]:
    if not (len(highs) == len(lows) == len(closes)):
        raise ValueError("ATR inputs must have the same length.")
    if not closes:
        return ()
    output = [highs[0] - lows[0]]
    for index in range(1, len(closes)):
        output.append(
            max(
                highs[index] - lows[index],
                abs(highs[index] - closes[index - 1]),
                abs(lows[index] - closes[index - 1]),
            )
        )
    return tuple(output)


def mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    result = sum(values) / len(values)
    return result if math.isfinite(result) else 0.0

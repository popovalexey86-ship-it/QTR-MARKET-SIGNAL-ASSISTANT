from __future__ import annotations

import math
from collections import OrderedDict
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlencode

from market_signal_assistant.providers import JsonGetter, public_json_get
from market_signal_assistant.qtr_signal_outcome.models import MarketCandle


class MarketDataProviderError(RuntimeError):
    pass


class MarketDataProvider(Protocol):
    def fetch(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[MarketCandle, ...]: ...


class BybitKlineProvider:
    """Public Bybit V5 one-minute kline provider; construction is offline."""

    def __init__(
        self,
        *,
        getter: JsonGetter = public_json_get,
        base_url: str = "https://api.bybit.com",
        timeout: float = 10.0,
    ) -> None:
        self._getter = getter
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def fetch(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[MarketCandle, ...]:
        normalized = symbol.strip().upper()
        start_utc = _utc(start)
        end_utc = _utc(end)
        if not normalized or end_utc <= start_utc:
            raise ValueError("Invalid kline request range.")
        span = max(1, math.ceil((end_utc - start_utc).total_seconds() / 60))
        query = urlencode(
            {
                "category": "linear",
                "symbol": normalized,
                "interval": "1",
                "start": int(start_utc.timestamp() * 1000),
                "end": int(end_utc.timestamp() * 1000),
                "limit": min(1000, span + 2),
            }
        )
        try:
            payload = self._getter(
                f"{self._base_url}/v5/market/kline?{query}", self._timeout
            )
        except Exception as error:
            raise MarketDataProviderError(
                f"Bybit kline request failed: {type(error).__name__}."
            ) from None
        try:
            if payload.get("retCode") != 0:
                raise ValueError("bad retCode")
            result = payload["result"]
            if not isinstance(result, Mapping):
                raise ValueError("bad result")
            rows = result["list"]
            if not isinstance(rows, list):
                raise ValueError("bad list")
            candles = tuple(_candle(normalized, row) for row in rows)
            ordered = tuple(sorted(candles, key=lambda item: item.opened_at))
            if any(
                left.opened_at >= right.opened_at
                for left, right in zip(ordered, ordered[1:], strict=False)
            ):
                raise ValueError("duplicate timestamp")
            return ordered
        except (KeyError, TypeError, ValueError, OverflowError):
            raise MarketDataProviderError("Malformed Bybit kline response.") from None


class CachingMarketDataProvider:
    """Small exact-range LRU; cached candles never cross signal windows."""

    def __init__(self, provider: MarketDataProvider, *, capacity: int = 256) -> None:
        if capacity <= 0:
            raise ValueError("Market data cache capacity must be positive.")
        self._provider = provider
        self._capacity = capacity
        self._cache: OrderedDict[
            tuple[str, datetime, datetime], tuple[MarketCandle, ...]
        ] = OrderedDict()

    def fetch(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[MarketCandle, ...]:
        key = (symbol.strip().upper(), _utc(start), _utc(end))
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached
        candles = self._provider.fetch(*key)
        self._cache[key] = candles
        self._cache.move_to_end(key)
        while len(self._cache) > self._capacity:
            self._cache.popitem(last=False)
        return candles


def _candle(symbol: str, row: Any) -> MarketCandle:
    if not isinstance(row, list) or len(row) < 5:
        raise ValueError("Malformed kline row.")
    timestamp = _positive(row[0])
    opened = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
    return MarketCandle(
        symbol=symbol,
        opened_at=opened,
        closed_at=opened + timedelta(minutes=1),
        open=_positive(row[1]),
        high=_positive(row[2]),
        low=_positive(row[3]),
        close=_positive(row[4]),
    )


def _positive(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("Invalid number.")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError("Invalid positive number.")
    return parsed


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Kline request timestamps must be timezone-aware.")
    return value.astimezone(UTC)

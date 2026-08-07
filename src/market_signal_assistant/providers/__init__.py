from __future__ import annotations

import csv
import json
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.request import urlopen

from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
)


class MarketDataError(RuntimeError):
    """A safe provider-level market data failure."""


class MarketDataProvider(Protocol):
    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries: ...


JsonGetter = Callable[[str, float], Mapping[str, Any]]


def public_json_get(url: str, timeout: float) -> Mapping[str, Any]:
    try:
        with urlopen(url, timeout=timeout) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError) as error:
        raise MarketDataError(type(error).__name__) from None
    if not isinstance(payload, dict):
        raise MarketDataError("Public endpoint returned a malformed payload.")
    return payload


class CsvMarketDataProvider:
    def __init__(self, paths: Mapping[str, Path]) -> None:
        self._paths = dict(paths)

    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        path = self._paths.get(instrument.symbol)
        if path is None:
            raise MarketDataError(
                f"No CSV path configured for {instrument.symbol}."
            )
        try:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                rows = tuple(csv.DictReader(stream))
            candles = tuple(_candle_from_csv(row) for row in rows)
            return MarketSeries(instrument, interval, candles[-limit:])
        except (OSError, KeyError, TypeError, ValueError) as error:
            raise MarketDataError(
                f"Invalid CSV for {instrument.symbol}: {type(error).__name__}"
            ) from None


class BybitPublicProvider:
    _INTERVALS = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}

    def __init__(
        self,
        *,
        getter: JsonGetter = public_json_get,
        sleep: Callable[[float], None] = time.sleep,
        timeout: float = 10.0,
    ) -> None:
        self._getter = getter
        self._sleep = sleep
        self._timeout = timeout

    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        bybit_interval = self._INTERVALS.get(interval)
        if bybit_interval is None:
            raise MarketDataError(f"Unsupported Bybit interval: {interval}.")
        query = urlencode(
            {
                "category": "linear",
                "symbol": instrument.symbol,
                "interval": bybit_interval,
                "limit": min(limit, 1000),
            }
        )
        payload = self._request(
            f"https://api.bybit.com/v5/market/kline?{query}"
        )
        if payload.get("retCode") != 0:
            raise MarketDataError(
                f"Bybit public API error code {payload.get('retCode')}."
            )
        try:
            rows = payload["result"]["list"]
            candles = tuple(
                Candle(
                    timestamp=datetime.fromtimestamp(
                        int(row[0]) / 1000,
                        tz=UTC,
                    ),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                )
                for row in rows
            )
        except (KeyError, TypeError, ValueError, IndexError):
            raise MarketDataError("Malformed Bybit kline response.") from None
        completed = _completed_candles(candles, interval)
        return MarketSeries(
            instrument,
            interval,
            tuple(sorted(completed, key=lambda item: item.timestamp)),
        )

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        info = self._request(
            "https://api.bybit.com/v5/market/instruments-info?"
            "category=linear&limit=1000"
        )
        tickers = self._request(
            "https://api.bybit.com/v5/market/tickers?category=linear"
        )
        try:
            if info.get("retCode") != 0 or tickers.get("retCode") != 0:
                raise ValueError
            info_rows = info["result"]["list"]
            ticker_rows = tickers["result"]["list"]
            if not isinstance(info_rows, list) or not isinstance(ticker_rows, list):
                raise ValueError
            ticker_by_symbol = {
                str(row["symbol"]): row
                for row in ticker_rows
                if isinstance(row, Mapping)
            }
            instruments = tuple(
                _catalog_instrument(row, ticker_by_symbol)
                for row in info_rows
                if isinstance(row, Mapping)
            )
        except (KeyError, TypeError, ValueError):
            raise MarketDataError(
                "Malformed Bybit instrument catalog response."
            ) from None
        return instruments

    def _request(self, url: str) -> Mapping[str, Any]:
        last_error: MarketDataError | None = None
        for attempt, delay in enumerate((0.0, 0.5, 1.5), start=1):
            if delay:
                self._sleep(delay)
            try:
                return self._getter(url, self._timeout)
            except MarketDataError as error:
                last_error = error
                if attempt == 3:
                    break
        raise MarketDataError(
            "Bybit public market data is temporarily unavailable."
        ) from last_error


def _catalog_instrument(
    row: Mapping[str, Any],
    ticker_by_symbol: Mapping[str, Mapping[str, Any]],
) -> CatalogInstrument:
    try:
        symbol = str(row["symbol"])
        ticker = ticker_by_symbol.get(symbol, {})
        is_pre_listing = row["isPreListing"]
        if not isinstance(is_pre_listing, bool):
            raise ValueError
        return CatalogInstrument(
            symbol=symbol,
            quote_coin=str(row["quoteCoin"]),
            status=str(row["status"]),
            turnover_24h=float(ticker.get("turnover24h", 0.0)),
            bid=float(ticker.get("bid1Price", 0.0)),
            ask=float(ticker.get("ask1Price", 0.0)),
            base_coin=str(row["baseCoin"]),
            settle_coin=str(row["settleCoin"]),
            contract_type=str(row["contractType"]),
            symbol_type=str(row["symbolType"]),
            is_pre_listing=is_pre_listing,
        )
    except (KeyError, TypeError, ValueError):
        raise MarketDataError("Malformed Bybit instrument catalog response.") from None


class YahooPublicProvider:
    _INTERVALS = {"5m": "5m", "15m": "15m", "1h": "1h", "1d": "1d"}

    def __init__(
        self,
        *,
        getter: JsonGetter = public_json_get,
        timeout: float = 10.0,
    ) -> None:
        self._getter = getter
        self._timeout = timeout

    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        yahoo_interval = self._INTERVALS.get(interval)
        if yahoo_interval is None:
            raise MarketDataError(f"Unsupported Yahoo interval: {interval}.")
        range_name = "1mo" if interval != "1d" else "1y"
        query = urlencode(
            {
                "interval": yahoo_interval,
                "range": range_name,
                "includePrePost": "false",
            }
        )
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{instrument.symbol}?{query}"
        )
        payload = self._getter(url, self._timeout)
        try:
            result = payload["chart"]["result"][0]
            timestamps = result["timestamp"]
            quote = result["indicators"]["quote"][0]
            rows = zip(
                timestamps,
                quote["open"],
                quote["high"],
                quote["low"],
                quote["close"],
                quote["volume"],
                strict=True,
            )
            candles = tuple(
                Candle(
                    timestamp=datetime.fromtimestamp(int(timestamp), tz=UTC),
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=float(volume or 0.0),
                )
                for timestamp, open_price, high, low, close, volume in rows
                if None not in (open_price, high, low, close)
            )
        except (KeyError, TypeError, ValueError, IndexError):
            raise MarketDataError("Malformed Yahoo chart response.") from None
        completed = _completed_candles(candles, interval)
        return MarketSeries(
            instrument,
            interval,
            tuple(sorted(completed, key=lambda item: item.timestamp)[-limit:]),
        )


class RoutingMarketDataProvider:
    def __init__(
        self,
        *,
        csv_provider: CsvMarketDataProvider | None = None,
        crypto_provider: MarketDataProvider | None = None,
        traditional_provider: MarketDataProvider | None = None,
    ) -> None:
        self._csv = csv_provider
        self._crypto = crypto_provider or BybitPublicProvider()
        self._traditional = traditional_provider or YahooPublicProvider()

    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        if self._csv is not None:
            try:
                return self._csv.load(instrument, interval, limit)
            except MarketDataError as error:
                if "No CSV path configured" not in str(error):
                    raise
        provider = (
            self._crypto
            if instrument.asset_class is AssetClass.CRYPTO
            else self._traditional
        )
        return provider.load(instrument, interval, limit)


def _candle_from_csv(row: Mapping[str, str]) -> Candle:
    timestamp = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
    return Candle(
        timestamp=timestamp,
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row["volume"]),
    )


def _completed_candles(
    candles: tuple[Candle, ...],
    interval: str,
    *,
    now: datetime | None = None,
) -> tuple[Candle, ...]:
    duration = {
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }.get(interval)
    if duration is None:
        raise MarketDataError(f"Unsupported interval: {interval}.")
    cutoff = now or datetime.now(UTC)
    return tuple(
        candle
        for candle in candles
        if candle.timestamp + duration <= cutoff
    )

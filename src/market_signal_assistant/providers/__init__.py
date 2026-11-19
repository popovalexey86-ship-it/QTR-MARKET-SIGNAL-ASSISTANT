from __future__ import annotations

import csv
import json
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class CatalogRowRejection:
    symbol: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class CatalogDiagnostics:
    instrument_rows: int
    valid_instruments: int
    rejected: tuple[CatalogRowRejection, ...]

    @property
    def invalid_rows(self) -> int:
        return len(self.rejected)

    @property
    def affected_symbols(self) -> tuple[str, ...]:
        return tuple(sorted({item.symbol for item in self.rejected if item.symbol}))

    @property
    def reason_counts(self) -> tuple[tuple[str, int], ...]:
        counts: dict[str, int] = {}
        for item in self.rejected:
            counts[item.reason] = counts.get(item.reason, 0) + 1
        return tuple(sorted(counts.items()))


class _CatalogRowError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


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
    _INTERVALS = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "1h": "60",
        "4h": "240",
        "1d": "D",
    }

    def __init__(
        self,
        *,
        getter: JsonGetter = public_json_get,
        sleep: Callable[[float], None] = time.sleep,
        timeout: float = 10.0,
        request_gate: Callable[[], None] | None = None,
        request_observer: Callable[[], None] | None = None,
    ) -> None:
        self._getter = getter
        self._sleep = sleep
        self._timeout = timeout
        self._request_gate = request_gate or (lambda: None)
        self._request_observer = request_observer or (lambda: None)
        self.catalog_diagnostics: CatalogDiagnostics | None = None

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
        self.catalog_diagnostics = None
        info_rows = self._list_linear_instrument_rows()
        tickers = self._request(
            "https://api.bybit.com/v5/market/tickers?category=linear"
        )
        try:
            if tickers.get("retCode") != 0:
                raise ValueError
            ticker_rows = tickers["result"]["list"]
            if not isinstance(ticker_rows, list):
                raise ValueError
            ticker_by_symbol: dict[str, Mapping[str, Any]] = {}
            duplicate_symbols: set[str] = set()
            rejected: list[CatalogRowRejection] = []
            for row in ticker_rows:
                if not isinstance(row, Mapping):
                    rejected.append(CatalogRowRejection(None, "invalid_ticker_row"))
                    continue
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not symbol.strip():
                    rejected.append(CatalogRowRejection(None, "invalid_ticker_symbol"))
                    continue
                if symbol in ticker_by_symbol:
                    duplicate_symbols.add(symbol)
                else:
                    ticker_by_symbol[symbol] = row
            instruments_list: list[CatalogInstrument] = []
            for row in info_rows:
                if not isinstance(row, Mapping):
                    rejected.append(CatalogRowRejection(None, "invalid_instrument_row"))
                    continue
                symbol = row.get("symbol")
                symbol_name = (
                    symbol if isinstance(symbol, str) and symbol.strip() else None
                )
                try:
                    if symbol_name in duplicate_symbols:
                        raise _CatalogRowError("duplicate_ticker")
                    instruments_list.append(_catalog_instrument(row, ticker_by_symbol))
                except _CatalogRowError as error:
                    rejected.append(CatalogRowRejection(symbol_name, error.reason))
            self.catalog_diagnostics = CatalogDiagnostics(
                len(info_rows), len(instruments_list), tuple(rejected)
            )
        except (KeyError, TypeError, ValueError):
            raise MarketDataError(
                "Malformed Bybit instrument catalog response."
            ) from None
        if not instruments_list:
            raise MarketDataError("Bybit instrument catalog has no valid instruments.")
        return tuple(instruments_list)

    def _list_linear_instrument_rows(self) -> list[object]:
        rows: list[object] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            parameters = {"category": "linear", "limit": 1000}
            if cursor is not None:
                parameters["cursor"] = cursor
            payload = self._request(
                "https://api.bybit.com/v5/market/instruments-info?"
                + urlencode(parameters)
            )
            try:
                if payload.get("retCode") != 0:
                    raise ValueError
                result = payload["result"]
                page = result["list"]
                next_cursor = result.get("nextPageCursor", "")
                if not isinstance(page, list) or not isinstance(next_cursor, str):
                    raise ValueError
                rows.extend(page)
            except (KeyError, TypeError, ValueError):
                raise MarketDataError(
                    "Malformed Bybit instrument catalog response."
                ) from None
            cursor = next_cursor.strip() or None
            if cursor is None:
                return rows
            if cursor in seen_cursors:
                raise MarketDataError(
                    "Bybit instrument catalog repeated a pagination cursor."
                )
            seen_cursors.add(cursor)

    def _request(self, url: str) -> Mapping[str, Any]:
        last_error: MarketDataError | None = None
        for attempt, delay in enumerate((0.0, 0.5, 1.5), start=1):
            if delay:
                self._sleep(delay)
            try:
                self._request_gate()
                self._request_observer()
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
    symbol = _required_catalog_text(row, "symbol")
    ticker = ticker_by_symbol.get(symbol)
    if ticker is None:
        raise _CatalogRowError("missing_ticker")
    is_pre_listing = row.get("isPreListing")
    if not isinstance(is_pre_listing, bool):
        raise _CatalogRowError("invalid_isPreListing")
    symbol_type = row.get("symbolType")
    if not isinstance(symbol_type, str):
        raise _CatalogRowError("invalid_symbolType")
    try:
        launch_time = _catalog_launch_time(row.get("launchTime"))
    except MarketDataError:
        raise _CatalogRowError("invalid_launchTime") from None
    bid = _catalog_market_number(ticker, "bid1Price", positive=True)
    ask = _catalog_market_number(ticker, "ask1Price", positive=True)
    if ask < bid:
        raise _CatalogRowError("crossed_top_of_book")
    quote_coin = _required_catalog_text(row, "quoteCoin")
    status = _required_catalog_text(row, "status")
    turnover = _catalog_market_number(ticker, "turnover24h")
    base_coin = _required_catalog_text(row, "baseCoin")
    settle_coin = _required_catalog_text(row, "settleCoin")
    contract_type = _required_catalog_text(row, "contractType")
    try:
        return CatalogInstrument(
            symbol=symbol,
            quote_coin=quote_coin,
            status=status,
            turnover_24h=turnover,
            bid=bid,
            ask=ask,
            base_coin=base_coin,
            settle_coin=settle_coin,
            contract_type=contract_type,
            symbol_type=symbol_type,
            is_pre_listing=is_pre_listing,
            launch_time=launch_time,
        )
    except ValueError:
        raise _CatalogRowError("invalid_catalog_metadata") from None


def _required_catalog_text(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _CatalogRowError(f"invalid_{field}")
    return value


def _catalog_market_number(
    row: Mapping[str, Any], field: str, *, positive: bool = False
) -> float:
    value = row.get(field)
    if isinstance(value, bool) or value is None or value == "":
        raise _CatalogRowError(f"invalid_{field}")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise _CatalogRowError(f"invalid_{field}") from None
    if not math.isfinite(number) or number < 0 or (positive and number <= 0):
        raise _CatalogRowError(f"invalid_{field}")
    return number


def _catalog_launch_time(value: object) -> datetime | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError
        milliseconds = int(value)
        if milliseconds <= 0:
            raise ValueError
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
    except (TypeError, ValueError, OSError):
        raise MarketDataError("Malformed Bybit instrument launch time.") from None


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
        "1m": timedelta(minutes=1),
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

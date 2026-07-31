from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.derivatives.provider import DerivativesDataError
from market_signal_assistant.providers import JsonGetter, public_json_get
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
)


class BybitDerivativesProvider:
    """Collect a normalized snapshot from public Bybit V5 REST endpoints.

    Construction is offline. Network access occurs only in :meth:`collect`.
    Price and volume changes come from kline data, so no ticker request is made.
    """

    name = "bybit"
    _BASE_URL = "https://api.bybit.com/v5/market"

    def __init__(
        self,
        liquidations: BybitLiquidationAccumulator,
        *,
        getter: JsonGetter = public_json_get,
        category: str = "linear",
        interval: str = "5",
        open_interest_interval: str = "5min",
        timeout: float = 10.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._liquidations = liquidations
        self._getter = getter
        self._category = category
        self._interval = interval
        self._open_interest_interval = open_interest_interval
        self._timeout = timeout
        self._clock = clock or (lambda: datetime.now(UTC))

    def collect(self, symbol: str) -> DerivativesSnapshot:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("Derivatives symbol cannot be empty.")
        funding = self._request(
            "funding/history",
            {"symbol": normalized_symbol, "limit": 1},
        )
        open_interest = self._request(
            "open-interest",
            {
                "symbol": normalized_symbol,
                "intervalTime": self._open_interest_interval,
                "limit": 2,
            },
        )
        klines = self._request(
            "kline",
            {
                "symbol": normalized_symbol,
                "interval": self._interval,
                "limit": 2,
            },
        )

        funding_rate = _number(
            _first_mapping(funding, "funding").get("fundingRate"),
            "fundingRate",
        )
        current_oi, previous_oi = _latest_two_mappings(
            _items(open_interest, "open interest"),
            "timestamp",
            "open interest",
        )
        current_kline, previous_kline = _latest_two_rows(
            _items(klines, "kline")
        )
        current_oi_value = _number(
            current_oi.get("openInterest"), "openInterest"
        )
        previous_oi_value = _number(
            previous_oi.get("openInterest"), "previous openInterest"
        )
        current_close = _row_number(current_kline, 4, "close")
        previous_close = _row_number(previous_kline, 4, "previous close")
        current_volume = _row_number(current_kline, 5, "volume")
        previous_volume = _row_number(previous_kline, 5, "previous volume")
        long_liquidations, short_liquidations = self._liquidations.totals(
            normalized_symbol
        )
        as_of = self._clock()
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise DerivativesDataError(
                "Bybit derivatives clock must be timezone-aware."
            )
        return DerivativesSnapshot(
            provider=self.name,
            symbol=normalized_symbol,
            as_of=as_of,
            funding_rate=funding_rate,
            open_interest=current_oi_value,
            open_interest_change=_change(
                current_oi_value, previous_oi_value, "open interest"
            ),
            price_change=_change(current_close, previous_close, "price"),
            volume_change=_change(current_volume, previous_volume, "volume"),
            long_liquidations=long_liquidations,
            short_liquidations=short_liquidations,
        )

    def _request(
        self,
        endpoint: str,
        parameters: Mapping[str, object],
    ) -> Mapping[str, Any]:
        query = urlencode({"category": self._category, **parameters})
        try:
            payload = self._getter(
                f"{self._BASE_URL}/{endpoint}?{query}", self._timeout
            )
        except Exception as error:
            if isinstance(error, DerivativesDataError):
                raise
            raise DerivativesDataError(
                f"Bybit derivatives request failed: {type(error).__name__}."
            ) from None
        if payload.get("retCode") != 0:
            raise DerivativesDataError(
                f"Bybit derivatives API error code {payload.get('retCode')}."
            )
        return payload


def _items(response: object, label: str) -> list[Any]:
    try:
        items = response["result"]["list"]  # type: ignore[index]
    except (KeyError, TypeError):
        raise DerivativesDataError(f"Malformed Bybit {label} response.") from None
    if not isinstance(items, list) or not items:
        raise DerivativesDataError(f"Empty Bybit {label} response.")
    return items


def _first_mapping(response: object, label: str) -> Mapping[str, Any]:
    item = _items(response, label)[0]
    if not isinstance(item, Mapping):
        raise DerivativesDataError(f"Malformed Bybit {label} item.")
    return item


def _latest_two_mappings(
    rows: list[Any], timestamp_field: str, label: str
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if len(rows) < 2 or not all(isinstance(row, Mapping) for row in rows[:2]):
        raise DerivativesDataError(
            f"Bybit {label} response requires two observations."
        )
    typed_rows: list[Mapping[str, Any]] = rows[:2]
    ordered = sorted(
        typed_rows,
        key=lambda row: _number(row.get(timestamp_field), timestamp_field),
        reverse=True,
    )
    return ordered[0], ordered[1]


def _latest_two_rows(rows: list[Any]) -> tuple[list[Any], list[Any]]:
    if len(rows) < 2 or not all(isinstance(row, list) for row in rows[:2]):
        raise DerivativesDataError(
            "Bybit kline response requires two observations."
        )
    typed_rows: list[list[Any]] = rows[:2]
    ordered = sorted(
        typed_rows,
        key=lambda row: _row_number(row, 0, "kline timestamp"),
        reverse=True,
    )
    return ordered[0], ordered[1]


def _row_number(row: list[Any], index: int, field: str) -> float:
    try:
        value = row[index]
    except IndexError:
        raise DerivativesDataError(f"Missing Bybit {field}.") from None
    return _number(value, field)


def _number(value: object, field: str) -> float:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise DerivativesDataError(f"Invalid Bybit {field}.") from None
    if not math.isfinite(parsed):
        raise DerivativesDataError(f"Invalid Bybit {field}.")
    return parsed


def _change(current: float, previous: float, label: str) -> float:
    if previous == 0:
        raise DerivativesDataError(f"Previous Bybit {label} cannot be zero.")
    return current / previous - 1.0

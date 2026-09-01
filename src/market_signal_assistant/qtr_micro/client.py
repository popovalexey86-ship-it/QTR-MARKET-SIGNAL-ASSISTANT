from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from market_signal_assistant.inplay.models import is_crypto_linear_usdt_metadata
from market_signal_assistant.qtr_micro.models import (
    DemoOrder,
    DemoPosition,
    ExecutionFill,
    InstrumentRules,
    InstrumentUniverseStatus,
    LeverageUpdateResult,
    OrderAcknowledgement,
)
from market_signal_assistant.qtr_micro.settings import DEMO_API_HOST, DEMO_BASE_URL

__all__ = (
    "BybitDemoTradingClient",
    "DemoApiError",
    "DemoOrder",
    "DemoPosition",
    "ExecutionFill",
    "InstrumentRules",
    "InstrumentUniverseStatus",
    "LeverageUpdateResult",
    "OrderAcknowledgement",
    "UrllibBybitDemoTransport",
)


class DemoApiError(RuntimeError):
    """Controlled Bybit Demo API failure without secret material."""

    def __init__(self, message: str, *, ret_code: int | None = None) -> None:
        super().__init__(message)
        self.ret_code = ret_code


class DemoTransport(Protocol):
    @property
    def base_url(self) -> str: ...

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, object],
        *,
        authenticated: bool,
    ) -> Mapping[str, Any]: ...


class DemoTradingClient(Protocol):
    @property
    def base_url(self) -> str: ...

    def connectivity(self) -> None: ...
    def wallet_equity(self) -> float: ...
    def account_type(self) -> str: ...
    def position_mode(self, symbol: str) -> str: ...
    def list_positions(self) -> tuple[DemoPosition, ...]: ...
    def list_active_orders(self) -> tuple[DemoOrder, ...]: ...
    def instrument_rules(self, symbol: str) -> InstrumentRules: ...
    def current_market_price(self, symbol: str) -> float: ...
    def set_leverage(self, symbol: str, leverage: int) -> LeverageUpdateResult: ...
    def create_market_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        order_link_id: str,
        reduce_only: bool = False,
    ) -> OrderAcknowledgement: ...
    def cancel_order(self, symbol: str, order_id: str) -> None: ...
    def execution_fill(self, order_id: str, symbol: str) -> ExecutionFill | None: ...
    def set_protective_stop(
        self, *, symbol: str, stop_price: float, position_idx: int = 0
    ) -> None: ...


class UrllibBybitDemoTransport:
    """Minimal authenticated V5 transport, permanently restricted to Demo host."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = DEMO_BASE_URL,
        timeout: float = 10.0,
        recv_window: int = 5000,
    ) -> None:
        _guard_demo_url(base_url)
        if not api_key.strip() or not api_secret.strip():
            raise DemoApiError("Demo API credentials отсутствуют.")
        self._api_key = api_key.strip()
        self._api_secret = api_secret.strip()
        self._base_url = DEMO_BASE_URL
        self._timeout = timeout
        self._recv_window = recv_window

    @property
    def base_url(self) -> str:
        return self._base_url

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, object],
        *,
        authenticated: bool,
    ) -> Mapping[str, Any]:
        _guard_demo_url(self._base_url)
        normalized_method = method.upper()
        if normalized_method not in {"GET", "POST"} or not path.startswith("/v5/"):
            raise DemoApiError("Недопустимый Demo API request.")
        query = urllib.parse.urlencode(
            [(key, str(value)) for key, value in sorted(params.items())]
        )
        body = b""
        url = f"{self._base_url}{path}"
        payload = query
        if normalized_method == "GET":
            if query:
                url = f"{url}?{query}"
        else:
            payload = json.dumps(params, separators=(",", ":"), sort_keys=True)
            body = payload.encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if authenticated:
            timestamp = str(int(time.time() * 1000))
            signing = f"{timestamp}{self._api_key}{self._recv_window}{payload}"
            signature = hmac.new(
                self._api_secret.encode(), signing.encode(), hashlib.sha256
            ).hexdigest()
            headers.update(
                {
                    "X-BAPI-API-KEY": self._api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": str(self._recv_window),
                    "X-BAPI-SIGN": signature,
                }
            )
        request = urllib.request.Request(
            url, data=body if normalized_method == "POST" else None, headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                parsed = json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError, urllib.error.URLError):
            raise DemoApiError("Bybit Demo API недоступен.") from None
        if not isinstance(parsed, dict):
            raise DemoApiError("Bybit Demo API вернул некорректный ответ.")
        if parsed.get("retCode") != 0:
            raw_code = parsed.get("retCode")
            try:
                code = int(str(raw_code))
            except ValueError:
                code = None
            raise DemoApiError(
                f"Bybit Demo API отклонил запрос, код {raw_code}.",
                ret_code=code,
            )
        return cast(Mapping[str, Any], parsed)


class BybitDemoTradingClient:
    def __init__(self, transport: DemoTransport) -> None:
        _guard_demo_url(transport.base_url)
        self._transport = transport

    @property
    def base_url(self) -> str:
        return self._transport.base_url

    def connectivity(self) -> None:
        self._request("GET", "/v5/market/time", {}, authenticated=False)

    def wallet_equity(self) -> float:
        payload = self._request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED"},
        )
        account = _first(_result_list(payload))
        return _positive_float(account.get("totalEquity"), "wallet equity")

    def account_type(self) -> str:
        payload = self._request("GET", "/v5/account/info", {})
        result = _result(payload)
        unified = result.get("unifiedMarginStatus")
        if unified in {1, 3, 4, 5, 6} or str(unified) in {"1", "3", "4", "5", "6"}:
            return "UNIFIED"
        return "UNKNOWN"

    def position_mode(self, symbol: str) -> str:
        payload = self._request(
            "GET", "/v5/position/list", {"category": "linear", "symbol": symbol}
        )
        rows = _result_list(payload)
        if not rows:
            raise DemoApiError("Position mode не подтверждён Demo API.")
        indexes = {int(str(row.get("positionIdx", "-1"))) for row in rows}
        return "ONE_WAY" if indexes == {0} else "HEDGE"

    def list_positions(self) -> tuple[DemoPosition, ...]:
        payload = self._request(
            "GET", "/v5/position/list", {"category": "linear", "settleCoin": "USDT"}
        )
        positions: list[DemoPosition] = []
        for row in _result_list(payload):
            size = _float(row.get("size"), 0.0)
            if size <= 0:
                continue
            positions.append(
                DemoPosition(
                    symbol=str(row.get("symbol", "")).upper(),
                    side=str(row.get("side", "")),
                    size=size,
                    average_price=_positive_float(row.get("avgPrice"), "average price"),
                    position_idx=int(str(row.get("positionIdx", "0"))),
                )
            )
        return tuple(positions)

    def list_active_orders(self) -> tuple[DemoOrder, ...]:
        payload = self._request(
            "GET", "/v5/order/realtime", {"category": "linear", "settleCoin": "USDT"}
        )
        return tuple(
            DemoOrder(
                symbol=str(row.get("symbol", "")).upper(),
                order_id=str(row.get("orderId", "")),
                order_link_id=str(row.get("orderLinkId", "")),
                side=str(row.get("side", "")),
                qty=_float(row.get("qty"), 0.0),
                status=str(row.get("orderStatus", "")),
            )
            for row in _result_list(payload)
        )

    def instrument_rules(self, symbol: str) -> InstrumentRules:
        normalized_symbol = symbol.strip().upper()
        payload = self._request(
            "GET",
            "/v5/market/instruments-info",
            {"category": "linear", "symbol": symbol},
            authenticated=False,
        )
        row = _first(_result_list(payload))
        returned_symbol = str(row.get("symbol", "")).strip().upper()
        if returned_symbol != normalized_symbol:
            raise DemoApiError("Instrument info не соответствует запрошенному symbol.")
        lot = row.get("lotSizeFilter")
        leverage = row.get("leverageFilter")
        if not isinstance(lot, dict) or not isinstance(leverage, dict):
            raise DemoApiError("Instrument info неполон.")
        return InstrumentRules(
            symbol=normalized_symbol,
            qty_step=_positive_float(lot.get("qtyStep"), "qtyStep"),
            min_order_qty=_positive_float(lot.get("minOrderQty"), "minOrderQty"),
            max_market_order_qty=_positive_float(
                lot.get("maxMktOrderQty"), "maxMktOrderQty"
            ),
            min_notional_value=_positive_float(
                lot.get("minNotionalValue"), "minNotionalValue"
            ),
            max_leverage=int(
                _positive_float(leverage.get("maxLeverage"), "maxLeverage")
            ),
            universe_status=_instrument_universe_status(row),
        )

    def current_market_price(self, symbol: str) -> float:
        payload = self._request(
            "GET",
            "/v5/market/tickers",
            {"category": "linear", "symbol": symbol.upper()},
            authenticated=False,
        )
        row = _first(_result_list(payload))
        returned_symbol = str(row.get("symbol", "")).upper()
        if returned_symbol != symbol.upper():
            raise DemoApiError("Ticker не соответствует запрошенному symbol.")
        return _positive_float(row.get("lastPrice"), "lastPrice")

    def set_leverage(self, symbol: str, leverage: int) -> LeverageUpdateResult:
        try:
            self._request(
                "POST",
                "/v5/position/set-leverage",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "buyLeverage": str(leverage),
                    "sellLeverage": str(leverage),
                },
            )
        except DemoApiError as error:
            if error.ret_code == 110043:
                return LeverageUpdateResult.ALREADY_SET
            raise
        return LeverageUpdateResult.CHANGED

    def create_market_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        order_link_id: str,
        reduce_only: bool = False,
    ) -> OrderAcknowledgement:
        if not order_link_id.startswith("QTRM-"):
            raise DemoApiError("QTR Micro orderLinkId обязан начинаться с QTRM-.")
        payload = self._request(
            "POST",
            "/v5/order/create",
            {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": str(qty),
                "orderLinkId": order_link_id,
                "reduceOnly": reduce_only,
            },
        )
        result = _result(payload)
        return OrderAcknowledgement(
            order_id=str(result.get("orderId", "")),
            order_link_id=str(result.get("orderLinkId", order_link_id)),
            accepted=True,
        )

    def cancel_order(self, symbol: str, order_id: str) -> None:
        self._request(
            "POST",
            "/v5/order/cancel",
            {"category": "linear", "symbol": symbol, "orderId": order_id},
        )

    def execution_fill(self, order_id: str, symbol: str) -> ExecutionFill | None:
        payload = self._request(
            "GET",
            "/v5/execution/list",
            {"category": "linear", "symbol": symbol, "orderId": order_id},
        )
        rows = _result_list(payload)
        if not rows:
            return None
        qty = sum(_float(row.get("execQty"), 0.0) for row in rows)
        if qty <= 0:
            return None
        notional = sum(
            _float(row.get("execQty"), 0.0) * _float(row.get("execPrice"), 0.0)
            for row in rows
        )
        fee = sum(_float(row.get("execFee"), 0.0) for row in rows)
        timestamp = max(int(str(row.get("execTime", "0"))) for row in rows)
        return ExecutionFill(
            order_id=order_id,
            average_price=notional / qty,
            filled_qty=qty,
            fee=fee,
            filled_at=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
        )

    def set_protective_stop(
        self, *, symbol: str, stop_price: float, position_idx: int = 0
    ) -> None:
        self._request(
            "POST",
            "/v5/position/trading-stop",
            {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": str(stop_price),
                "tpslMode": "Full",
                "positionIdx": position_idx,
            },
        )

    def _request(
        self,
        method: str,
        path: str,
        params: Mapping[str, object],
        *,
        authenticated: bool = True,
    ) -> Mapping[str, Any]:
        _guard_demo_url(self._transport.base_url)
        return self._transport.request(
            method, path, params, authenticated=authenticated
        )


def _guard_demo_url(base_url: str) -> None:
    parsed = urllib.parse.urlsplit(base_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != DEMO_API_HOST
        or parsed.netloc != DEMO_API_HOST
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise DemoApiError("TRADE BLOCKED: разрешён только api-demo.bybit.com.")


def _instrument_universe_status(
    row: Mapping[str, Any],
) -> InstrumentUniverseStatus:
    quote_coin = str(row.get("quoteCoin", ""))
    settle_coin = str(row.get("settleCoin", ""))
    contract_type = str(row.get("contractType", ""))
    symbol_type = str(row.get("symbolType", ""))
    status = str(row.get("status", ""))
    is_pre_listing = row.get("isPreListing")
    if not isinstance(is_pre_listing, bool):
        raise DemoApiError("Instrument rules содержат некорректный isPreListing.")
    if symbol_type.casefold() == "stock":
        return InstrumentUniverseStatus.STOCK
    if is_pre_listing or status.casefold() == "prelaunch":
        return InstrumentUniverseStatus.PRELAUNCH
    if quote_coin.upper() != "USDT" or settle_coin.upper() != "USDT":
        return InstrumentUniverseStatus.NON_USDT
    if not is_crypto_linear_usdt_metadata(
        quote_coin=quote_coin,
        settle_coin=settle_coin,
        contract_type=contract_type,
        symbol_type=symbol_type,
        is_pre_listing=is_pre_listing,
    ):
        return InstrumentUniverseStatus.UNSUPPORTED_CONTRACT
    if status != "Trading":
        return InstrumentUniverseStatus.UNSUPPORTED_STATUS
    return InstrumentUniverseStatus.ELIGIBLE


def _result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("result")
    if not isinstance(value, dict):
        raise DemoApiError("Bybit Demo result отсутствует.")
    return cast(Mapping[str, Any], value)


def _result_list(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    value = _result(payload).get("list")
    if not isinstance(value, list):
        raise DemoApiError("Bybit Demo list отсутствует.")
    rows: list[Mapping[str, Any]] = []
    for row in value:
        if not isinstance(row, dict):
            raise DemoApiError("Bybit Demo row некорректен.")
        rows.append(cast(Mapping[str, Any], row))
    return tuple(rows)


def _first(rows: tuple[Mapping[str, Any], ...]) -> Mapping[str, Any]:
    if not rows:
        raise DemoApiError("Bybit Demo список пуст.")
    return rows[0]


def _float(value: object, default: float) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _positive_float(value: object, name: str) -> float:
    parsed = _float(value, -1.0)
    if parsed <= 0:
        raise DemoApiError(f"Некорректное поле {name}.")
    return parsed

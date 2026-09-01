from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from urllib.parse import urlsplit

from market_signal_assistant.qtr_micro.client import (
    DemoApiError,
    DemoTradingClient,
)
from market_signal_assistant.qtr_micro.models import (
    DemoOrder,
    DemoPosition,
    InstrumentRules,
    InstrumentUniverseStatus,
    LeverageUpdateResult,
    PreflightCheck,
    PreflightResult,
)
from market_signal_assistant.qtr_micro.reconciliation import reconcile_demo_state
from market_signal_assistant.qtr_micro.settings import (
    DEMO_API_HOST,
    DEMO_BASE_URL,
    QtrMicroSettings,
)
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore


class QtrMicroPreflight:
    def __init__(
        self,
        settings: QtrMicroSettings,
        client: DemoTradingClient | None,
        *,
        state_store: JsonQtrMicroStateStore | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._state_store = state_store
        self._clock = clock or (lambda: datetime.now(UTC))

    def run(self, symbol: str | None = "BTCUSDT") -> PreflightResult:
        normalized_symbol = symbol.strip().upper() if symbol is not None else ""
        checks: list[PreflightCheck] = []
        common = _ResultContext(
            mode=self._settings.mode,
            host=_safe_host(self._settings.base_url),
            symbol=normalized_symbol or "DYNAMIC",
            base_leverage=max(5, self._settings.base_leverage),
            max_allowed_leverage=self._settings.max_leverage,
        )
        if not self._settings.enabled:
            return _blocked(common, checks, "QTR Micro выключен.")
        if self._settings.mode != "demo":
            return _blocked(common, checks, "Поддерживается только режим demo.")
        if self._settings.base_url != DEMO_BASE_URL:
            checks.append(_check("demo_host", "Demo host", False, common.host))
            return _blocked(common, checks, "Demo API domain не подтверждён.")
        checks.append(_check("demo_host", "Demo host", True, DEMO_API_HOST))
        if not self._settings.credentials_present or self._client is None:
            checks.append(
                _check(
                    "authentication",
                    "Аутентификация",
                    False,
                    "Demo API credentials отсутствуют",
                )
            )
            return _blocked(common, checks, "Demo API credentials отсутствуют.")
        if self._client.base_url != DEMO_BASE_URL:
            checks.append(
                _check(
                    "demo_host",
                    "Demo API client host",
                    False,
                    _safe_host(self._client.base_url),
                )
            )
            return _blocked(
                common, checks, "Demo API client использует запрещённый host."
            )
        stage = "connectivity"
        try:
            self._client.connectivity()
            checks.append(
                _check(
                    "connectivity",
                    "Соединение с Bybit Demo",
                    True,
                    "public REST доступен",
                )
            )
            stage = "authentication"
            equity = self._client.wallet_equity()
            checks.extend(
                (
                    _check(
                        "authentication",
                        "Аутентификация",
                        True,
                        "private REST доступен",
                    ),
                    _check(
                        "wallet",
                        "Demo wallet/equity",
                        True,
                        f"{_number(equity)} USDT",
                    ),
                )
            )
            stage = "account_type"
            account_type = self._client.account_type()
            checks.append(
                _check(
                    "account_type",
                    "Тип аккаунта",
                    account_type == "UNIFIED",
                    account_type,
                )
            )
            if account_type != "UNIFIED":
                return _blocked(
                    common.with_private(equity, account_type, None),
                    checks,
                    "Account type UNIFIED не подтверждён.",
                )
            position_mode = "SYMBOL_LEVEL"
            if normalized_symbol:
                stage = "position_mode"
                position_mode = self._client.position_mode(normalized_symbol)
                checks.append(
                    _check(
                        "position_mode",
                        "Режим позиции",
                        position_mode == "ONE_WAY",
                        _position_mode_ru(position_mode),
                    )
                )
                if position_mode != "ONE_WAY":
                    return _blocked(
                        common.with_private(equity, account_type, position_mode),
                        checks,
                        "V1 требует односторонний position mode.",
                    )
            else:
                checks.append(
                    _check(
                        "position_mode",
                        "Режим позиции",
                        True,
                        "проверяется отдельно для symbol перед entry",
                        blocking=False,
                        informational=True,
                    )
                )
            stage = "positions_orders"
            positions = self._client.list_positions()
            orders = self._client.list_active_orders()
            rules: InstrumentRules | None = None
            if normalized_symbol:
                stage = "instrument"
                rules = self._client.instrument_rules(normalized_symbol)
                if rules.universe_status is not InstrumentUniverseStatus.ELIGIBLE:
                    checks.append(
                        _check(
                            "instrument_universe",
                            f"Инструмент {normalized_symbol}",
                            False,
                            "не входит в торговую вселенную QTR Micro",
                        )
                    )
                    return _blocked(
                        common.with_market(
                            equity,
                            account_type,
                            position_mode,
                            rules,
                            positions,
                            orders,
                        ),
                        checks,
                        "Инструмент не входит в торговую вселенную QTR Micro.",
                    )
                checks.extend(
                    (
                        _check(
                            "instrument",
                            f"Инструмент {normalized_symbol}",
                            True,
                            "доступен",
                        ),
                        _check(
                            "instrument_rules",
                            "Правила инструмента",
                            True,
                            "загружены",
                        ),
                    )
                )
                leverage = min(common.base_leverage, rules.max_leverage)
                leverage_ok = leverage >= common.base_leverage
                checks.append(
                    _check(
                        "leverage",
                        "Рабочее плечо",
                        leverage_ok,
                        f"x{common.base_leverage}",
                    )
                )
                if not leverage_ok:
                    return _blocked(
                        common.with_market(
                            equity,
                            account_type,
                            position_mode,
                            rules,
                            positions,
                            orders,
                        ),
                        checks,
                        "Рабочее плечо x5 недоступно.",
                    )
                stage = "leverage"
                leverage_result = self._client.set_leverage(normalized_symbol, leverage)
                if leverage_result is LeverageUpdateResult.ALREADY_SET:
                    checks.append(
                        _check(
                            "leverage_already_set",
                            "Плечо",
                            True,
                            f"x{leverage} уже было установлено, изменение не требуется",
                            blocking=False,
                            informational=True,
                        )
                    )
        except DemoApiError as error:
            label = {
                "connectivity": "Соединение с Bybit Demo",
                "authentication": "Аутентификация",
                "account_type": "Тип аккаунта",
                "position_mode": "Режим позиции",
                "positions_orders": "Позиции и активные ордера",
                "instrument": f"Инструмент {normalized_symbol}",
                "leverage": "Рабочее плечо",
            }[stage]
            checks.append(_check(stage, label, False, str(error)))
            return _blocked(common, checks, str(error))
        except Exception as error:
            reason = f"Demo preflight завершился ошибкой {type(error).__name__}."
            checks.append(_check("technical", "Техническая проверка", False, reason))
            return _blocked(common, checks, reason)

        context = common.with_market(
            equity, account_type, position_mode, rules, positions, orders
        )
        reconciliation_ok, reconciliation_reason, local_symbols = self._reconciliation(
            context, positions, orders
        )
        qtr_positions, foreign_positions = _position_counts(
            context, local_symbols=local_symbols
        )
        qtr_orders, foreign_orders = _order_counts(orders)
        warnings = _warnings(
            qtr_positions,
            qtr_orders,
            foreign_positions,
            foreign_orders,
        )
        checks.extend(
            (
                _check(
                    "qtr_positions",
                    "Открытые QTR Micro позиции",
                    True,
                    str(qtr_positions),
                    blocking=False,
                ),
                _check(
                    "qtr_orders",
                    "Активные QTR Micro ордера",
                    True,
                    str(qtr_orders),
                    blocking=False,
                ),
                _check(
                    "foreign_positions",
                    "Foreign/manual позиции",
                    foreign_positions == 0,
                    str(foreign_positions),
                    blocking=False,
                ),
                _check(
                    "foreign_orders",
                    "Foreign/manual ордера",
                    foreign_orders == 0,
                    str(foreign_orders),
                    blocking=False,
                ),
            )
        )
        checks.append(
            _check(
                "reconciliation",
                "Reconciliation",
                reconciliation_ok,
                "OK" if reconciliation_ok else reconciliation_reason,
            )
        )
        if not reconciliation_ok:
            return _blocked(
                context,
                checks,
                reconciliation_reason,
                qtr_positions=qtr_positions,
                qtr_orders=qtr_orders,
                foreign_positions=foreign_positions,
                foreign_orders=foreign_orders,
                warnings=warnings,
            )
        return _result(
            context,
            ready=True,
            reason=None,
            checks=checks,
            qtr_positions=qtr_positions,
            qtr_orders=qtr_orders,
            foreign_positions=foreign_positions,
            foreign_orders=foreign_orders,
            warnings=warnings,
            reconciliation_ok=True,
        )

    def _reconciliation(
        self,
        context: _ResultContext,
        positions: tuple[DemoPosition, ...],
        orders: tuple[DemoOrder, ...],
    ) -> tuple[bool, str, frozenset[str]]:
        if self._state_store is None:
            owned = frozenset(
                item.symbol for item in orders if item.order_link_id.startswith("QTRM-")
            )
            return (
                True,
                "State store не передан; reconciliation только обзорный.",
                owned,
            )
        now = self._clock()
        state = self._state_store.load(
            today=now.date(), trading_enabled=self._settings.enabled
        )
        if state.blocked_reason is not None:
            return False, state.blocked_reason, frozenset()
        owned = frozenset(
            item.symbol
            for item in state.positions.values()
            if item.entry_order_link_id.startswith("QTRM-")
        )
        reconciled = reconcile_demo_state(
            state,
            positions=positions,
            orders=orders,
            now=now,
        )
        if reconciled.blocked_reason is not None:
            return False, reconciled.blocked_reason, owned
        del context
        return True, "OK", owned


class _ResultContext:
    def __init__(
        self,
        *,
        mode: str,
        host: str,
        symbol: str,
        base_leverage: int,
        max_allowed_leverage: int,
        equity: float | None = None,
        account_type: str | None = None,
        position_mode: str | None = None,
        rules: InstrumentRules | None = None,
        positions: tuple[DemoPosition, ...] = (),
        orders: tuple[DemoOrder, ...] = (),
    ) -> None:
        self.mode = mode
        self.host = host
        self.symbol = symbol
        self.base_leverage = base_leverage
        self.max_allowed_leverage = max_allowed_leverage
        self.equity = equity
        self.account_type = account_type
        self.position_mode = position_mode
        self.rules = rules
        self.positions = positions
        self.orders = orders

    def with_private(
        self,
        equity: float,
        account_type: str,
        position_mode: str | None,
    ) -> _ResultContext:
        return _ResultContext(
            mode=self.mode,
            host=self.host,
            symbol=self.symbol,
            base_leverage=self.base_leverage,
            max_allowed_leverage=self.max_allowed_leverage,
            equity=equity,
            account_type=account_type,
            position_mode=position_mode,
        )

    def with_market(
        self,
        equity: float,
        account_type: str,
        position_mode: str,
        rules: InstrumentRules | None,
        positions: tuple[DemoPosition, ...],
        orders: tuple[DemoOrder, ...],
    ) -> _ResultContext:
        context = self.with_private(equity, account_type, position_mode)
        context.rules = rules
        context.positions = positions
        context.orders = orders
        return context


def configuration_failure(
    reason: str,
    *,
    base_url: str,
    symbol: str,
) -> PreflightResult:
    context = _ResultContext(
        mode="demo",
        host=_safe_host(base_url),
        symbol=symbol.strip().upper(),
        base_leverage=5,
        max_allowed_leverage=10,
    )
    host_is_demo = base_url.rstrip("/") == DEMO_BASE_URL
    checks = [
        _check("demo_host", "Demo host", host_is_demo, context.host),
        _check("configuration", "Конфигурация Demo", False, reason),
    ]
    return _blocked(context, checks, reason)


def format_preflight(result: PreflightResult) -> str:
    lines = ["QTR MICRO — ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА BYBIT DEMO", ""]
    for check in result.checks:
        if check.informational:
            marker = "ℹ️"
        else:
            marker = "✅" if check.passed else "❌" if check.blocking else "⚠️"
        lines.append(f"{marker} {check.label}: {check.detail}")
    if result.warnings:
        lines.extend(("", "ПРЕДУПРЕЖДЕНИЯ:"))
        lines.extend(f"⚠️ {item}" for item in result.warnings)
    rules = result.instrument_rules
    if result.equity is not None:
        lines.extend(("", f"Demo equity: {_number(result.equity)} USDT"))
    if rules is not None:
        lines.extend(
            (
                f"Минимальный объём {result.symbol}: {_number(rules.min_order_qty)}",
                f"Шаг количества: {_number(rules.qty_step)}",
                "Максимальное рыночное количество: "
                f"{_number(rules.max_market_order_qty)}",
                f"Минимальный notional: {_number(rules.min_notional_value)} USDT",
                f"Максимальное плечо инструмента: x{rules.max_leverage}",
            )
        )
    lines.extend(("", "ИТОГ:"))
    if result.ready:
        lines.append("🟢 QTR Micro Demo: ГОТОВ К ДЕМО-ТОРГОВЛЕ")
    else:
        lines.extend(
            (
                "🔴 QTR Micro Demo: ЗАБЛОКИРОВАН",
                "",
                "Причина:",
                result.reason or "Неизвестная блокирующая ошибка.",
            )
        )
    return "\n".join(lines)


def preflight_json(result: PreflightResult) -> str:
    rules = result.instrument_rules
    payload = {
        "ready": result.ready,
        "mode": result.mode,
        "host": result.host,
        "account_type": result.account_type,
        "position_mode": result.position_mode,
        "symbol": result.symbol,
        "equity": result.equity,
        "base_leverage": result.base_leverage,
        "max_allowed_leverage": result.max_allowed_leverage,
        "instrument_max_leverage": rules.max_leverage if rules else None,
        "instrument_rules": asdict(rules) if rules else None,
        "qtr_positions": result.qtr_positions,
        "qtr_orders": result.qtr_orders,
        "foreign_positions": result.foreign_positions,
        "foreign_orders": result.foreign_orders,
        "reconciliation_ok": result.reconciliation_ok,
        "blocking_reasons": list(result.blocking_reasons),
        "warnings": list(result.warnings),
        "checks": [asdict(check) for check in result.checks],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _result(
    context: _ResultContext,
    *,
    ready: bool,
    reason: str | None,
    checks: list[PreflightCheck],
    qtr_positions: int = 0,
    qtr_orders: int = 0,
    foreign_positions: int = 0,
    foreign_orders: int = 0,
    warnings: tuple[str, ...] = (),
    reconciliation_ok: bool = False,
) -> PreflightResult:
    blocking = tuple(
        check.detail for check in checks if not check.passed and check.blocking
    )
    return PreflightResult(
        ready=ready and not blocking,
        reason=reason,
        equity=context.equity,
        account_type=context.account_type,
        position_mode=context.position_mode,
        open_positions=len(context.positions),
        active_orders=len(context.orders),
        mode=context.mode,
        host=context.host,
        symbol=context.symbol,
        base_leverage=context.base_leverage,
        max_allowed_leverage=context.max_allowed_leverage,
        instrument_rules=context.rules,
        qtr_positions=qtr_positions,
        qtr_orders=qtr_orders,
        foreign_positions=foreign_positions,
        foreign_orders=foreign_orders,
        reconciliation_ok=reconciliation_ok,
        blocking_reasons=blocking,
        warnings=warnings,
        checks=tuple(checks),
    )


def _blocked(
    context: _ResultContext,
    checks: list[PreflightCheck],
    reason: str,
    *,
    qtr_positions: int = 0,
    qtr_orders: int = 0,
    foreign_positions: int = 0,
    foreign_orders: int = 0,
    warnings: tuple[str, ...] = (),
) -> PreflightResult:
    if not any(not check.passed and check.blocking for check in checks):
        checks.append(_check("blocking", "Блокирующая проверка", False, reason))
    return _result(
        context,
        ready=False,
        reason=reason,
        checks=checks,
        qtr_positions=qtr_positions,
        qtr_orders=qtr_orders,
        foreign_positions=foreign_positions,
        foreign_orders=foreign_orders,
        warnings=warnings,
    )


def _position_counts(
    context: _ResultContext,
    *,
    local_symbols: frozenset[str],
) -> tuple[int, int]:
    active_order_symbols = {
        order.symbol
        for order in context.orders
        if order.order_link_id.startswith("QTRM-")
    }
    owned_symbols = local_symbols.union(active_order_symbols)
    qtr = sum(item.symbol in owned_symbols for item in context.positions)
    return qtr, len(context.positions) - qtr


def _order_counts(orders: tuple[DemoOrder, ...]) -> tuple[int, int]:
    qtr = sum(item.order_link_id.startswith("QTRM-") for item in orders)
    return qtr, len(orders) - qtr


def _warnings(
    qtr_positions: int,
    qtr_orders: int,
    foreign_positions: int,
    foreign_orders: int,
) -> tuple[str, ...]:
    values: list[str] = []
    if qtr_positions:
        values.append(f"Уже открыто QTR Micro позиций: {qtr_positions}.")
    if qtr_orders:
        values.append(f"Уже активно QTR Micro ордеров: {qtr_orders}.")
    if foreign_positions:
        values.append(
            "Обнаружены foreign/manual позиции: "
            f"{foreign_positions}; управление запрещено."
        )
    if foreign_orders:
        values.append(
            f"Обнаружены foreign/manual ордера: {foreign_orders}; управление запрещено."
        )
    return tuple(values)


def _check(
    check_id: str,
    label: str,
    passed: bool,
    detail: str,
    *,
    blocking: bool = True,
    informational: bool = False,
) -> PreflightCheck:
    return PreflightCheck(
        check_id,
        label,
        passed,
        detail,
        blocking,
        informational,
    )


def _safe_host(url: str) -> str:
    parsed = urlsplit(url)
    return parsed.hostname or "не определён"


def _position_mode_ru(value: str) -> str:
    return "односторонний" if value == "ONE_WAY" else value


def _number(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".").replace(".", ",")

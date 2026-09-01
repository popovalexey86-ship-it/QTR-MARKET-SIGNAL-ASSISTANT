from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from market_signal_assistant.qtr_micro.models import (
    DemoOrder,
    DemoPosition,
    MicroDirection,
    MicroPosition,
    MicroStage,
    MicroState,
)
from market_signal_assistant.setup_engine.models import SetupType

ORDER_PREFIX = "QTRM-"


def reconcile_demo_state(
    state: MicroState,
    *,
    positions: tuple[DemoPosition, ...],
    orders: tuple[DemoOrder, ...],
    now: datetime,
) -> MicroState:
    """Reconcile owned records; never claim or alter foreign/manual orders."""
    records = dict(state.positions)
    actual = {item.symbol: item for item in positions}
    owned_orders = tuple(
        item for item in orders if item.order_link_id.startswith(ORDER_PREFIX)
    )
    owned_order_links = {item.order_link_id for item in owned_orders}
    for trade_id, record in tuple(records.items()):
        if record.stage is MicroStage.CLOSED:
            continue
        remote = actual.get(record.symbol)
        if record.stage is MicroStage.BLOCKED:
            if remote is None and record.average_fill is not None:
                records[trade_id] = replace(
                    record,
                    current_qty=0.0,
                    stage=MicroStage.CLOSED,
                    last_updated=now,
                )
            continue
        if remote is None:
            if (
                record.stage in {MicroStage.PREPARED, MicroStage.ENTRY_ACKNOWLEDGED}
                and record.entry_order_link_id in owned_order_links
            ):
                continue
            if (
                record.stage is MicroStage.EXIT_ACKNOWLEDGED
                and record.pending_exit_order_link_id is not None
                and record.pending_exit_order_link_id in owned_order_links
            ):
                continue
            records[trade_id] = replace(
                record,
                current_qty=0.0,
                stage=MicroStage.CLOSED,
                last_updated=now,
            )
            continue
        records[trade_id] = replace(
            record,
            average_fill=remote.average_price,
            filled_qty=remote.size,
            current_qty=remote.size,
            last_updated=now,
        )
    known_symbols = {item.symbol for item in records.values()}
    for order in owned_orders:
        remote = actual.get(order.symbol)
        if remote is None or order.symbol in known_symbols:
            continue
        direction = (
            MicroDirection.LONG if remote.side == "Buy" else MicroDirection.SHORT
        )
        records[order.order_link_id] = MicroPosition(
            trade_id=order.order_link_id,
            setup_episode_id="recovered",
            symbol=order.symbol,
            direction=direction,
            setup_type=SetupType.NO_TRADE,
            setup_confidence=0.0,
            entry_order_link_id=order.order_link_id,
            entry_order_id=order.order_id,
            average_fill=remote.average_price,
            filled_qty=remote.size,
            initial_qty=remote.size,
            current_qty=remote.size,
            leverage=0,
            risk_pct=0.0,
            risk_amount=0.0,
            structural_stop=0.0,
            current_stop=0.0,
            initial_r=0.0,
            tp1_price=0.0,
            tp1_qty=0.0,
            tp2_price=0.0,
            tp2_qty=0.0,
            runner_target_price=0.0,
            runner_qty=remote.size,
            realised_partial_pnl=0.0,
            fees=0.0,
            opened_at=None,
            last_updated=now,
            stage=MicroStage.BLOCKED,
            signal_at=now,
            signal_price=remote.average_price,
        )
    return replace(
        state,
        updated_at=now,
        positions=records,
        trading_enabled=(
            state.trading_enabled
            and not any(item.stage is MicroStage.BLOCKED for item in records.values())
        ),
        blocked_reason=(
            "Обнаружена восстановленная QTR-позиция без полного local state."
            if any(item.stage is MicroStage.BLOCKED for item in records.values())
            else state.blocked_reason
        ),
    )

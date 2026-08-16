from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import (
    OrderBookProcessStatus,
    OrderBookState,
    simulate_orderbook,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def level(price: float, quantity: float) -> OrderBookLevel:
    return OrderBookLevel(price=price, quantity=quantity)


def snapshot(
    *,
    update_id: int = 100,
    cross_sequence: int | None = 1_000,
    bids: tuple[OrderBookLevel, ...] | None = None,
    asks: tuple[OrderBookLevel, ...] | None = None,
    symbol: str = "BTCUSDT",
    exchange_at: datetime = NOW,
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol=symbol,
        event_type=OrderBookEventType.SNAPSHOT,
        exchange_at=exchange_at,
        received_at=exchange_at + timedelta(milliseconds=2),
        update_id=update_id,
        cross_sequence=cross_sequence,
        bids=bids if bids is not None else (level(99.0, 2.0), level(98.0, 3.0)),
        asks=asks if asks is not None else (level(101.0, 1.0), level(102.0, 4.0)),
    )


def delta(
    update_id: int,
    *,
    bids: tuple[OrderBookLevel, ...] = (),
    asks: tuple[OrderBookLevel, ...] = (),
    cross_sequence: int | None = None,
    symbol: str = "BTCUSDT",
    exchange_at: datetime = NOW + timedelta(milliseconds=10),
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol=symbol,
        event_type=OrderBookEventType.DELTA,
        exchange_at=exchange_at,
        received_at=exchange_at + timedelta(milliseconds=2),
        update_id=update_id,
        cross_sequence=cross_sequence,
        bids=bids,
        asks=asks,
    )


def test_snapshot_builds_sorted_ready_book() -> None:
    state = OrderBookState(" btcusdt ")
    result = state.process(
        snapshot(
            bids=(level(97.0, 1.0), level(99.0, 2.0), level(98.0, 3.0)),
            asks=(level(103.0, 1.0), level(101.0, 2.0), level(102.0, 3.0)),
        )
    )

    bids, asks = state.levels()
    assert result.status is OrderBookProcessStatus.APPLIED_SNAPSHOT
    assert result.ready is True
    assert [item.price for item in bids] == [99.0, 98.0, 97.0]
    assert [item.price for item in asks] == [101.0, 102.0, 103.0]
    assert state.symbol == "BTCUSDT"


def test_delta_adds_updates_and_deletes_levels_atomically() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot())
    result = state.process(
        delta(
            101,
            bids=(level(99.0, 5.0), level(98.0, 0.0), level(98.5, 2.0)),
            asks=(level(101.0, 0.0), level(100.5, 1.0)),
            cross_sequence=1_001,
        )
    )

    bids, asks = state.levels()
    assert result.status is OrderBookProcessStatus.APPLIED_DELTA
    assert [(item.price, item.quantity) for item in bids] == [
        (99.0, 5.0),
        (98.5, 2.0),
    ]
    assert [(item.price, item.quantity) for item in asks] == [
        (100.5, 1.0),
        (102.0, 4.0),
    ]


def test_delta_before_snapshot_is_rejected_without_mutation() -> None:
    state = OrderBookState("BTCUSDT")
    result = state.process(delta(1, bids=(level(99.0, 1.0),)))

    assert result.status is OrderBookProcessStatus.SNAPSHOT_REQUIRED
    assert state.levels() == ((), ())
    assert state.ready is False


def test_stale_delta_is_ignored() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot())
    before = state.levels()
    result = state.process(delta(100, bids=(level(99.0, 9.0),)))

    assert result.status is OrderBookProcessStatus.IGNORED_STALE
    assert state.levels() == before
    assert state.ready is True


def test_update_gap_requires_fresh_snapshot() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot())
    result = state.process(delta(102, bids=(level(99.0, 9.0),)))

    assert result.status is OrderBookProcessStatus.DESYNCHRONIZED
    assert result.reason == "update_gap"
    assert state.ready is False
    follow_up = state.process(delta(101))
    assert follow_up.status is OrderBookProcessStatus.SNAPSHOT_REQUIRED


def test_cross_sequence_rollback_desynchronizes_book() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot(cross_sequence=1_000))
    result = state.process(delta(101, cross_sequence=999))

    assert result.status is OrderBookProcessStatus.DESYNCHRONIZED
    assert result.reason == "sequence_rollback"


def test_new_snapshot_recovers_desynchronized_book_even_with_lower_id() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot(update_id=100))
    state.process(delta(102))

    recovery = state.process(snapshot(update_id=1, cross_sequence=2_000))
    assert recovery.status is OrderBookProcessStatus.APPLIED_SNAPSHOT
    assert state.ready is True
    assert state.metrics(as_of=NOW).update_id == 1


def test_crossed_delta_is_not_committed_and_requires_snapshot() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot())
    before = state.levels()
    result = state.process(delta(101, bids=(level(102.0, 1.0),)))

    assert result.status is OrderBookProcessStatus.DESYNCHRONIZED
    assert result.reason == "crossed_book"
    assert state.levels() == before
    assert state.metrics(as_of=NOW + timedelta(seconds=1)).health_reasons == (
        "desynchronized",
    )


def test_mid_spread_microprice_and_age_are_calculated() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(
        snapshot(
            bids=(level(99.0, 3.0),),
            asks=(level(101.0, 1.0),),
        )
    )
    metrics = state.metrics(as_of=NOW + timedelta(milliseconds=250))

    assert metrics.best_bid == 99.0
    assert metrics.best_ask == 101.0
    assert metrics.mid_price == 100.0
    assert metrics.spread_bps == 200.0
    assert metrics.microprice == 100.5
    assert metrics.book_age_ms == 250.0


def test_top_level_imbalances_use_base_quantity() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(
        snapshot(
            bids=tuple(level(100.0 - index, float(index + 1)) for index in range(6)),
            asks=tuple(level(101.0 + index, 1.0) for index in range(6)),
        )
    )
    metrics = state.metrics(as_of=NOW)

    assert metrics.imbalance_l1 == 0.0
    assert metrics.imbalance_l5 == pytest.approx(0.5)
    assert metrics.imbalance_l10 == pytest.approx(15 / 27)


def test_liquidity_bands_are_quote_notional_not_base_quantity() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(
        snapshot(
            bids=(level(99.99, 2.0), level(99.90, 3.0), level(99.70, 4.0)),
            asks=(level(100.01, 1.0), level(100.10, 2.0), level(100.30, 3.0)),
        )
    )
    metrics = state.metrics(as_of=NOW)

    assert metrics.mid_price == 100.0
    assert metrics.bid_depth_5bps == pytest.approx(99.99 * 2)
    assert metrics.ask_depth_5bps == pytest.approx(100.01)
    assert metrics.bid_depth_10bps == pytest.approx(99.99 * 2 + 99.90 * 3)
    assert metrics.ask_depth_10bps == pytest.approx(100.01 + 100.10 * 2)
    assert metrics.bid_depth_25bps == pytest.approx(99.99 * 2 + 99.90 * 3)
    assert metrics.ask_depth_25bps == pytest.approx(100.01 + 100.10 * 2)


def test_empty_snapshot_is_not_ready_and_has_null_liquidity_metrics() -> None:
    state = OrderBookState("BTCUSDT")
    result = state.process(snapshot(bids=(), asks=()))
    metrics = state.metrics(as_of=NOW)

    assert result.status is OrderBookProcessStatus.DESYNCHRONIZED
    assert metrics.ready is False
    assert metrics.health_reasons == (
        "desynchronized",
        "empty_bid_book",
        "empty_ask_book",
    )
    assert metrics.mid_price is None
    assert metrics.imbalance_l1 is None
    assert metrics.bid_depth_5bps is None


def test_depth_limit_keeps_only_nearest_levels() -> None:
    state = OrderBookState("BTCUSDT", depth=3)
    state.process(
        snapshot(
            bids=tuple(level(float(price), 1.0) for price in range(90, 100)),
            asks=tuple(level(float(price), 1.0) for price in range(101, 111)),
        )
    )
    bids, asks = state.levels()
    assert [item.price for item in bids] == [99.0, 98.0, 97.0]
    assert [item.price for item in asks] == [101.0, 102.0, 103.0]


def test_offline_simulation_replays_arrival_order_without_network() -> None:
    simulation = simulate_orderbook(
        (
            snapshot(),
            delta(
                101,
                bids=(level(99.0, 4.0),),
                cross_sequence=1_001,
            ),
        ),
        symbol="BTCUSDT",
        as_of=NOW + timedelta(seconds=1),
    )

    assert [result.status for result in simulation.results] == [
        OrderBookProcessStatus.APPLIED_SNAPSHOT,
        OrderBookProcessStatus.APPLIED_DELTA,
    ]
    assert simulation.metrics.ready is True
    assert simulation.metrics.imbalance_l1 == pytest.approx(0.6)


def test_metrics_are_immutable_and_reject_time_before_latest_event() -> None:
    state = OrderBookState("BTCUSDT")
    state.process(snapshot())
    metrics = state.metrics(as_of=NOW)
    with pytest.raises(FrozenInstanceError):
        metrics.ready = False  # type: ignore[misc]
    with pytest.raises(ValueError, match="precedes latest event"):
        state.metrics(as_of=NOW - timedelta(milliseconds=1))


def test_symbol_mismatch_and_non_event_are_controlled_errors() -> None:
    state = OrderBookState("BTCUSDT")
    with pytest.raises(ValueError, match="does not match"):
        state.process(snapshot(symbol="ETHUSDT"))
    with pytest.raises(TypeError, match="OrderBookEvent"):
        state.process(object())  # type: ignore[arg-type]

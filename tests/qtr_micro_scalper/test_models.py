from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    LiquidationSide,
    MicrostructureSnapshot,
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
    PublicTradeEvent,
    TradeSide,
)

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)


def trade(**overrides: object) -> PublicTradeEvent:
    values: dict[str, object] = {
        "symbol": " btcusdt ",
        "trade_id": "trade-1",
        "exchange_at": NOW,
        "received_at": NOW + timedelta(milliseconds=5),
        "side": TradeSide.BUY,
        "price": 100.0,
        "quantity": 2.0,
        "quote_notional": 200.0,
        "sequence": 10,
    }
    values.update(overrides)
    return PublicTradeEvent(**values)  # type: ignore[arg-type]


def ready_snapshot(**overrides: object) -> MicrostructureSnapshot:
    values: dict[str, object] = {
        "symbol": "btcusdt",
        "generated_at": NOW,
        "window_started_at": NOW - timedelta(seconds=60),
        "market_price": 100.0,
        "best_bid": 99.5,
        "best_ask": 100.5,
        "mid_price": 100.0,
        "spread_bps": 100.0,
        "delta_1s": 1.0,
        "delta_5s": 2.0,
        "delta_15s": -3.0,
        "delta_60s": 4.0,
        "book_exchange_at": NOW - timedelta(milliseconds=5),
        "trade_exchange_at": NOW - timedelta(milliseconds=2),
        "ready": True,
        "health_reasons": (),
    }
    values.update(overrides)
    return MicrostructureSnapshot(**values)  # type: ignore[arg-type]


def test_public_trade_is_normalized_immutable_and_explicit() -> None:
    event = trade()

    assert event.symbol == "BTCUSDT"
    assert event.exchange_at.tzinfo is UTC
    assert event.quote_notional == event.price * event.quantity
    with pytest.raises(FrozenInstanceError):
        event.price = 101.0  # type: ignore[misc]


def test_timestamps_are_converted_to_utc_and_naive_values_are_rejected() -> None:
    plus_two = timezone(timedelta(hours=2))
    event = trade(exchange_at=datetime(2026, 8, 15, 14, tzinfo=plus_two))
    assert event.exchange_at == NOW
    assert event.exchange_at.tzinfo is UTC

    with pytest.raises(ValueError, match="timezone-aware"):
        trade(exchange_at=datetime(2026, 8, 15, 12))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", " "),
        ("trade_id", ""),
        ("price", 0.0),
        ("quantity", -1.0),
        ("quote_notional", float("nan")),
        ("sequence", -1),
        ("schema_version", 0),
    ],
)
def test_public_trade_rejects_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        trade(**{field: value})


def test_public_trade_requires_matching_quote_notional() -> None:
    with pytest.raises(ValueError, match="price multiplied by quantity"):
        trade(quote_notional=199.0)


def test_event_sides_are_typed() -> None:
    with pytest.raises(ValueError, match="Trade side"):
        trade(side="BUY")


def test_order_book_snapshot_and_delta_have_distinct_zero_semantics() -> None:
    zero_level = OrderBookLevel(price=100.0, quantity=0.0)
    with pytest.raises(ValueError, match="snapshot quantities"):
        OrderBookEvent(
            symbol="BTCUSDT",
            event_type=OrderBookEventType.SNAPSHOT,
            exchange_at=NOW,
            received_at=NOW,
            update_id=1,
            bids=(zero_level,),
        )

    delta = OrderBookEvent(
        symbol="btcusdt",
        event_type=OrderBookEventType.DELTA,
        exchange_at=NOW,
        received_at=NOW,
        update_id=2,
        bids=[zero_level],  # type: ignore[arg-type]
    )
    assert delta.symbol == "BTCUSDT"
    assert delta.bids == (zero_level,)


def test_order_book_rejects_duplicate_prices() -> None:
    level = OrderBookLevel(price=100.0, quantity=1.0)
    with pytest.raises(ValueError, match="duplicate prices"):
        OrderBookEvent(
            symbol="BTCUSDT",
            event_type=OrderBookEventType.DELTA,
            exchange_at=NOW,
            received_at=NOW,
            update_id=2,
            bids=(level, level),
        )


def test_liquidation_preserves_position_side_and_validates_notional() -> None:
    event = LiquidationEvent(
        symbol="ethusdt",
        liquidation_id="liq-1",
        exchange_at=NOW,
        received_at=NOW,
        side=LiquidationSide.SHORT,
        bankruptcy_price=2_000.0,
        quantity=1.5,
        quote_notional=3_000.0,
    )
    assert event.symbol == "ETHUSDT"
    assert event.side is LiquidationSide.SHORT

    with pytest.raises(ValueError, match="price multiplied by quantity"):
        LiquidationEvent(
            symbol="ETHUSDT",
            exchange_at=NOW,
            received_at=NOW,
            side=LiquidationSide.LONG,
            bankruptcy_price=2_000.0,
            quantity=1.5,
            quote_notional=2_999.0,
        )


def test_snapshot_preserves_none_instead_of_inventing_zero() -> None:
    snapshot = MicrostructureSnapshot(
        symbol="solusdt",
        generated_at=NOW,
        window_started_at=NOW - timedelta(seconds=60),
    )
    assert snapshot.symbol == "SOLUSDT"
    assert snapshot.market_price is None
    assert snapshot.long_liquidations_60s is None
    assert snapshot.ready is False


def test_ready_snapshot_is_normalized_and_immutable() -> None:
    snapshot = ready_snapshot()
    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.ready is True
    with pytest.raises(FrozenInstanceError):
        snapshot.ready = False  # type: ignore[misc]


def test_ready_snapshot_requires_mandatory_data_and_clean_health() -> None:
    with pytest.raises(ValueError, match="mandatory market data"):
        ready_snapshot(delta_60s=None)
    with pytest.raises(ValueError, match="cannot contain health reasons"):
        ready_snapshot(health_reasons=("stale_book",))


def test_not_ready_snapshot_requires_an_explanation() -> None:
    with pytest.raises(ValueError, match="must explain"):
        MicrostructureSnapshot(
            symbol="BTCUSDT",
            generated_at=NOW,
            window_started_at=NOW,
            ready=False,
            health_reasons=(),
        )


def test_snapshot_rejects_invalid_ranges_and_crossed_ready_book() -> None:
    with pytest.raises(ValueError, match="between -1 and 1"):
        MicrostructureSnapshot(
            symbol="BTCUSDT",
            generated_at=NOW,
            window_started_at=NOW,
            imbalance_l1=1.1,
        )
    with pytest.raises(ValueError, match="must not be crossed"):
        ready_snapshot(best_bid=101.0, best_ask=100.0)
    with pytest.raises(ValueError, match="cannot start after"):
        MicrostructureSnapshot(
            symbol="BTCUSDT",
            generated_at=NOW,
            window_started_at=NOW + timedelta(seconds=1),
        )


def test_health_reasons_are_immutable_unique_values() -> None:
    snapshot = MicrostructureSnapshot(
        symbol="BTCUSDT",
        generated_at=NOW,
        window_started_at=NOW,
        health_reasons=["stale_book"],  # type: ignore[arg-type]
    )
    assert snapshot.health_reasons == ("stale_book",)
    with pytest.raises(ValueError, match="unique"):
        MicrostructureSnapshot(
            symbol="BTCUSDT",
            generated_at=NOW,
            window_started_at=NOW,
            health_reasons=("stale_book", "stale_book"),
        )

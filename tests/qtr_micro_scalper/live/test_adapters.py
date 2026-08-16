from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    LiquidationSide,
    OrderBookEvent,
    OrderBookEventType,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper.live.liquidations_ws import (
    LiquidationCollector,
    parse_liquidation_message,
)
from market_signal_assistant.qtr_micro_scalper.live.orderbook_ws import (
    OrderBookCollector,
    parse_orderbook_message,
)
from market_signal_assistant.qtr_micro_scalper.live.trades_ws import (
    PublicTradeCollector,
    parse_public_trade_message,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def test_public_trades_are_normalized_and_forwarded_to_accumulator() -> None:
    payload = {
        "data": [
            {
                "T": NOW_MS,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "2",
                "p": "100",
                "i": "trade-1",
                "seq": 7,
            }
        ]
    }
    events = parse_public_trade_message(payload, received_at=NOW)
    assert len(events) == 1
    assert events[0].side is TradeSide.BUY
    assert events[0].quote_notional == 200

    accumulator = TradeFlowAccumulator(clock=lambda: NOW)
    collector = PublicTradeCollector(("BTCUSDT",), accumulator)
    assert collector.handle_payload(payload) == 1
    assert accumulator.metrics("BTCUSDT", as_of=NOW).delta_1s == 200


def test_trade_and_orderbook_collectors_publish_normalized_events() -> None:
    published: list[object] = []
    trade_payload = {
        "data": [
            {
                "T": NOW_MS,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "1",
                "p": "100",
                "i": "published-trade",
            }
        ]
    }
    trade_collector = PublicTradeCollector(
        ("BTCUSDT",),
        TradeFlowAccumulator(clock=lambda: NOW),
        event_sink=published.append,
    )
    book_collector = OrderBookCollector(
        ("BTCUSDT",),
        event_sink=published.append,
    )
    assert trade_collector.handle_payload(trade_payload) == 1
    assert (
        book_collector.handle_payload(
            {
                "type": "snapshot",
                "cts": NOW_MS,
                "data": {
                    "s": "BTCUSDT",
                    "u": 1,
                    "b": [["99", "1"]],
                    "a": [["101", "1"]],
                },
            }
        )
        == 1
    )
    assert isinstance(published[0], PublicTradeEvent)
    assert isinstance(published[1], OrderBookEvent)


def test_trade_parser_isolates_malformed_rows() -> None:
    payload = {
        "data": [
            {"bad": "row"},
            {
                "T": NOW_MS,
                "s": "ETHUSDT",
                "S": "Sell",
                "v": "1",
                "p": "3000",
                "i": "ok",
            },
        ]
    }
    events = parse_public_trade_message(payload, received_at=NOW)
    assert [event.trade_id for event in events] == ["ok"]
    assert events[0].side is TradeSide.SELL


def test_orderbook_snapshot_and_delta_are_forwarded() -> None:
    snapshot = {
        "type": "snapshot",
        "cts": NOW_MS,
        "data": {
            "s": "BTCUSDT",
            "u": 10,
            "seq": 20,
            "b": [["99", "2"]],
            "a": [["101", "3"]],
        },
    }
    delta = {
        "type": "delta",
        "cts": NOW_MS + 1,
        "data": {"s": "BTCUSDT", "u": 12, "seq": 21, "b": [["99", "4"]], "a": []},
    }
    event = parse_orderbook_message(snapshot, received_at=NOW)
    assert event is not None and event.event_type is OrderBookEventType.SNAPSHOT
    collector = OrderBookCollector(("BTCUSDT",))
    assert collector.handle_payload(snapshot) == 1
    assert collector.handle_payload(delta) == 1
    bids, _ = collector.state("BTCUSDT").levels()
    assert bids[0].quantity == 4


def test_orderbook_update_id_one_resets_as_snapshot() -> None:
    payload = {
        "type": "delta",
        "ts": NOW_MS,
        "data": {"s": "BTCUSDT", "u": 1, "b": [["99", "1"]], "a": [["101", "1"]]},
    }
    event = parse_orderbook_message(payload, received_at=NOW)
    assert event is not None and event.event_type is OrderBookEventType.SNAPSHOT


def test_liquidations_are_normalized_and_sent_to_sink() -> None:
    payload = {
        "data": [
            {"T": NOW_MS, "s": "BTCUSDT", "S": "Buy", "v": "3", "p": "90"},
            {"T": NOW_MS, "s": "ETHUSDT", "S": "Sell", "v": "2", "p": "80"},
        ]
    }
    events = parse_liquidation_message(payload, received_at=NOW)
    assert [event.side for event in events] == [
        LiquidationSide.LONG,
        LiquidationSide.SHORT,
    ]
    assert events[0].quote_notional == 270
    received: list[LiquidationEvent] = []
    collector = LiquidationCollector(("BTCUSDT", "ETHUSDT"), received.append)
    assert collector.handle_payload(payload) == 2
    assert [event.symbol for event in received] == ["BTCUSDT", "ETHUSDT"]


class FakeSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.sent: list[dict[str, object]] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str:
        if self.messages:
            return self.messages.pop(0)
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


class FailingSocket(FakeSocket):
    async def recv(self) -> str:
        raise ConnectionError("disconnected")


def test_import_and_construction_do_not_open_socket() -> None:
    calls = 0

    async def factory(_url: str) -> FakeSocket:
        nonlocal calls
        calls += 1
        return FakeSocket([])

    PublicTradeCollector(
        ("BTCUSDT",),
        TradeFlowAccumulator(clock=lambda: NOW),
        websocket_factory=factory,
    )
    assert calls == 0


def test_start_subscribes_heartbeats_and_stop_is_idempotent() -> None:
    async def scenario() -> None:
        socket = FakeSocket([])

        async def factory(_url: str) -> FakeSocket:
            return socket

        collector = PublicTradeCollector(
            ("btcusdt", "BTCUSDT"),
            TradeFlowAccumulator(clock=lambda: NOW),
            websocket_factory=factory,
            heartbeat_seconds=0.001,
        )
        await collector.start()
        await asyncio.sleep(0.05)
        await collector.stop()
        await collector.stop()
        assert socket.sent[0] == {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}
        assert {"op": "ping"} in socket.sent
        assert socket.closed

    asyncio.run(scenario())


def test_socket_message_is_processed_offline() -> None:
    async def scenario() -> None:
        raw = json.dumps(
            {
                "data": [
                    {
                        "T": NOW_MS,
                        "s": "BTCUSDT",
                        "S": "Buy",
                        "v": "1",
                        "p": "100",
                        "i": "ws-1",
                    }
                ]
            }
        )
        socket = FakeSocket([raw])

        async def factory(_url: str) -> FakeSocket:
            return socket

        accumulator = TradeFlowAccumulator(clock=lambda: NOW)
        collector = PublicTradeCollector(
            ("BTCUSDT",),
            accumulator,
            websocket_factory=factory,
        )
        await collector.start()
        await asyncio.sleep(0.01)
        await collector.stop()
        assert accumulator.event_count("BTCUSDT") == 1
        assert collector.metrics.accepted_events == 1

    asyncio.run(scenario())


def test_disconnected_socket_reconnects_without_stopping_collector() -> None:
    async def scenario() -> None:
        sockets = [FailingSocket([]), FakeSocket([])]

        async def factory(_url: str) -> FakeSocket:
            return sockets.pop(0)

        collector = PublicTradeCollector(
            ("BTCUSDT",),
            TradeFlowAccumulator(clock=lambda: NOW),
            websocket_factory=factory,
            reconnect_seconds=0,
        )
        await collector.start()
        for _ in range(20):
            if collector.metrics.connections == 2:
                break
            await asyncio.sleep(0.005)
        await collector.stop()
        assert collector.metrics.connections == 2
        assert collector.metrics.reconnects == 1
        assert "disconnected" in (collector.metrics.last_error or "")

    asyncio.run(scenario())

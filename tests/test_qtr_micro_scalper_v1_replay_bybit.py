from __future__ import annotations

import gzip
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from market_signal_assistant.qtr_micro.micro_scalper_v1.replay.bybit import (
    iter_bybit_orderbook,
    iter_bybit_public_trades,
)
from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEventType,
    TradeSide,
)


def test_iter_bybit_public_trades_normalizes_archive(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDT.csv.gz"

    with gzip.open(path, "wt", newline="") as stream:
        stream.write(
            "timestamp,symbol,side,size,price,tickDirection,"
            "trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n"
        )
        stream.write(
            "1787184000.304,BTCUSDT,Buy,0.003,69305.10,"
            "PlusTick,trade-1,0,0,0,0\n"
        )

    events = list(iter_bybit_public_trades(path))

    assert len(events) == 1

    event = events[0]
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == "trade-1"
    assert event.exchange_at == datetime(
        2026,
        8,
        20,
        0,
        0,
        0,
        304000,
        tzinfo=UTC,
    )
    assert event.received_at == event.exchange_at
    assert event.side is TradeSide.BUY
    assert event.price == 69305.10
    assert event.quantity == 0.003
    assert event.quote_notional == event.price * event.quantity
    assert event.sequence is None
    assert not event.is_block_trade
    assert not event.is_rpi_trade


def test_iter_bybit_orderbook_uses_production_normalizer(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT.data.zip"

    snapshot = (
        '{"topic":"orderbook.200.BTCUSDT","type":"snapshot",'
        '"ts":1787184000129,'
        '"data":{"s":"BTCUSDT",'
        '"b":[["69305.00","2.895"]],'
        '"a":[["69305.10","4.974"]],'
        '"u":23732349,"seq":779429549000},'
        '"cts":1787184000127}\n'
    )

    delta = (
        '{"topic":"orderbook.200.BTCUSDT","type":"delta",'
        '"ts":1787184000229,'
        '"data":{"s":"BTCUSDT",'
        '"b":[["69305.00","5.040"]],'
        '"a":[["69305.10","1.586"]],'
        '"u":23732350,"seq":779429551797},'
        '"cts":1787184000227}\n'
    )

    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(
            "2026-08-20_BTCUSDT_ob200.data",
            snapshot + delta,
        )

    events = list(iter_bybit_orderbook(path))

    assert len(events) == 2

    first, second = events

    assert first.event_type is OrderBookEventType.SNAPSHOT
    assert first.update_id == 23732349
    assert first.cross_sequence == 779429549000
    assert first.exchange_at == datetime(
        2026,
        8,
        20,
        0,
        0,
        0,
        127000,
        tzinfo=UTC,
    )
    assert first.received_at == first.exchange_at

    assert second.event_type is OrderBookEventType.DELTA
    assert second.update_id == 23732350
    assert second.cross_sequence == 779429551797
    assert second.received_at == second.exchange_at

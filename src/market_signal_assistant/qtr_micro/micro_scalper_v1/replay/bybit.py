from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.live.orderbook_ws import (
    parse_orderbook_message,
)


def iter_bybit_public_trades(path: Path) -> Iterator[PublicTradeEvent]:
    """Read a Bybit public-trades CSV.gz archive as production trade events."""

    with gzip.open(path, "rt", newline="") as stream:
        reader = csv.DictReader(stream)

        for row in reader:
            exchange_at = datetime.fromtimestamp(
                float(row["timestamp"]),
                tz=UTC,
            )
            price = float(row["price"])
            quantity = float(row["size"])

            side_raw = row["side"]
            if side_raw == "Buy":
                side = TradeSide.BUY
            elif side_raw == "Sell":
                side = TradeSide.SELL
            else:
                raise ValueError(f"Unknown Bybit trade side: {side_raw!r}")

            yield PublicTradeEvent(
                symbol=row["symbol"],
                trade_id=row["trdMatchID"],
                exchange_at=exchange_at,
                received_at=exchange_at,
                side=side,
                price=price,
                quantity=quantity,
                quote_notional=price * quantity,
                sequence=None,
                is_block_trade=False,
                is_rpi_trade=_csv_bool(row.get("RPI")),
            )


def iter_bybit_orderbook(path: Path) -> Iterator[OrderBookEvent]:
    """Read a Bybit orderbook ZIP archive through the production normalizer."""

    with ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError(
                f"Expected exactly one orderbook member, found {len(names)}."
            )

        with archive.open(names[0]) as stream:
            for raw in stream:
                payload = json.loads(raw)

                raw_timestamp = payload.get("cts", payload.get("ts"))
                if raw_timestamp is None:
                    raise ValueError("Orderbook event has no cts/ts timestamp.")

                exchange_at = datetime.fromtimestamp(
                    int(str(raw_timestamp)) / 1000,
                    tz=UTC,
                )

                event = parse_orderbook_message(
                    payload,
                    received_at=exchange_at,
                )
                if event is None:
                    raise ValueError("Invalid historical orderbook event.")

                yield event


def _csv_bool(value: str | None) -> bool:
    if value is None:
        return False

    normalized = value.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false", ""}:
        return False

    raise ValueError(f"Unknown CSV boolean value: {value!r}")

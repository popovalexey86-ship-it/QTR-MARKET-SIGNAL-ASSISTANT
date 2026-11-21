"""Offline closed-candle, bounded-memory, and fail-closed shadow contracts."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.fast_watch_v2 import (
    MAX_FRAME_BYTES,
    FastWatchConfig,
    FastWatchIntegrityError,
    FastWatchShadow,
    parse_confirmed_minute,
)
from market_signal_assistant.watchdog.runtime.fast_watch_v2 import consume_socket

BASE = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)
SHA = "a" * 40


def _frame(
    index: int,
    *,
    symbol: str = "DOTUSDT",
    confirm: bool = True,
    close: float = 10.0,
    volume: float = 100.0,
) -> str:
    start = BASE + timedelta(minutes=index)
    start_ms = int(start.timestamp() * 1000)
    return json.dumps(
        {
            "topic": f"kline.1.{symbol}",
            "type": "snapshot",
            "ts": start_ms + 60_000,
            "data": [
                {
                    "start": start_ms,
                    "end": start_ms + 59_999,
                    "interval": "1",
                    "open": "10",
                    "high": str(max(10, close)),
                    "low": str(min(10, close)),
                    "close": str(close),
                    "volume": str(volume),
                    "confirm": confirm,
                }
            ],
        }
    )


def _received(index: int, seconds: int = 1) -> datetime:
    return BASE + timedelta(minutes=index + 1, seconds=seconds)


def _pilot(root: Path, *, symbols: tuple[str, ...] = ("DOTUSDT",)) -> FastWatchShadow:
    pilot = FastWatchShadow(root, FastWatchConfig(symbols, SHA))
    pilot.start()
    pilot.begin_subscription(BASE - timedelta(seconds=1))
    return pilot


def test_parser_accepts_only_finite_confirmed_closed_one_minute_candles() -> None:
    allowed = frozenset({"DOTUSDT"})
    assert (
        parse_confirmed_minute(
            _frame(0, confirm=False), received_at=_received(0), allowed_symbols=allowed
        )
        is None
    )
    minute = parse_confirmed_minute(
        _frame(0), received_at=_received(0), allowed_symbols=allowed
    )
    assert minute is not None
    assert minute.exchange_close_at == BASE + timedelta(minutes=1)
    assert minute.available_at == _received(0)
    with_trade_time = json.loads(_frame(0))
    with_trade_time["data"][0]["timestamp"] = int(
        (BASE + timedelta(seconds=59)).timestamp() * 1000
    )
    parsed = parse_confirmed_minute(
        json.dumps(with_trade_time),
        received_at=_received(0),
        allowed_symbols=allowed,
    )
    assert parsed is not None
    assert parsed.last_trade_at == BASE + timedelta(seconds=59)
    for bad in (
        _frame(0, close=float("nan")),
        _frame(0, close=0.0),
        _frame(0, symbol="ETHUSDT"),
        _frame(0).replace('"confirm": true', '"confirm": "true"'),
        "{" + " " * MAX_FRAME_BYTES,
    ):
        with pytest.raises(FastWatchIntegrityError):
            parse_confirmed_minute(
                bad, received_at=_received(0), allowed_symbols=allowed
            )
    with pytest.raises(FastWatchIntegrityError, match="before close"):
        parse_confirmed_minute(_frame(0), received_at=BASE, allowed_symbols=allowed)


def test_subscription_snapshot_duplicate_gap_and_late_close_fail_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-gap"
    pilot = _pilot(root)
    pilot.begin_subscription(BASE + timedelta(minutes=1, seconds=1))
    assert pilot.ingest(_frame(0), received_at=_received(0)) is None
    assert pilot.confirmed_count == 0
    pilot.begin_subscription(BASE - timedelta(seconds=1))
    assert pilot.ingest(_frame(0), received_at=_received(0)) is None
    assert pilot.confirmed_count == 1
    assert pilot.ingest(_frame(0), received_at=_received(0, 2)) is None
    assert pilot.confirmed_count == 1
    with pytest.raises(FastWatchIntegrityError, match="Conflicting duplicate"):
        pilot.ingest(_frame(0, close=9.9), received_at=_received(0, 2))
    with pytest.raises(FastWatchIntegrityError, match="gap"):
        pilot.ingest(_frame(2), received_at=_received(2))
    pilot.close()
    assert len((root / "operational/gaps.jsonl").read_text().splitlines()) == 1
    with pytest.raises(FastWatchIntegrityError, match="Prior shadow gap"):
        _pilot(root)

    late = _pilot(tmp_path / "watchdog-fast-watch-v2-late")
    with pytest.raises(FastWatchIntegrityError, match="too late"):
        late.ingest(_frame(0), received_at=_received(0, 181))
    late.close()


def test_one_silent_symbol_fails_closed_while_other_symbols_progress(
    tmp_path: Path,
) -> None:
    pilot = _pilot(
        tmp_path / "watchdog-fast-watch-v2-silent",
        symbols=("DOTUSDT", "GRAMUSDT"),
    )
    for index in range(4):
        pilot.ingest(_frame(index), received_at=_received(index))
    with pytest.raises(FastWatchIntegrityError, match="GRAMUSDT"):
        pilot.assert_progress(_received(3))
    assert pilot.transport_failures == 1
    pilot.close()


def test_transport_failure_count_survives_restart(tmp_path: Path) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-transport"
    pilot = _pilot(root)
    pilot.record_transport_failure("ConnectionClosed", at=_received(0))
    pilot.close()
    recovered = _pilot(root)
    assert recovered.transport_failures == 1
    recovered.record_transport_failure("TimeoutError", at=_received(1))
    assert recovered.transport_failures == 2
    recovered.close()


def test_shadow_has_separate_provenance_no_outcomes_and_recovers_exact_cursor(
    tmp_path: Path,
) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-recovery"
    pilot = _pilot(root)
    for index in range(45):
        pilot.ingest(_frame(index), received_at=_received(index))
    assert pilot.confirmed_count == 45
    assert pilot.retained_counts["baseline_observations"] > 0
    assert pilot.retained_counts["historical_jsonl_ram_index"] == 0
    assert not (root / "outcomes").exists()
    assert not (root / "telegram").exists()
    manifest = json.loads((root / "provenance.json").read_text())
    assert manifest["mode"] == "FAST_WATCH_V2_SHADOW"
    assert manifest["commit"] == SHA
    rows = (root / "observations/confirmed.jsonl").read_text().splitlines()
    last = json.loads(rows[-1])
    assert last["source"] == "bybit-public-linear-ws-kline.1-confirmed"
    assert last["exchange_candle_close"] == (BASE + timedelta(minutes=45)).isoformat()
    assert last["received_at"] == _received(44).isoformat()
    assert last["available_at"] == _received(44).isoformat()
    assert last["detected_at"] >= last["available_at"]
    pilot.close()

    recovered = _pilot(root)
    assert recovered.confirmed_count == 45
    recovered.begin_subscription(BASE + timedelta(minutes=45, seconds=1))
    assert recovered.ingest(_frame(44), received_at=_received(44, 2)) is None
    assert recovered.confirmed_count == 45
    recovered.ingest(_frame(45), received_at=_received(45))
    assert recovered.confirmed_count == 46
    recovered.close()


def test_crash_after_engine_state_before_observation_is_quarantined(
    tmp_path: Path,
) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-crash"
    pilot = _pilot(root)
    for index in range(5):
        pilot.ingest(_frame(index), received_at=_received(index))
    assert pilot._engine is not None
    from market_signal_assistant.models import (
        AssetClass,
        Candle,
        Instrument,
        MarketSeries,
    )

    candles = tuple(pilot._candles["DOTUSDT"])
    next_candle = Candle(BASE + timedelta(minutes=5), 10, 10, 10, 10, 100)
    pilot._engine.evaluate(
        MarketSeries(
            Instrument("DOTUSDT", AssetClass.CRYPTO), "1m", (*candles, next_candle)
        ),
        detected_at=_received(5),
    )
    pilot.close()
    with pytest.raises(FastWatchIntegrityError, match="cursor diverged"):
        _pilot(root)


def test_300_minute_ingest_retains_bounded_candles_and_baselines(
    tmp_path: Path,
) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-bounds"
    pilot = _pilot(root)
    for index in range(300):
        pilot.ingest(_frame(index), received_at=_received(index))
    counts = pilot.retained_counts
    assert pilot.confirmed_count == 300
    assert counts["candles"] == 240
    assert counts["baseline_observations"] <= counts["baseline_keys"] * 240
    assert counts["historical_jsonl_ram_index"] == 0
    pilot.close()


def test_existing_volume_detector_emits_only_to_shadow_journal(tmp_path: Path) -> None:
    root = tmp_path / "watchdog-fast-watch-v2-detector"
    pilot = _pilot(root)
    for index in range(45):
        pilot.ingest(_frame(index), received_at=_received(index))
    event_id = pilot.ingest(_frame(45, volume=10_000), received_at=_received(45))
    assert event_id is not None
    assert pilot._events is not None
    event = pilot._events.get(event_id)
    assert event is not None
    assert "VOLUME_SHOCK" in {kind.value for kind in event.anomaly_types}
    assert event.universe_tier == "FAST_WATCH_SHADOW"
    assert event.available_at == _received(45)
    assert not (root / "outcomes").exists()
    assert not (root / "telegram").exists()
    pilot.close()


def test_async_socket_subscribes_only_selected_symbols(tmp_path: Path) -> None:
    pilot = _pilot(
        tmp_path / "watchdog-fast-watch-v2-socket",
        symbols=("DOTUSDT", "GRAMUSDT", "FARTCOINUSDT"),
    )
    stop = asyncio.Event()

    class FakeSocket:
        sent: str = ""

        async def send(self, message: str) -> None:
            self.sent = message

        async def recv(self) -> str:
            stop.set()
            return _frame(0)

    socket = FakeSocket()
    asyncio.run(
        consume_socket(
            pilot,
            socket,
            stop=stop,
            now=lambda: _received(0),
        )
    )
    assert json.loads(socket.sent)["args"] == [
        "kline.1.DOTUSDT",
        "kline.1.GRAMUSDT",
        "kline.1.FARTCOINUSDT",
    ]
    assert pilot.confirmed_count == 0  # The fake stopped before the receive loop.
    pilot.close()

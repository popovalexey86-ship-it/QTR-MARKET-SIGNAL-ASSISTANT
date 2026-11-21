"""Isolated, prospective one-minute Watchdog shadow detector.

This module has no Telegram, outcomes, or trading dependencies. The standard
Watchdog detector engine is reused without modifying its decision semantics.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.watchdog.aggregation import ExplainableAnomalyAggregator
from market_signal_assistant.watchdog.baselines import (
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.detectors import (
    DetectorPipeline,
    default_detectors,
)
from market_signal_assistant.watchdog.engine import WatchdogDetectionEngine
from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.features import WatchdogFeatureBuilder
from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal
from market_signal_assistant.watchdog.runtime.lock import SingleInstanceLock
from market_signal_assistant.watchdog.state_machine import WatchdogStateMachine
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogStateRepository,
)

MINUTE = timedelta(minutes=1)
MAX_SYMBOLS = 8
MAX_CANDLES = 240
MAX_FRAME_BYTES = 65_536
SOURCE = "bybit-public-linear-ws-kline.1-confirmed"


class FastWatchIntegrityError(ValueError):
    """Shadow evidence is incomplete, ambiguous, or no longer prospective."""


@dataclass(frozen=True, slots=True)
class ConfirmedMinute:
    symbol: str
    candle: Candle
    exchange_close_at: datetime
    received_at: datetime
    source_timestamp: datetime | None
    last_trade_at: datetime | None

    @property
    def available_at(self) -> datetime:
        return max(self.exchange_close_at, self.received_at)


@dataclass(frozen=True, slots=True)
class FastWatchConfig:
    symbols: tuple[str, ...]
    commit: str
    maximum_candles: int = MAX_CANDLES
    maximum_delivery_lag: timedelta = timedelta(minutes=3)

    def __post_init__(self) -> None:
        normalized = tuple(symbol.strip().upper() for symbol in self.symbols)
        if (
            not normalized
            or len(normalized) > MAX_SYMBOLS
            or len(set(normalized)) != len(normalized)
            or any(
                not value.endswith("USDT") or not value.isalnum()
                for value in normalized
            )
        ):
            raise ValueError("FAST WATCH requires 1-8 distinct linear USDT symbols.")
        if len(self.commit) != 40 or any(
            ch not in "0123456789abcdef" for ch in self.commit
        ):
            raise ValueError("FAST WATCH requires an exact lowercase Git SHA.")
        if self.maximum_candles < 41 or self.maximum_candles > MAX_CANDLES:
            raise ValueError("FAST WATCH candle retention must be 41-240.")
        if self.maximum_delivery_lag <= timedelta(0):
            raise ValueError("FAST WATCH delivery lag must be positive.")
        object.__setattr__(self, "symbols", normalized)


def parse_confirmed_minute(
    raw: str | bytes,
    *,
    received_at: datetime,
    allowed_symbols: frozenset[str],
) -> ConfirmedMinute | None:
    """Validate a Bybit public frame; open candles are never market evidence."""
    encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
    if len(encoded) > MAX_FRAME_BYTES:
        raise FastWatchIntegrityError("WebSocket frame exceeds the bounded size.")
    try:
        frame: Any = json.loads(encoded)
        if not isinstance(frame, dict):
            raise ValueError
        if "op" in frame:
            if frame.get("op") == "subscribe" and frame.get("success") is False:
                raise FastWatchIntegrityError("Bybit rejected the subscription.")
            return None
        topic = frame["topic"]
        if not isinstance(topic, str) or not topic.startswith("kline.1."):
            return None
        symbol = topic.removeprefix("kline.1.")
        if symbol not in allowed_symbols:
            raise FastWatchIntegrityError("Unsubscribed symbol in kline stream.")
        rows = frame["data"]
        if (
            not isinstance(rows, list)
            or len(rows) != 1
            or not isinstance(rows[0], dict)
        ):
            raise ValueError
        row = rows[0]
        if row["interval"] != "1":
            raise ValueError
        if row["confirm"] is False:
            return None
        if row["confirm"] is not True:
            raise ValueError
        start_ms = _integer(row["start"])
        end_ms = _integer(row["end"])
        if start_ms % 60_000 or end_ms != start_ms + 59_999:
            raise ValueError
        start = datetime.fromtimestamp(start_ms / 1000, UTC)
        close = start + MINUTE
        received = _utc(received_at)
        if received < close:
            raise FastWatchIntegrityError("Confirmed candle received before close.")
        source_ms = frame.get("ts")
        source_at = (
            datetime.fromtimestamp(_integer(source_ms) / 1000, UTC)
            if source_ms is not None
            else None
        )
        trade_ms = row.get("timestamp")
        last_trade_at = (
            datetime.fromtimestamp(_integer(trade_ms) / 1000, UTC)
            if trade_ms is not None
            else None
        )
        candle = Candle(
            start,
            _number(row["open"]),
            _number(row["high"]),
            _number(row["low"]),
            _number(row["close"]),
            _number(row["volume"]),
        )
        return ConfirmedMinute(
            symbol, candle, close, received, source_at, last_trade_at
        )
    except FastWatchIntegrityError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise FastWatchIntegrityError(
            "Malformed confirmed Bybit kline frame."
        ) from error


class FastWatchShadow:
    """Bounded single-writer shadow with fail-closed cross-file recovery."""

    def __init__(
        self,
        root: Path,
        config: FastWatchConfig,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.root = root.resolve()
        self.config = config
        self.clock = clock
        self._lock = SingleInstanceLock(self.root / "state" / "writer.lock")
        self._started = False
        self._floor: datetime | None = None
        self._candles: dict[str, deque[Candle]] = {
            symbol: deque(maxlen=config.maximum_candles) for symbol in config.symbols
        }
        self._last_received: dict[str, datetime] = {}
        self._events: WatchdogEventJournal | None = None
        self._observations: ImmutableJsonlJournal | None = None
        self._gaps: ImmutableJsonlJournal | None = None
        self._transport: ImmutableJsonlJournal | None = None
        self._baselines: RollingBaselineEngine | None = None
        self._states: WatchdogStateRepository | None = None
        self._engine: WatchdogDetectionEngine | None = None
        self.confirmed_count = 0
        self.event_count = 0
        self.transport_failures = 0

    def start(self) -> None:
        if self._started:
            return
        if not self.root.name.startswith("watchdog-fast-watch-v2-"):
            raise FastWatchIntegrityError("FAST WATCH requires a dedicated data root.")
        self._lock.acquire()
        try:
            self._check_manifest()
            self._events = WatchdogEventJournal(self.root / "events" / "events.jsonl")
            self._observations = ImmutableJsonlJournal(
                self.root / "observations" / "confirmed.jsonl",
                id_field="observation_id",
            )
            self._gaps = ImmutableJsonlJournal(
                self.root / "operational" / "gaps.jsonl", id_field="gap_id"
            )
            self._transport = ImmutableJsonlJournal(
                self.root / "operational" / "transport.jsonl",
                id_field="transport_id",
            )
            self._events.assert_complete()
            self._observations.assert_complete()
            self._gaps.assert_complete()
            self._transport.assert_complete()
            self.transport_failures = self._transport.recovery.record_count
            if self._gaps.recovery.record_count:
                raise FastWatchIntegrityError("Prior shadow gap requires review.")
            self._baselines = RollingBaselineEngine(
                JsonBaselineStore(self.root / "state" / "baselines.json")
            )
            self._states = WatchdogStateRepository(
                JsonWatchdogStateStore(self.root / "state" / "symbols.json")
            )
            self._engine = WatchdogDetectionEngine(
                WatchdogFeatureBuilder(self._baselines),
                DetectorPipeline(default_detectors()),
                ExplainableAnomalyAggregator(),
                WatchdogStateMachine(),
                self._states,
            )
            self._recover()
            self._started = True
        except Exception:
            self._lock.release()
            raise

    def close(self) -> None:
        self._started = False
        self._lock.release()

    def begin_subscription(self, at: datetime | None = None) -> None:
        if not self._started:
            raise FastWatchIntegrityError("FAST WATCH is not started.")
        self._floor = _utc(at or self.clock())

    def assert_progress(self, at: datetime | None = None) -> None:
        """Fail closed if any subscribed symbol stops producing closed minutes."""
        if not self._started or self._floor is None:
            raise FastWatchIntegrityError("FAST WATCH subscription is not active.")
        current = _utc(at or self.clock())
        for symbol, ring in self._candles.items():
            last_close = ring[-1].timestamp + MINUTE if ring else self._floor
            if current - last_close > self.config.maximum_delivery_lag:
                self.record_transport_failure(f"progress_stalled:{symbol}", at=current)
                raise FastWatchIntegrityError(
                    f"Confirmed minute progress stalled for {symbol}."
                )

    def record_transport_failure(self, reason: str, *, at: datetime) -> None:
        if not self._started or self._transport is None:
            raise FastWatchIntegrityError(
                "FAST WATCH transport journal is unavailable."
            )
        recorded_at = _utc(at)
        sequence = self.transport_failures + 1
        record_id = f"fast-transport:{sequence}"
        self._transport.append(
            record_id,
            {
                "transport_id": record_id,
                "reason": reason,
                "recorded_at": recorded_at.isoformat(),
            },
        )
        self.transport_failures = sequence

    def ingest(
        self, raw: str | bytes, *, received_at: datetime | None = None
    ) -> str | None:
        if not self._started or self._floor is None:
            raise FastWatchIntegrityError("FAST WATCH subscription is not active.")
        minute = parse_confirmed_minute(
            raw,
            received_at=received_at or self.clock(),
            allowed_symbols=frozenset(self.config.symbols),
        )
        if minute is None:
            return None
        if minute.exchange_close_at <= self._floor and not self._candles[minute.symbol]:
            return None  # Never process a retrospective subscription snapshot.
        if (
            minute.received_at - minute.exchange_close_at
            > self.config.maximum_delivery_lag
        ):
            self._record_gap(
                minute.symbol,
                minute.candle.timestamp,
                minute.candle.timestamp,
                "late_confirmed",
                minute.received_at,
            )
            raise FastWatchIntegrityError(
                "Closed candle arrived too late for the pilot."
            )
        ring = self._candles[minute.symbol]
        if ring:
            previous = ring[-1]
            if minute.candle.timestamp == previous.timestamp:
                if minute.candle != previous:
                    raise FastWatchIntegrityError(
                        "Conflicting duplicate minute candle."
                    )
                return None
            if minute.candle.timestamp != previous.timestamp + MINUTE:
                if minute.candle.timestamp > previous.timestamp + MINUTE:
                    self._record_gap(
                        minute.symbol,
                        previous.timestamp + MINUTE,
                        minute.candle.timestamp - MINUTE,
                        "missing_confirmed",
                        minute.received_at,
                    )
                raise FastWatchIntegrityError(
                    "Confirmed minute candle gap or reversal."
                )
        previous_received = self._last_received.get(minute.symbol)
        if previous_received is not None and minute.received_at <= previous_received:
            raise FastWatchIntegrityError("Non-monotonic market receive time.")
        if self._engine is None or self._events is None or self._observations is None:
            raise FastWatchIntegrityError("FAST WATCH engine is unavailable.")
        series = MarketSeries(
            Instrument(minute.symbol, AssetClass.CRYPTO),
            "1m",
            (*ring, minute.candle),
        )
        detected_at = max(_utc(self.clock()), minute.received_at)
        result = self._engine.evaluate(series, detected_at=detected_at)
        event_id: str | None = None
        if result.event is not None:
            evidence = WatchdogEventEvidence.from_detection(
                result,
                price_at_detection=minute.candle.close,
                universe_tier="FAST_WATCH_SHADOW",
            )
            # The exchange close is not the time this WebSocket observation
            # became available to this process. Preserve actual receipt in the
            # event's PIT boundary without changing detection decisions.
            evidence = replace(
                evidence, available_at=max(evidence.available_at, minute.available_at)
            )
            self._events.append(evidence)
            event_id = evidence.event_id
        record: dict[str, object] = {
            "observation_id": _observation_id(minute.symbol, minute.candle.timestamp),
            "symbol": minute.symbol,
            "source": SOURCE,
            "exchange_candle_start": minute.candle.timestamp.isoformat(),
            "exchange_candle_close": minute.exchange_close_at.isoformat(),
            "source_timestamp": (
                minute.source_timestamp.isoformat()
                if minute.source_timestamp is not None
                else None
            ),
            "last_trade_at": (
                minute.last_trade_at.isoformat()
                if minute.last_trade_at is not None
                else None
            ),
            "received_at": minute.received_at.isoformat(),
            "available_at": minute.available_at.isoformat(),
            "detected_at": detected_at.isoformat(),
            "candle": {
                "open": minute.candle.open,
                "high": minute.candle.high,
                "low": minute.candle.low,
                "close": minute.candle.close,
                "volume": minute.candle.volume,
            },
            "event_id": event_id,
            "feature_names": [item.name for item in result.snapshot.features],
        }
        self._observations.append(str(record["observation_id"]), record)
        ring.append(minute.candle)
        self._last_received[minute.symbol] = minute.received_at
        self.confirmed_count += 1
        if event_id is not None:
            self.event_count += 1
        return event_id

    @property
    def retained_counts(self) -> dict[str, int]:
        baseline = self._baselines.retained_counts if self._baselines else (0, 0, 240)
        journal_ram_indexes = sum(
            journal.retained_index_entries
            for journal in (
                self._events,
                self._observations,
                self._gaps,
                self._transport,
            )
            if journal is not None
        )
        return {
            "symbols": len(self._candles),
            "candles": sum(len(ring) for ring in self._candles.values()),
            "candles_per_symbol_bound": self.config.maximum_candles,
            "baseline_keys": baseline[0],
            "baseline_observations": baseline[1],
            "baseline_per_key_bound": baseline[2],
            "historical_jsonl_ram_index": journal_ram_indexes,
        }

    def _record_gap(
        self,
        symbol: str,
        first: datetime,
        last: datetime,
        reason: str,
        recorded_at: datetime,
    ) -> None:
        if self._gaps is None:
            raise FastWatchIntegrityError("Gap journal is unavailable.")
        gap_id = f"fast-gap:{symbol}:{first.isoformat()}:{last.isoformat()}:{reason}"
        self._gaps.append(
            gap_id,
            {
                "gap_id": gap_id,
                "symbol": symbol,
                "first_candle_start": first.isoformat(),
                "last_candle_start": last.isoformat(),
                "reason": reason,
                "recorded_at": recorded_at.isoformat(),
            },
        )

    def _check_manifest(self) -> None:
        if any(
            (self.root / name).exists()
            for name in (
                "outcomes",
                "telegram",
                "state/pending.json",
                "state/health.json",
            )
        ):
            raise FastWatchIntegrityError("Non-shadow state in FAST WATCH root.")
        path = self.root / "provenance.json"
        expected = {
            "version": 1,
            "mode": "FAST_WATCH_V2_SHADOW",
            "commit": self.config.commit,
            "symbols": list(self.config.symbols),
            "source": SOURCE,
            "interval": "1m",
            "maximum_candles": self.config.maximum_candles,
            "maximum_delivery_lag_seconds": (
                self.config.maximum_delivery_lag.total_seconds()
            ),
        }
        if path.exists():
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                raise FastWatchIntegrityError(
                    "Shadow provenance is invalid."
                ) from error
            if not isinstance(value, dict) or any(
                value.get(k) != v for k, v in expected.items()
            ):
                raise FastWatchIntegrityError(
                    "Shadow provenance differs from launch config."
                )
            return
        if self.root.exists() and any(self.root.iterdir()):
            # The writer.lock is created by the just-acquired lock.
            existing = {
                p.relative_to(self.root).as_posix()
                for p in self.root.rglob("*")
                if p.is_file()
            }
            if existing - {"state/writer.lock"}:
                raise FastWatchIntegrityError(
                    "Fresh shadow root contains existing data."
                )
        expected["started_at"] = _utc(self.clock()).isoformat()
        _atomic_json(path, expected)

    def _recover(self) -> None:
        assert self._observations is not None
        assert self._events is not None
        assert self._states is not None
        assert self._baselines is not None
        last_detected: dict[str, datetime] = {}
        last_start: dict[str, datetime] = {}
        last_features: dict[str, tuple[str, ...]] = {}
        for row in self._observations.iter_records():
            try:
                symbol = str(row["symbol"])
                start = datetime.fromisoformat(str(row["exchange_candle_start"]))
                close = datetime.fromisoformat(str(row["exchange_candle_close"]))
                received = datetime.fromisoformat(str(row["received_at"]))
                detected = datetime.fromisoformat(str(row["detected_at"]))
                candle_data = row["candle"]
                if not isinstance(candle_data, dict):
                    raise ValueError
                candle = Candle(
                    start,
                    _number(candle_data["open"]),
                    _number(candle_data["high"]),
                    _number(candle_data["low"]),
                    _number(candle_data["close"]),
                    _number(candle_data["volume"]),
                )
                event_id = row["event_id"]
                feature_names = row["feature_names"]
                if (
                    symbol not in self._candles
                    or row["source"] != SOURCE
                    or row["observation_id"] != _observation_id(symbol, start)
                    or close != start + MINUTE
                    or row["available_at"] != max(close, received).isoformat()
                    or not close <= received <= detected
                    or (symbol in last_start and start != last_start[symbol] + MINUTE)
                    or (symbol in last_detected and detected <= last_detected[symbol])
                    or not isinstance(feature_names, list)
                    or any(not isinstance(name, str) for name in feature_names)
                ):
                    raise ValueError
                if event_id is not None:
                    if not isinstance(event_id, str):
                        raise ValueError
                    event = self._events.get(event_id)
                    if (
                        event is None
                        or event.event_time != start
                        or event.detected_at != detected
                        or event.available_at != max(close, received)
                    ):
                        raise ValueError
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                raise FastWatchIntegrityError(
                    "Shadow observation recovery failed."
                ) from error
            self._candles[symbol].append(candle)
            self._last_received[symbol] = received
            last_detected[symbol] = detected
            last_start[symbol] = start
            last_features[symbol] = tuple(feature_names)
            self.confirmed_count += 1
            if event_id is not None:
                self.event_count += 1
        for event in self._events.iter_records():
            observation = self._observations.get(
                _observation_id(event.symbol, event.event_time)
            )
            if observation is None or observation.get("event_id") != event.event_id:
                raise FastWatchIntegrityError(
                    "Event lacks committed market observation."
                )
        for symbol in self._states.symbols:
            state = self._states.persisted(symbol)
            if (
                state is None
                or state.symbol_state.last_detected_at != last_detected.get(symbol)
            ):
                raise FastWatchIntegrityError("State and observation cursor diverged.")
        if set(last_detected) != set(self._states.symbols):
            raise FastWatchIntegrityError("Observation cursor lacks durable state.")
        for item in self._baselines.observations:
            if (
                item.symbol not in last_start
                or item.scope != "1m"
                or item.observed_at > last_start[item.symbol]
                or item.available_at > last_detected[item.symbol]
            ):
                raise FastWatchIntegrityError(
                    "Baseline is ahead of observation cursor."
                )
        last_baseline = {
            (item.symbol, item.feature): item for item in self._baselines.observations
        }
        for symbol, names in last_features.items():
            for name in names:
                baseline_item = last_baseline.get((symbol, name))
                if (
                    baseline_item is None
                    or baseline_item.observed_at != last_start[symbol]
                ):
                    raise FastWatchIntegrityError(
                        "Committed baseline history is missing."
                    )


def _observation_id(symbol: str, start: datetime) -> str:
    return f"fast-watch:{symbol}:{_utc(start).strftime('%Y%m%dT%H%M%SZ')}"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("FAST WATCH timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Timestamp must be an integer.")
    return value


def _number(value: object) -> float:
    import math

    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Invalid candle value.")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("Non-finite candle value.")
    return parsed


def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

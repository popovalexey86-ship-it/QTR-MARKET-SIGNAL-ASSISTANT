from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.inplay.early_discovery import (
    DiscoveryStage,
    EarlyDiscoveryResult,
    EarlyDiscoveryService,
    JsonlEarlyDiscoveryAuditStore,
    MarketDirection,
)
from market_signal_assistant.inplay.models import (
    CatalogInstrument,
    InPlayDirection,
    InPlayResult,
)
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
)
from market_signal_assistant.providers import MarketDataError
from market_signal_assistant.settings import EarlyDiscoverySettings

NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)


def instrument(
    symbol: str,
    *,
    symbol_type: str = "",
    pre_listing: bool = False,
    spread_pct: float = 0.1,
) -> CatalogInstrument:
    midpoint = 100.0
    half = midpoint * spread_pct / 200.0
    return CatalogInstrument(
        symbol=symbol,
        quote_coin="USDT",
        status="Trading",
        turnover_24h=100_000_000.0,
        bid=midpoint - half,
        ask=midpoint + half,
        base_coin=symbol.removesuffix("USDT"),
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type=symbol_type,
        is_pre_listing=pre_listing,
    )


def series(
    symbol: str,
    interval: str,
    *,
    count: int = 80,
    breakout_age: int | None = None,
    breakout_price: float = 102.0,
    last_volume: float = 300.0,
    change_24h_pct: float = 0.0,
) -> MarketSeries:
    minutes = {"5m": 5, "15m": 15, "1h": 60}[interval]
    closes = [100.0] * count
    if interval == "1h" and change_24h_pct:
        target = 100.0 * (1.0 + change_24h_pct / 100.0)
        closes[-24:] = [
            100.0 + (target - 100.0) * (index + 1) / 24
            for index in range(24)
        ]
    if interval == "5m" and breakout_age is not None:
        index = count - 1 - breakout_age
        closes[index:] = [breakout_price] * (count - index)
    candles = tuple(
        Candle(
            timestamp=NOW - timedelta(minutes=minutes * (count - index)),
            open=close,
            high=close + 0.5,
            low=close - 0.5,
            close=close,
            volume=last_volume if index == count - 1 else 100.0,
        )
        for index, close in enumerate(closes)
    )
    return MarketSeries(
        Instrument(symbol, AssetClass.CRYPTO),
        interval,
        candles,
    )


class Catalog:
    def __init__(self, items: tuple[CatalogInstrument, ...]) -> None:
        self.items = items

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        return self.items


class Provider:
    def __init__(
        self,
        symbols: tuple[str, ...],
        *,
        breakout_age: int | None = 0,
        breakout_price: float = 102.0,
        spread_change_24h: float = 0.0,
    ) -> None:
        self.symbols = frozenset(symbols)
        self.breakout_age = breakout_age
        self.breakout_price = breakout_price
        self.change_24h = spread_change_24h

    def load(
        self,
        instrument_value: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        del limit
        assert instrument_value.symbol in self.symbols
        return series(
            instrument_value.symbol,
            interval,
            breakout_age=self.breakout_age,
            breakout_price=self.breakout_price,
            change_24h_pct=self.change_24h,
            last_volume=300 if interval == "5m" else 100,
        )


class PartialFailureProvider(Provider):
    def load(
        self,
        instrument_value: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        if instrument_value.symbol == "BADUSDT":
            raise MarketDataError("offline test failure")
        return super().load(instrument_value, interval, limit)


class Evaluator:
    def evaluate_existing_series(
        self,
        catalog: CatalogInstrument,
        series_value: MarketSeries,
        observed_at: datetime,
    ) -> InPlayResult:
        del series_value
        return InPlayResult(
            symbol=catalog.symbol,
            direction=InPlayDirection.LONG,
            inplay_score=float(int(catalog.symbol.removeprefix("COIN").removesuffix("USDT"))),
            directional_score=70.0,
            reasons=("Тестовый production score",),
            warnings=(),
            first_seen=observed_at,
        )


def scan_one(
    tmp_path: Path,
    *,
    change_24h: float = 0.0,
    breakout_age: int | None = 0,
    breakout_price: float = 102.0,
    spread_pct: float = 0.1,
) -> EarlyDiscoveryResult:
    symbol = "BTCUSDT"
    service = EarlyDiscoveryService(
        catalog_provider=Catalog((instrument(symbol, spread_pct=spread_pct),)),
        market_provider=Provider(
            (symbol,),
            breakout_age=breakout_age,
            breakout_price=breakout_price,
            spread_change_24h=change_24h,
        ),
        audit_store=JsonlEarlyDiscoveryAuditStore(tmp_path / "audit.jsonl"),
        clock=lambda: NOW,
        maximum_workers=1,
    )
    return service.scan().results[0]


def test_early_discovery_is_disabled_by_default() -> None:
    assert EarlyDiscoverySettings().enabled is False


def test_stock_and_prelaunch_stock_are_excluded(tmp_path: Path) -> None:
    items = (
        instrument("BTCUSDT"),
        instrument("JNJUSDT", symbol_type="stock"),
        instrument("XOMUSDT", symbol_type="stock", pre_listing=True),
    )
    report = EarlyDiscoveryService(
        catalog_provider=Catalog(items),
        market_provider=Provider(("BTCUSDT",)),
        audit_store=JsonlEarlyDiscoveryAuditStore(tmp_path / "audit.jsonl"),
        clock=lambda: NOW,
        maximum_workers=1,
    ).scan()

    assert tuple(item.symbol for item in report.results) == ("BTCUSDT",)


def test_one_market_data_error_does_not_stop_universe_scan(tmp_path: Path) -> None:
    symbols = ("BTCUSDT", "BADUSDT")
    report = EarlyDiscoveryService(
        catalog_provider=Catalog(tuple(instrument(symbol) for symbol in symbols)),
        market_provider=PartialFailureProvider(symbols),
        audit_store=JsonlEarlyDiscoveryAuditStore(tmp_path / "audit.jsonl"),
        clock=lambda: NOW,
        maximum_workers=2,
    ).scan()

    assert tuple(item.symbol for item in report.results) == ("BTCUSDT",)
    assert report.errors == 1


def test_full_universe_is_audited_before_top_twenty(tmp_path: Path) -> None:
    symbols = tuple(f"COIN{index}USDT" for index in range(1, 26))
    path = tmp_path / "audit.jsonl"
    report = EarlyDiscoveryService(
        catalog_provider=Catalog(tuple(instrument(symbol) for symbol in symbols)),
        market_provider=Provider(symbols),
        audit_store=JsonlEarlyDiscoveryAuditStore(path),
        inplay_evaluator=Evaluator(),
        clock=lambda: NOW,
        maximum_workers=4,
    ).scan()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert report.universe_size == 25
    assert report.successfully_analyzed == 25
    assert len(lines) == 25
    assert sum(item.is_in_current_top20 for item in report.results) == 20
    maximum_rank = max(
        item.rank_in_current_inplay_universe or 0 for item in report.results
    )
    assert maximum_rank == 25
    highest = next(item for item in report.results if item.symbol == "COIN25USDT")
    assert highest.current_inplay_score == 25.0
    assert highest.current_inplay_direction == "ЛОНГ"
    assert highest.current_inplay_display_status == "ЛОНГ"


def test_24h_growth_does_not_increase_discovery_score(tmp_path: Path) -> None:
    calm = scan_one(tmp_path / "calm", change_24h=0.0)
    extended = scan_one(tmp_path / "extended", change_24h=14.0)

    assert extended.discovery_score == calm.discovery_score


def test_24h_fifteen_percent_blocks_ready_candidate(tmp_path: Path) -> None:
    result = scan_one(tmp_path, change_24h=15.0)

    assert result.discovery_stage is DiscoveryStage.LATE


def test_24h_thirty_percent_is_do_not_chase(tmp_path: Path) -> None:
    result = scan_one(tmp_path, change_24h=30.0)

    assert result.discovery_stage is DiscoveryStage.DO_NOT_CHASE


def test_fresh_5m_breakout_changes_stage_without_new_1h_candle(
    tmp_path: Path,
) -> None:
    quiet = scan_one(tmp_path / "quiet", breakout_age=None)
    fresh = scan_one(tmp_path / "fresh", breakout_age=0)

    assert fresh.discovery_score > quiet.discovery_score
    assert fresh.breakout_direction is MarketDirection.UP
    assert fresh.discovery_stage is DiscoveryStage.READY_CANDIDATE


def test_breakout_age_uses_completed_5m_bars(tmp_path: Path) -> None:
    result = scan_one(tmp_path, breakout_age=4)

    assert result.breakout_age_5m_bars == 4


def test_distance_over_two_atr_blocks_ready_candidate(tmp_path: Path) -> None:
    result = scan_one(tmp_path, breakout_age=0, breakout_price=130.0)

    assert result.distance_from_breakout_atr is not None
    assert abs(result.distance_from_breakout_atr) > 2.0
    assert result.discovery_stage is not DiscoveryStage.READY_CANDIDATE


def test_wide_spread_blocks_ready_candidate(tmp_path: Path) -> None:
    result = scan_one(tmp_path, spread_pct=0.3)

    assert result.discovery_stage is not DiscoveryStage.READY_CANDIDATE


def test_market_direction_is_independent_from_stage(tmp_path: Path) -> None:
    result = scan_one(tmp_path, change_24h=30.0)

    assert result.market_direction is MarketDirection.UP
    assert result.discovery_stage is DiscoveryStage.DO_NOT_CHASE


def test_corrupt_last_jsonl_line_does_not_block_append(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    path.write_text("{broken", encoding="utf-8")

    scan_one(tmp_path)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "{broken"
    assert json.loads(lines[-1])["symbol"] == "BTCUSDT"


def test_audit_prunes_rows_older_than_seven_days(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    old = NOW - timedelta(days=8)
    path.write_text(
        json.dumps({"scanned_at": old.isoformat(), "symbol": "OLDUSDT"}) + "\n",
        encoding="utf-8",
    )

    scan_one(tmp_path)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert all("OLDUSDT" not in line for line in lines)


def test_early_discovery_does_not_touch_notification_state(tmp_path: Path) -> None:
    notification_path = tmp_path / "inplay_notifications.json"
    original = '{"version":2,"records":{}}'
    notification_path.write_text(original, encoding="utf-8")

    scan_one(tmp_path)

    assert notification_path.read_text(encoding="utf-8") == original

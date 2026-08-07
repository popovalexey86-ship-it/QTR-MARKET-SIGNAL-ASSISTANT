import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.derivatives.intelligence import DerivativesIntelligence
from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.inplay.audit import InPlayAuditCandidate, ScanSource
from market_signal_assistant.inplay.models import (
    CatalogInstrument,
    InPlayDirection,
    ListingStatus,
)
from market_signal_assistant.inplay.service import (
    INPLAY_MIN_SCORE,
    InPlayService,
    TimingAuditor,
)
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)
from market_signal_assistant.providers import MarketDataError

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def catalog_item(
    symbol: str,
    *,
    quote: str = "USDT",
    status: str = "Trading",
    turnover: float = 100_000_000,
    bid: float = 100,
    ask: float = 100.1,
    symbol_type: str = "",
    contract_type: str = "LinearPerpetual",
    settle_coin: str = "USDT",
    pre_listing: bool = False,
) -> CatalogInstrument:
    base_coin = symbol.removesuffix(quote)
    return CatalogInstrument(
        symbol,
        quote,
        status,
        turnover,
        bid,
        ask,
        base_coin,
        settle_coin,
        contract_type,
        symbol_type,
        pre_listing,
    )


def market_series(
    symbol: str,
    *,
    count: int = 60,
    final_close: float = 108,
    final_volume: float = 250,
    range_size: float = 2,
) -> MarketSeries:
    candles: list[Candle] = []
    for index in range(count):
        close = 100.0 if index < count - 1 else final_close
        high = close + range_size / 2
        low = close - range_size / 2
        candles.append(
            Candle(
                NOW - timedelta(hours=count - index),
                close,
                high,
                low,
                close,
                100.0 if index < count - 1 else final_volume,
            )
        )
    return MarketSeries(
        Instrument(symbol, AssetClass.CRYPTO),
        "1h",
        tuple(candles),
    )


def technical_signal(
    symbol: str,
    direction: SignalDirection,
    *,
    score: float = 80,
    confidence: float = 80,
    confirmations: int = 3,
) -> MarketSignal:
    return MarketSignal(
        instrument=Instrument(symbol, AssetClass.CRYPTO),
        interval="1h",
        timestamp=NOW,
        direction=direction,
        score=score,
        confidence=confidence,
        confirmations=confirmations,
        conflicts=0,
        price=100,
        evidence=(
            SignalEvidence("trend", direction, 25, "test evidence"),
        ),
    )


class Catalog:
    def __init__(self, items: tuple[CatalogInstrument, ...]) -> None:
        self.items = items

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        return self.items


class BlockingCatalog(Catalog):
    def __init__(self, items: tuple[CatalogInstrument, ...]) -> None:
        super().__init__(items)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.active = 0
        self.maximum_active = 0
        self.guard = threading.Lock()

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        with self.guard:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        self.entered.set()
        self.release.wait(timeout=2)
        with self.guard:
            self.active -= 1
        return self.items


class Markets:
    def __init__(self, values: Mapping[str, MarketSeries | Exception]) -> None:
        self.values = dict(values)
        self.loaded: list[str] = []

    def load(
        self, instrument: Instrument, interval: str, limit: int
    ) -> MarketSeries:
        del interval, limit
        self.loaded.append(instrument.symbol)
        value = self.values[instrument.symbol]
        if isinstance(value, Exception):
            raise value
        return value


class Analyzer:
    def __init__(self, signals: dict[str, MarketSignal | None]) -> None:
        self.signals = signals

    def analyze(self, series: MarketSeries) -> MarketSignal | None:
        return self.signals.get(series.instrument.symbol)


class Listings:
    def __init__(self, new_symbols: frozenset[str] = frozenset()) -> None:
        self.new_symbols = new_symbols

    def observe(
        self, symbols: tuple[str, ...], observed_at: datetime
    ) -> tuple[ListingStatus, ...]:
        return tuple(
            ListingStatus(
                symbol,
                observed_at - timedelta(hours=2),
                symbol in self.new_symbols,
                10.0 if symbol in self.new_symbols else 0.0,
            )
            for symbol in symbols
        )


class Derivatives:
    name = "test"

    def collect(self, symbol: str) -> DerivativesSnapshot:
        return DerivativesSnapshot(
            provider="test",
            symbol=symbol,
            as_of=NOW,
            funding_rate=0,
            open_interest=1_000_000,
            open_interest_change=0.03,
            price_change=0.02,
            volume_change=0.3,
        )


class AuditRecorder:
    def __init__(self) -> None:
        self.calls: list[
            tuple[tuple[InPlayAuditCandidate, ...], datetime, ScanSource]
        ] = []

    def record(
        self,
        candidates: tuple[InPlayAuditCandidate, ...],
        scanned_at: datetime,
        *,
        scan_source: ScanSource,
    ) -> None:
        self.calls.append((candidates, scanned_at, scan_source))


class FailingAudit:
    def record(
        self,
        candidates: tuple[InPlayAuditCandidate, ...],
        scanned_at: datetime,
        *,
        scan_source: ScanSource,
    ) -> None:
        del candidates, scanned_at, scan_source
        raise OSError("audit unavailable")


def service(
    catalog: tuple[CatalogInstrument, ...],
    markets: Mapping[str, MarketSeries | Exception],
    signals: dict[str, MarketSignal | None] | None = None,
    *,
    new_symbols: frozenset[str] = frozenset(),
    timing_auditor: TimingAuditor | None = None,
) -> InPlayService:
    return InPlayService(
        catalog_provider=Catalog(catalog),
        market_provider=Markets(markets),
        analyzer=Analyzer(signals or {}),
        listing_tracker=Listings(new_symbols),
        clock=lambda: NOW,
        timing_auditor=timing_auditor,
    )


def test_timing_audit_observes_pre_filter_candidate_without_changing_report() -> None:
    catalog = (catalog_item("BTCUSDT"),)
    markets = {"BTCUSDT": market_series("BTCUSDT")}
    baseline = service(catalog, markets).scan()
    recorder = AuditRecorder()

    audited = service(catalog, markets, timing_auditor=recorder).scan()

    assert audited == baseline
    assert len(recorder.calls) == 1
    assert recorder.calls[0][0][0].result == audited.results[0]
    assert recorder.calls[0][1] == NOW
    assert recorder.calls[0][2] == "manual"


def test_timing_audit_failure_does_not_stop_inplay_scan() -> None:
    report = service(
        (catalog_item("BTCUSDT"),),
        {"BTCUSDT": market_series("BTCUSDT")},
        timing_auditor=FailingAudit(),
    ).scan()

    assert tuple(item.symbol for item in report.results) == ("BTCUSDT",)


def test_manual_and_shadow_audit_scans_do_not_run_in_parallel() -> None:
    catalog = BlockingCatalog((catalog_item("BTCUSDT"),))
    inplay = InPlayService(
        catalog_provider=catalog,
        market_provider=Markets({"BTCUSDT": market_series("BTCUSDT")}),
        analyzer=Analyzer({}),
        listing_tracker=Listings(),
        clock=lambda: NOW,
    )
    second_attempted = threading.Event()

    def scan(source: ScanSource) -> None:
        if source == "timing_audit_auto":
            second_attempted.set()
        inplay.scan(scan_source=source)

    manual = threading.Thread(target=scan, args=("manual",))
    shadow = threading.Thread(target=scan, args=("timing_audit_auto",))
    manual.start()
    assert catalog.entered.wait(timeout=1)
    shadow.start()
    assert second_attempted.wait(timeout=1)
    assert catalog.maximum_active == 1
    catalog.release.set()
    manual.join(timeout=2)
    shadow.join(timeout=2)

    assert not manual.is_alive()
    assert not shadow.is_alive()
    assert catalog.maximum_active == 1


def test_inplay_results_are_sorted_by_separate_score_and_limited_to_ten() -> None:
    symbols = tuple(f"COIN{index}USDT" for index in range(12))
    catalog = tuple(catalog_item(symbol) for symbol in symbols)
    markets: dict[str, MarketSeries | Exception] = {
        symbol: market_series(symbol, final_volume=150 + index * 20)
        for index, symbol in enumerate(symbols)
    }

    report = service(catalog, markets).scan()

    assert len(report.results) == 10
    scores = tuple(item.inplay_score for item in report.results)
    assert scores == tuple(sorted(scores, reverse=True))


def test_inplay_minimum_output_score_is_fifty() -> None:
    assert INPLAY_MIN_SCORE == 50.0
    report = service(
        (catalog_item("LOWUSDT"),),
        {
            "LOWUSDT": market_series(
                "LOWUSDT", final_close=103, final_volume=150, range_size=1
            )
        },
    ).scan()

    assert report.results == ()


def test_inplay_can_return_fewer_than_ten_results() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    report = service(
        tuple(catalog_item(symbol) for symbol in symbols),
        {symbol: market_series(symbol) for symbol in symbols},
    ).scan()

    assert len(report.results) == 3


def test_inplay_returns_empty_when_every_activity_score_is_below_fifty() -> None:
    symbols = ("LOW1USDT", "LOW2USDT")
    report = service(
        tuple(catalog_item(symbol) for symbol in symbols),
        {
            symbol: market_series(
                symbol, final_close=103, final_volume=150, range_size=1
            )
            for symbol in symbols
        },
    ).scan()

    assert report.results == ()


def test_inplay_excludes_invalid_non_usdt_inactive_and_illiquid_items() -> None:
    catalog = (
        catalog_item("GOODUSDT"),
        catalog_item("GOODUSDT", turnover=200_000_000),
        catalog_item("ETHUSDC", quote="USDC", settle_coin="USDC"),
        catalog_item("INACTIVEUSDT", status="PreLaunch"),
        catalog_item("ILLIQUIDUSDT", turnover=1000),
        catalog_item("WIDEUSDT", bid=100, ask=102),
        catalog_item("BROKENUSDT"),
        catalog_item("SHORTUSDT"),
    )
    markets: dict[str, MarketSeries | Exception] = {
        "GOODUSDT": market_series("GOODUSDT"),
        "BROKENUSDT": MarketDataError("offline"),
        "SHORTUSDT": market_series("SHORTUSDT", count=10),
    }

    report = service(catalog, markets).scan()

    assert tuple(item.symbol for item in report.results) == ("GOODUSDT",)


def test_inplay_excludes_tradfi_metadata_and_keeps_crypto_usdt() -> None:
    catalog = (
        catalog_item("BTCUSDT", symbol_type=""),
        catalog_item("NEWCRYPTOUSDT", symbol_type="innovation"),
        catalog_item("AAPLUSDT", symbol_type="stock"),
        catalog_item("XAUUSDT", symbol_type="commodity"),
        catalog_item("EURUSDT", symbol_type="forex"),
        catalog_item("TOKENIZEDUSDT", symbol_type="xstocks"),
    )
    markets = {
        "BTCUSDT": market_series("BTCUSDT"),
        "NEWCRYPTOUSDT": market_series("NEWCRYPTOUSDT"),
    }

    report = service(catalog, markets).scan()

    assert {item.symbol for item in report.results} == {
        "BTCUSDT",
        "NEWCRYPTOUSDT",
    }


def test_inplay_direction_supports_long_short_and_watch() -> None:
    symbols = ("LONGUSDT", "SHORTUSDT", "WATCHUSDT")
    catalog = tuple(catalog_item(symbol) for symbol in symbols)
    markets = {symbol: market_series(symbol) for symbol in symbols}
    signals = {
        "LONGUSDT": technical_signal("LONGUSDT", SignalDirection.BULLISH),
        "SHORTUSDT": technical_signal("SHORTUSDT", SignalDirection.BEARISH),
        "WATCHUSDT": None,
    }

    report = service(catalog, markets, signals).scan()
    directions = {item.symbol: item.direction for item in report.results}

    assert directions == {
        "LONGUSDT": InPlayDirection.LONG,
        "SHORTUSDT": InPlayDirection.SHORT,
        "WATCHUSDT": InPlayDirection.WATCH,
    }


def test_inplay_score_does_not_use_directional_final_score_as_activity() -> None:
    catalog = (catalog_item("ACTIVEUSDT"), catalog_item("SIGNALUSDT"))
    markets = {
        "ACTIVEUSDT": market_series(
            "ACTIVEUSDT", final_close=115, final_volume=400, range_size=8
        ),
        "SIGNALUSDT": market_series(
            "SIGNALUSDT", final_close=101, final_volume=170, range_size=1
        ),
    }
    signals = {
        "ACTIVEUSDT": None,
        "SIGNALUSDT": technical_signal(
            "SIGNALUSDT", SignalDirection.BULLISH, score=100, confidence=100
        ),
    }

    report = service(catalog, markets, signals).scan()

    assert report.results[0].symbol == "ACTIVEUSDT"
    signal_result = next(
        item for item in report.results if item.symbol == "SIGNALUSDT"
    )
    assert signal_result.inplay_score != signal_result.directional_score


def test_new_listing_with_short_history_is_watch_and_bonus_cannot_make_direction(
) -> None:
    symbol = "NEWUSDT"
    report = service(
        (catalog_item(symbol),),
        {symbol: market_series(symbol, count=10, final_close=110, final_volume=400)},
        {symbol: technical_signal(symbol, SignalDirection.BULLISH)},
        new_symbols=frozenset({symbol}),
    ).scan()

    assert len(report.results) == 1
    result = report.results[0]
    assert result.is_new_listing is True
    assert result.listing_bonus == 10
    assert result.direction is InPlayDirection.WATCH
    assert result.directional_score is None
    assert any("Короткая история" in warning for warning in result.warnings)


def test_inplay_returns_no_results_when_no_candidate_is_eligible() -> None:
    report = service(
        (catalog_item("BADUSDT", turnover=1),),
        {},
    ).scan()

    assert report.results == ()


def test_inactive_and_illiquid_new_listings_are_excluded() -> None:
    symbols = frozenset({"INACTIVEUSDT", "ILLIQUIDUSDT"})
    report = service(
        (
            catalog_item("INACTIVEUSDT", status="PreLaunch"),
            catalog_item("ILLIQUIDUSDT", turnover=1000),
        ),
        {},
        new_symbols=symbols,
    ).scan()

    assert report.results == ()


def test_listing_bonus_does_not_displace_clearly_more_active_coin() -> None:
    catalog = (catalog_item("ACTIVEUSDT"), catalog_item("NEWUSDT"))
    markets = {
        "ACTIVEUSDT": market_series(
            "ACTIVEUSDT", final_close=115, final_volume=400, range_size=8
        ),
        "NEWUSDT": market_series(
            "NEWUSDT", final_close=103, final_volume=160, range_size=1
        ),
    }

    report = service(
        catalog,
        markets,
        new_symbols=frozenset({"NEWUSDT"}),
    ).scan()

    assert report.results[0].symbol == "ACTIVEUSDT"


def test_successful_derivatives_context_adds_activity_but_remains_separate() -> None:
    symbol = "BTCUSDT"
    catalog = Catalog((catalog_item(symbol),))
    markets = Markets({symbol: market_series(symbol)})
    analyzer = Analyzer(
        {symbol: technical_signal(symbol, SignalDirection.BULLISH)}
    )
    baseline = InPlayService(
        catalog_provider=catalog,
        market_provider=markets,
        analyzer=analyzer,
        listing_tracker=Listings(),
        clock=lambda: NOW,
    ).scan().results[0]
    enriched = InPlayService(
        catalog_provider=catalog,
        market_provider=markets,
        analyzer=analyzer,
        listing_tracker=Listings(),
        derivatives_provider=Derivatives(),
        derivatives_intelligence=DerivativesIntelligence(),
        clock=lambda: NOW,
    ).scan().results[0]

    assert enriched.inplay_score > baseline.inplay_score
    assert enriched.directional_score != enriched.inplay_score
    assert any("Деривативы" in reason for reason in enriched.reasons)


@pytest.mark.parametrize(
    ("final_close", "expected"),
    (
        (115, "Движение уже значительно реализовано; повышен риск отката."),
        (
            130,
            "Резкое движение уже состоялось; высокий риск позднего входа "
            "и сильного отката.",
        ),
        (85, "Движение уже значительно реализовано; повышен риск отката."),
        (
            70,
            "Резкое движение уже состоялось; высокий риск позднего входа "
            "и сильного отката.",
        ),
    ),
    ids=("plus-15", "plus-30", "minus-15", "minus-30"),
)
def test_absolute_price_move_adds_priority_warning(
    final_close: float,
    expected: str,
) -> None:
    symbol = "MOVEUSDT"
    report = service(
        (catalog_item(symbol),),
        {
            symbol: market_series(
                symbol,
                final_close=final_close,
                final_volume=400,
                range_size=8,
            )
        },
    ).scan()

    assert report.results[0].warnings[0] == expected

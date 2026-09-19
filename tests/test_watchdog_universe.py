from datetime import UTC, datetime, timedelta

from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.watchdog.universe import (
    DynamicUniverse,
    UniverseTier,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def instrument(
    index: int,
    *,
    turnover: float = 10_000_000.0,
    launch_time: datetime | None = None,
) -> CatalogInstrument:
    return CatalogInstrument(
        symbol=f"COIN{index}USDT",
        quote_coin="USDT",
        status="Trading",
        turnover_24h=turnover,
        bid=100.0,
        ask=100.1,
        base_coin=f"COIN{index}",
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type="",
        is_pre_listing=False,
        launch_time=launch_time,
    )


def test_universe_has_no_top_fifty_cap() -> None:
    instruments = tuple(instrument(index) for index in range(125))
    counts = {item.symbol: 20 for item in instruments}

    snapshot = DynamicUniverse().build(
        instruments,
        observed_at=NOW,
        baseline_samples=counts,
    )

    assert len(snapshot.eligible) == 125
    assert all(item.tier is UniverseTier.STANDARD for item in snapshot.eligible)


def test_new_or_under_sampled_market_stays_in_cold_start() -> None:
    established = instrument(1, turnover=150_000_000.0)
    new = instrument(2, launch_time=NOW - timedelta(hours=2))

    snapshot = DynamicUniverse().build(
        (established, new),
        observed_at=NOW,
        baseline_samples={established.symbol: 20, new.symbol: 100},
    )
    entries = {item.instrument.symbol: item for item in snapshot.eligible}

    assert entries[established.symbol].tier is UniverseTier.CORE
    assert entries[new.symbol].tier is UniverseTier.COLD_START
    assert entries[new.symbol].is_new_market is True


def test_quality_filters_reject_untradeable_instruments() -> None:
    bad = instrument(1, turnover=100.0)

    snapshot = DynamicUniverse().build((bad,), observed_at=NOW)

    assert snapshot.eligible == ()
    assert snapshot.rejected[0][1] == ("turnover_below_minimum",)

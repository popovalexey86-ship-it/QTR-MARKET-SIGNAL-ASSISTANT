from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.dynamic_targets import (
    DynamicTargetSettings,
    DynamicVerifiedTargetManager,
    TargetExclusionReason,
)
from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    VerifiedSetupRecord,
)

NOW = datetime(2026, 8, 18, 12, tzinfo=UTC)


class FakeProvider:
    def __init__(self, records: tuple[VerifiedSetupRecord, ...] = ()) -> None:
        self.records = records
        self.calls = 0

    def latest_records(self) -> tuple[VerifiedSetupRecord, ...]:
        self.calls += 1
        return self.records


def record(
    symbol: str,
    *,
    state: str = "FORMING",
    confidence: float = 70.0,
    observed_at: datetime = NOW,
    source_direction: str = "UP",
    setup_direction: str = "UP",
    atr: float = 1.0,
    liquidity: bool = True,
    volume: bool = True,
    volatility: bool = True,
) -> VerifiedSetupRecord:
    return VerifiedSetupRecord(
        symbol=symbol,
        observed_at=observed_at,
        source_direction=source_direction,
        setup_direction=setup_direction,
        market_price=100.0,
        atr=atr,
        trigger_price=100.0,
        invalidation_price=98.0,
        local_range_low=99.0,
        local_range_high=101.0,
        setup_state=state,
        setup_confidence=confidence,
        volume_confirmation=volume,
        volatility_confirmation=volatility,
        liquidity_ok=liquidity,
    )


def manager(
    provider: FakeProvider,
    *,
    maximum: int = 5,
) -> DynamicVerifiedTargetManager:
    return DynamicVerifiedTargetManager(
        provider,
        DynamicTargetSettings(max_active_symbols=maximum),
    )


def test_empty_audit_fails_closed() -> None:
    result = manager(FakeProvider()).refresh(at=NOW)
    assert result.active_symbols == ()
    assert result.eligible == ()


@pytest.mark.parametrize("state", ("CANCELLED", "LATE"))
def test_cancelled_and_late_are_excluded(state: str) -> None:
    result = manager(FakeProvider((record("BTCUSDT", state=state),))).refresh(
        at=NOW
    )
    assert result.active_symbols == ()
    assert result.exclusions[0].reason.value == state


def test_stale_and_incomplete_records_are_excluded() -> None:
    provider = FakeProvider(
        (
            record("STALEUSDT", observed_at=NOW - timedelta(minutes=16)),
            record("BROKENUSDT", atr=0.0),
        )
    )
    result = manager(provider).refresh(at=NOW)
    reasons = {item.symbol: item.reason for item in result.exclusions}
    assert reasons == {
        "BROKENUSDT": TargetExclusionReason.INCOMPLETE,
        "STALEUSDT": TargetExclusionReason.STALE,
    }


@pytest.mark.parametrize(
    "state",
    ("FORMING", "CONFIRMING", "READY_TO_CONSIDER"),
)
def test_explicit_watch_states_are_eligible(state: str) -> None:
    result = manager(
        FakeProvider((record("BTCUSDT", state=state),))
    ).refresh(at=NOW)
    assert result.desired_symbols == ("BTCUSDT",)


def test_ranking_is_deterministic_and_does_not_use_scalper_score() -> None:
    provider = FakeProvider(
        (
            record("FORMUSDT", state="FORMING", confidence=100.0),
            record("CONFUSDT", state="CONFIRMING", confidence=100.0),
            record("READYBUSDT", state="READY_TO_CONSIDER", confidence=80.0),
            record("READYAUSDT", state="READY_TO_CONSIDER", confidence=80.0),
        )
    )
    result = manager(provider).refresh(at=NOW)
    assert result.desired_symbols == (
        "READYAUSDT",
        "READYBUSDT",
        "CONFUSDT",
        "FORMUSDT",
    )


def test_top_n_and_tie_break_use_symbol_ascending() -> None:
    provider = FakeProvider(
        tuple(
            record(f"{symbol}USDT", state="READY_TO_CONSIDER", confidence=90.0)
            for symbol in ("EEE", "DDD", "CCC", "BBB", "AAA", "FFF")
        )
    )
    result = manager(provider, maximum=5).refresh(at=NOW)
    assert result.desired_symbols == (
        "AAAUSDT",
        "BBBUSDT",
        "CCCUSDT",
        "DDDUSDT",
        "EEEUSDT",
    )
    assert result.exclusions[-1].reason is TargetExclusionReason.OUTSIDE_TOP_N


def test_replacement_and_unchanged_set_do_not_churn() -> None:
    provider = FakeProvider(
        (
            record("BTCUSDT", state="READY_TO_CONSIDER"),
            record("ETHUSDT", state="CONFIRMING"),
        )
    )
    target_manager = manager(provider, maximum=1)
    first = target_manager.refresh(at=NOW)
    unchanged = target_manager.refresh(at=NOW + timedelta(seconds=30))
    provider.records = (
        record(
            "ETHUSDT",
            state="READY_TO_CONSIDER",
            confidence=100.0,
            observed_at=NOW + timedelta(seconds=31),
        ),
    )
    replaced = target_manager.refresh(at=NOW + timedelta(seconds=31))

    assert first.added == ("BTCUSDT",)
    assert unchanged.added == unchanged.removed == ()
    assert replaced.replaced == (("BTCUSDT", "ETHUSDT"),)


def test_freshness_rollover_removes_symbol() -> None:
    target_manager = manager(FakeProvider((record("BTCUSDT"),)))
    assert target_manager.refresh(at=NOW).active_symbols == ("BTCUSDT",)
    expired = target_manager.refresh(at=NOW + timedelta(minutes=16))
    assert expired.active_symbols == ()
    assert expired.removed == ("BTCUSDT",)


@pytest.mark.parametrize("stage", ("WAITING_ENTRY", "OPEN", "TP1_HIT"))
def test_active_trade_symbol_is_protected(stage: str) -> None:
    del stage
    provider = FakeProvider((record("BTCUSDT", state="CANCELLED"),))
    result = manager(provider).refresh(
        at=NOW,
        protected_symbols=("BTCUSDT",),
    )
    assert result.desired_symbols == ()
    assert result.active_symbols == ("BTCUSDT",)
    assert result.protected_trade_symbols == ("BTCUSDT",)


def test_terminal_trade_can_unsubscribe_after_protection_is_removed() -> None:
    target_manager = manager(FakeProvider())
    protected = target_manager.refresh(at=NOW, protected_symbols=("BTCUSDT",))
    terminal = target_manager.refresh(at=NOW + timedelta(seconds=30))
    assert protected.added == ("BTCUSDT",)
    assert terminal.removed == ("BTCUSDT",)


def test_hundred_unchanged_refreshes_keep_bounded_state() -> None:
    provider = FakeProvider(
        tuple(record(f"S{index:03d}USDT") for index in range(120))
    )
    target_manager = manager(provider, maximum=5)
    first = target_manager.refresh(at=NOW)
    for index in range(100):
        current = target_manager.refresh(
            at=NOW + timedelta(seconds=index),
        )
        assert current.added == current.removed == ()
    metrics = target_manager.metrics()
    assert provider.calls == 101
    assert len(first.desired_symbols) == 5
    assert metrics.target_refreshes == 101
    assert metrics.active_symbols == 5
    assert metrics.symbols_added == 5
    assert metrics.symbols_removed == 0
    assert metrics.aggregate_state_size <= 125



def test_dynamic_settings_are_opt_in_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "QTR_SCALPER_V2_DYNAMIC_TARGETS_ENABLED",
        "QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS",
        "QTR_SCALPER_V2_TARGET_REFRESH_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = DynamicTargetSettings.from_environment()

    assert not settings.enabled
    assert settings.max_active_symbols == 5
    assert settings.refresh_seconds == 30.0


def test_dynamic_settings_read_explicit_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QTR_SCALPER_V2_DYNAMIC_TARGETS_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS", "3")
    monkeypatch.setenv("QTR_SCALPER_V2_TARGET_REFRESH_SECONDS", "45")

    settings = DynamicTargetSettings.from_environment()

    assert settings.enabled
    assert settings.max_active_symbols == 3
    assert settings.refresh_seconds == 45.0

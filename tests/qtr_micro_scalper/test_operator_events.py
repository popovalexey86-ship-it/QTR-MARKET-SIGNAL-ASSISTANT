from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta

import pytest
from test_control_plane import snapshot
from test_orchestrator import activate, analysis_input, orchestrator
from test_shadow_runtime import bar
from test_snapshot import NOW

from market_signal_assistant.qtr_micro_scalper.control_models import (
    snapshot_from_bytes,
    snapshot_to_bytes,
)
from market_signal_assistant.qtr_micro_scalper.operator_events import RecentEventFeed
from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTradeStage
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEvent,
    ShadowRuntimeEventType,
)


def opened(tmp_path):
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    created = coordinator.analyze(analysis_input())
    assert created.trade is not None
    update = coordinator.process_bars((bar(1),))[0]
    assert update.error is None
    return coordinator, update.trade


def test_existing_lifecycle_captured_once_and_survives_repeated_projections(tmp_path):
    coordinator, trade = opened(tmp_path)
    assert trade is not None
    coordinator.process_bars((bar(3, high=102.3, low=100, close=102),))
    coordinator.process_bars((bar(5, open_price=102, high=104.5, low=101, close=104),))
    events, failures = coordinator.control_recent_events()
    assert failures == 0
    assert [e.event_type for e in events] == ["ENTRY_CANDIDATE", "OPEN", "CLOSED"]
    assert events[-1].status == "WIN" and events[-1].result_r is not None
    assert events[-1].reason == "TP2"
    for _ in range(100):
        assert coordinator.control_recent_events() == (events, 0)
    for _ in range(5):
        data = snapshot_to_bytes(replace(snapshot(tmp_path), recent_events=events))
        assert len(data) <= 128 * 1024
        assert snapshot_from_bytes(data).recent_events == events


def test_expired_is_existing_not_triggered_outcome_without_fabricated_close(tmp_path):
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    result = coordinator.analyze(analysis_input())
    assert result.trade is not None
    coordinator.process_bars((bar(61, open_price=104, high=105, low=103, close=104),))
    events, _ = coordinator.control_recent_events()
    event = events[-1]
    assert event.event_type == "EXPIRED" and event.status == "NOT_TRIGGERED"
    assert (
        event.entry_price is None
        and event.exit_price is None
        and event.result_r is None
    )


def test_loss_uses_runtime_result_and_close_reason(tmp_path):
    coordinator, _ = opened(tmp_path)
    result = coordinator.process_bars(
        (bar(3, open_price=100, high=100, low=95, close=96),)
    )[0]
    assert result.trade is not None
    event = coordinator.control_recent_events()[0][-1]
    assert event.event_type == "CLOSED" and event.status == "LOSS"
    assert event.result_r == result.trade.realized_r and event.reason == "STOP"


def test_fixed_ring_stable_identity_order_and_no_history_reconstruction(
    tmp_path, monkeypatch
):
    coordinator, trade = opened(tmp_path)
    assert trade is not None
    feed = RecentEventFeed()
    for i in range(1000):
        event = ShadowRuntimeEvent(
            str(i),
            ShadowRuntimeEventType.OPEN,
            NOW + timedelta(seconds=i),
            trade.trade_id,
            trade.symbol,
            ShadowTradeStage.OPEN,
            trade.entry_price,
            0,
            "opened",
        )
        feed.capture(trade, (event,))
        feed.capture(trade, (event,))
    events = feed.snapshot()
    assert len(feed._events) == len(events) == 20
    assert [e.order_key for e in events] == sorted(e.order_key for e in events)
    fresh = RecentEventFeed()
    fresh.capture(trade, (event,))
    assert fresh.snapshot()[0].event_id == events[-1].event_id
    from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
        ShadowTradeJournal,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("journal recovery")

    monkeypatch.setattr(ShadowTradeJournal, "records", forbidden)
    from market_signal_assistant.qtr_micro_scalper.shadow_runtime import ShadowRuntime

    assert ShadowRuntime().operator_events() == ((), 0)


def test_telemetry_exception_never_changes_trade_transition(tmp_path, monkeypatch):
    coordinator = orchestrator(tmp_path)
    activate(coordinator)

    def failure(*args, **kwargs):
        raise RuntimeError("operator telemetry unavailable")

    monkeypatch.setattr(RecentEventFeed, "capture", failure)
    created = coordinator.analyze(analysis_input())
    assert created.successful and created.trade is not None
    result = coordinator.process_bars((bar(1),))[0]
    assert result.error is None and result.trade.stage is ShadowTradeStage.OPEN
    assert coordinator.control_recent_events() == ((), 2)
    assert coordinator.metrics().errors == 0


def test_v2_reader_accepts_exact_v1_and_old_reader_rejects_v2(tmp_path):
    current = json.loads(snapshot_to_bytes(snapshot(tmp_path)))
    assert current["schema_version"] == 2
    legacy = dict(current)
    legacy["schema_version"] = 1
    legacy["version"] = dict(legacy["version"], schema_version=1)
    legacy.pop("recent_events")
    legacy["resources"].pop("recent_event_failures")
    parsed = snapshot_from_bytes(json.dumps(legacy).encode())
    assert parsed.schema_version == 1 and parsed.recent_events == ()
    import subprocess
    import sys
    import types

    text = subprocess.check_output(
        [
            "git",
            "show",
            "3358fa0420fce2e34007f1ef29c53e6f520ca37b:src/market_signal_assistant/qtr_micro_scalper/control_models.py",
        ],
        text=True,
    )
    module = types.ModuleType("_baseline_control_model")
    sys.modules[module.__name__] = module
    exec(compile(text, "<V1 reader>", "exec"), module.__dict__)
    with pytest.raises(ValueError, match="Unsupported"):
        module.snapshot_from_bytes(json.dumps(current).encode())


def test_recent_event_schema_bounds_and_duplicates_fail_closed(tmp_path):
    coordinator, _ = opened(tmp_path)
    event = coordinator.control_recent_events()[0][-1]
    for events in (
        (event, event),
        tuple(replace(event, event_id="operator-" + f"{i:064x}") for i in range(21)),
    ):
        with pytest.raises(ValueError):
            snapshot_to_bytes(replace(snapshot(tmp_path), recent_events=events))


def test_unexpected_operator_projection_exception_does_not_stop_runtime(tmp_path):
    from test_control_plane import plane
    from test_scalper_cli import FakeService, FakeSource, observation

    from market_signal_assistant.qtr_micro_scalper.cli import ShadowCliRunner
    from market_signal_assistant.qtr_micro_scalper.service import ShadowServiceStatus

    service = FakeService(status=ShadowServiceStatus.RUNNING)

    def failure():
        raise KeyError("operator-only failure")

    output = []
    runner = ShadowCliRunner(
        service,
        FakeSource(observation()),
        clock=lambda: NOW,
        output=output.append,
        control_plane=plane(tmp_path),
        control_details=failure,
    )
    runner._print_refresh()
    assert service.status is ShadowServiceStatus.RUNNING and service.stops == 0
    assert any("Control Plane недоступен" in text for text in output)

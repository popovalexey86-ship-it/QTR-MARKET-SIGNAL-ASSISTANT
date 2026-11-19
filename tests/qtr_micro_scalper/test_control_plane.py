from __future__ import annotations

import asyncio
import json
import os
import tracemalloc
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_scalper_cli import NOW, FakeService, health, observation, service_metrics

from market_signal_assistant.qtr_micro_scalper.cli import (
    JournalObservationSource,
    ShadowCliRunner,
)
from market_signal_assistant.qtr_micro_scalper.control_models import (
    MAX_ACTIVE_TRADES,
    MAX_SNAPSHOT_BYTES,
    ActiveTrade,
    Blocker,
    RuntimeDetails,
    snapshot_from_bytes,
    snapshot_to_bytes,
)
from market_signal_assistant.qtr_micro_scalper.control_plane import (
    ScalperControlPlane,
    resolve_code_sha,
)
from market_signal_assistant.qtr_micro_scalper.control_store import (
    ControlSnapshotStore,
    SnapshotState,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import ShadowTradeJournal
from market_signal_assistant.telegram.bot import execute_command
from market_signal_assistant.telegram.formatting import SAFE_MESSAGE_LIMIT
from market_signal_assistant.telegram.scalper_commands import (
    SCALPER_COMMANDS,
    ScalperCommands,
)


class ClockStore(ControlSnapshotStore):
    def read(self, *, now=None):
        return super().read(now=now or NOW)


class ForbiddenScreen:
    def screen(self, request):
        raise AssertionError("Scalper invoked Scanner")


def plane(tmp_path: Path):
    return ScalperControlPlane(ClockStore(tmp_path / "control.json"), code_sha="a" * 40)


def snapshot(tmp_path: Path, *, count: int = 0):
    active = tuple(
        ActiveTrade(
            f"trade-{i}",
            "BTCUSDT",
            "LONG",
            "OPEN",
            80.0,
            100.0,
            99.0,
            101.0,
            102.0,
            NOW,
            0.0,
        )
        for i in range(count)
    )
    return plane(tmp_path).build(
        health(),
        service_metrics(),
        observation(),
        RuntimeDetails(
            active_count=count,
            active=active,
            live_enabled=False,
            holding_enabled=False,
            micro_profit_enabled=True,
            protected_runner_enabled=False,
        ),
        generated_at=NOW,
    )


def test_immutable_model_roundtrip_and_no_lifecycle(tmp_path):
    s = snapshot(tmp_path, count=1)
    assert snapshot_from_bytes(snapshot_to_bytes(s)) == s
    with pytest.raises(FrozenInstanceError):
        s.schema_version = 2
    raw = json.loads(snapshot_to_bytes(s))
    assert set(raw) == {
        "schema_version",
        "generated_at",
        "observed_at",
        "recent_events",
        "stale_after_seconds",
        "runtime",
        "market",
        "decisions",
        "trades",
        "blockers",
        "experiments",
        "resources",
        "version",
    }
    assert "events" not in raw["trades"]["active"][0]
    assert len(snapshot_to_bytes(s)) < 10_000


def test_atomic_replace_and_no_growth(tmp_path, monkeypatch):
    p = plane(tmp_path)
    s = snapshot(tmp_path)
    assert p.publish(s)
    old = p.store.path.read_bytes()
    original_replace = os.replace
    replaced = []

    def checked_replace(source, destination):
        assert Path(source).parent == Path(destination).parent
        assert p.store.path.read_bytes() == old
        assert snapshot_from_bytes(Path(source).read_bytes()) == s
        replaced.append(source)
        original_replace(source, destination)

    monkeypatch.setattr(os, "replace", checked_replace)
    for _ in range(30):
        assert p.publish(s)
        assert p.store.path.read_bytes() == old
    assert len(replaced) == 30
    assert not list(tmp_path.glob(".scalper-control-*"))


@pytest.mark.parametrize(
    "seconds,state", [(0, "FRESH"), (90, "FRESH"), (91, "STALE"), (-6, "FUTURE")]
)
def test_freshness(tmp_path, seconds, state):
    p = plane(tmp_path)
    p.publish(snapshot(tmp_path))
    assert p.store.read(now=NOW + timedelta(seconds=seconds)).state == state


def test_fresh_publication_cannot_hide_old_observation(tmp_path):
    p = plane(tmp_path)
    p.publish(replace(snapshot(tmp_path), observed_at=NOW - timedelta(seconds=91)))
    assert p.store.read(now=NOW).state is SnapshotState.STALE
    assert (
        "последние данные"
        in ScalperCommands(p.store).execute("scalper_status", now=NOW)[0]
    )


def test_reader_rejects_journal_path_before_open(tmp_path, monkeypatch):
    path = tmp_path / "trades.jsonl"

    def forbidden(*args, **kwargs):
        raise AssertionError("journal opened")

    monkeypatch.setattr(Path, "open", forbidden)
    assert ControlSnapshotStore(path).read(now=NOW).state is SnapshotState.MALFORMED


def test_model_rejects_false_healthy_corruption(tmp_path):
    s = snapshot(tmp_path)
    for changed in (
        replace(s, runtime=replace(s.runtime, uptime_seconds=-1)),
        replace(s, version=replace(s.version, code_sha="secret-not-a-sha")),
        replace(s, trades=replace(s.trades, win_rate=101)),
        replace(s, experiments=(s.experiments[0],) * 3),
    ):
        with pytest.raises(ValueError):
            snapshot_to_bytes(changed)


@pytest.mark.parametrize(
    "damage,state",
    [
        ("missing", "MISSING"),
        ("json", "MALFORMED"),
        ("utf8", "MALFORMED"),
        ("schema", "UNSUPPORTED"),
        ("boolschema", "MALFORMED"),
        ("field", "MALFORMED"),
        ("nan", "MALFORMED"),
        ("naive", "MALFORMED"),
        ("boolean", "MALFORMED"),
        ("huge", "MALFORMED"),
    ],
)
def test_bad_snapshots_fail_closed(tmp_path, damage, state):
    p = plane(tmp_path)
    raw = json.loads(snapshot_to_bytes(snapshot(tmp_path)))
    if damage == "missing":
        assert p.store.read(now=NOW).state == state
        return
    if damage == "schema":
        raw["schema_version"] = 999
    if damage == "boolschema":
        raw["schema_version"] = True
    if damage == "field":
        del raw["runtime"]["ready"]
    if damage == "nan":
        raw["trades"]["average_r"] = float("nan")
    if damage == "naive":
        raw["generated_at"] = "2026-08-16T12:00:00"
    if damage == "boolean":
        raw["runtime"]["ready"] = "yes"
    data = json.dumps(raw).encode()
    if damage == "json":
        data = b"broken"
    if damage == "utf8":
        data = b"\xff"
    if damage == "huge":
        data = b"x" * (MAX_SNAPSHOT_BYTES + 1)
    p.store.path.write_bytes(data)
    assert p.store.read(now=NOW).state == state


def test_invalid_nested_collection_rejected(tmp_path):
    with pytest.raises(ValueError):
        snapshot_to_bytes(snapshot(tmp_path, count=MAX_ACTIVE_TRADES + 1))
    s = snapshot(tmp_path, count=1)
    raw = json.loads(snapshot_to_bytes(s))
    raw["trades"]["active"][0]["events"] = []
    with pytest.raises(ValueError):
        snapshot_from_bytes(json.dumps(raw).encode())


def test_publication_failure_preserves_old_snapshot_and_does_not_crash(
    tmp_path, monkeypatch
):
    p = plane(tmp_path)
    s = snapshot(tmp_path)
    p.publish(s)
    old = p.store.path.read_bytes()

    def failure(*args):
        raise OSError("secret-sensitive-details")

    monkeypatch.setattr(os, "replace", failure)
    assert not p.publish(s)
    assert p.last_publish_error and "secret" not in p.last_publish_error
    assert p.store.path.read_bytes() == old
    assert not list(tmp_path.glob(".scalper-control-*"))


def test_publication_cannot_overwrite_journal(tmp_path, monkeypatch):
    path = tmp_path / "evidence.jsonl"
    path.write_bytes(b"untouched")
    with pytest.raises(ValueError):
        ControlSnapshotStore(path).publish(snapshot(tmp_path))
    assert path.read_bytes() == b"untouched"
    other = tmp_path / "custom.json"
    other.write_bytes(b"untouched")
    monkeypatch.setenv("QTR_SCALPER_V2_JOURNAL_PATH", str(other))
    with pytest.raises(ValueError):
        ControlSnapshotStore(other).publish(snapshot(tmp_path))
    assert other.read_bytes() == b"untouched"


@pytest.mark.parametrize("command", sorted(SCALPER_COMMANDS))
def test_namespaced_commands_use_authorized_existing_router(tmp_path, command):
    p = plane(tmp_path)
    p.publish(snapshot(tmp_path))
    result = execute_command(
        "/" + command + "@QTRBot",
        chat_id=7,
        allowed_chat_ids=frozenset({7}),
        service=ForbiddenScreen(),
        scalper_commands=ScalperCommands(p.store),
    )
    assert result.command.name == command
    assert result.messages and all(
        len(m) <= SAFE_MESSAGE_LIMIT for m in result.messages
    )
    assert not any("{" in m for m in result.messages)
    if command == "scalper_active":
        assert result.messages == ("Активных теневых сделок нет.",)
    if command == "scalper_recent":
        assert "пока не опубликованы" in result.messages[0]
    if command == "scalper_version":
        assert "a" * 40 in result.messages[0]


def test_authorization_precedes_any_snapshot_read(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("unauthorized file access")

    monkeypatch.setattr(ControlSnapshotStore, "read", forbidden)
    with pytest.raises(PermissionError):
        execute_command(
            "/scalper_status",
            chat_id=8,
            allowed_chat_ids=frozenset({7}),
            service=ForbiddenScreen(),
        )


@pytest.mark.parametrize(
    "command", ["scalper_status", "scalper_active", "scalper_stats", "scalper_health"]
)
def test_stale_commands_never_present_historical_state_as_current(tmp_path, command):
    p = plane(tmp_path)
    p.publish(snapshot(tmp_path, count=1))
    replies = ScalperCommands(p.store).execute(command, now=NOW + timedelta(seconds=91))
    assert "устарел" in replies[0] and "неизвестно" in replies[0]
    assert "trade-0" not in replies[0]


def test_bounded_output_splits_long_active_list(tmp_path):
    p = plane(tmp_path)
    s = snapshot(tmp_path, count=20)
    trades = tuple(
        replace(t, trade_id=(str(i) + "x" * 280)) for i, t in enumerate(s.trades.active)
    )
    s = replace(
        s,
        trades=replace(s.trades, active=trades),
        blockers=tuple(Blocker("п" * 300, 3) for _ in range(10)),
    )
    p.publish(s)
    result = ScalperCommands(p.store).execute("scalper_active", now=NOW)
    assert len(result) > 1 and all(len(m) <= SAFE_MESSAGE_LIMIT for m in result)


def test_git_unavailable_never_blocks_control_plane(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("git unavailable")

    monkeypatch.setattr("subprocess.run", fail)
    assert resolve_code_sha() == "unknown"


def test_runner_observes_once_then_reuses_result_for_cli_and_final_snapshot(tmp_path):
    class Source:
        calls = 0

        def observe(self, *, generated_at):
            self.calls += 1
            return observation()

    source = Source()
    p = plane(tmp_path)
    output = []
    service = FakeService()
    runner = ShadowCliRunner(
        service,
        source,
        output=output.append,
        clock=lambda: NOW,
        control_plane=p,
        control_details=lambda: RuntimeDetails(active_count=0, live_enabled=False),
    )
    asyncio.run(runner.run(stop_requested=asyncio.Event(), once=True))
    assert source.calls == 1
    assert p.store.read(now=NOW).snapshot.runtime.status == "STOPPED"
    assert any("QTR MICRO SCALPER V2" in x for x in output)


def test_large_history_commands_have_zero_source_reads_and_bounded_memory(
    tmp_path, monkeypatch
):
    decisions = tmp_path / "decisions.jsonl"
    trades = tmp_path / "trades.jsonl"
    # Large source size must be irrelevant to every command.
    with decisions.open("wb") as f:
        f.truncate(64 * 1024**2)
    with trades.open("wb") as f:
        f.truncate(32 * 1024**2)
    p = plane(tmp_path)
    p.publish(snapshot(tmp_path))
    original = Path.open

    def guarded(path, *args, **kwargs):
        assert path not in {decisions, trades}, "Telegram scanned source history"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)

    def forbidden(*args, **kwargs):
        raise AssertionError("journal construction/observation")

    monkeypatch.setattr(ShadowTradeJournal, "__init__", forbidden)
    monkeypatch.setattr(JournalObservationSource, "observe", forbidden)
    adapter = ScalperCommands(p.store)
    tracemalloc.start()
    for _ in range(25):
        for cmd in SCALPER_COMMANDS:
            assert adapter.execute(cmd, now=NOW)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 5 * 1024 * 1024


def test_runtime_unknown_not_falsely_empty_and_errors_do_not_leak(tmp_path):
    p = plane(tmp_path)
    s = p.build(
        replace(
            health(),
            last_error="bot-token=secret-sensitive-details",
            legacy_quarantine_warning="ids",
        ),
        replace(service_metrics(), quarantined_legacy_trades=6),
        observation(),
        RuntimeDetails(),
        generated_at=NOW,
    )
    assert "secret" not in snapshot_to_bytes(s).decode()
    p.publish(s)
    assert (
        "недоступны" in ScalperCommands(p.store).execute("scalper_active", now=NOW)[0]
    )
    assert "6" in ScalperCommands(p.store).execute("scalper_health", now=NOW)[0]


def test_read_only_command_set_has_no_controls():
    from market_signal_assistant.telegram.parsing import parse_command

    for text in (
        "/scalper_restart",
        "/scalper_start",
        "/scalper_stop",
        "/scalper_enable_live",
        "/scalper_status live=true",
    ):
        with pytest.raises(ValueError):
            parse_command(text)


def test_runtime_projection_is_bounded_and_never_uses_historical_active(
    tmp_path, monkeypatch
):
    from test_metrics import record
    from test_snapshot import build_complete

    from market_signal_assistant.qtr_micro_scalper.legacy_recovery import (
        classify_legacy_shadow_recovery,
    )
    from market_signal_assistant.qtr_micro_scalper.orchestrator import (
        ShadowOrchestrator,
    )
    from market_signal_assistant.qtr_micro_scalper.pipeline import LiveShadowPipeline
    from market_signal_assistant.qtr_micro_scalper.service import ShadowService
    from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
        ShadowTradeStage,
    )
    from market_signal_assistant.qtr_micro_scalper.shadow_runtime import ShadowRuntime
    from market_signal_assistant.settings import QtrScalperV2LiveSettings

    monkeypatch.setenv("QTR_SCALPER_V2_LIVE_ENABLED", "false")
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    for i in range(6):
        journal.append(record(f"legacy-{i}", stage=ShadowTradeStage.OPEN))
    before = journal.path.read_bytes()
    runtime = ShadowRuntime()
    orchestrator = ShadowOrchestrator(runtime=runtime, journal=journal)
    pipeline = LiveShadowPipeline(
        symbols=[f"SYM{i}" for i in range(75)],
        price_context_provider=lambda *args: None,
        orchestrator=orchestrator,
    )
    service = ShadowService(
        pipeline,
        journal,
        QtrScalperV2LiveSettings(enabled=True),
        legacy_quarantine=classify_legacy_shadow_recovery(journal),
    )
    assert service.control_details().active_count == 0
    projected_empty = plane(tmp_path).build(
        service.health(),
        service.metrics_snapshot(),
        observation(),
        service.control_details(),
        generated_at=NOW,
    )
    assert projected_empty.runtime.quarantine_count == 6
    assert projected_empty.trades.active_count == 0
    assert projected_empty.trades.active == ()
    assert len(service.control_details().symbols) == 50
    result = runtime.process_snapshot(build_complete())
    assert result.trade is not None
    runtime._active = {
        f"current-{i}": replace(result.trade, trade_id=f"current-{i}")
        for i in range(30)
    }
    projected = service.control_details()
    assert projected.active_count == 30 and len(projected.active) == 20
    assert all(t.trade_id.startswith("current-") for t in projected.active)
    assert projected.live_enabled is False
    assert os.environ["QTR_SCALPER_V2_LIVE_ENABLED"] == "false"
    assert journal.path.read_bytes() == before


def test_publisher_and_telegram_leave_real_index_counters_unchanged(
    tmp_path, monkeypatch
):
    from test_metrics import record

    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    for i in range(100):
        journal.append(record(str(i)))
    decisions = tmp_path / "decisions.jsonl"
    decisions.write_bytes(b"")
    source = JournalObservationSource(
        decision_journal_path=decisions,
        trade_journal_path=journal.path,
        trade_journal=journal,
    )
    observed = source.observe(generated_at=NOW)
    before = (journal.index_metrics, source.metrics())
    p = plane(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("source reread")

    monkeypatch.setattr(JournalObservationSource, "observe", forbidden)
    monkeypatch.setattr(ShadowTradeJournal, "snapshot", forbidden)
    for _ in range(30):
        s = p.build(
            health(),
            service_metrics(),
            observed,
            RuntimeDetails(active_count=0, live_enabled=False),
            generated_at=NOW,
        )
        assert p.publish(s)
        for command in SCALPER_COMMANDS:
            assert ScalperCommands(p.store).execute(command, now=NOW)
    assert (journal.index_metrics, source.metrics()) == before


def test_publish_error_does_not_stop_cli_and_recovers_next_refresh(
    tmp_path, monkeypatch
):
    class Source:
        def observe(self, *, generated_at):
            return observation()

    p = plane(tmp_path)
    original = p.store.publish
    outputs = []

    def fail(*args):
        raise OSError("token=never-print-this")

    monkeypatch.setattr(p.store, "publish", fail)
    runner = ShadowCliRunner(
        FakeService(),
        Source(),
        clock=lambda: NOW,
        output=outputs.append,
        control_plane=p,
    )
    runner._print_refresh()
    assert p.last_publish_error
    assert "never-print-this" not in " ".join(outputs)
    assert any("QTR MICRO" in x for x in outputs)
    monkeypatch.setattr(p.store, "publish", original)
    runner._print_refresh()
    assert p.last_publish_error is None
    assert p.store.read(now=NOW).snapshot.resources.previous_publish_error


@pytest.mark.parametrize(
    "damage,expected",
    [
        ("missing", "ещё не опубликован"),
        ("json", "повреждён"),
        ("schema", "не поддерживается"),
    ],
)
def test_sdk_single_polling_owner_and_denied_reply(
    tmp_path, monkeypatch, damage, expected
):
    from types import SimpleNamespace

    from market_signal_assistant.settings import (
        EarlyDiscoverySettings,
        InPlayAutoSettings,
        InPlayTimingAuditSettings,
        NewsAutoSettings,
        TelegramSettings,
    )
    from market_signal_assistant.telegram.bot import _run_sdk_bot_handlers

    monkeypatch.setenv(
        "QTR_SCALPER_V2_CONTROL_SNAPSHOT_PATH", str(tmp_path / "missing.json")
    )
    if damage == "json":
        (tmp_path / "missing.json").write_bytes(b"broken")
    elif damage == "schema":
        (tmp_path / "missing.json").write_bytes(b'{"schema_version":999}')

    class App:
        handlers = []
        polls = 0

        def add_handler(self, handler):
            self.handlers.append(handler)

        def run_polling(self):
            self.polls += 1

    app = App()

    class Builder:
        def token(self, value):
            return self

        def build(self):
            return app

    _run_sdk_bot_handlers(
        TelegramSettings("not-a-real-token", frozenset({7})),
        ForbiddenScreen(),
        None,
        False,
        InPlayAutoSettings(enabled=False),
        None,
        Builder,
        lambda *args: args[1],
        SimpleNamespace(COMMAND=True),
        news_auto_settings=NewsAutoSettings(enabled=False),
        timing_audit_settings=InPlayTimingAuditSettings(enabled=False),
        early_discovery_settings=EarlyDiscoverySettings(enabled=False),
    )
    assert app.polls == 1 and len(app.handlers) == 1
    replies = []

    async def reply(text):
        replies.append(text)

    async def scenario():
        for chat_id in (8, 7):
            await app.handlers[0](
                SimpleNamespace(
                    effective_chat=SimpleNamespace(id=chat_id),
                    effective_message=SimpleNamespace(
                        text="/scalper_status", reply_text=reply
                    ),
                ),
                None,
            )

    asyncio.run(scenario())
    assert replies[0] == "Доступ запрещён."
    assert expected in replies[1]


def test_existing_scanner_routes_do_not_touch_scalper_reader(tmp_path, monkeypatch):
    from market_signal_assistant.application.models import (
        MarketSummary,
        ScreeningReport,
    )

    class Scanner:
        calls = 0

        def screen(self, request):
            self.calls += 1
            return ScreeningReport(NOW, (), (), (), MarketSummary(0, 0, 0, 0, 0, 0))

    class InPlay:
        def scan(self, **kwargs):
            return object()

    class News:
        def get_important(self):
            return object()

    def forbidden(*args, **kwargs):
        raise AssertionError("Scanner route touched Scalper")

    monkeypatch.setattr(ScalperCommands, "execute", forbidden)
    monkeypatch.setattr(
        "market_signal_assistant.telegram.bot.format_inplay_report",
        lambda x: ("INPLAY",),
    )
    monkeypatch.setattr(
        "market_signal_assistant.telegram.bot.format_news_report", lambda x: ("NEWS",)
    )
    service = Scanner()
    for text in ("/status", "/screen BTCUSDT", "/inplay", "/news"):
        result = execute_command(
            text,
            chat_id=7,
            allowed_chat_ids=frozenset({7}),
            service=service,
            inplay_service=InPlay(),
            news_service=News(),
        )
        if text == "/status":
            assert result.messages == (
                "Статус: готов\nОнлайн-поток ликвидаций: отключён\n"
                "Режим деривативов: REST\nАвтосканирование IN PLAY: отключено\n"
                "Интервал IN PLAY: 15 минут\nАвтоновости: отключены\n"
                "Интервал новостей: 60 минут",
            )
        if text == "/inplay":
            assert result.messages == ("INPLAY",)
        if text == "/news":
            assert result.messages == ("NEWS",)
    assert service.calls == 1

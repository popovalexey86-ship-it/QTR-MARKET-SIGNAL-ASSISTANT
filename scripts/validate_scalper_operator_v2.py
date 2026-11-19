"""Offline proof with fake sends; never starts Telegram or runtime services."""

from __future__ import annotations

import asyncio
import gc
import json
import resource
import sys
import tracemalloc
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests/qtr_micro_scalper")]


def main() -> None:
    from test_control_plane import NOW, plane, snapshot
    from test_snapshot import build_complete

    from market_signal_assistant.qtr_micro_scalper.operator_events import (
        RecentEventFeed,
    )
    from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
        ShadowTradeStage,
    )
    from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
        ShadowRuntime,
        ShadowRuntimeEvent,
        ShadowRuntimeEventType,
    )
    from market_signal_assistant.telegram.scalper_commands import ScalperCommands
    from market_signal_assistant.telegram.scalper_notifications import (
        ScalperCursorStore,
        ScalperNotifier,
        ScalperNotifyConfig,
    )

    folder = ROOT / "reports/operator-v2-proof"
    folder.mkdir(parents=True, exist_ok=True)
    sources = [folder / "large-trades.jsonl", folder / "large-setup.jsonl"]
    for path in sources:
        with path.open("wb") as stream:
            stream.truncate(4 * 1024**3)
    p = plane(folder)
    cursor = ScalperCursorStore(folder / "cursor.json", p.store.path)
    cursor.path.unlink(missing_ok=True)
    config = ScalperNotifyConfig((7,), 10, cursor.path)
    sent = 0

    async def send(chat: int, text: str) -> None:
        nonlocal sent
        assert chat == 7 and len(text) <= 3900
        sent += 1

    worker = ScalperNotifier(p.store, cursor, config, send)
    adapter = ScalperCommands(p.store)
    feed = RecentEventFeed()
    trade = ShadowRuntime().process_snapshot(build_complete()).trade
    assert trade is not None
    trade = replace(trade, stage=ShadowTradeStage.OPEN, entry_at=NOW)
    template = snapshot(folder)
    original = Path.open
    reads = {"snapshot": 0, "cursor": 0, "journals": 0, "max_request": 0}
    opened: set[str] = set()

    class Guard:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def fileno(self):
            return self.stream.fileno()

        def read(self, size=-1):
            assert 0 < size <= 128 * 1024 + 1
            reads["max_request"] = max(reads["max_request"], size)
            return self.stream.read(size)

    def guarded(path, *args, **kwargs):
        if path.suffix == ".jsonl":
            reads["journals"] += 1
            raise AssertionError("Large source journal opened")
        opened.add(str(path))
        if path == p.store.path:
            reads["snapshot"] += 1
        elif path == cursor.path:
            reads["cursor"] += 1
        stream = original(path, *args, **kwargs)
        return Guard(stream) if args and args[0] == "rb" else stream

    Path.open = guarded
    sizes = []  # Only two checkpoints, never one element per poll.

    async def replay():
        tracemalloc.start()
        for i in range(1000):
            at = NOW + timedelta(seconds=i)
            source = ShadowRuntimeEvent(
                str(i),
                ShadowRuntimeEventType.OPEN,
                at,
                trade.trade_id,
                trade.symbol,
                ShadowTradeStage.OPEN,
                trade.entry_price,
                0,
                "proof",
            )
            feed.capture(trade, (source,))
            current = replace(
                template, generated_at=at, observed_at=at, recent_events=feed.snapshot()
            )
            assert p.publish(current)
            assert adapter.execute("scalper_recent", now=at)
            await worker.poll_once(now=at)
            assert len(feed._events) <= 20
            if i in {199, 999}:
                gc.collect()
                sizes.append(tracemalloc.get_traced_memory()[0])
        retained, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return retained, peak

    try:
        retained, peak = asyncio.run(replay())
    finally:
        Path.open = original
    state = cursor.load()
    result = {
        "cycles": 1000,
        "interactive_recent_commands": 1000,
        "fake_deliveries": sent,
        "journal_reads": reads["journals"],
        "snapshot_reads": reads["snapshot"],
        "cursor_reads": reads["cursor"],
        "max_requested_read_bytes": reads["max_request"],
        "traced_peak_bytes": peak,
        "traced_retained_bytes": retained,
        "retained_at_200_and_1000": sizes,
        "process_peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        / 1024,
        "snapshot_bytes": p.store.path.stat().st_size,
        "cursor_bytes": cursor.path.stat().st_size,
        "cursor_chats": len(state or {}),
        "max_feed_events": len(feed._events),
        "lock_bytes": cursor.path.with_suffix(".json.lock").stat().st_size,
        "source_logical_bytes": [path.stat().st_size for path in sources],
        "opened_paths": sorted(opened),
    }
    print(json.dumps(result, indent=2), flush=True)
    assert reads["journals"] == 0 and len(state or {}) == 1
    assert peak < 5 * 1024 * 1024
    assert sizes[1] - sizes[0] < 256 * 1024
    assert len(opened) == 3
    target = ROOT / "reports/operator-v2-resource.json"
    target.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()

# Scalper Telegram Operator V2 acceptance

Worktree: /opt/qtr/scalper-control-plane-v2
Branch: feature/scalper-control-plane-telegram-v2
Base: 3358fa0420fce2e34007f1ef29c53e6f520ca37b
Producer commit: 5fce4c9b35b1e288342a4b0fdf2998e02ee72272
Consumer commit: this commit (obtain exact SHA with git rev-parse HEAD).

## Scope and architecture

All implementation and validation took place in the specified feature worktree.
No production checkout, environment, systemd unit, service lifecycle or live
trading setting was changed. No deployment, push, merge, real Telegram polling
or real message send was performed. SDK transports in tests are fake.

Producer is separate from Telegram consumer. Runtime transitions feed a fixed
20-entry deque. Existing immutable runtime IDs plus operator type determine the
stable SHA-256 event ID. Capture failure increments a separate counter and cannot
alter runtime return values, trade state, journals or error counts. No startup
history reconstruction, new append journal, or large JSONL scan was introduced.
The producer may lose its pre-restart in-memory recent feed, as permitted by V2.

Schema 2 publishes recent_events oldest first by UTC occurred_at, transition
priority and event_id, and a recent_event_failures resource counter. The entire
snapshot remains limited to 128 KiB. New readers accept exact schema 1 with an
empty feed; /scalper_recent explains that V1 has no V2 feed. Existing V1 readers
reject V2 explicitly. Install the compatible consumer/shared model before
switching a producer; the divergent production Telegram tree requires a reviewed
port, not a blind overwrite/cherry-pick of every producer runtime file.

OPEN, CLOSED and EXPIRED are existing runtime semantics. ENTRY_CREATED is
presented as ENTRY_CANDIDATE, not an executed entry. EXPIRED carries existing
NOT_TRIGGERED outcome without fabricated exit/R. There is no separate second
NOT_TRIGGERED event for the same expiry. CLOSED uses existing cumulative R,
existing outcome classification and reason; BREAKEVEN is an operator FLAT label.
No PnL is recalculated. Partial TP1 is not mislabeled CLOSED. Unknown result or
outcome is not guessed. Strings remain bounded by the model's 300-character cap.

## Interactive UI and polling ownership

/scalper_recent shows at most ten recent events, newest first, with occurrence
timestamps and an explicit distinction from current positions. Empty feed and
V1 feed absence have explicit Russian messages. All V1 missing, malformed,
unsupported, stale and future-date fail-closed behavior remains. Existing
/start, /help, /status, /screen, /inplay, /news and other target-tree integrations
are preserved. No trading/action command exists.

There is one existing SDK Application and one run_polling() call. The observer
is an asyncio task attached to the same application's post_init/post_shutdown
callbacks alongside existing auto loops, not a second poller/process. Existing
is_allowed() authorizes all explicit notification recipients. Invalid/unauthorized
configuration disables only automatic notifications; interactive commands remain.

## Configuration

- QTR_SCALPER_V2_CONTROL_SNAPSHOT_PATH: same shared absolute file path in both
  producer and bot processes, as in V1. Default remains checkout-local data/
  qtr_micro_scalper_control_snapshot.json; different checkouts need explicit
  shared configuration in a separately approved rollout.
- QTR_SCALPER_V2_NOTIFY_CHAT_IDS: comma-separated explicit chat IDs, maximum ten.
  Empty/unset means automatic notifications disabled, even if an allow-list exists.
  Each ID must satisfy the existing bot's authorization policy. No inferred chats.
- QTR_SCALPER_V2_NOTIFY_INTERVAL_SECONDS: default 10; accepted range 5..60 seconds.
- QTR_SCALPER_V2_NOTIFY_CURSOR_PATH: default checkout-local data/
  qtr_micro_scalper_notify_cursor.json. Must differ from the snapshot and every
  configured evidence path. The cursor is consumer-only mutable state.

None of these environment values was changed in production. LIVE remains
unmodified; trade notifications fail closed unless the snapshot explicitly says
Shadow mode and live_enabled=false. Unknown/LIVE flags produce only health alerts.

## Durable cursor and delivery contract

Cursor schema 1 is capped at 16 KiB and ten chat cursors; each contains only a
seeded bit, canonical event watermark and last health class. It binds the exact
snapshot path. Atomic same-directory temp/flush/fsync/replace publication keeps
latest state only. The fixed zero-byte .json.lock file uses nonblocking POSIX
flock to serialize competing claims; it never stores lock history. Cursor temp
files are private; failed writes clean them up. The one reserved .json.tmp name
is reused under the lock, so SIGKILL cannot accumulate UUID-named orphan files.
The writer unlinks that reserved entry then creates it with O_EXCL, preventing
write-through to stale symlinks/hardlinks. No cursor backup/archive grows.
This implementation targets the Linux/POSIX deployment environment.

First observation without a valid cursor seeds every configured chat to the
current buffer watermark without replay. If the snapshot is unavailable, event
seeding waits for the first eligible fresh V2 snapshot. Malformed, oversized or
source-mismatched cursor is safely reseeded, never replayed as new history. New
recipients seed independently; removed recipients are pruned. Restart preserves
acknowledged watermarks and health classes. Concurrent polls/observers sharing
the cursor cannot claim the same event twice.

Only events strictly newer than the durable (time, transition priority, ID)
watermark are eligible. Equal-time events have deterministic tie ordering. A
late event behind that watermark is deliberately not replayed; gaps larger than
the bounded ring may lose events. There is no journal backfill. These are operator
telemetry limits, not changes to trade semantics.

Claims for the bounded batch (at most 210 chat messages: 20 events + one health
edge per ten chats) are persisted BEFORE the first send. If persistence fails,
no message is sent. A send has a ten-second timeout. Failed/ambiguous sends and
cancellation retain their claims and are not automatically retried. This is
at-most-once delivery ATTEMPT, not a claim of transport-level exactly-once
DELIVERY: a crash after durable claim and before/during send can lose messages.
Telegram and a local JSON replace cannot be one atomic transaction. The chosen
fail-closed policy prevents duplicate replay/storms; first delivery/state errors
are logged generically without secret payloads.

## Health edges

Persisted per-chat health class distinguishes HEALTHY, DEGRADED, stale, missing,
malformed, unsupported, future timestamp, stopped/starting/disabled/blocked and
unsafe/unknown LIVE/Shadow flags. First poll seeds the health baseline without a startup alert. Only class transitions send alerts; repeated
DEGRADED/stale does not spam and restart does not reset alert state. Recovery to
fresh RUNNING/ready emits one recovery edge. No invalid/stale snapshot emits
trade notifications or asserts current trade state. Fresh recovered feeds can
report buffered events after the prior cursor, labeled with their actual times.

## Tests and proofs

Focused V2 tests: 37 passed (producer, notifier, recent UI, lifecycle integration).
Complete Scalper: 643 passed.
Telegram regression: 152 passed.
Full repository: 1148 passed.
Ruff: PASS. Mypy: PASS (97 source files). git diff --check: PASS.
Producer plus V1 Control Plane regression before commit A: 56 passed.

AST proof confirms existing ShadowRuntime code is identical after removing
additive telemetry initialization/capture/getters. Existing scoring/entry/exit,
TP/SL, risk, holding/experiment logic, outcomes and journal semantics are unchanged.
AST also finds exactly one run_polling call in bot.py and zero in the notifier.

Resource proof: scripts/validate_scalper_operator_v2.py runs 1,000 recent commands
AND 1,000 background poll cycles, with 999 fake new-event deliveries after initial
seeding. It opens only snapshot, cursor and zero-byte lock. Two 4-GiB logical
sparse trade/setup fixtures are guarded against every open: zero JSONL reads.
Snapshot reads: 2,000; cursor reads: 999. Max read request: 131,073 bytes. These
are synthetic sparse fixtures, not production dataset validation.

Representative snapshot: 7,474 bytes. Cursor: 298 bytes. Lock: zero bytes.
Event ring: exactly 20 maximum; persisted recipients: one (hard max ten).
Message batch has a hard 210 bound and is local to a poll; no per-poll history is
retained. Type-hint cache contains only the finite schema classes.
Traced peak: 2,000,643 bytes (1.91 MiB); process peak RSS: 45.89 MiB.
Live traced heap after GC at poll 200/1000: 83,976 / 257,900 bytes, below the
unchanged 256-KiB checkpoint-growth threshold. Initial pre-GC diagnostic failed
that threshold; full-GC checkpoints separate collectible temporary objects from
live retained state. Peak measurement includes the between-checkpoint temporaries.
No production GC tuning was introduced. Reports are retained under reports/.

The user-provided CODEX_SCALPER_OPERATOR_V2.md remains byte-for-byte unchanged
and untracked as it was at baseline. Its SHA-256 is
647e98301a3b709960479bbbb8ddc7f1daecfc821c42aae18a704da87debed17.
All implementation/docs/tests are committed separately from that original task
file. No user instruction file is silently ignored, modified or committed.

## Deployment boundary

No deployment/restart was performed. Review common control_models.py compatibility
from producer commit A as a prerequisite for porting consumer commit B into the
diverged production Telegram checkout. Review/merge only the added lifecycle
hooks while preserving existing transport integrations. A separately authorized
rollout must choose shared snapshot/cursor paths, explicit recipients and
permissions. Start/restart/enable commands are absent from this release.


## Commit file manifests

Producer A:
- docs/scalper_operator_v2_producer.md
- src/market_signal_assistant/qtr_micro_scalper/cli.py
- src/market_signal_assistant/qtr_micro_scalper/control_models.py
- src/market_signal_assistant/qtr_micro_scalper/control_plane.py
- src/market_signal_assistant/qtr_micro_scalper/operator_events.py
- src/market_signal_assistant/qtr_micro_scalper/orchestrator.py
- src/market_signal_assistant/qtr_micro_scalper/pipeline.py
- src/market_signal_assistant/qtr_micro_scalper/shadow_runtime.py
- tests/qtr_micro_scalper/test_control_plane.py
- tests/qtr_micro_scalper/test_operator_events.py

Consumer B:
- docs/scalper_operator_v2_acceptance.md
- scripts/validate_scalper_operator_v2.py
- src/market_signal_assistant/telegram/bot.py
- src/market_signal_assistant/telegram/scalper_formatting.py
- src/market_signal_assistant/telegram/scalper_notifications.py
- tests/qtr_micro_scalper/test_control_plane.py
- tests/qtr_micro_scalper/test_scalper_notifications.py
- tests/test_scalper_telegram_v2.py

READY_FOR_REVIEW

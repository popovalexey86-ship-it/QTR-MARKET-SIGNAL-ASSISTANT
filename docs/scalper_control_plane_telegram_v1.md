# Scalper V2 Control Plane + Telegram Operator V1

Branch: `feature/scalper-control-plane-telegram-v1`.
Exact verified base: `origin/deploy/scalper-shadow-bounded-memory-oom-e149`
at `6ae322d956c8ff1c8e7f77549cff1bb0b46a9a14`.

No deployment, VPS access, restart, systemd change, live enablement, push or merge
was performed. This is a local review candidate.

## Architecture

The existing `ShadowCliRunner._print_refresh()` calls `observe()` once and
captures health/metrics once. That same observation feeds the existing CLI
renderer and `ScalperControlPlane`; there is no second observation pipeline.
Only one latest observation is retained for a final stopped-state publication,
which never replays source journals. A failed observation does not refresh the
snapshot with a new healthy timestamp.

Read-only accessors in service/pipeline/orchestrator project actual current
runtime trades and the current symbol set. They never use historical unfinished
rows as active runtime trades. Historical statistics keep their original
aggregation semantics, while active count and summaries refer to runtime only.
Unknown current state is null, not a fabricated zero.

`ControlSnapshotStore` writes UTF-8 JSON to a unique temporary file in the same
directory, flushes/fsyncs it, closes it and publishes with `os.replace()`. It
replaces the latest model; it does not append history. Failed replacement leaves
the old model intact and cleans up its temporary file. POSIX snapshots have
mode 0644 because this projection contains no credentials and must be readable
by the existing bot process. Atomic visibility is promised, not a crash-durable
history/audit log. Missing files after a crash fail closed at the reader.

The existing bot still has one polling owner and one SDK command handler.
`execute_command()` applies the existing `is_allowed()` policy before parsing
and dispatching namespaced commands to `ScalperCommands`. The adapter reads
only the compact snapshot; it has no live ShadowService reference. Existing
Scanner, IN PLAY, News and `/status` behavior remains unchanged.

## Schema 1 and bounds

Immutable frozen/slots models contain only typed scalars, nested models and
tuples. Writer and reader apply the same strict schema/finite-number/type,
timestamp, collection and identity validation. Unknown fields, unsupported
versions, invalid flags, duplicate active IDs and lifecycle extensions fail
closed. No complete trade object or event/lifecycle history is serialized.

| Section | Contents |
| --- | --- |
| Top level | schema_version, generated_at, observed_at, stale_after_seconds |
| runtime | status, ready, Shadow/LIVE flags, started_at, uptime, reconnects, pipeline errors, last-error indicator, quarantine count/warning |
| market | snapshot count, total current symbol count, up to 50 current symbols, last target refresh, nullable last market activity |
| decisions | total/blocked/entries, dominant historical market state and score bucket where available |
| trades | current runtime active count, historical completed/win/R/expiry/TP1/TP2/stop statistics, up to 20 current trade summaries |
| blockers | up to 10 reason/count pairs |
| experiments | Holding Experiment, Micro Profit, Protected Runner enabled flags, error counts and warning indicators |
| resources | existing queue depth, retained-event count, stale-data suppressions, previous publication-error indicator |
| version | schema_version and code SHA, otherwise unknown |

Active summaries contain only ID, symbol, direction, stage, score, entry,
current stop, TP1/TP2, entry time and realized R. No legacy orphan hydration
or fabricated transitions are introduced. Summary count can exceed the 20
displayed items; `/scalper_active` explicitly states both counts.

JSON is capped at 128 KiB; readers request at most 128 KiB + one overflow byte.
Text fields are capped at 300 characters. Source journals are never read to
answer a command. Both reader and publisher reject non-`.json` destinations and
configured evidence-path aliases. Publishing or reading cannot be redirected
onto a configured source journal as a workaround.

Runtime errors and experiment/quarantine warnings are exposed as bounded status
indicators, not raw exception payloads. Raw exception details stay in existing
service logs to avoid leaking tokens, URLs with credentials or environment
contents. Public blocker reasons remain visible and bounded. Git SHA is resolved
once at publisher construction with a one-second timeout; missing/inaccessible
Git metadata yields `unknown` and does not block runtime startup. No `/proc`,
systemd, process-memory or journal shell scans run on refresh or Telegram commands.

## Location and cross-process configuration

Default: `data/qtr_micro_scalper_control_snapshot.json` beneath the checkout
root. Override with `QTR_SCALPER_V2_CONTROL_SNAPSHOT_PATH`.

Separate Scalper and Scanner checkouts have different default roots. During a
separately approved rollout, configure **the same absolute snapshot path** in
both processes and ensure existing directory permissions allow the bot to read
it. This task does not change either environment or service unit. Publishing
is integrated into the existing CLI runner; invoking `service.main` alone does
not create a second observation loop or publish this model.

## Freshness and failures

TTL is `max(90, min(3600, 3 * refresh_seconds))`, so the default 30-second
publisher cadence permits 90 seconds. Readers compare both publication time
and observation time against their clock and use the older timestamp for age.
An observation failure or stopped-state publication cannot make old analytics
look fresh just by updating `generated_at`. Timestamps more than five seconds
in the future are rejected as a separate clock-error state.

Missing, malformed/unreadable, unsupported-schema, stale, future-dated and
fresh snapshots are distinct reader states. Invalid/stale replies contain a
Russian warning and never render old active trades as current. Stale replies
show observation/publication times and say current state is unknown. Malformed
snapshot errors never escape into the existing SDK handler.

Publication failure degrades the **operator channel**, not runtime/trading
health. The CLI reports a generic warning immediately. The last valid snapshot
remains until its TTL expires; consumers cannot know a write failed before that
deadline. On recovery, `previous_publish_error` reports the preceding failure.
No service stop/restart, journal write, retry of trading work or risk change
occurs because publication failed. A graceful CLI shutdown attempts a final
STOPPED model using the last observation and its original observation timestamp.

## Read-only commands

- `/scalper_status`: status, Shadow/LIVE, uptime, snapshots, decisions, entries,
  runtime/historical trade counts, R and snapshot freshness.
- `/scalper_active`: bounded current runtime summaries or
  `Активных теневых сделок нет.` Unknown runtime state is reported as unavailable.
- `/scalper_stats`: decisions/blocks/entries/completed, win/R and expiry/TP/stop rates.
- `/scalper_blocks`: ten bounded reasons/counts.
- `/scalper_experiments`: read-only enabled/error/warning status for all three experiments.
- `/scalper_health`: readiness, errors/reconnects, freshness, quarantine and error indicators.
- `/scalper_version`: snapshot schema, publisher code/deploy SHA, generated_at.
- `/scalper_recent`: `Недавние события пока не опубликованы Control Plane.`
  A bounded cross-process recent-event feed is deferred to V1.1; no history scan
  was added to implement it.

All commands reject parameters and reuse current TelegramSettings/allow-list
policy, `split_messages()` and SAFE_MESSAGE_LIMIT. Unauthorized SDK replies remain
`Доступ запрещён.` There are no remote start/stop/restart, close, risk/config or
live-enable commands.

## Verification and semantic isolation

The focused tests cover typed round trips, immutable projections, exact schema
bounds, no lifecycle serialization, atomic visibility/replacement and cleanup,
all reader states, stale observation under fresh publication, authorization
before file access, every command, Russian output, message limits, single SDK
polling ownership, SDK parse failures, and unchanged Scanner command routing.

Runtime projection checks publish six quarantined historical trades with zero
restored active trades, then independently verify a 30-trade current runtime is
projected as count 30 with at most 20 summaries. Symbol projection is capped at
50. LIVE_ENABLED=false is captured without changing it. Real index counters
and offsets remain identical over repeated publications and commands. A large
96 MiB source fixture is guarded against any opens, and per-command journal
construction/observation is forbidden in the test.

The measured 1,000-command replay used a 10,630-byte snapshot and configured
the preserved 1,421,767,577-byte decision and 202,123,034-byte trade sources.
Instrumented file access opened only the snapshot: 1,000 bounded reads, zero
source-journal reads. Peak traced allocation was 485,143 bytes (0.46 MiB), RSS
50.71–51.12 MiB, elapsed 6.52 seconds with tracing enabled. Thirty publications
replaced the file without increasing its size. These are local Windows
measurements, not a VPS acceptance or full live-runtime memory measurement.

AST comparison confirms all existing orchestrator/pipeline/service methods are
unchanged after removing the additive read-only accessors and LIVE flag capture.
No scoring, thresholds, entry/exit, TP/SL, risk, holding rules, experiment logic,
outcomes, journal recovery/indexing, Setup parsing or analytics algorithms were
changed. Existing bounded-memory fixes and quarantine remain intact. Historical
source files are not included in the commit and were not modified.

Final results from this exact feature worktree:

| Check | Result |
| --- | --- |
| Focused Control Plane / Telegram adapter tests | 48 passed |
| Complete Scalper suite | 609 passed |
| Full repository suite | 1,111 passed |
| Mypy | PASS, 95 source files |
| Ruff | PASS |
| Diff whitespace / semantic checks | PASS |

These counts belong to the requested deployed baseline, rather than the newer
main-derived checkouts used in earlier tasks. All tests use local/fake transports;
no real Telegram message or polling session was started.

Verdict: **READY_FOR_REVIEW**. Operational deployment/shared-path configuration
and a live-service acceptance test remain separate, unperformed steps.

# Scalper Shadow V2: legacy recovery quarantine

Implementation base: `764bc3df991b5d09ee6fec0d49fad3e0c754fc21`.
Branch: `fix/scalper-shadow-legacy-recovery-quarantine`.
No deployment, production restart, environment change or live execution.

## Meaning and inspected callers

Before this change, the baseline trade index's `active_records` returned each
trade's **last appended**, non-terminal row (anything except CLOSED/EXPIRED).
It did not mean an exchange position or a resumable runtime checkpoint. In
particular this is file order, not the chronological `latest` used in some
historical aggregates. Neither ordering nor reporting rules have changed.

All `active_records` usages in source and tests were inspected:

| Location | Meaning / action |
| --- | --- |
| `shadow_journal_index.py`, `shadow_journal.py` | Historical non-terminal rows; retain compatibility alias and add explicit `historical_unfinished_records`. |
| `service.py` factory | Previous unconditional unfinished-state blocker; now invokes explicit quarantine classification. |
| `test_bounded_trade_journal.py` | Historical preservation assertions retained; replace old blocking expectation with explicit quarantine expectation. |
| `holding_experiment.py` recovery/model/start, `test_holding_experiment.py` | Separate experiment journal; startup deliberately records INTERRUPTED recovery. Unchanged. |
| `micro_profit_experiment.py` recovery/model/start | Separate experiment interruption policy. Unchanged. |
| `protected_runner_experiment.py` recovery/model/start, `test_protected_runner_experiment.py` | Separate experiment interruption policy/tests. Unchanged. |

The baseline orchestrator constructs an empty ShadowRuntime and empty score and
context maps; it has no historical hydration implementation. Runtime tracks
expiry, current stop, remaining fraction, processing cursor and other fields
which schema-1 history does not persist completely. Those fields cannot be
invented from prices, current time or record age.

## Policy and scope

`LegacyShadowQuarantine` explicitly identifies valid schema-1 historical
unfinished rows as `LEGACY_SCHEMA_1_WITHOUT_RUNTIME_CHECKPOINT`. Classification
uses the compact streaming index, not IDs, dates, a six-record allowlist or full
history materialization. It does not append transitions, filter historical
aggregates, construct current plans, or mutate the runtime.

The service factory computes this startup classification before constructing
collectors. Health exposes a separate quarantine warning; it is not a pipeline
error. Operational metrics expose quarantine count, trade IDs/stages, resumable
active count (zero for the only supported format), and restored legacy count
(zero). A warning with reason, stage counts and IDs is logged once per service
instance at its first enabled Shadow start. Health reads, refreshes, supervisor
reconnects and stop/start on the same instance do not emit repeated warnings.
A new process rebuilds the same classification and reports it once.

There is deliberately no new durable quarantine journal/sidecar. JSONL remains
the authority; the deterministic model, startup log and metrics provide the audit
trail without rewriting evidence.

Unknown top-level fields are now rejected by the schema-1 deserializer. Startup
classification fails closed if any row was malformed/unsupported or if an
unterminated tail remains. This prevents a future checkpoint-bearing row being
silently stripped down and quarantined as legacy, including when a prior row for
the same trade exists. Future checkpoint formats need a separately versioned
validator, identity/config/freshness checks and an actual runtime/score/context
hydrator; they are not implemented by this historical-quarantine change. Even
complete future runtime/score/context payloads fail closed rather than being
ignored or quarantined. All newly written schema-1 unfinished rows likewise
remain non-resumable on a later process restart; this is a schema capability
rule, not an age-based strategy rule.

Changed implementation: `legacy_recovery.py`, `service.py`,
`shadow_journal.py`, `shadow_journal_index.py`.
Tests: new `test_legacy_recovery.py`, updated `test_bounded_trade_journal.py`.
This document is the seventh intended file. Scoring, risk, entry/exit, runtime,
orchestrator, settings, experiments, observer and analytics algorithms are
unchanged. The existing incremental tailing, compact state and disk-backed
deduplication architecture is retained.

## Historical validation

Validation used the complete preserved November 14 dataset locally, not a
truncated sample. Both SHA-256 hashes were checked before and after classification:

| Journal | Bytes | SHA-256 unchanged |
| --- | ---: | --- |
| Decisions | 1,421,767,577 | `f849c1ba215c7d4ea94081f65a5e9112907f4242211f767e1c940e9ccf827bf2` |
| Trades | 202,123,034 | `0b68f2859bb1ba6523c05ef2980aeeb2c99a8ed4ebc53f8fc1840b2b54f0f589` |

Trade history: 166,560 accepted rows, 22,586 unique trades, CLOSED 12,235,
EXPIRED 10,345, terminal 22,580, WAITING_ENTRY 5, OPEN 1, unfinished 6.
Quarantined legacy trades: 6. Active trades in a fresh baseline orchestrator: 0.
Thirty repeated classifications caused no extra index reads; bootstrap scans: 1.

| Preserved trade ID | Symbol | Historical stage |
| --- | --- | --- |
| `shadow-46e279513c11e8e6466a` | 1000PEPEUSDT | OPEN |
| `shadow-541873bf0db2497920cb` | STRKUSDT | WAITING_ENTRY |
| `shadow-7d1c3107178202bc8ad5` | XPLUSDT | WAITING_ENTRY |
| `shadow-7fa7e00e9369e36a05d7` | ZECUSDT | WAITING_ENTRY |
| `shadow-b5991f2178104f926cf0` | BTCUSDT | WAITING_ENTRY |
| `shadow-ccaa0cbe94d6321c12d7` | GRASSUSDT | WAITING_ENTRY |

The OPEN row retains entry 0.003803, direction SHORT, original entry timestamp
2026-08-26T09:04:26.217433+00:00 and outcome OPEN. The five waiting rows retain
null entry times. No close, expiry, result or timestamp was fabricated.

Local evidence is retained under the new worktree's ignored `reports/` directory;
raw historical data remains in the previous OOM worktree's ignored data directory.
Neither dataset nor generated reports is part of this commit.

## Validation results

Recovery regressions: 9 passed, including real service factory with fake
collectors, successful start/stop/restart, runtime isolation, unchanged bytes,
metrics/analytics equality, empty/terminal histories, deterministic repeated
classification, no full recovery calls, damaged tail and future checkpoint
rejection. Complete Scalper V2 suite: 534 passed. Full repository: 1,679 passed.
Mypy: 139 source files passed. Ruff: passed. Git diff whitespace check: passed.
The full suite includes the existing bounded-memory growth and incremental
observation regressions.

Full-data replay completed in 439.49 seconds against all 1,689,233 decision rows
and 166,560 trade rows. The complete metrics and analytics dictionaries exactly
equal the bounded-memory baseline at the same generated timestamp (2026-11-14
12:00 UTC); combined snapshot SHA-256:
`eee48f1e8708f2c71bcc84351d713d484cd1cc2427c3316c76c780416514eea6`.
This includes every reason/outcome slice, not just top-level counts.

Windows measured peak RSS: **258.74 MiB** (previous bounded-memory replay:
255.11 MiB; original unbounded baseline: 1,502.03 MiB). Twenty unchanged
observation cycles: 243.86–248.14 MiB; five growth cycles: 247.43–248.42 MiB.
Growth rows were appended only to a disposable benchmark copy, never the
historical input. Bootstrap scans 1, incremental reads 5, records processed
166,565, malformed rows 0, rotations 0, retained trades 22,586, same shared
journal throughout. This remains O(distinct trades) retained state with a
bounded SQLite page cache, not constant memory for arbitrarily many trades.
Memory noise of a few MiB is not evidence of a multi-GiB historical replay.

## Controlled VPS validation (operator steps, not performed)

Read-only checks during implementation confirmed deployed port
`9f7f5496a1627f5aa1e2a799ee265cd095a671e5`, service inactive/dead, NRestarts 0,
ENABLED true, SHADOW_MODE true, LIVE_ENABLED false. No service command changing
state was issued. No deployment or live Linux runtime validation is claimed.

The commit is based on the requested OOM implementation, not the VPS-specific
port. Before an operator deploys, review/port only this seven-file delta onto the
approved VPS line, preserving all VPS-specific telemetry and configuration.
Do not replace that checkout with the entire local branch blindly. Deployment
and starting the service require a separate controlled operational step.

After the reviewed port is available, with the service still stopped, run from
the intended checkout using the existing locked virtualenv. These commands do
not start collectors, services or trades, and do not write source journals:

```bash
systemctl show qtr-scanner-scalper-shadow.service \
  -p ActiveState -p SubState -p NRestarts -p InvocationID
git rev-parse HEAD
sha256sum data/qtr_micro_scalper_decisions.jsonl \
  data/qtr_micro_scalper_shadow_journal.jsonl
PYTHONPATH=src /path/to/approved/venv/bin/python -B - <<'PY'
from collections import Counter
from pathlib import Path
from market_signal_assistant.qtr_micro_scalper.shadow_journal import ShadowTradeJournal
from market_signal_assistant.qtr_micro_scalper.legacy_recovery import classify_legacy_shadow_recovery
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowOrchestrator
j = ShadowTradeJournal(Path('data/qtr_micro_scalper_shadow_journal.jsonl'))
try:
    plan = classify_legacy_shadow_recovery(j)
    stages = Counter(s.last_appended.stage.value for s in j.snapshot().states)
    print('rows', j.record_count, 'unique', sum(stages.values()), 'stages', dict(stages))
    print('quarantine', plan.trade_ids_and_stages, 'warning', plan.warning)
    print('fresh_orchestrator_active', ShadowOrchestrator(journal=j).metrics().active_shadow_trades)
    assert stages == {'CLOSED': 12235, 'EXPIRED': 10345, 'WAITING_ENTRY': 5, 'OPEN': 1}
    assert len(plan.legacy_unresumable_records) == 6
finally:
    j.close()
PY
sha256sum data/qtr_micro_scalper_decisions.jsonl \
  data/qtr_micro_scalper_shadow_journal.jsonl
```

Require unchanged hashes, the exact six IDs/stages, restored count zero, complete
logs and LIVE_ENABLED false / SHADOW_MODE true before a controlled service start.
Validate effective flags through the service's actual Environment/EnvironmentFile
without dumping unrelated credentials. Do not source an unreviewed env file.
On a separately authorized start, check the single quarantine warning and
operational metrics, current-only observation progress, RSS over repeated 30s
cycles, queue/worker/write errors, and no restart growth. Stop for unexpected
history, unsupported checkpoints, live-enabled flags or increasing RSS. A local
pass is readiness for that controlled validation, not proof of a live VPS start.

Verdict: **SAFE_FOR_CONTROLLED_VPS_START**, subject to the reviewed port and
preflight checks above. This does not authorize or claim deployment/start.

# Scalper Shadow V2 — bounded-memory OOM fix

Issue: [#10](https://github.com/popovalexey86-ship-it/QTR-MARKET-SIGNAL-ASSISTANT/issues/10).

Implementation branch: `fix/scalper-shadow-bounded-memory-oom`.
Repository base: `86fe277eb335ef950b1dbf73a2ed3a2879eff81d`.
Observed deployed SHA: `e149e0ce51f2ad9e9f9fb23bf68fb042ab4272cf`.

No production deployment, service start/restart, environment/configuration
change, journal rewrite/truncation, statistics reset or real execution occurred.
The service was read as **inactive/dead** before validation. Its working directory
is `/opt/qtr/scalper-shadow`, not `/opt/qtr/scanner`.

## RCA

`ShadowTradeJournal` recovered all rows, retained both recovery tuples and a list,
and held an all-history Python ID set. `records()` added tuple copies. The CLI
reconstructed the entire journal on each changed file signature, despite sharing
the original writer journal with the service. After the first append, the writer
and observer retained separate complete histories; replacement could temporarily
retain another full history. Metrics and analytics then materialized/grouped/sorted
the historical rows again during every observation.

This is history-sized object retention and amplification in the live path, not a
requirement of JSONL storage. The fix removes those live-path materializations.
Other service/runtime memory is not claimed to have been measured by this offline
validation, and the recorded Linux Global OOM peaks are not equated with the
Windows measurements below.

## Design

`ShadowTradeJournalIndex` performs one streaming bootstrap and then reads complete
appended lines from a byte offset. It tracks opened-file identity, size, timestamps,
line position and bounded prefix/boundary anchors. Replacement, truncation and
detected copy-truncate/regrowth clear the derived state and safely bootstrap the
new generation. An incomplete tail is deferred until a newline arrives.

Python retains one compact state per distinct trade: first and ordered-latest
records, last-appended state, entered/TP1 flags, a bounded event-type set, and the
historical stop-reason flag. These preserve the exact legacy ordering, event flags,
loss reasons and decision-entry matching. Normally first/latest/last-appended
share record references; at most three record objects are retained per trade.

Exact all-history duplicate IDs live in a private temporary SQLite B-tree, with a
2 MiB page-cache budget, spilling enabled, mmap disabled and schema version 1.
There is no eviction-based loss of old record-ID protection. Restart rebuilds the
index from durable JSONL, including old IDs. No SQLite checkpoint is trusted and
no persistent sidecar is required; closing/crashing the process discards its
temporary derived database. Index failure makes that instance unavailable rather
than serving partially consumed state.

Source journals remain the only authority. Append still flushes/fsyncs JSONL
before indexing. A crash tail is preserved verbatim; a separator is appended and
re-indexed before duplicate checking. No source bytes are repaired or removed.
As before, one writer per journal is the operational contract; this does not add
a multi-process append transaction protocol.

RAM is O(distinct trades + existing bounded decision-ID cache/entry analytics +
largest input row), not O(historical trade updates). SQLite disk usage is O(unique
record IDs). Bootstrap remains a full streaming scan; reports work over compact
trade state, and ordinary growth does not cause another full scan.

The CLI keeps the same shared writer/index instead of replacing it. Metrics and
incremental decision analytics consume compact snapshots. Orchestrator journal
counts use `record_count`, not `len(records())`. The already-streaming decision
index and its retention/deduplication behavior are unchanged.

Compatibility: `records()` and `recover_shadow_journal()` remain explicit,
potentially expensive offline full-history APIs. `recovery.records` is empty on
the bounded journal's metadata view; use `record_count`/`snapshot()` in live code
and `records()` when full offline history is explicitly required. No schema or
serialized record-ID change was made to either source journal.

## Full immutable history audit

Read-only copies were downloaded after hashing the source files on the stopped
VPS. Local audit hashes match those source hashes:

| Journal | Bytes | Rows | Malformed |
|---|---:|---:|---:|
| Decisions | 1,421,767,577 | 1,689,233 | 0 |
| Shadow trades | 202,123,034 | 166,560 | 0 |

- Decisions SHA-256: `f849c1ba215c7d4ea94081f65a5e9112907f4242211f767e1c940e9ccf827bf2`.
- Trades SHA-256: `0b68f2859bb1ba6523c05ef2980aeeb2c99a8ed4ebc53f8fc1840b2b54f0f589`.

All eight decision event counts match the incident: TARGET_FOUND 35,464;
ANALYSIS_STARTED 32,358; SNAPSHOT_READY 308,363; SCORE_CREATED 308,363;
DECISION_BLOCKED 308,363; SHADOW_ENTRY_CREATED 22,245; TRADE_UPDATED 651,837;
TRADE_FINISHED 22,240. The raw audit itself peaked at 30.23 MiB locally.

Recovered unique trades: **22,586**. Latest durable stages:

| Stage | Count |
|---|---:|
| CLOSED | 12,235 |
| EXPIRED | 10,345 |
| WAITING_ENTRY | 5 |
| OPEN | 1 |

The six unfinished records are preserved exactly:

| Trade ID | Symbol | Stage |
|---|---|---|
| shadow-46e279513c11e8e6466a | 1000PEPEUSDT | OPEN |
| shadow-541873bf0db2497920cb | STRKUSDT | WAITING_ENTRY |
| shadow-7d1c3107178202bc8ad5 | XPLUSDT | WAITING_ENTRY |
| shadow-7fa7e00e9369e36a05d7 | ZECUSDT | WAITING_ENTRY |
| shadow-b5991f2178104f926cf0 | BTCUSDT | WAITING_ENTRY |
| shadow-ccaa0cbe94d6321c12d7 | GRASSUSDT | WAITING_ENTRY |

## Equivalence and memory measurements

The pristine repository base and fixed source were run in fresh, separate local
Windows/Python 3.14 processes against the complete downloaded dataset. A 1,536 MiB
self-limit protected each offline worker; neither worker exceeded that limit.
Growth stress used disposable trade-journal copies, never the supplied histories.
Processes ran concurrently, so elapsed times are validation cost, not comparable
latency claims. These are local RSS/working-set measurements, not live VPS RSS.

| Measurement | Legacy | Fixed |
|---|---:|---:|
| Bootstrap/report RSS, MiB | 567.89 | 238.15 |
| Max RSS including append/re-observe stress, MiB | **1,502.03** | **255.11** |
| Unchanged-history report cycles | 3 | 20 |
| Appended benchmark updates | 3 | 5 |
| Shared writer/observer instance after growth | no | yes |

Fixed report-cycle RSS ranged from 240.54 to 244.59 MiB; append-cycle RSS ranged
from 243.76 to 244.82 MiB. Max RSS fell **83.02%**. Reports ran back-to-back to stress
allocation; no claim of a timed 30-second live-service soak is made.

After five appended updates the fixed index reported exactly one bootstrap,
five incremental reads, 166,565 processed records, 22,586 retained trade states
and 202,129,529 payload bytes read: original history plus only 6,495 appended bytes.
No second full-history replay occurred. Anchor checks are separately bounded reads.

The **entire** baseline and fixed metrics/analytics snapshots compare equal,
including warnings, every breakdown, outcomes, rates, MFE/MAE, hit/stop/expired
rates and decision-entry matching. Overall results include 12,235 resolved closed
trades, 22,580 terminal lifecycles, six active lifecycles, 5,910 wins, 6,169 losses,
156 breakeven and total R `-3.5242883557657856`. These are descriptive equivalence
checks, not a strategy-performance claim.

Canonical metrics+analytics snapshot SHA-256:
`eee48f1e8708f2c71bcc84351d713d484cd1cc2427c3316c76c780416514eea6`.

Reproduction tool: `scripts/validate_scalper_bounded_memory.py` accepts immutable
decision/trade paths and an output path. `--audit` counts/hashes both inputs;
default mode measures bootstrap/reports; `--source-root` selects a pristine base;
`--grow N` appends only to a disposable copied journal. Output/source collisions
are rejected. JSON reports are local ignored validation artifacts, not source data.

## Tests and semantic boundary

The new regression module covers exact append/restart deduplication; incremental
tails; replacement/truncation/regrowth/deletion; incomplete/malformed tails and
bounded warnings; all six unfinished states; refused unsafe live recovery; random
ordering equivalence for metrics/analytics; observation with full-history APIs
forbidden; repeated refresh; fixed-cardinality 10,000-row Python-memory regression;
and validator output/source collision safety.

Final validation: **20 new OOM/safety regressions passed**; complete Scalper V2/V3
suites **558 passed**; full repository **1,670 passed**; mypy **PASS** (138 source
files); Ruff **PASS** for source/tests and the new validation script. The deployed
SHA port independently passed its **538** available Scalper tests.

Trading/scoring/entry/exit code, Demo Micro, collectors, risk settings, real order
execution, and the separate production episode guard are unchanged. The only
startup behavior change is an explicit refusal to silently drop unfinished legacy
state. No unfinished record is expired, closed or fabricated by this fix.

## Deployment provenance and unresolved runtime recovery

The deployed SHA differs from repository main. The four OOM-path files
`shadow_journal.py`, `cli.py`, `metrics.py` and `analytics.py` match the pristine
repository-base files byte-for-byte. Deployed e149e0c also contains separate causal
entry telemetry/setup changes. The fix was applied locally to a detached copy of
that exact deployed SHA without overwriting those features; its Scalper suite
passed **538 tests**. This is a compatibility check, not a production deployment.

**Do not deploy all of repository main over the deployed checkout.** Port/review
the approved fix onto the deployed release lineage and retain its telemetry.

**Do not start the service yet.** The legacy record schema does not persist the
complete `ShadowTrade`/runtime state: e.g. entry expiry, current stop, remaining
fraction, bars held, last processed bar, full score/confidence/cooldown state.
Exact runtime resumption of the six unfinished trades cannot be proven from these
two journals alone. The previous runtime did not restore them either. This fix
recovers their durable records but refuses an enabled live start rather than
silently discarding them or inventing missing state. Disabled/offline observation
remains possible. No automatic migration or state reconstruction is attempted.

## Safe manual recovery plan — authorization still required

1. Keep `qtr-scanner-scalper-shadow.service` stopped. Preserve/hash both original
   journals, unit/environment configuration and any independent runtime snapshots.
   The observed unit has Restart=on-failure, MemoryMax=infinity; do not enter
   another uncontrolled restart loop.
2. Review the fix and the deployed-lineage port. Pin the reviewed SHA; retain
   causal telemetry changes and run that release's tests. Do not merge/deploy
   unrelated main changes as a shortcut.
3. Re-run the offline validator on immutable copies on the target Python/Linux
   environment. Require the counts/hashes and exact statistics above, zero
   malformed rows, stable bounded RSS and one trade bootstrap across growth.
4. Resolve the six unfinished states explicitly. Prefer a verified complete
   checkpoint/replay with original configuration if one exists. If exact recovery
   is impossible, obtain a separately approved per-trade historical-accounting
   policy. Do not auto-expire the five waiting records, fabricate a close for the
   open record, truncate history or bypass the guard by selecting an empty journal.
   No such policy is authorized or implemented by this change.
5. Only after recovery is resolved and deployment/start is separately authorized,
   deploy the reviewed release while stopped and start manually. Monitor live RSS,
   systemd restarts, journal offsets, malformed counts and all six state identities.
   Compare repeated 30-second reports and append counts. A rising memory trend,
   missing state or replay on ordinary growth is a stop condition.
6. An unsuccessful recovery leaves the service stopped with all histories intact;
   restarting the old OOM-prone code is not a safe fallback. Consider a separately
   reviewed service memory limit as an additional protection, not as this fix.

No production service has been started as part of this work.

Final read-only VPS verification: Type=simple, NRestarts=0, ActiveState=inactive,
SubState=dead. Both journal sizes and SHA-256 hashes still match the pre-work
values above. No remote code, environment, configuration or service action was
performed. History and benchmark data are excluded from the implementation commit.

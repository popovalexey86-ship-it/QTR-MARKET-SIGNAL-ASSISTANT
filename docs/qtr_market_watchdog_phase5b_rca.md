# QTR Market Watchdog Phase 5B RCA and stability repair

## Scope and evidence preservation

The failed acceptance dataset is immutable forensic evidence. It is not an
acceptance dataset and must never be resumed or merged with a later run.

- VPS: `qtr-market-node-01`
- failed data root: `/opt/qtr/watchdog-shadow-data/phase5-soak`
- immutable snapshots:
  `/opt/qtr/watchdog-shadow-data/diagnostics/phase5b-failed-20260920T1014Z`
- final manifest SHA-256:
  `a917e78b665fdb90cffb7bb0e39ce171d77a363f741668aad67abd02d977ef13`
- top-level manifest SHA-256:
  `be75c529f9a31b3445af06f273feb3fa46c2b12ec98c3f9d66a36b31069eef8c`
- systemd journal SHA-256:
  `a8735e3ece4d691e288f23d5f9378834c6f229d72f29a979de12c7f80444337c`
- final SQLite integrity: `ok`
- stopped unit: `inactive/dead`, `MainPID=0`, `NRestarts=82`
- final checkpoint: `6601.625126694329` seconds, status `signal_15`

The production checkout `/opt/qtr/scanner` and all production QTR services
were outside this repair scope.

## Restart storm classification

The 82 automatic restarts are completely partitioned as follows:

| Cause | Count | Classification | Repair |
|---|---:|---|---|
| sustained `degraded_timeout` | 29 | false fatal: symbol-local faults were promoted to global degradation | symbol failures remain isolated and get structured evidence; only zero market progress blocks acceptance |
| `market_progress_stale` | 21 | false fatal: fixed 300 s timeout was shorter than legitimate 10/15 minute polling | health publishes the next expected market deadline; liveness uses deadline plus grace |
| `progress_uninitialized_timeout` | 16 | false fatal: same fixed timeout during legitimate slow-tier startup | same schedule-aware deadline and startup handshake |
| `startup_health_race` | 16 | false fatal: a new supervisor read the preceding process's persisted health | persisted `started_at` must belong to the current process segment |

There were no runtime `FATAL_ERROR` audits and no `JournalConflictError` in
this run. The unit itself was correctly shaped: `Type=simple`, `KillMode=control-group`,
and `ExecStart` directly owned the soak supervisor. `Restart=on-failure` did
exactly what it was configured to do; the bad restart decisions came from the
supervisor's liveness inputs.

## Symbol ValueError RCA

The old runtime persisted only `ValueError`, losing the exception message,
traceback and candidate payload. Consequently an exact historical stack trace
cannot be recovered honestly from this dataset. This is an evidence gap, not a
reason to invent a payload.

Two deterministic faults are demonstrated from code and persisted temporal
state:

1. Baselines were keyed only by `(symbol, feature)`, while runtime tiers switch
   the same symbol among `15m`, `5m`, and `1m`. Same-time or older observations
   with different interval-derived values raised either `Conflicting baseline
   observation timestamp` or `Baseline observations must arrive in
   chronological order`.
2. Outcome paths were symbol-only. A coarser candle could arrive after a newer
   fine-grained candle and raise `Outcome prices must arrive chronologically`.

The four requested live traces all show interval switching immediately before
or during repeated failures:

| Symbol | failures | first/last UTC | interval cursors | last durable context |
|---|---:|---|---|---|
| ONDOUSDT | 28 | 08:16:22 / 10:38:01 | 15m 00:15; 5m 08:05 | baseline available 08:10; price 5m observed 08:05 |
| PONSUSDT | 95 | 01:40:37 / 10:38:28 | 15m 00:15; 5m 07:45 | baseline available 07:50; price 5m observed 07:45 |
| PUMPFUNUSDT | 100 | 01:42:03 / 10:38:28 | 15m 00:15; 5m 08:10; 1m 09:20 | baseline available 09:20; price 1m observed 09:20 |
| STRKUSDT | 86 | 01:41:02 / 10:31:54 | 15m 00:15; 5m 08:45; 1m 09:22 | baseline available 09:22; price 1m observed 09:22 |

Repair:

- baseline identity is now `(symbol, interval scope, feature)` with a v2 store;
- old unscoped state is read as `legacy`, but a fresh stability root writes
  only scoped observations;
- coarser/older outcome points remain immutable raw evidence but cannot regress
  a pending chronological path;
- a rejected baseline commit occurs before symbol-state persistence, preventing
  partial state advancement;
- `SYMBOL_FAILURE` now records symbol, interval, bucket boundaries, exception
  class, message and traceback;
- one symbol fault cannot terminate the runtime or by itself block acceptance;
  an all-symbol market failure does block acceptance.

## Rate-limit RCA

The old `RATE_LIMIT` audit mixed two different facts:

- 5,337 internal rolling-budget waits;
- 9 actual provider rate-limit errors;
- 0 other provider failures.

Across 83 process snapshots, 30,510 calls covered 52,566.6 process-seconds:
weighted mean 34.82 requests/minute. The persisted telemetry cannot provide an
exact per-minute p95, because it did not store request timestamps. Segment
averages give p95 116.31 and peak 590.24 requests/minute, proving startup
burstiness even though the long-run rolling window was capped.

Repair:

- the global gate reserves calls at one-second spacing for a 60/minute budget;
- internal waits are `THROTTLE_WAIT`, never `RATE_LIMIT`;
- `rate_limit_events` counts only real provider rate-limit responses;
- health now reports current rolling calls/minute, peak rolling calls/minute,
  minimum call spacing and throttle wait count.

## Outcome timing RCA

The failed dataset contains 19,491 outcomes: 4,160 at +1m, 4,055 at +5m,
3,927 at +15m, 3,795 at +30m and 3,554 at +60m. Every one is `LATE`; none is
`ON_TIME`. The prior definition required exactly zero seconds between target
and provider availability, which is impossible for polled candle data.

There are 2,298 event/observation groups where one observation closed more than
one horizon. This explains repeated equal price/return values: it was actual
reuse of one delayed point, not independent horizon sampling.

Repair:

- an observation can close at most one horizon for a given event;
- `ON_TIME` means the first distinct observation arrived within its source
  candle interval plus 30 seconds of delivery allowance;
- later observations remain `LATE`; expired horizons can still be explicitly
  `MISSING`;
- old outcomes are immutable and are not recomputed. Any historical
  recomputation requires a separately approved versioned migration plan.

## Memory RCA

Observed process RSS rose from 38,100,992 bytes to 432,640,000 bytes, with a
maximum of 550,998,016 bytes. Diagnostic linear growth was about 26.57 MB/hour.
If unchanged, the purely linear projection was about 676 MB at 24h, 4.50 GB at
7d and 19.17 GB at 30d; these are risk projections, not capacity guarantees.

The dominant retained structures were:

- every immutable JSONL payload stored twice (`_payloads` and `_records`);
- full rich event objects plus all completed outcome keys in the scheduler;
- bounded baseline observations;
- bounded loop durations;
- SQLite remained file-backed and was not the primary contributor.

An old-code tracemalloc construction measured a 221.8 MB traced peak. A deeper
retained-object diagnostic itself reached about 1.2 GiB due to tracemalloc
overhead and was terminated; it did not modify the forensic dataset.

Repair:

- journals retain only ID digest and byte offset, stream records on demand, and
  no longer rescan the complete file after every append;
- conflicting payloads are still loaded and exposed exactly on invariant
  violation;
- completed scheduler events, completed outcome keys and used-observation keys
  are pruned at recovery;
- Linux RSS telemetry now reads current `/proc/self/statm`, not lifetime peak
  `ru_maxrss`.

Post-repair construction against the same immutable forensic dataset retained
27,774,976 bytes RSS without tracing. With bounded one-frame tracemalloc, live
Python allocations were 42.50 MB and the construction peak was 149.59 MB; the
tracer-inflated process RSS is not a runtime projection. The largest retained
Python groups were scoped baselines (16.43 MB), JSON decoder objects (12.88 MB),
journal digest/offset indexes (7.43 MB), event recovery objects (4.52 MB), and
the pruned outcome scheduler (0.67 MB).

## Descriptive event report (no threshold optimization)

Final immutable count is 4,266 events (the earlier 4,180 was an intermediate
snapshot). Anomaly memberships were: COMPRESSION 2,653; RANGE_BUILDUP 2,005;
VOLUME_ACCELERATION 1,209; VOLUME_SHOCK 1,068; VOLATILITY_EXPANSION 601;
PRICE_ACCELERATION 457; FUNDING_ANOMALY 60; OI_SHOCK 3.

Transitions were NORMAL->NORMAL 3,410; WATCH->WATCH 394;
COOLDOWN->COOLDOWN 230; WATCH->COOLDOWN 89; NORMAL->WATCH 85;
COOLDOWN->NORMAL 47; COOLDOWN->WATCH 11. No directional, profit or future
outcome information was used to change detector thresholds.

## Validation contract before a new 24h request

The repair must pass:

1. full Linux `pytest`, `mypy`, `ruff`;
2. regression tests for interval-scoped baselines, chronological tier changes,
   distinct outcomes, startup health ownership, scheduled slow polls,
   worker death, frozen loop, stale health, provider outage, forced kill and
   systemd restart;
3. an isolated 2-4 hour VPS stability run on a new data root with no production
   service changes;
4. zero unexplained restarts, non-zero `ON_TIME`, bounded rate telemetry,
   bounded RSS, no false healthy seconds and no unexplained gaps.

Only after those conditions pass may permission for a new 24-hour acceptance
run be requested. Phase 6 remains out of scope.

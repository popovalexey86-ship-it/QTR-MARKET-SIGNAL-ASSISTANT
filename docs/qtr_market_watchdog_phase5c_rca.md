# QTR Market Watchdog Phase 5C chronology, gap, and memory RCA

## Frozen evidence

The failed Phase 5B Extended dataset is preserved separately from every future
validation root:

- source: `/opt/qtr/watchdog-shadow-data/phase5b-extended-20260920T1840Z`;
- sealed copy:
  `/opt/qtr/watchdog-shadow-data/diagnostics/phase5b-extended-failed-20260921T065901Z/dataset`;
- source and copy contain 14 files and 42,042,192 bytes;
- `hash-diff.txt` is empty;
- final checkpoint is 29,070.519606726244 seconds with status `signal_15`;
- the service stopped `inactive/dead`, result `success`, with zero systemd
  restarts.

The preliminary Windows run, earlier failed VPS runs, and this failed Extended
run are not acceptance time.

## Chronology causal sequence

The immutable audit contains 2,237 occurrences across 47 symbols of
`ValueError: State evaluations must be chronological.` The failure is not a
bad Bybit value and not a relaxation candidate for the state-machine invariant.

The causal sequence is:

1. bucket cursors are interval-local, while `last_detected_at` is global per
   symbol;
2. a provider future can exceed the outer wait timeout; the old executor used
   `shutdown(wait=False)`, so the future remains alive after the loop records a
   timeout;
3. a later loop may plan the same symbol from the still-old interval cursor;
4. the older worker eventually enters the serialized pipeline and advances
   global symbol state using a precise wall-clock `detected_at`;
5. the already-planned worker later enters the pipeline with an exact completed
   bucket at or before that timestamp;
6. the state machine correctly rejects the retrograde evaluation;
7. because the exception occurs before cursor persistence, the stale bucket is
   retried and the backlog grows.

Tier/interval transitions expose the same mismatch even without an overlapping
worker: a 15m, 5m, or 1m cursor can be behind state legitimately advanced by a
different interval. Restart/recovery was not causal in this run: there was one
runtime start, one clean stop, and no systemd restart.

Representative `4STOCKUSDT` evidence:

| Item | UTC value |
|---|---|
| durable 15m cursor before retry | `2026-09-20T19:00:00+00:00` |
| durable symbol `last_detected_at` | `2026-09-20T19:15:02.452214+00:00` |
| first attempted retry boundary | `2026-09-20T19:15:00+00:00` |
| next attempted boundary | `2026-09-20T19:30:00+00:00` |
| first chronology failure audit | `2026-09-20T19:30:00.330154+00:00` |

The first retry is 2.452214 seconds older than durable state. The stored stack
is `runtime.service._process_symbol -> engine.evaluate ->
state_machine.evaluate`; the invariant rejects it before state mutation.

## Gap causality

There are 31 explicit `catchup_cap_exceeded` ledger records. Twenty-seven have
one or more earlier chronology failures for the same symbol and interval (1,219
prior failures in total). Their chain is directly:

`chronology rejection -> cursor does not advance -> repeated retry -> backlog -> catch-up cap -> gap`.

Four gaps have no earlier same-symbol/interval exception. They are the dormant
variant of the same stale-cursor condition: the symbol left an interval long
enough for more than 64 buckets to accumulate, then re-entered that interval.
The old implementation emitted a gap and advanced the cursor before evaluation,
so no chronology exception preceded those four records.

The repair does not increase the catch-up cap and does not hide a real outage.
It advances an interval cursor only to the completed boundary already covered
by durable global symbol state, and emits `CURSOR_REALIGNMENT` evidence with the
old cursor, chronology floor, last detection, interval, and reason. Genuine
unobserved time still uses the existing immutable gap ledger.

## Chronology hardening

- a symbol with a still-running timed-out future cannot be submitted again;
- graceful stop waits for all surviving symbol futures;
- planning reconciles the selected interval cursor with durable global symbol
  chronology before calculating catch-up;
- state chronology remains strict and unchanged;
- rejected observations still cannot partially mutate baseline/state;
- symbol-local exceptions remain isolated;
- provider work remains concurrent across different symbols.

## Memory RCA

Extended-run RSS rose from approximately 27 MiB to more than 124 MiB. A
read-only reconstruction against the sealed dataset measured these retained
Python heap checkpoints:

| Component added | Retained heap MiB | Process high-water MiB |
|---|---:|---:|
| bounded baselines (52,905 observations) | 18.19 | 136.24 |
| state and cursors (121 / 196) | 18.35 | 136.24 |
| event/outcome/price journal indexes | 21.86 | 136.61 |
| audit/gap/storage journal indexes | 27.48 | 137.61 |
| outcome scheduler recovery | 31.55 | 247.11 |

The reconstruction peak (76.06 MiB traced heap and 247.11 MiB process
high-water) is diagnostic and includes transient full-journal decoding; it is
not a live RSS forecast.

Contributors and bounds:

- baselines are intentionally bounded to 240 observations per
  `(symbol, scope, feature)`;
- state is bounded by known symbols; cursors by symbol/interval;
- loop duration samples are bounded to 10,000 and the API window to 60 seconds;
- immutable journal conflict indexes grow with evidence by design, but are now
  one compact `(binary digest, byte offset)` index rather than two dictionaries
  with hexadecimal strings;
- the live outcome scheduler incorrectly retained complete rich event objects,
  five completed keys, and five used-observation keys until restart;
- each health write rebuilt full event and gap tuples and every pending-horizon
  dataclass, creating repeated allocator high-water growth.

Hardening releases the complete scheduler object graph immediately, checks
idempotent re-registration through immutable outcome IDs, caches daily event
counts, uses journal recovery counts for gaps/events, and counts pending
horizons without materializing a tuple. Immutable JSONL evidence and conflict
detection are unchanged.

## Cold-start validation duration

The unchanged baseline contract requires 20 samples. A cold-start symbol is
polled on completed 15-minute buckets. Twenty samples span 19 intervals, or
17,100 seconds (4h45m), and the universe refresh that observes sample 20 occurs
on the next 15-minute refresh. Therefore a from-empty validation needs at least
18,000 healthy seconds (5h), not 4h, to prove `baseline-ready > 0` without
changing baseline requirements or polling.

## Non-goals

This repair does not change detector thresholds, outcome timing rules, PIT
checks, event/outcome immutability, unique observation-per-horizon semantics,
trading direction, execution, Telegram, Scanner, or production services.

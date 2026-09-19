# QTR Market Watchdog Phase 4

Phase 4 adds a shadow-only runtime over the Phase 1-3 domain, detector, state,
evidence, and outcome services. Import and construction perform no network I/O.
The runtime starts only through `start()` or `run_once()` and stops through
`stop()`.

## Runtime flow

```text
paginated Bybit catalog
  -> DynamicUniverse (full eligible set, no rank cap)
  -> tier/state polling rule
  -> completed-bucket cursor (1m/5m/15m)
  -> bounded public REST fetch
  -> PIT feature builder and detectors
  -> anomaly aggregation and persisted state machine
  -> immutable event evidence
  -> pending forward outcomes
  -> immutable raw prices and outcome evidence
```

`WatchdogShadowRuntime` orchestrates existing services but does not duplicate
their models or persistence logic. Baselines, symbol states, events, raw prices,
outcomes, and pending horizons retain their Phase 1-3 ownership.

## Polling policy

| Interest | Feature interval | Eligibility check | Derivatives |
|---|---:|---:|---:|
| COLD_START | 15m | 15m | no |
| STANDARD | 15m | 10m | no |
| ACTIVE | 5m | 5m | yes |
| CORE | 5m | 2m | yes |
| WATCH | 1m | 1m | yes |
| IN_PLAY | 1m | 30s | yes |
| HIGH_ATTENTION | 1m | 15s | yes |

Checks faster than the candle interval never reprocess the same completed
bucket. A persisted `(symbol, interval) -> completed boundary` cursor makes a
restart idempotent. After a short outage, every missing completed boundary is
processed chronologically, subject to a configurable safety cap of 64 buckets.
Open candles never enter a feature snapshot.

The entire eligible universe remains in the runtime snapshot. Work is admitted
in rotating bounded batches (24 symbols by default) and executed by four
workers by default. This is a provider-load boundary, not a universe rank cap.

## Provider safety and API budget

The default public Bybit composition has:

- a rolling 90-request/minute gate applied immediately before each HTTP call;
- four symbol workers and 24 admitted symbols per loop;
- a ten-second outer loop;
- provider timeouts;
- three runtime attempts with exponential 0.5s/1s backoff, bounded at 8s;
- symmetric 25% jitter;
- per-symbol exception isolation.

The request gate observes catalog pagination, ticker, kline, funding, OI, and
derivatives kline calls individually. Rate-limit responses (`429`, Bybit
`10006`, or an explicit rate-limit error) and local budget waits are recorded.
The bounded live verification command lowers this further to 60 calls/minute
and eight admitted symbols per loop.

## PIT derivatives

Funding and OI use the Phase 1-3 adapters. Exchange observation timestamps and
local availability timestamps remain separate. A snapshot is used only when
`snapshot.as_of <= detected_at`; catch-up buckets do not receive a current
derivatives snapshot. The feature adapter excludes values older than its
freshness window and marks them stale/missing.

The reused derivatives provider is composed with an unconnected accumulator
only because that is part of its existing constructor contract. No liquidation
stream is started and no liquidation feature enters Watchdog detection.

## Durable runtime layout

```text
watchdog-shadow/
  events/events.jsonl
  outcomes/price_observations.jsonl
  outcomes/outcomes.jsonl
  operational/runtime.jsonl
  state/baselines.json
  state/symbols.json
  state/pending.json
  state/buckets.json
  state/health.json
```

Operational events are separate from anomaly evidence. The operational journal
records runtime start/stop, universe refresh, provider errors, rate limiting,
degraded operation, recovery, and fatal loop errors.

`health.json` is an atomic local read-only snapshot containing start/loop and
market timestamps, universe and processing counts, failures, events, pending
outcomes, provider/rate-limit errors, loop and symbol latency, stale/missing
counts, API calls, and explicit degraded reasons. No web endpoint is created.

## Recovery order

Construction restores baseline observations, state-machine state, event and
outcome journals, pending raw price paths, and completed-bucket cursors. Start
reconciles due outcomes from immutable price evidence, records a recovery audit,
then rebuilds the current universe from public metadata.

This prevents a normal restart from resetting active states, replaying a
completed bucket, duplicating an event, or losing a recoverable outcome.

## Bounded public shadow verification

After offline gates, a read-only run can be started explicitly:

```text
python -m market_signal_assistant.watchdog.runtime.live_shadow \
  --data-root <isolated-directory> \
  --duration-seconds 60 \
  --symbols-per-loop 8 \
  --api-calls-per-minute 60
```

It requires no credentials, creates no orders or messages, and changes no
production service. Its final JSON reports universe size, processing/API rates,
average and p95 loop latency, provider/rate-limit counts, event/pending counts,
baseline readiness, Python peak memory, and process CPU usage.

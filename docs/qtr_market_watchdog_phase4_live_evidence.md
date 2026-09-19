# QTR Market Watchdog Phase 4 live shadow evidence

Run date: 2026-09-18 (UTC)

The bounded run used only unauthenticated public Bybit REST market endpoints.
It did not place orders, load credentials, send messages, deploy files, or
change a production service.

## Limits

```text
duration target:        60 seconds
symbols admitted/loop:  8
worker limit:           4
HTTP budget:            60 calls/minute
mode:                   shadow-only
```

## Observed result

```text
wall duration:                 60.0828 s
eligible universe:             99
catalog rejected:              780
symbols processed:             40
symbols processed/minute:      39.9449
HTTP calls:                    42
HTTP calls/minute:             41.9421
average loop latency:          2.3127 s
p95 loop latency:              4.7377 s
maximum symbol latency:        1.0142 s
provider errors:               0
rate-limit events:             0
symbol failures:               0
events generated:              0
pending outcomes:              0
baseline-ready symbols:        0
baseline observations stored:  360
completed-bucket cursors:      40
persisted symbol states:       40
Python peak traced memory:      4,988,947 bytes
process CPU / wall ratio:       8.3999%
```

The run was healthy and did not enter degraded mode. Zero generated events is
the expected honest cold-start result: no symbol had the minimum persisted
history needed by anomaly detectors. The runtime did not manufacture a
baseline to make the live test emit events. Event-to-outcome wiring is instead
proved by the deterministic offline integration test, which persists one real
detection event, restores it after restart, and completes its +1m and +5m
outcomes from immutable price observations.

The operational journal contains four records: `RUNTIME_START`, `RECOVERY`,
`UNIVERSE_REFRESH`, and `RUNTIME_STOP`. The universe refresh recorded 99
eligible instruments, 780 rejected instruments, 99 additions, and zero
removals. The health snapshot reports `degraded=false`.

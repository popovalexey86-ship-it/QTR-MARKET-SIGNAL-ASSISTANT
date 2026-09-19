# QTR Market Watchdog Phase 3

Phase 3 records immutable point-in-time evidence and direction-neutral forward
outcomes. Outcomes are descriptive data and are never imported by the detection
layer or used to tune detector thresholds.

## Durable layout

```text
watchdog/
  events/events.jsonl
  outcomes/price_observations.jsonl
  outcomes/outcomes.jsonl
  operational/
  state/pending.json
```

Event, raw-price, and outcome JSONL files are append-only sources of truth.
`pending.json` is an atomic, rebuildable checkpoint. Every append is followed by
`flush` and `fsync`.

Logical IDs are deterministic. An identical repeated payload is a no-op; reuse
of an ID with a different payload raises `JournalConflictError`.

## Event evidence

Each record contains the event identity and PIT chronology, before/after state,
anomaly types and score, explainable contributors, reasons, inline feature
snapshot, baseline counts, missing/stale fields, sequence context, reference
price, and universe tier. Outcomes are never added to this record.

## Forward outcomes

The fixed horizons are `+1/+5/+15/+30/+60m`. The first real observation on or
after `target_time` is used; prices are not interpolated. Lateness is:

```text
max(0, available_at - target_time)
```

Movement metrics are decimal returns:

```text
signed_return = horizon_price / reference_price - 1
abs_return = abs(signed_return)
mfe_up = max(high / reference_price - 1, 0)
mfe_down = max(1 - low / reference_price, 0)
max_abs_excursion = max(mfe_up, mfe_down)
realized_range = (max(high) - min(low)) / reference_price
```

Time-to-MFE and time-to-MAE use the earliest observation attaining the maximum.
Missing horizons are explicit immutable records with `data_quality=MISSING` and
null movement metrics. Late observations retain actual `observed_at`,
`available_at`, and `lateness_seconds`.

Compression/range expansion is a descriptive post-event calculation using a
configurable default 1% excursion. `breakout_side` is UP, DOWN, BOTH, or NONE;
it is not a recommendation. Volume persistence/reversal and volatility
persistence are also descriptive fields.

## Recovery boundaries

- Crash before event append: deterministic replay appends the same logical ID.
- Crash after event append: restart dedup prevents a second event.
- Crash before outcome append: raw price evidence survives; replay produces the
  missing logical outcome.
- Crash after outcome append but before checkpoint: outcome journal wins and
  checkpoint reconciliation marks the horizon complete.
- A valid JSON tail without newline is finalized rather than duplicated.
- An incomplete/corrupt tail is preserved, reported by line number, and skipped;
  later valid records remain readable.

## Dependency firewall

Detection modules do not import `watchdog.events` or `watchdog.outcomes`.
Evidence may import the detection result to capture it, and outcomes may import
event evidence. A source-level architecture test enforces this direction:

```text
detection -> event evidence -> outcomes/statistics
```

No inverse outcome-to-detection dependency is allowed.

## Statistics

Statistics are descriptive only: counts by anomaly/state/score bucket; medians
of abs return, maximum excursion, MFE-up, and MFE-down by horizon; expansion
rate and median time to expansion; missing/late rates; and breakdowns by anomaly
type, anomaly combination, score bucket, symbol, and universe tier.

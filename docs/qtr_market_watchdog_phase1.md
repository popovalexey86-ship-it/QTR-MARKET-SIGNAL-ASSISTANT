# QTR Market Watchdog Phase 1

Phase 1 implements only offline, point-in-time foundations. It does not start a
service, send Telegram messages, mutate Scanner, subscribe to order books, or
create another liquidation pipeline.

## Reused QTR components

- `BybitPublicProvider` remains the catalog and candle transport. Its instrument
  catalog now follows every `nextPageCursor` and retains `launchTime`.
- `CatalogInstrument` remains the normalized Bybit instrument metadata model.
- Existing IN PLAY metadata rules remain the shared USDT linear-perpetual gate.
- Existing UTC-aware domain-model conventions and injectable `JsonGetter` are
  preserved. Import and construction perform no network I/O.
- Existing Early Discovery calculations and derivatives providers are left
  unchanged. Future detectors can consume their observations through the new
  detector contract instead of embedding provider calls.

## PIT contract

Every feature and anomaly has separate `observed_at`, `available_at`, and
`detected_at` timestamps where applicable. Events, candidates, detector inputs,
baseline updates, and state evaluations reject data with
`available_at > detected_at`.

Rolling baselines persist real observations only. Queries filter observations by
their availability timestamp, even if the store already contains later data.
Before `minimum_samples` is reached, the returned snapshot is `cold_start=True`
and all baseline statistics are `None`; no zero, global, or cross-symbol baseline
is invented.

## Universe semantics

`DynamicUniverse` applies tradeability, turnover, top-of-book, spread, contract,
and launch-time gates to the complete paginated catalog. It deliberately has no
top-N cap. Eligible instruments are classified as `COLD_START`, `STANDARD`,
`ACTIVE`, or `CORE`. New instruments and instruments without sufficient baseline
history stay in `COLD_START` regardless of turnover.

## Detector and score semantics

`AnomalyDetector` is a pure protocol over feature and baseline snapshots.
`AnomalyObservation` requires a severity, reasons, feature decomposition, and
explicit missing-data fields. `anomaly_score` is bounded to 0..100 and is not a
directional, trading, confidence-of-profit, or execution score.

## State-machine contract

The lifecycle is:

`NORMAL -> WATCH -> IN_PLAY -> HIGH_ATTENTION`

De-escalation uses lower thresholds than escalation. Repeated weak observations
move `HIGH_ATTENTION -> IN_PLAY -> WATCH -> COOLDOWN`; cooldown expiry resolves to
`NORMAL` or re-enters `WATCH`. Confirmation counts and cooldown duration are
configurable. Phase 1 defaults are operational placeholders, not optimized
thresholds and not evidence of predictive value.

## Deferred explicitly

Detector implementations, multi-factor weighting, event journal, outcomes,
Telegram, Scanner handoff, dashboard, systemd, deployment, and production soak
testing belong to later approved phases.

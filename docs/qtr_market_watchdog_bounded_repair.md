# Watchdog boundedness repair (review-only; not deployed)

Base: `927847b916ac5025df0ead7332bb71094558ba29`. This change is confined to
the isolated Watchdog checkout. It does not change universe eligibility,
detectors, thresholds, catch-up cap, outcome timing, or the existing gap ledger.

## Architecture review and decision

- The dynamic universe previously emitted only aggregate refresh counts. A
  separate immutable JSONL journal now records per-symbol eligible/tier changes,
  rejection reasons, observed/available/recorded UTC timestamps and state names.
  Replay retains only current eligible membership in RAM. The transition
  journal's historical Python index remains empty.
- Each baseline ring remains bounded at `maximum_samples` (240 by default),
  but the number of rings can grow with historically seen symbols. The explicit
  24-hour inactive window retains recent working state; after expiration,
  inactive baseline rings, state and bucket cursors leave RAM. An SQLite
  tombstone is persisted first, with state-machine chronology/cooldown and
  acknowledged interval cursors, before removal from working snapshots.
- Re-entry always discards the prior baseline, even inside the retention
  window. The universe tier is recalculated after this eviction, so it starts
  `COLD_START`. State-machine state/cooldown and cursor chronology are restored
  unchanged from the tombstone when applicable. Inactive completed buckets are
  explicitly recorded in the existing gap ledger as `inactive_reentry`, and
  only the current completed bucket is eligible for forward processing. There
  is no retrospective detector evaluation or synthetic baseline backfill.
- Eviction is triggered on successful universe refresh, not by a background
  timer. Thus resident symbols are bounded by the current eligible universe
  plus symbols removed within the retention window and one refresh cadence.
  Baseline observations are bounded by resident keys times per-key
  `maximum_samples`. Disk tombstones and the append-only evidence journal are
  durable history, not in-memory working indexes; their storage growth still
  needs operational monitoring.

## Parked issue

If no future price observations arrive, the existing pending outcome graph may
remain pending indefinitely. This repair does **not** call `mark_missing`, add a
grace period, or alter `ON_TIME`/`LATE`/`MISSING` semantics. It is a known
architectural limitation requiring a separate QTR AI decision.

## Verification boundary

The offline stress test forces 3,600 rings beyond their 3-sample bound, then
expires 1,180 of 1,200 transient symbols. The resulting working set is 60 keys,
180 observations (20 active symbols x 3 keys x 3 samples), 20 symbol states,
and 20 interval cursors, and remains at those cardinalities on a later refresh.
Re-entry, restart recovery, chronology,
explicit gap accounting and transition PIT ordering are checked separately.
This does not substitute for an authorized VPS validation or prove bounded RSS
under live market load.

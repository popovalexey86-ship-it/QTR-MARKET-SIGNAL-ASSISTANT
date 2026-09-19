# QTR Market Watchdog Phase 5 VPS failure RCA

## Failure boundary and preserved evidence

- Runtime failure: `2026-09-19T18:16:23.622068+00:00`.
- Fatal type: `JournalConflictError`.
- Acceptance time after that instant is invalid.
- Preserved VPS snapshot:
  `/opt/qtr/watchdog-shadow-data/diagnostics/failure-20260919T181623Z`.
- Snapshot contains the complete data root, installed systemd unit, commit,
  systemd state, failure boundary, and `SHA256SUMS`.
- The failed service was stopped only after the snapshot was created. It ended
  `inactive (dead)` with its final checkpoint marked `signal_15`.

## Exact logical conflict

The conflict came from `operational/gaps.jsonl`, not the event, price, outcome,
storage, or operational-audit journals.

The reused logical ID was:

```text
c842ad7066ac045909417eba7f646721271987ee8e22e3d51bf5eaa8d9fede90
```

The persisted payload was:

```json
{"first_missing_boundary":"2026-09-19T17:11:00+00:00","gap_id":"c842ad7066ac045909417eba7f646721271987ee8e22e3d51bf5eaa8d9fede90","interval":"1m","last_missing_boundary":"2026-09-19T18:15:00+00:00","missing_bucket_count":65,"reason":"catchup_cap_exceeded","recorded_at":"2026-09-19T18:16:02.324620+00:00","safe_backfill":"OHLCV_BASELINE_ONLY_AT_RECOVERY_AVAILABILITY","symbol":"AKEUSDT"}
```

The rejected payload had the same ID and every same field except a later
`recorded_at` from the fatal loop. The old runtime recorded only the exception
class and did not serialize the exception object, so the attempted
microsecond timestamp is not recoverable from durable evidence. It is bounded
after `2026-09-19T18:16:02.324620+00:00` and no later than the fatal audit time
`2026-09-19T18:16:23.622068+00:00`. The regression fixture uses the fatal audit
timestamp as the deterministic witness payload and proves that it raises for
the live ID while exposing both payloads. The enhanced exception now retains
the journal path, logical ID, existing payload, and attempted payload so a
future invariant failure is fully attributable.

## Causal chain

1. `AKEUSDT` repeatedly failed symbol processing with `ValueError`; its 1-minute
   cursor remained at `17:10:00Z`.
2. Catch-up planning at `18:16:02Z` exceeded the cap and persisted a gap from
   `17:11:00Z` through `18:15:00Z`.
3. The old code reduced processing to the latest bucket but did not acknowledge
   the skipped range in the durable cursor unless latest-bucket processing
   later succeeded.
4. The next loop was still in the same completed 1-minute boundary. It produced
   the same deterministic gap ID because gap identity correctly described the
   same symbol, interval, first/last boundary, and reason.
5. It produced a different payload because wall-clock `recorded_at` was part of
   the immutable payload but not the ID. `ImmutableJsonlJournal` correctly
   rejected this invariant violation.
6. The background runtime caught and recorded the fatal error, then died. The
   soak supervisor did not observe worker death and continued adding wall time,
   leaving systemd falsely `active (running)`.

This was same-gap retry after a failed latest-bucket attempt. It was not a
restart, event replay, timestamp normalization error, outcome collision, or
state-transition ID collision.

## Corrections

- Gap `recorded_at` is now deterministic: the first boundary after the skipped
  range, rather than retry wall time.
- After the immutable gap is persisted, the cursor durably acknowledges the
  skipped range before attempting the latest bucket. A latest-bucket failure is
  therefore retried without re-emitting the gap. Crash between journal and
  cursor writes is also idempotent because retry payloads are identical.
- Background runtime exceptions are retained and observable by the supervisor;
  journal conflicts additionally expose journal path, ID, and both payloads.
- The soak uses a confirmed-healthy clock. Only an interval closed by fresh,
  non-degraded market progress is added. Worker death, fatal exception, stale
  heartbeat, stale/frozen market progress, or sustained degraded/provider
  outage freezes the clock and exits non-zero.
- The tracked `Type=simple` unit runs that supervisor as `MainPID` with
  `Restart=on-failure`, so systemd observes the process that owns acceptance.

The failed data root is diagnostic evidence only and must never be resumed for
acceptance. A future approved run must use a fresh data root and start at zero.

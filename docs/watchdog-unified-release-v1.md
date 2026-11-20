# Watchdog Unified Release V1 — post-soak cutover plan

This branch combines the existing detection-only Observer control snapshot, one
Telegram consumer, notification selection, and Russian presentation. It does
not change detector scores, thresholds, market polling, outcomes, or immutable
event journals. Deployment requires a separate final review.

## Operator contract

- Start the approved new Observer in a clean checkout and a new empty data root.
  Record the exact SHA and runtime configuration in provenance. The old 24-hour
  dataset and SQLite are preserved read-only.
- Start the new Telegram adapter first against the new empty event journal and
  a new SQLite file. Use `--panel-root`, `--alert-policy-v1`, and an explicitly
  configured display timezone (default `Europe/Moscow`). Keep the same protected
  bot token/chat ID and allowed user IDs. Do not copy the old cursor or outbox.
- Verify a single `getUpdates` consumer and a single journal consumer. The
  adapter holds a SQLite-adjacent process lock. A second unit with another
  database is still an operational conflict and must remain stopped.
- Check SQLite `quick_check`, cursor device/inode/offset at zero, empty alert
  and command outboxes, unit `Restart=no`, and fresh health. Then start the new
  Observer. Verify `/start`, `/status`, `/market`, `/inplay`, control snapshot
  publication, real event projection, and delivery only for new natural events.
- The old frozen adapter must not resume backlog sending. Before cutover,
  verify its outbox and absence of an in-flight request. If a controlled stop
  cannot guarantee no send after unfreezing, stop the cutover for review.
- Roll back by stopping the new Observer and new adapter while preserving their
  dataset and SQLite. Never reactivate the old unfiltered adapter or migrate
  its pending outbox into the new consumer.

## Notification and UX behavior

- Every Watchdog event remains in the original journal. Suppressed event IDs
  are recorded in SQLite and remain available to the panel from its activation
  boundary. The panel does not backfill previous datasets.
- Immediate alerts use the existing score and state evidence. The 12-hour
  per-symbol cooldown admits material escalation after 30 minutes and a new
  observed NORMAL→WATCH episode after 30 minutes. The rolling maximum is 36
  immediate alerts per 24 hours. A qualifying alert beyond the cap is retained
  in the journal and summarized in the digest; this is an explicit policy
  tradeoff requiring observation after launch.
- A selected alert expires 10 minutes after detection if not delivered.
  Pending Telegram sends are at-least-once attempts, so an ambiguous API
  acknowledgement may still duplicate a message; durable event IDs prevent
  duplicate enqueue. Manual commands use a separate bounded reply outbox and
  are not subject to the automatic-alert cap.
- The compact alert gives factual price, volume, open-interest and funding
  values only when present. Open-interest and funding measurement periods are
  not recorded in current event features, so the alert explicitly says so.
  Interpretations are labelled and never suggest trade entry, exit, or future
  direction. Detailed callbacks show the underlying technical feature names.
- Digest current leaders require a fresh control snapshot. Historical leaders
  are labelled separately. A stale/missing snapshot is never rendered as zero
  current anomalies. All user-facing text is Russian; technical identifiers
  remain in the details view.

## Warm-up and resource gates

The snapshot publisher is bounded to 2,048 symbols and 2 MiB. Panel event
retention is 24 hours / 100,000 records; command replies stop polling at 1,000
pending. New Observer data roots start with a cold baseline. No supported
baseline/checkpoint transfer between the running provider-hotfix Observer and
this candidate has been validated, so plan a complete re-warm. Baseline and
polling optimizations are separate future work.

Before an authorized cutover, measure `MemAvailable`, cgroup `memory.events`,
disk headroom, Observer market progress, old/new adapter ownership, and
integrity of both SQLite files. Keep current Observer memory containment and
`Restart=no`; do not deploy this branch during the existing soak.

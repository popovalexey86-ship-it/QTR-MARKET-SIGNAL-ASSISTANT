# Watchdog Telegram Control Panel V1

This is an optional extension of the existing Watchdog Telegram adapter. The
adapter remains the only process holding the bot token. It sends anomaly alerts
first and handles commands in the same loop. The Watchdog runtime never calls
Telegram and the panel never calls the market provider.

## Data contract

- Detection-only Watchdog publishes `state/control-panel.json` by atomic replace.
  It retains at most 2,048 current symbol rows and writes at most 2 MiB. The
  snapshot contains only existing score, state, detector types and selected
  feature values. It is an operational read model, not a journal or evidence
  source. The runtime keeps its existing event and bucket persistence order.
- The adapter still tails `events/events.jsonl` by byte offset. When the panel
  is enabled, event metadata is inserted into `panel_events` in the same SQLite
  transaction as the alert outbox and journal cursor. `panel_events` keeps at
  most 100,000 rows and 24 hours. No outcome information is used.
  Statistics begin when the panel is enabled; older journal records are not
  replayed into the panel.
- `panel_cursor` persists the Telegram update offset. First launch uses the
  Telegram negative-offset request to discard already queued commands. New
  commands are persisted into `panel_outbox` before the update cursor advances.
  Duplicate updates cannot create duplicate replies. A crash after an accepted
  `sendMessage` but before the local SENT commit can produce a second reply;
  Telegram does not offer an idempotency key for this call.
- At 1,000 pending command replies the panel pauses update polling until the
  outbox drains. Automatic anomaly alerts still take priority. Telegram's own
  update retention can expire commands during a prolonged outage; the panel
  does not silently advance its local cursor while backpressured.
- Snapshot, health and supervisor reads have hard byte limits. The panel does
  not read full JSONL history. A stale snapshot cannot become a current
  IN-PLAY item. `/scan` shows the most recent snapshot and never requests fresh
  market data. `/settings` is informational in V1.

## Isolated deployment review procedure

Do not use these steps during the current Observer soak. After separate approval:

1. Verify the candidate SHA, Linux quality gates, Observer health, adapter
   outbox and SQLite integrity. Confirm the approved Observer and adapter
   checkout paths and that no other process calls `getUpdates` for this token.
2. Prepare the unified adapter command using a fresh Observer event journal,
   a new separate Telegram SQLite state, `--alert-policy-v1`, and `--panel-root`
   pointing to the matching new Observer data root. Preserve the old SQLite
   and cursor without migration or replay. Configure
   `QTR_WATCHDOG_TELEGRAM_ALLOWED_USER_IDS` in the protected environment file.
   Preserve token and chat ID unchanged. Do not log or copy credentials.
   Render `deployment/systemd/qtr-watchdog-telegram-control-v1.service.template`
   with approved checkout, interpreter, data and state paths; verify the
   rendered unit with `systemd-analyze verify` before replacing any adapter.
3. After the 24-hour soak, preserve the old adapter unit/config/SQLite. The
   frozen adapter must be terminated without resuming backlog delivery. Verify
   it has no in-flight request before a controlled TERM/CONT stop; if that
   cannot be established, stop and review. Start exactly one unified adapter
   against the new empty journal before starting the new Observer.
4. Check alert outbox/cursor, panel cursor, SQLite `quick_check`, and a read-only
   `/status` request. Verify one updates consumer and automatic alerts. An old
   event journal is not replayed. Do not create test market anomalies.
5. Roll back by stopping the new Observer and unified adapter while preserving
   both new datasets. Never resume the old unfiltered adapter or its backlog.
   If command bootstrap or alert delivery is uncertain, stop and review.

No systemd unit or running service is changed by this candidate. Credentials,
chat IDs and runtime datasets are not included in this repository change.

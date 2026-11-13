# Watchdog Telegram V1 — offline implementation and deployment template

This independent process reads only `events/events.jsonl`. It never imports or
starts the detector runtime, writes Watchdog evidence, or touches Scanner/Trader
Telegram. Installing or starting it requires separate authorization.

## Delivery contract

- A fresh state database starts at the journal's current byte EOF. Historical
  events are not bulk delivered. All later complete lines are staged with the
  byte cursor in one SQLite transaction (`synchronous=FULL`).
- `event_id` is the durable SQLite primary key. An identical repeated journal
  line is ignored; a conflicting ID stops the projector. Sent IDs remain on disk;
  delivered message text is removed from the working outbox.
- The sender is rate limited by a persisted global next-send time. Telegram 429,
  5xx, network errors, and timeouts retry with bounded backoff. A client 4xx
  blocks delivery and stops the service for operator review. No API response
  text or request URL is logged.
- A complete corrupt JSONL line, file truncation, inode rotation, or journal
  disappearance stops the projector without advancing its cursor. The operator
  must inspect immutable evidence; it must never silently re-bootstrap.
- Telegram `sendMessage` has no idempotency key. A crash after Telegram accepts
  a message but before the SQLite `SENT` commit can produce one duplicate after
  the 60-second in-flight lease. This is **at-least-once**, not exactly-once
  delivery. The event ID in the message lets recipients identify a repeat.
- The source journal must exist at first start. A missing journal fails closed
  instead of making a later historical copy look like new events. The state DB
  pins a hash of the destination chat ID; changing recipients requires a
  reviewed new bootstrap/replay decision.
- The adapter is a separate process. Telegram latency/backoff cannot block
  Watchdog market work. A prolonged outage can grow the disk-backed pending
  outbox; monitor its count and free disk before enabling.

## Example (illustrative; never sent by this implementation review)

```text
🚨 QTR Watchdog — рыночная аномалия
Символ: ABCUSDT
Тип: Сжатие диапазона, Всплеск объёма
Время: 2026-09-18 08:00:00 UTC
Score: 65.0/100
Состояние: NORMAL → WATCH
📊 Доступные метрики:
• normalized_range_5: 0.005
⚠️ Часть данных отсутствует или устарела
ID: watchdog:ABCUSDT:20260918T080000.000000Z
```

## Proposed VPS installation (not executed)

1. Confirm the exact Watchdog checkout SHA, clean status, target data root,
   event journal ownership, and absence of another projector process.
2. Choose a separate adapter state directory, for example
   `/var/lib/qtr/watchdog-telegram-v1`; create it owned by `qtr`, mode `0700`.
   Keep it **outside** the immutable Watchdog dataset. Back it up with the
   journal if restart recovery is required.
3. Render `deployment/systemd/qtr-watchdog-telegram.service.template` with
   `__WATCHDOG_DATA_ROOT__` and `__ADAPTER_STATE_ROOT__` replaced by reviewed
   absolute paths. Do not point it at a historical completed dataset without
   explicitly deciding the bootstrap boundary.
4. Place a filled-in copy of `watchdog-telegram.env.template` at
   `/etc/qtr/watchdog-telegram.env`, owned by root, mode `0600`. Use the
   dedicated Watchdog BotFather token and reviewed destination chat ID. Never
   place the filled-in file in Git or process arguments. Keep it separate from
   Scanner and Trader credentials.
5. Run `systemd-analyze verify` on the rendered unit and review its effective
   paths, `EnvironmentFile`, and `Restart=no`. Only after a separate send
   authorization should an operator install/enable/start the unit and perform
   a controlled single-message acceptance test. There is no dry-run send path.
6. Monitor service status, SQLite outbox counts (`PENDING`, `INFLIGHT`, `SENT`,
   `BLOCKED`), journal cursor and disk space. Never reset the SQLite state to
   resolve delivery errors without a reviewed replay decision.

No token or chat ID is included in this repository. This review does not send
messages, create a VPS unit, or deploy the adapter.

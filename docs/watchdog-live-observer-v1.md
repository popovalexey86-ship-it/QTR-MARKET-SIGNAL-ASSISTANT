# Watchdog Live Observer V1: controlled launch draft

This is an **offline deployment review artifact**. No runtime, unit, Telegram
request, or data root is started or created by this document. The observer uses
the existing public-market shadow composition with 8 symbols per loop and a
60-call/minute API budget. The pilot unit selects `--detection-only`: detectors,
scoring, PIT checks, state transitions and event evidence keep their contracts,
while the outcome scheduler is not constructed.

## Detection-only dataset contract

- Use a new empty data root. The supervisor pins `DETECTION_ONLY` in
  `state/runtime-mode.json` before the worker starts. A missing or corrupt mode
  manifest after events have been recorded, a mode switch, or any pending,
  price-observation or outcome state blocks recovery. Never reuse an Evidence
  V1/V2 or full-outcome data root.
- The worker persists the same deterministic event evidence before acknowledging
  a bucket cursor. No pending graph, raw-price journal, outcome journal or
  `state/pending.json` is produced. Health reports `outcome_mode=DETECTION_ONLY`
  and zero pending outcomes. An event can be sent by the separate Telegram
  adapter only after its append-only journal record exists.
- Restart after a clean stop reopens the event journal and acknowledged bucket
  cursors. Missing or corrupt cursor/checkpoint evidence fails closed. This
  dataset cannot be used as an Evidence Baseline outcome cohort: it contains
  detections only, with no `+1/+5/+15/+30/+60m` outcomes.

## Pair and source switch

1. Record exact clean checkout SHA and verify that no Watchdog runtime or other
   Telegram projector is active. Preserve the old Evidence V1 journal and the
   existing adapter SQLite database. Do not reuse the old cursor database.
2. Under `/opt/qtr/watchdog-shadow-data`, create a new, empty
   `watchdog-live-observer-v1-<UTC>` root. Record immutable provenance before
   starting. Pre-create only `events/events.jsonl` as an empty file. Choose a
   **different** qtr-owned mode-0700 adapter state directory under `ops`.
3. Gracefully stop the old Telegram adapter after its outbox is empty. Save its
   rendered unit and SQLite files including WAL/SHM. Render the existing adapter
   template with the new root and state directory; keep the existing root-only
   environment file and credentials unchanged. Validate with
   `systemd-analyze verify`. Do not enable either unit for the first pilot.
4. Start the adapter **before** the observer. Require `active`, SQLite
   `quick_check=ok`, one cursor at offset zero, matching inode/device for the
   empty source file, and zero outbox rows. If any check fails, do not start the
   observer. There is no old-event replay or cursor migration.
5. Render and validate the Live Observer unit. The supervisor requires the
   active adapter to name the new journal and new state DB exactly. It owns a
   separate process lock and starts one worker. The worker owns the existing
   Watchdog writer lock. Neither service has automatic restart or autostart.
6. Monitor two PIDs, health freshness and advancing market progress, observer
   checkpoint, disk/memory reserve, SQLite integrity, journal identity, outbox
   pending/blocked count and oldest enqueue age. Send no synthetic anomaly.

## Failure and recovery contract

- Supervisor exit `0`: clean SIGTERM; worker stopped and checkpoint persisted.
- Exit `75`: transient liveness, provider, adapter availability/backlog or memory
  failure; no automatic restart. Preserve evidence and investigate first.
- Exit `78`: integrity/capacity failure, corrupt checkpoint/SQLite, cursor or
  journal identity mismatch, chronology/PIT/journal conflict, or incomplete
  graceful stop. Fail closed; do not resume until reviewed.
- Existing runtime disk-pressure threshold is 1 GiB or 5% free. The supervisor
  stops earlier, below 4 GiB or 10% free. Startup requires >=1 GiB
  MemAvailable; operation requires >=512 MiB. These are operational reserves,
  not changes to runtime detection semantics.
- Outbox stops the pair at >1000 outstanding rows or oldest enqueue age >1h.
  A BLOCKED row stops it immediately. Telegram 429/5xx/network retries remain
  adapter behavior; a prolonged outage becomes observable backlog. Old rows
  without enqueue timestamps are fail-closed. `enqueued_at` is not a market
  observation time and does not affect delivery order or event content.
- The observer checkpoint lives at `state/observer.json` and is separate from
  `state/soak.json`. A missing checkpoint is allowed only before any runtime
  evidence exists. A corrupt checkpoint or prior failed status requires review.
  A manual restart after a clean stop resumes the accumulated healthy seconds;
  the clock never advances through downtime or no-progress intervals.
- On reboot both units stay disabled. An operator must inspect the prior stop,
  checkpoint and journals, then manually restart adapter first and observer
  second. This is the approved first-pilot recovery policy.
- `SendSIGKILL=no` avoids an automatic force kill at stop timeout. A stuck
  worker is left for explicit review; the process locks prevent a second writer.

## Rollback

Gracefully stop **only** the Live Observer and the new-source Telegram adapter.
Verify worker exit and final checkpoint, preserve the entire live data root and
adapter SQLite (including WAL/SHM), and leave the units disabled. Restore the
saved old adapter unit and its old SQLite only after confirming no outbox loss;
the old Evidence V1 journal remains immutable. Never reset a cursor, remove a
journal or restart Scanner/Micro/Trader/Long as part of rollback.

## Resource and acceptance review

The 2026-11-13 VPS preflight had 2 CPU, 7.71 GiB RAM, 2.69 GiB MemAvailable,
no swap, and 13.56 GiB disk free. Evidence V1 used about 60 MiB over 11.3h
(roughly 127 MiB/day at that load) and ended near 91 MiB Watchdog RSS. This
is a planning estimate, not a storage bound; sample disk, RSS, retained counts,
outbox and PSI daily. Evidence V1 averaged about 225 events/hour, so assess
Telegram delivery volume without changing detector thresholds or selecting
events during the pilot.

Operational acceptance requires clean checkout/provenance, one worker/one
adapter, no Watchdog trading credentials or orders, fresh health with advancing
market progress, no chronology/PIT/journal conflict, bounded outbox and storage,
readable SQLite, explicit universe and gap evidence, and no production change.
Any failure stops the pilot for review. This is observational readiness, not
predictive or trading validation.

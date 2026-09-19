# QTR Market Watchdog Phase 5 operations

Phase 5 is an observational shadow acceptance run. JSONL remains the immutable
source of truth; SQLite is a disposable lookup index and may be deleted and
rebuilt from JSONL at any time.

## Hardening

- `state/writer.lock` is an OS advisory single-writer lock. A second runtime
  fails before provider or storage work. Forced process termination releases
  the OS lock.
- `state/evidence.sqlite3` indexes event, raw-price, and outcome logical IDs,
  line numbers, symbols, chronology, and payload hashes. Source size/mtime is
  verified before reuse; corruption or drift triggers a full rebuild.
- `operational/gaps.jsonl` records long scheduling gaps. When the catch-up cap
  is exceeded, the runtime does not create retrospective anomaly decisions for
  the missing range. Historical OHLCV may only be admitted to future baselines
  with its actual recovery availability time; it may not create past events or
  state transitions.
- `operational/storage.jsonl` samples RSS, process CPU time, disk free space,
  JSONL/SQLite/total bytes, and individual file sizes.
- Disk pressure is checked before provider work. At the configured safety
  boundary the loop enters explicit degraded mode and stops new market writes;
  raw evidence is never deleted to make a check pass.
- `operator` is a read-only health/audit/state/gap/storage/index inspector.
- `soak` is resumable through atomic `state/soak.json` checkpoints. Its clock
  advances only when a fresh, non-degraded runtime heartbeat and new market
  progress jointly confirm the preceding interval. Worker death, a fatal
  invariant error, stale health, or stalled market progress freezes the clock
  and exits non-zero so the service enters its restart path.

## Resumable soak commands

```text
python -m market_signal_assistant.watchdog.runtime.soak \
  --data-root <isolated-shadow-root> \
  --duration-hours 24 \
  --symbols-per-loop 8 \
  --api-calls-per-minute 60

python -m market_signal_assistant.watchdog.runtime.operator \
  --data-root <isolated-shadow-root>

python -m market_signal_assistant.watchdog.runtime.baseline_report \
  --data-root <isolated-shadow-root> \
  --output <report.json>
```

Stopping and restarting the soak continues only its accumulated confirmed
healthy duration. Startup, degraded intervals, provider outages, frozen loops,
and time after the last confirmed market progress are never backfilled into the
acceptance clock. A forced termination loses any unconfirmed interval after the
last atomic checkpoint; event, price, outcome, baseline, state, gap, and cursor
durability do not depend on the soak checkpoint.

## Phase 5 isolated systemd configuration

```ini
[Unit]
Description=QTR Market Watchdog Phase 5 isolated shadow soak
Wants=network-online.target
After=network-online.target
ConditionPathIsDirectory=/opt/qtr/watchdog-shadow
ConditionPathIsDirectory=/opt/qtr/watchdog-shadow-data/phase5-soak

[Service]
Type=simple
User=qtr
Group=qtr
WorkingDirectory=/opt/qtr/watchdog-shadow
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/qtr/watchdog-shadow/.venv/bin/python -m \
  market_signal_assistant.watchdog.runtime.soak \
  --data-root /opt/qtr/watchdog-shadow-data/phase5-soak \
  --duration-hours 24 --symbols-per-loop 8 --api-calls-per-minute 60
Restart=on-failure
RestartSec=15s
TimeoutStopSec=75s
KillSignal=SIGTERM
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/qtr/watchdog-shadow-data/phase5-soak
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
```

The tracked unit has no `EnvironmentFile` and requires no exchange credentials.
`Type=simple` makes the fail-fast soak supervisor the systemd main process;
runtime failure therefore produces a non-zero service result instead of a
false `active (running)` state.

Before any future installation, paths, qtr ownership, Python environment,
resource limits, network policy, health command, startup recovery, and shutdown
behavior must be revalidated on the target host. This proposal is not a deploy.

## Future Telegram architecture (not implemented)

```text
immutable Watchdog journals
  -> read-only notification projector
  -> durable delivery outbox with logical event ID
  -> formatter (anomaly evidence, state, missing/stale warnings)
  -> dedicated QTR Watchdog bot adapter
  -> delivery receipt journal
```

The bot must never run detection, alter scores, select direction, or feed
delivery outcomes back into Watchdog. It should default to state-transition and
high-attention notifications, use per-chat rate limiting and deduplication, and
support a read-only `/health` summary. Token entry and storage must be handled
outside source control through an operator-controlled secret mechanism. No bot
token, Telegram dependency, chat, or message is created in Phase 5.

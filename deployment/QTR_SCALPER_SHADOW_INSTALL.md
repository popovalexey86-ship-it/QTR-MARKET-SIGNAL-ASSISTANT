# Установка QTR Micro Scalper V2 Shadow Observer

Этот deployment предназначен только для теневого наблюдения. Unit не содержит
execution-компонентов, торговых API, реальных ордеров или Telegram-команд.

## Безопасные значения по умолчанию

В unit зафиксированы:

```text
QTR_SCALPER_V2_ENABLED=false
QTR_SCALPER_V2_SHADOW_MODE=true
QTR_SCALPER_V2_LIVE_ENABLED=false
QTR_SCALPER_V2_SETUP_AUDIT_PATH=/opt/qtr/scanner/data/qtr_setup_telegram_pilot_audit.jsonl
QTR_SCALPER_V2_HOLDING_EXPERIMENT_ENABLED=false
```

Поэтому установка, `daemon-reload` и даже случайный ручной запуск не открывают
WebSocket. В unit намеренно отсутствует секция `[Install]`: autostart не
настраивается.

## Пользователь процесса

Проект уже использует выделенного системного пользователя и группу `qtr:qtr`.
Сначала проверьте их наличие:

```bash
getent passwd qtr
getent group qtr
```

Только на новом сервере, где обе записи отсутствуют, подготовьте identity:

```bash
sudo useradd --system --user-group --home-dir /opt/qtr --no-create-home \
  --shell /usr/sbin/nologin qtr
```

Не создавайте второго пользователя для Scalper V2: checkout, `.venv` и каталог
данных должны принадлежать тому же production identity.

## Подготовка runtime

Команды выполняют Python-операции сразу от `qtr`, не создавая root-owned `.venv`:

```bash
sudo install -d -o qtr -g qtr -m 0750 /opt/qtr/scalper-shadow/data
sudo -u qtr python3 -m venv /opt/qtr/scalper-shadow/.venv
sudo -u qtr /opt/qtr/scalper-shadow/.venv/bin/python -m pip install --upgrade pip
sudo -u qtr /opt/qtr/scalper-shadow/.venv/bin/python -m pip install \
  -e '/opt/qtr/scalper-shadow[websocket]'
```

WebSocket dependency устанавливается, но импорт package, установка unit и
`daemon-reload` соединение не открывают.

## Установка unit без запуска

Из корня checkout `/opt/qtr/scalper-shadow`:

```bash
sudo install -o root -g root -m 0644 \
  deployment/systemd/qtr-scanner-scalper-shadow.service \
  /etc/systemd/system/qtr-scanner-scalper-shadow.service
sudo systemctl daemon-reload
sudo systemctl cat qtr-scanner-scalper-shadow.service
systemctl is-enabled qtr-scanner-scalper-shadow.service || true
systemctl is-active qtr-scanner-scalper-shadow.service || true
```

Ожидаемое состояние после установки: `disabled`/`static` и `inactive`. На этом
этапе не выполняйте запуск сервиса и не добавляйте autostart.

## Логи и остановка

После отдельного ручного решения о запуске stdout/stderr будут доступны в
systemd journal под идентификатором `qtr-scanner-scalper-shadow`. `SIGTERM`
обрабатывается CLI как graceful shutdown: останавливается ShadowService и
сбрасывается журнал. systemd ожидает до 30 секунд, затем завершает зависший
процесс.

Unit разрешает запись только в `/opt/qtr/scalper-shadow/data`. Production audit
`/opt/qtr/scanner/data/qtr_setup_telegram_pilot_audit.jsonl` доступен Shadow
только для чтения. API keys и торговые
credentials для Shadow Observer не требуются и не должны добавляться в unit.


## Dynamic Target Universe (Shadow only)

Dynamic targets are opt-in and disabled by default:

```text
QTR_SCALPER_V2_DYNAMIC_TARGETS_ENABLED=false
QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS=5
QTR_SCALPER_V2_TARGET_REFRESH_SECONDS=30
```

When enabled, the Shadow service reads the existing verified Setup Pilot JSONL
incrementally, ranks complete fresh records deterministically, and subscribes to
the TOP-N symbols. Active WAITING_ENTRY, OPEN, or TP1_HIT shadow trades remain
subscribed until their existing lifecycle reaches a terminal state.

Recommended rollout:

1. Start with `QTR_SCALPER_V2_MAX_ACTIVE_SYMBOLS=3`.
2. Observe WebSocket subscription metrics, shadow journal growth, CPU, and RAM.
3. Increase to 5 only after the TOP-3 shadow run remains stable.

Rollback requires no code change. Set
`QTR_SCALPER_V2_DYNAMIC_TARGETS_ENABLED=false` and use the existing
`QTR_SCALPER_V2_SYMBOLS` fixed-symbol setting. Dynamic mode never places real
or Demo orders and does not change scoring, risk, or lifecycle thresholds.

## Parallel Holding Horizon Experiment (Shadow only)

The controlled A30/B60/C120/D300 experiment is independently opt-in:

```text
QTR_SCALPER_V2_HOLDING_EXPERIMENT_ENABLED=false
QTR_SCALPER_V2_HOLDING_EXPERIMENT_MAX_ACTIVE_GROUPS=1000
QTR_SCALPER_V2_HOLDING_EXPERIMENT_JOURNAL_PATH=/opt/qtr/scalper-shadow/data/qtr_micro_scalper_holding_experiment.jsonl
```

When enabled, one accepted baseline shadow entry is mirrored into four virtual
variants. Entry, stop, TP1, TP2, score, setup metadata and observed public-trade
stream are identical; only `maximum_holding_bars` differs. A30 remains a mirror:
the authoritative production baseline is still
`qtr_micro_scalper_shadow_journal.jsonl`.

The experiment writes only lifecycle transitions to its separate append-only
JSONL. It never creates another score, setup decision, signal or order. Active
experimental variants protect their symbol subscription after baseline A30 has
finished, without changing Dynamic Target ranking. On shutdown or restart,
unfinished variants are recorded as `INTERRUPTED/INCOMPLETE` and are excluded
from performance analytics.

## Micro Profit + Continuation Experiment (Shadow only)

The micro-profit experiment is independently opt-in and remains disabled in the
deployment unit:

```text
QTR_SCALPER_V2_MICRO_PROFIT_EXPERIMENT_ENABLED=false
QTR_SCALPER_V2_MICRO_PROFIT_MAX_ACTIVE_GROUPS=1000
QTR_SCALPER_V2_MICRO_PROFIT_EXPERIMENT_JOURNAL_PATH=/opt/qtr/scalper-shadow/data/qtr_micro_scalper_micro_profit_experiment.jsonl
QTR_SCALPER_V2_COST_MODEL_ENABLED=true
QTR_SCALPER_V2_COST_SCENARIO=taker_taker
QTR_SCALPER_V2_COST_TAKER_FEE_RATE=0.00055
QTR_SCALPER_V2_COST_MAKER_FEE_RATE=0.00020
QTR_SCALPER_V2_COST_SLIPPAGE_BPS=0
QTR_SCALPER_V2_COST_FUNDING_RATE_8H=0
QTR_SCALPER_V2_RUNNER_TRAILING_R=0.10
QTR_SCALPER_V2_RUNNER_MAXIMUM_SAFETY_BARS=300
```

The default fee values are the Bybit VIP0 derivatives reference used for
validation, not a permanent account assumption. Select `taker_taker`,
`maker_taker`, `maker_maker`, or `custom`; custom entry/exit rates use
`QTR_SCALPER_V2_COST_ENTRY_FEE_RATE` and
`QTR_SCALPER_V2_COST_EXIT_FEE_RATE`.

Each accepted baseline entry is mirrored into M05/M10/M15/M20/M25 targets. The
baseline entry, initial stop, score and observed public market stream remain
unchanged. A virtual runner starts only after its micro target is observed and
continues while Setup Context, Market State, trade flow and liquidity do not
contradict the original direction. It exits on structural invalidation,
opposite/conflicted evidence, configured trailing excursion, or the 300-bar
safety horizon.

The experiment writes lifecycle transitions only to the separate
`qtr_micro_scalper_micro_profit_experiment.jsonl`. It never writes B/C/D or
micro records to either authoritative `qtr_micro_scalper_shadow_journal.jsonl`
or `qtr_micro_scalper_holding_experiment.jsonl`, and it has no execution or
order-placement authority.

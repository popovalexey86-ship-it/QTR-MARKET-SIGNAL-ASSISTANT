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

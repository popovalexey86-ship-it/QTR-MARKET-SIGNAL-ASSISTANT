# Установка QTR Micro Scalper V2 Shadow Observer

Этот deployment предназначен только для теневого наблюдения. Unit не содержит
execution-компонентов, торговых API, реальных ордеров или Telegram-команд.

## Безопасные значения по умолчанию

В unit зафиксированы:

```text
QTR_SCALPER_V2_ENABLED=false
QTR_SCALPER_V2_SHADOW_MODE=true
QTR_SCALPER_V2_LIVE_ENABLED=false
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
sudo install -d -o qtr -g qtr -m 0750 /opt/qtr/scanner/data
sudo -u qtr python3 -m venv /opt/qtr/scanner/.venv
sudo -u qtr /opt/qtr/scanner/.venv/bin/python -m pip install --upgrade pip
sudo -u qtr /opt/qtr/scanner/.venv/bin/python -m pip install \
  -e '/opt/qtr/scanner[websocket]'
```

WebSocket dependency устанавливается, но импорт package, установка unit и
`daemon-reload` соединение не открывают.

## Установка unit без запуска

Из корня checkout `/opt/qtr/scanner`:

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

Unit разрешает запись только в `/opt/qtr/scanner/data`. API keys и торговые
credentials для Shadow Observer не требуются и не должны добавляться в unit.

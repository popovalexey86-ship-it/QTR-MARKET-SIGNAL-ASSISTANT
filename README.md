# Market Signal Assistant

Независимый информационный скринер финансовых рынков. Он анализирует
завершённые свечи, отфильтровывает слабые и противоречивые наблюдения и
выдаёт ранжированные объяснимые сигналы.

Поддерживаемые классы активов:

- криптовалюты — публичные свечи Bybit;
- акции и ETF — публичный chart endpoint Yahoo Finance;
- валютные пары — Yahoo Finance symbols, например `EURUSD=X`;
- полностью офлайн CSV для воспроизводимого анализа.

Система не содержит брокера, не принимает API-ключи и не отправляет заявки.
Сигнал является аналитическим наблюдением, а не гарантией движения или
инвестиционной рекомендацией.

## Быстрый запуск

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
```

```powershell
.\.venv\Scripts\python.exe -m market_signal_assistant.cli `
  --instrument BTCUSDT:crypto `
  --instrument AAPL:stock `
  --instrument SPY:fund `
  --instrument EURUSD=X:forex `
  --interval 1h
```

Офлайн CSV:

```powershell
.\.venv\Scripts\python.exe -m market_signal_assistant.cli `
  --instrument BTCUSDT:crypto `
  --csv BTCUSDT=data/BTCUSDT_1h.csv `
  --interval 1h
```

Формат CSV:

```text
timestamp,open,high,low,close,volume
2026-01-01T00:00:00Z,100,102,99,101,1250
```

Полезные параметры:

```text
--min-score 45
--min-confirmations 2
--limit 250
--json-output reports/latest.json
```

## Что анализируется в V1

- положение EMA20 относительно EMA50;
- положение цены относительно EMA20;
- RSI(14);
- пробой диапазона последних 20 завершённых свечей;
- расширение ATR;
- всплеск объёма;
- конфликт направлений и число независимых подтверждений.

Слабые, единичные и противоречивые признаки не публикуются как сигнал.
Каждый результат содержит score, confidence и список причин.

## Следующие безопасные расширения

- экономический календарь;
- новости и sentiment с указанием источника;
- open interest, funding и liquidations для crypto;
- корпоративные отчёты и earnings calendar;
- корреляционные и межрыночные сигналы;
- долговременное хранилище и Telegram-дайджест.

Такие источники должны подключаться отдельными providers и не смешиваться
с расчётами цены без явной атрибуции.

## Crypto Derivatives Intelligence

Модуль добавляет к техническому сигналу независимый информационный контекст
Bybit: funding rate, текущее open interest, изменения OI/цены/объёма и
15-минутный quote-notional ликвидаций. Он классифицирует устойчивый рост,
перегретый LONG, накопление SHORT, short/long squeeze и движение без
подтверждения OI. Исходный технический сигнал не изменяется.

REST snapshot использует публичные endpoints и не требует API-ключей:

```python
from market_signal_assistant.composition import build_derivatives_components

components = build_derivatives_components()
snapshot = components.provider.collect("BTCUSDT")  # сеть только здесь
positioning = components.intelligence.analyze(snapshot)
print(positioning.regime.value, positioning.directional_score)
```

Для liquidation WebSocket установите optional dependency:

```powershell
python -m pip install -e ".[websocket]"
```

Поток запускается и останавливается только явно:

```python
components.stream.start("BTCUSDT")
try:
    snapshot = components.provider.collect("BTCUSDT")
finally:
    components.stream.stop()
```

Создание компонентов и импорт package не открывают HTTP или WebSocket
соединения. REST вызывается только через `collect()`, WebSocket — через
`start()`. Для корректной интерпретации изменений сравниваемые точки должны
иметь одинаковые интервалы; до накопления событий liquidation totals равны
нулю. Поток ликвидаций является live-контекстом и сам по себе не предоставляет
исторический backfill.

Technical score сохраняется на шкале `0..100`, derivatives direction — на
шкале `-1..1`. Fusion приводит оба источника к confidence-adjusted signed
шкале `-100..100`: положительное значение означает bullish-контекст,
отрицательное — bearish-контекст.

Derivatives intelligence является информационным контекстом, а не торговой
или инвестиционной рекомендацией.

## Единый application service

CLI, Web и Telegram используют один use case:

```text
CLI / Web / Telegram
          ↓
MarketScreeningService
          ↓
SignalEngine + optional derivatives context
          ↓
ScreeningReport
```

Отчёт сохраняет отдельно technical score, derivatives score, signed combined
score, confidence, fusion effect, market regime, confirmations, explanations и
warnings. Для SHORT combined score отрицательный; фильтрация выполняется по его
модулю. Ошибка одного инструмента или derivatives-контекста не останавливает
остальные инструменты.

## CLI

```powershell
python -m pip install -e .
market-signal-cli --help
market-signal-cli --instrument BTCUSDT:crypto --instrument SPY:fund `
  --interval 1h --min-score 60 --min-confidence 50 `
  --include-derivatives --maximum-results 10
```

Старое имя `market-screener` оставлено как совместимый alias. Импорт CLI и
`--help` не создают providers и не открывают соединения.

## Web dashboard

Web adapter использует лёгкий WSGI из стандартной библиотеки, поэтому web extra
не добавляет тяжёлый frontend или обязательные runtime-зависимости:

```powershell
python -m pip install -e ".[web]"
market-signal-web --host 127.0.0.1 --port 8000
```

Откройте `http://127.0.0.1:8000/`.

Endpoints:

- `GET /health` — локальная проверка без внешних API;
- `GET /api/instruments` — локальный каталог;
- `POST /api/screen` — запуск общего screening use case;
- `GET /` — HTML dashboard.

Пример запроса:

```json
{
  "instruments": ["BTCUSDT:crypto", "ETHUSDT:crypto"],
  "interval": "1h",
  "minimum_score": 60,
  "minimum_confidence": 50,
  "include_derivatives": true,
  "maximum_results": 10
}
```

Только `POST /api/screen` обращается к market-data providers. `/health`,
catalog и dashboard работают без внешней сети.

## Telegram bot

```powershell
python -m pip install -e ".[telegram]"
$env:TELEGRAM_BOT_TOKEN="token-from-botfather"
$env:TELEGRAM_ALLOWED_CHAT_IDS="123456789,987654321"
market-signal-telegram
```

Переменные окружения:

- `TELEGRAM_BOT_TOKEN` — обязателен и никогда не выводится в logs или repr;
- `TELEGRAM_ALLOWED_CHAT_IDS` — optional comma-separated allowlist. Пустое
  значение запрещает все chat IDs;
- `TELEGRAM_ALLOW_ALL=true` — отдельное явное разрешение принимать команды от
  любых chat IDs; по умолчанию выключено;
- `INPLAY_AUTO_ENABLED=true` — включает автоматический IN PLAY-цикл внутри
  процесса Telegram-бота; по умолчанию `false`;
- `INPLAY_SCAN_INTERVAL_MINUTES` — интервал автосканирования, по умолчанию 15
  минут, минимум 5 минут;
- `INPLAY_TIMING_AUDIT_ENABLED=true|false` — включает локальный диагностический
  аудит своевременности ручных и автоматических IN PLAY scan; по умолчанию
  `false`;
- `INPLAY_TIMING_AUDIT_AUTO_ENABLED=true|false` — включает теневой audit-runner
  внутри Telegram-процесса; по умолчанию `false` и действует только вместе с
  `INPLAY_TIMING_AUDIT_ENABLED=true`;
- `INPLAY_TIMING_AUDIT_INTERVAL_MINUTES` — интервал теневого scan от 5 до 60
  минут, по умолчанию 5;
- `INPLAY_AUDIT_EPISODE_SCORE` — диагностический порог начала эпизода `0..100`,
  по умолчанию 40; не влияет на пользовательский `INPLAY_MIN_SCORE`;
- `INPLAY_AUDIT_EPISODE_RESET_MINUTES` — отсутствие инструмента, после которого
  следующий активный scan начинает новый эпизод; от 15 до 1440 минут, по
  умолчанию 60;
- `INPLAY_EARLY_DISCOVERY_ENABLED=true|false` — включает отдельный теневой
  Early Discovery V1; по умолчанию `false`;
- `INPLAY_EARLY_DISCOVERY_INTERVAL_MINUTES` — fixed-schedule интервал Early
  Discovery от 5 до 60 минут, по умолчанию 5;
- `NEWS_ENABLED=true|false` — включает ручную команду `/news`; по умолчанию
  `true`;
- `NEWS_LOOKBACK_HOURS` — период важных объявлений, по умолчанию 24 часа,
  допустимо от 1 до 168;
- `NEWS_NOTIFICATION_RETENTION_DAYS` — срок хранения истории будущих
  автоматических новостных уведомлений, по умолчанию 30 дней, допустимо от 7
  до 365;
- `NEWS_AUTO_ENABLED=true|false` — включает внутренний цикл автоматических
  важных новостей; по умолчанию `false`;
- `NEWS_SCAN_INTERVAL_MINUTES` — интервал новостного цикла, по умолчанию 60
  минут, допустимо от 15 до 1440;
- `BYBIT_PUBLIC_BASE_URL` — production HTTPS base URL публичного Bybit API;
  по умолчанию `https://api.bybit.com`, Testnet отклоняется;
- `DERIVATIVES_LIVE_ENABLED=true` — явно включает liquidation WebSocket;
- `DERIVATIVES_LIVE_SYMBOLS=BTCUSDT,ETHUSDT` — подписки live-контекста.

Команды:

```text
/start
/help
/screen BTCUSDT ETHUSDT SOLUSDT interval=1h min_score=60
/crypto
/inplay
/news
/markets
/status
```

Команд для выставления заявок, открытия/закрытия позиций или запуска торговли
нет. Длинные ответы разбиваются ниже Telegram API limit.

Все пользовательские сообщения CLI, Web и Telegram формируются через единый
presentation mapping на русском языке. Машинные JSON keys сохраняются
стабильными. Один Telegram update обрабатывается одним command handler и
создаёт один набор ответных сообщений; автоматическая рассылка сигналов не
включена.

### Ручной поиск IN PLAY

`/inplay` вручную получает каталог линейных инструментов через уже используемый
публичный Bybit provider и рассматривает только активные ликвидные USDT-пары с
допустимым spread. Результат ограничен десятью инструментами.

IN PLAY score имеет собственную шкалу `0..100` и не является final score
направленного сигнала. Он складывается из ограниченных вкладов относительного
объёма, ATR-волатильности, изменения цены, пробоя локального диапазона,
технической силы и успешно полученного derivatives context. Бонус нового
листинга ограничен 10 баллами и сам по себе не создаёт ЛОНГ или ШОРТ.
В Telegram показываются только результаты с `inplay_score >= 50`; поэтому
выдача может содержать меньше десяти инструментов.

Для отделения криптовалют от linear TradFi используются общие поля Bybit
`instruments-info`: `symbolType`, `contractType`, `baseCoin`, `quoteCoin`,
`settleCoin`, `status` и `isPreListing`. Допускаются только `LinearPerpetual`
с USDT quote/settle, активным статусом, непустым baseCoin и crypto-compatible
`symbolType` (`""` либо `innovation`). Документированные типы `stock`, `forex`,
`commodity` и защитно `xstocks` исключаются без списка тикеров. Это надёжно для
текущей схемы Bybit; неизвестный будущий `symbolType` консервативно исключается
до явного обновления поддержки.

Изменение цены по модулю от 15% получает предупреждение о риске отката, а от
30% — предупреждение о высоком риске позднего входа и сильного отката. Эти
предупреждения одинаковы для роста и падения и выводятся раньше нейтрального
fallback о рисках.

Локальный snapshot `data/inplay_listings.json` создаётся только при первом
ручном вызове `/inplay`. Первый каталог становится baseline. Новые символы
получают неизменяемый `first_seen`; исчезнувшие символы остаются только в
локальной истории и не выводятся как IN PLAY.

Отдельный `InPlayNotificationService` хранит решения автоматических уведомлений
по единому абсолютному пути `data/inplay_notifications.json` относительно корня
установленного проекта, независимо от current working directory. Ручной
`/inplay` его не использует. Состояние записывается атомарно и содержит
последнее внутреннее направление, отображаемое состояние, класс риска,
пользовательское действие, видимые подтверждения, score, fingerprint причин,
UTC-время, признак нового листинга и время исчезновения. Текущая схема state —
version 2: `last_notification` отдельно хранит `internal_direction`,
`display_status`, `risk_class`, `user_action`, `visible_confirmations` и
durable `semantic_fingerprint`; symbol record хранит текущее внутреннее
направление и отображаемое состояние. Fingerprint не включает цену, ATR, объём,
score или timestamp. Старый JSON version 1 загружается с безопасными semantic
defaults; неизвестное исходное направление старой safety-записи остаётся
UNKNOWN до следующего живого scan и не выводится из знака изменения цены.

Автоматическая рассылка имеет hard safety gate. При абсолютном изменении цены
за 24 часа от 15% либо предупреждении о значительно реализованном движении
ЛОНГ/ШОРТ заменяется внутренним состоянием `ПОЗДНИЙ ВХОД` и не отправляется.
От 30% либо при предупреждении о резком состоявшемся движении используется
`НЕ ДОГОНЯТЬ`: внутреннее направление сохраняется, а защитное WATCH-сообщение
может быть отправлено только один раз и только при score не ниже 70.

Одинаковое семантическое событие определяется по symbol, внутреннему
направлению, отображаемому состоянию, классу риска, пользовательскому действию
и реально видимым подтверждениям. Изменения чисел цены, ATR, объёма и score сами
по себе не являются новым событием. Первые 60 минут действуют как абсолютный
cooldown; исключения — ЛОНГ ↔ ШОРТ, безопасный переход НАБЛЮДЕНИЕ → направление,
переход в `НЕ ДОГОНЯТЬ` и новый листинг. Затем повтор разрешают рост score
минимум на 10, новое видимое существенное подтверждение либо общий cooldown
шесть часов. Общая фраза о derivatives positioning не считается новым
существенным подтверждением. Ослабление ЛОНГ или ШОРТ до НАБЛЮДЕНИЯ только
обновляет состояние и не создаёт уведомление.

При `INPLAY_TIMING_AUDIT_ENABLED=true` общий IN PLAY scanner записывает каждый
успешно рассчитанный shortlist-кандидат до фильтра `INPLAY_MIN_SCORE` в
append-only UTF-8 файл `data/inplay_timing_audit.jsonl`. Диагностика не меняет
score, направление, Telegram-формат или решение об уведомлении. Отдельный
atomic state `data/inplay_detection_state.json` сохраняет первое обнаружение,
первую цену наблюдения, идентификатор и начало текущего эпизода, первое
достижение пользовательского порога, последнее наблюдение и peak/trough после
начала эпизода. Version 1 с полями `first_detected_*` загружается обратно
совместимо как первое наблюдение, но не объявляется началом эпизода. Эти файлы
не связаны с `data/inplay_notifications.json`.

Эпизод начинается при первом наблюдаемом переходе score снизу через
`INPLAY_AUDIT_EPISODE_SCORE`, при возвращении после заданного периода отсутствия
или при новом независимом ЛОНГ/ШОРТ-направлении после затухания. Первое
достижение `INPLAY_MIN_SCORE` фиксируется отдельно. JSONL сохраняет источник
scan: `manual`, `inplay_auto` либо `timing_audit_auto`. Старое поле
`move_before_first_detection_pct` оставлено только для совместимости и считается
deprecated; для оценки задержки используются метрики от `episode_started_at` до
qualification/current.

Изменения за 5 и 15 минут рассчитываются по завершённым 5m-свечам существующего
Bybit provider; 1h и 24h — по завершённым 1h-свечам основного IN PLAY анализа.
Последний подтверждённый пробой ищется на 1h: close должен выйти выше или ниже
диапазона предыдущих 20 завершённых свечей. Возраст — число завершённых 1h-bars
после свечи пробоя; расстояние сохраняется в процентах от уровня и в ATR.
Это подтверждённый 1h-пробой: текущий этап не обнаруживает отдельный
внутрисвечной 5m-пробой, что является известным ограничением аудита.
Недоступные показатели записываются как JSON `null`. Строки старше семи дней
удаляются при первом обращении после запуска и затем не чаще раза в сутки;
ошибка диагностики не останавливает scan или Telegram-процесс.

Теневой audit-runner принадлежит lifecycle существующего Telegram-процесса:
первый scan выполняется сразу, следующие — по заданному интервалу. Runner не
форматирует и не отправляет сообщения, не вызывает notification prepare/commit
и может работать при `INPLAY_AUTO_ENABLED=false`. Общий lock внутри
`InPlayService` не допускает параллельного manual, notification-auto и shadow
scan; близкие последовательные записи различаются по `scan_source` и не
создают новый эпизод только из-за источника.

### Early Discovery V1

Early Discovery — отдельный теневой диагностический контур. Он не отправляет
Telegram-сообщения, не читает и не изменяет notification state, не влияет на
`inplay_score`, направление, `/inplay`, новости или production top-20.
Результаты записываются в UTF-8 JSONL
`data/inplay_early_discovery_audit.jsonl` и хранятся семь дней; retention
выполняется не чаще раза в сутки. Повреждённая последняя строка отделяется
переводом строки и не блокирует следующую запись.

Universe формируется из полного Bybit catalog до любого ранжирования: только
активные crypto `LinearPerpetual` с USDT quote/settle, достаточным turnover,
валидными bid/ask и spread не шире 0,5%. STOCK, ETF, COMMODITY, FOREX и
PreLaunch исключаются общими metadata-фильтрами. Для каждого прошедшего
инструмента bounded pool до четырёх workers получает завершённые 5m, 15m и 1h
свечи. Ошибка одного symbol не останавливает остальные. 5m отвечает за ранний
импульс и breakout, 15m — за структуру, hold/retest и подтверждение, 1h — только
за старший контекст и 24h-risk.

`discovery_score` имеет независимую шкалу `0..100` и следующие максимальные
веса:

- ускорение относительного объёма 5m/15m — 20;
- расширение 5m ATR относительно предыдущей базы — 15;
- приближение к краю локального 15m-диапазона — 15;
- свежесть 5m/15m breakout — 20;
- предимпульсное сжатие — 10;
- качество turnover/ликвидности — 10;
- качество spread — 10.

Изменение за 24 часа не добавляет discovery points. `entry_readiness_score`
оценивает свежесть пробоя до 30, расстояние от уровня до 20, закрепление/retest
до 15, spread до 15, ликвидность до 10 и объём breakout до 10. При движении за
24 часа от 15% readiness ограничивается ниже READY и stage становится `LATE`;
от 30% используется `DO_NOT_CHASE`. Расстояние больше 2 ATR или spread шире
0,2% запрещают `READY_CANDIDATE`. При отсутствии направления сильная активность
остаётся `SETUP_FORMING`.

JSONL дополнительно хранит диагностическое сравнение с текущим production
evaluator: score, direction, safety display status, rank в рассчитанной IN PLAY
вселенной и принадлежность top-20. Сравнение использует уже загруженную 1h
series без отдельного derivatives/OI fan-out; недоступные значения остаются
JSON `null`.

Runner запускается внутри существующего Telegram lifecycle только при
`INPLAY_EARLY_DISCOVERY_ENABLED=true`. Первый scan выполняется сразу. Далее
плановая точка вычисляется как `previous_scheduled_run + interval`; длительность
scan не прибавляется к интервалу. Пропущенные точки перескакиваются без серии
догоняющих запусков, а lock запрещает параллельные Early Discovery scans.

### Офлайн-анализ аудита Early Discovery

Уже собранный JSONL можно анализировать без сети, Telegram и запуска рабочего
контура. Анализатор читает исходный файл потоково, пропускает повреждённые
строки и не изменяет входные данные:

```powershell
python -m market_signal_assistant.inplay.early_discovery_audit_analyzer `
  --input data/inplay_early_discovery_audit.jsonl `
  --output data/early_discovery_analysis
```

В каталоге результата создаются русский Markdown-отчёт, полная таблица
эпизодов, машинно-читаемые метрики, отдельные таблицы лучших и худших эпизодов
и рекомендации для будущей калибровки. CSV записываются с разделителем `;` и
маркером UTF-8 BOM для корректного открытия в Excel. Анализатор не меняет
формулы, пороги, рабочее состояние или правила уведомлений Early Discovery.

### Теневая калибровка модуля раннего обнаружения V2

V2 — отдельный диагностический контур для уменьшения ложных готовых состояний.
Он сравнивает неизменённый результат V1 и консервативный результат V2 на одном
наборе уже загруженных завершённых свечей 5 минут, 15 минут и 1 час. Повторная
загрузка свечей для сравнения V1/V2 не выполняется.

Последовательные состояния V2 интерпретируются так:

- одно готовое сканирование — `РАННЕЕ ВНИМАНИЕ`;
- два готовых сканирования — `ФОРМИРУЕТСЯ`;
- три готовых сканирования одного направления — `ПОДТВЕРЖДЁННОЕ НАБЛЮДЕНИЕ`;
- два состояния `БЕЗ СИГНАЛА`, смена направления или отсутствие не менее
  30 минут сбрасывают последовательность;
- краткая техническая ошибка последовательность не сбрасывает.

Подтверждение дополнительно требует правильной стороны уровня, отсутствия
провала пробоя, удержания завершённой свечой или подтверждённого ретеста,
абсолютного расстояния не более двух средних истинных диапазонов, спреда не
более 0,2% и абсолютного движения за 24 часа менее 15%. Незавершённая текущая
свеча не используется. Статусы `ПОЗДНО` и `НЕ ДОГОНЯТЬ` сохраняют прежние
пороги 15% и 30%.

Настройки:

- `INPLAY_EARLY_DISCOVERY_V2_ENABLED=false` — V2 выключена по умолчанию;
- `INPLAY_EARLY_DISCOVERY_V2_INTERVAL_MINUTES=5` — интервал 5–60 минут;
- `INPLAY_EARLY_DISCOVERY_V2_FORMING_SCANS=2` — число формирующих сканирований;
- `INPLAY_EARLY_DISCOVERY_V2_REQUIRED_READY_SCANS=3` — число подтверждений;
- `INPLAY_EARLY_DISCOVERY_V2_EPISODE_GAP_MINUTES=30` — пауза завершения
  эпизода.

Отдельные данные V2:

- аудит: `data/inplay_early_discovery_v2_audit.jsonl`;
- состояние последовательностей: `data/inplay_early_discovery_v2_state.json`.

Состояние записывается атомарно. Повреждённый файл сохраняется в резервную
копию, после чего используется безопасное пустое состояние. Аудит хранится
семь дней, а повреждённая последняя строка не блокирует следующую запись. Для
каждого результата сохраняются исходные значения, баллы, максимумы и
объяснения всех компонентов оценки.

Непрерывный теневой запуск из PowerShell:

```powershell
$env:INPLAY_EARLY_DISCOVERY_V2_ENABLED = "true"
python -m market_signal_assistant.inplay.early_discovery_v2
```

Один диагностический проход:

```powershell
$env:INPLAY_EARLY_DISCOVERY_V2_ENABLED = "true"
python -m market_signal_assistant.inplay.early_discovery_v2 --once
```

Первый проход выполняется сразу; дальнейшее расписание привязано к предыдущей
плановой точке. Пропущенные точки перескакиваются без серии догоняющих запусков.
V2 не подключена к Telegram, не отправляет уведомления и не использует
`data/inplay_notifications.json`. После накопления независимой выборки аудит
следует анализировать отдельной офлайн-задачей; следующий аудит автоматически
не запускается.

При `INPLAY_AUTO_ENABLED=true` Telegram-процесс запускает собственную asyncio-
задачу: первый scan выполняется сразу, следующие — через заданный интервал.
Отдельный cron, Windows Task Scheduler или второй процесс не создаётся. Для
автоматической отправки всегда требуется непустой `TELEGRAM_ALLOWED_CHAT_IDS`,
даже при `TELEGRAM_ALLOW_ALL=true`. Одновременно выполняется только один scan.

Автоматические пороги строже ручных: ЛОНГ/ШОРТ требуют score не ниже 60,
НАБЛЮДЕНИЕ — не ниже 70. Новый листинг может остаться НАБЛЮДЕНИЕМ от ручного
порога 50, поскольку до попадания в отчёт он уже прошёл проверки статуса,
ликвидности, spread и данных. За цикл отправляется одно объединённое сообщение
не более чем с тремя результатами по убыванию score. Notification state
фиксирует разрешённые события только после успешной доставки во все явно
разрешённые чаты; при ошибке Telegram следующий цикл повторит попытку.

### Официальные новости Bybit

Ручная команда `/news` получает публичные объявления через
`GET /v5/announcements/index` с `locale=en-US`, `page` и `limit`. Используется
тот же injectable JSON transport, что и у остальных публичных providers;
API-ключи, подпись, Testnet и загрузка HTML-страниц не требуются. Сеть вызывается
только при выполнении `/news`.

Отдельный модуль `market_signal_assistant.news` разделяет получение данных,
нормализацию, классификацию, фильтрацию и Telegram presentation. За настроенный
период выводится до десяти записей уровней CRITICAL, HIGH и MEDIUM: сначала по
важности, затем от новых к старым. Категории включают листинги, делистинги,
технические работы, безопасность, сети, изменения торговли и регулирование.

Конкурсы, бонусы, prize pool, Launchpool/Earn, VIP, referral, AMA и другие
кампании без операционного события отбрасываются. Если рекламный текст добавлен
к реальному листингу, листинг сохраняется, но краткое описание формируется
только из подтверждённого факта и не учитывает величину награды. Команда не
создаёт BUY/SELL/LONG/SHORT и не связана с торговлей или IN PLAY.

При `NEWS_AUTO_ENABLED=true` существующий Telegram-процесс запускает отдельную
внутреннюю asyncio-задачу новостей через lifecycle приложения. Первый scan
выполняется сразу после запуска, следующие — через
`NEWS_SCAN_INTERVAL_MINUTES`. Отдельный процесс, cron или Windows Task
Scheduler не создаётся; при shutdown задача отменяется и ожидается.

Автоматические новости отправляются только в явно заданные
`TELEGRAM_ALLOWED_CHAT_IDS`, даже при `TELEGRAM_ALLOW_ALL=true`. Пустой allowlist
не запускает цикл и оставляет ручные команды работоспособными. Новостной scan
имеет собственный `asyncio.Lock`: пересекающийся запуск пропускается, а IN PLAY
использует независимый lock.

Автоматически допускаются INITIAL уровней CRITICAL и HIGH, а также значимые
UPDATED и CANCELLED ранее отправленных событий. MEDIUM остаётся в ручной
`/news`, LOW автоматически не отправляется. За цикл выбираются максимум три
события в порядке: CANCELLED, CRITICAL UPDATED, CRITICAL INITIAL, HIGH UPDATED,
HIGH INITIAL; внутри одного приоритета более свежие события идут первыми.
Пустой результат не создаёт heartbeat или служебное сообщение.

Существующий `NewsNotificationService` хранит состояние в
`data/news_notifications.json`, отдельно от IN PLAY. В JSON находятся версия
схемы, UTC-время обновления и записи по `stable_id`: источник, категория,
важность, fingerprints заголовка и содержания, затронутые символы, времена
публикации/события/первой и последней отправки/последнего наблюдения, счётчик
отправок и статус.

Решение двухфазное: `prepare()` только классифицирует INITIAL, UPDATED,
CANCELLED, DOWNGRADED или подавленный повтор; `commit()` фиксирует отправку лишь
для выбранных `stable_id` после успешной доставки всех частей сообщения во все
разрешённые чаты. При сетевой или Telegram-ошибке `commit()` не вызывается,
процесс бота продолжает работу, а событие остаётся доступным для следующего
цикла. События вне лимита трёх также не фиксируются. Повторы с тем же
содержанием подавляются после
нормализации регистра, пробелов, переносов, пунктуации, рекламных фраз, порядка
тегов, timestamps и списка символов.

UPDATED разрешается при росте важности, изменении категории, срока события,
затронутых символов/сетей, появлении экстренной приостановки либо существенном
изменении официального текста или рекомендуемого действия. Понижение важности
обновляет состояние без отправки и не стирает историю. Явная официальная отмена
отправляется один раз как CANCELLED; отсутствие новости в очередной выдаче
отменой не считается. История хранится не меньше настроенного retention-периода;
будущие события и статусы, требующие сохранения, безопасной очисткой не
удаляются. Файл записывается через временный JSON и атомарную замену; отсутствующий
или повреждённый файл восстанавливается как безопасное пустое состояние с
предупреждением в журнале.

Ручная `/news` не читает notification state и при каждом вызове по-прежнему
показывает актуальные важные объявления за `NEWS_LOOKBACK_HOURS`.

### Почему технический и итоговый баллы различаются

Техническая сила сигнала показывает исходную оценку `SignalEngine` на шкале
`0..100`. Итоговый балл — signed результат fusion на шкале `-100..100`, где
отрицательное значение сохраняет направление ШОРТ. Он учитывает уверенность,
вес technical и derivatives источников и поэтому не обязан совпадать с
технической силой.

- нейтральные деривативы могут уменьшить модуль итогового балла, потому что
  fusion учитывает вес и уверенность обоих источников;
- недоступные деривативы не участвуют в fusion: итог сохраняет signed technical
  score;
- конфликтующий derivatives context отображается явно и не меняет направление
  технического сигнала скрытым образом.

### Доступность `/markets`

Команда `/markets` использует общий каталог: BTCUSDT направляется в Bybit,
AAPL, SPY и EURUSD=X — в существующий публичный Yahoo provider. Эти классы
явно поддержаны routing-кодом, поэтому они остаются в команде. При временной
недоступности, ограничении или изменении ответа Yahoo инструмент отображается
как отдельная ошибка анализа без фиктивных данных и без остановки остальных
инструментов. Новый provider на этом этапе не добавлялся.

## Режимы dependencies

```powershell
# REST-only, без pybit
python -m pip install -e .

# Только liquidation WebSocket
python -m pip install -e ".[websocket]"

# Telegram polling
python -m pip install -e ".[telegram]"

# Все optional adapters
python -m pip install -e ".[all]"
```

REST-only является стандартным режимом. Наличие `include_derivatives` запускает
REST-анализ, но не открывает WebSocket. WebSocket создаётся только при явном
runtime lifecycle и включённом `DERIVATIVES_LIVE_ENABLED`.

## Ограничения пользовательских интерфейсов

- публичные providers могут быть временно недоступны или задерживать данные;
- liquidation stream не выполняет исторический backfill;
- NEUTRAL означает отсутствие сигнала, прошедшего заданные фильтры;
- derivatives context может усиливать или ослаблять технический результат, но
  не скрывает конфликтующие факторы;
- приложение не выставляет ордера, не управляет позициями и не запускает
  trading loop;
- результаты не являются гарантией будущего движения.

**Информационный анализ, не торговая рекомендация.**

## Production deploy и rollback v1.2

Скрипты предназначены только для production checkout `/opt/qtr/scanner` на
ветке `main`. Запускайте их из корня этого checkout с правами, достаточными для
управления systemd-сервисами и записи в `/opt/qtr/.deploy`:

```bash
cd /opt/qtr/scanner
sudo bash scripts/deploy.sh
```

Deploy требует запуска от root, проверяет наличие production user/group
`qtr:qtr`, чистое Git working tree, выполняет `git fetch origin`, сохраняет
текущий commit в `/opt/qtr/.deploy/scanner-previous-commit` и обновляет checkout
строго до проверенного `origin/main`. Сервисы `qtr-scanner-web` и
`qtr-scanner-telegram` останавливаются перед сменой версии. Root используется
только для systemd, защищённого state-файла и удаления старой `.venv`.

Все Git-операции production checkout с самого начала выполняются от `qtr` через
единый helper `git_as_qtr`, который запускает
`git -C /opt/qtr/scanner ...` посредством `runuser`. Это относится к `status`,
`fetch`, `rev-parse`, `symbolic-ref`, `cat-file`, `reset` и любым добавляемым в
скрипты Git-командам. Скрипты не запускают Git от root и не исправляют ownership
после операции через `chown`: `.git/index`, `.git/ORIG_HEAD`, refs и рабочие
файлы изначально создаются или изменяются процессом `qtr`.

Все операции с production Python environment также выполняются через `runuser`
от `qtr` с `umask 022`: создание venv, pip install, pytest, mypy, ruff и
локальный health-check.

Виртуальное окружение всегда удаляется и создаётся заново только по финальному
пути `/opt/qtr/scanner/.venv`; перенос `.venv` между release paths не
используется. После создания скрипт требует ownership `qtr:qtr` и проверяет от
лица `qtr` traverse access к `.venv`, а после установки — execute access к
`market-signal-web` и `market-signal-telegram`. Root-owned venv останавливает
deploy до запуска сервисов.

После установки `.[web,telegram]`, `pytest`, `mypy` и `ruff` deploy выполняет
полный test suite, `mypy src` и `ruff check .`, затем неблокирующе запускает оба
сервиса. Для каждого сервиса до 30 секунд опрашивается `systemctl is-active`:
`activating` ожидается дальше, `active` считается успехом, а `failed` —
немедленной ошибкой. Только после `active` Web service до 30 секунд проверяется
локальный endpoint `http://127.0.0.1:8000/health`; временный connection refused
повторяется. Ответ должен иметь HTTP 200 и JSON `{"status": "ok"}`. Telegram
проверяется только через systemd, без обращения к Telegram API.

Ошибка после остановки сервисов автоматически через Git от `qtr` возвращает
сохранённый commit, заново строит `.venv` от `qtr` по финальному пути,
устанавливает runtime,
запускает сервисы, ждёт `active` и повторяет Web health-check. При окончательной
ошибке выводятся только ограниченные systemd-диагностики: `systemctl status` и
последние 50 строк `journalctl` для каждого сервиса. Environment file и значения
переменных окружения не читаются. Если `HEAD` уже равен `origin/main`, deploy
завершается без изменений.

Для ручного возврата к commit, сохранённому последним deploy:

```bash
cd /opt/qtr/scanner
sudo bash scripts/rollback.sh
```

Rollback проверяет state-файл и существование commit от лица `qtr`,
останавливает оба сервиса, выполняет `git reset --hard` от `qtr` на сохранённый
commit, заново создаёт от `qtr` только `/opt/qtr/scanner/.venv`, проверяет
ownership и executables, устанавливает runtime dependencies и повторяет ожидание
systemd active и Web health retry.
Скрипты не читают
`/etc/qtr/scanner-telegram.env`, не выводят environment values, не используют
внешний health-check и не удаляют persistent data. Перед ручным rollback
убедитесь, что в production checkout нет нужных незакоммиченных изменений:
`git reset --hard` удаляет изменения tracked files.

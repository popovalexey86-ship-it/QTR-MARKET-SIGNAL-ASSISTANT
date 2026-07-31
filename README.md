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

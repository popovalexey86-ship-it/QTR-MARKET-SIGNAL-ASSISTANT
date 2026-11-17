"""Russian read-only operator views of the compact cross-process snapshot."""

from __future__ import annotations

from market_signal_assistant.qtr_micro_scalper.control_store import (
    ControlRead,
    SnapshotState,
)
from market_signal_assistant.telegram.formatting import split_messages


def _enabled(value: bool | None) -> str:
    return "неизвестно" if value is None else "включён" if value else "отключён"


def _count(value: int | None) -> str:
    return "нет данных" if value is None else str(value)


def format_scalper_command(command: str, read: ControlRead) -> tuple[str, ...]:
    errors = {
        SnapshotState.MISSING: "⚠️ Снимок Scalper ещё не опубликован.",
        SnapshotState.MALFORMED: "⚠️ Снимок Scalper повреждён или недоступен.",
        SnapshotState.UNSUPPORTED: "⚠️ Версия снимка Scalper не поддерживается.",
        SnapshotState.FUTURE: (
            "⚠️ Время снимка Scalper находится в будущем; проверьте часы."
        ),
    }
    if read.state in errors:
        return (errors[read.state],)
    s = read.snapshot
    if s is None:
        return (errors[SnapshotState.MALFORMED],)
    generated = s.generated_at.isoformat()
    age = int(read.age_seconds or 0)
    if read.state is SnapshotState.STALE:
        return (
            "⚠️ Снимок Scalper устарел: последние данные "
            f"{s.observed_at.isoformat()}; публикация {generated}; "
            f"возраст {age} с. Текущее состояние неизвестно.",
        )
    r, d, t = s.runtime, s.decisions, s.trades
    status = {
        "RUNNING": "работает",
        "DEGRADED": "деградация",
        "STOPPED": "остановлен",
        "STOPPING": "останавливается",
        "STARTING": "запускается",
        "DISABLED": "отключён",
        "BLOCKED": "заблокирован",
    }.get(r.status, "неизвестно")
    freshness = f"Обновлено: {generated}\nВозраст снимка: {age} с (свежий)"
    if command == "scalper_status":
        text = (
            f"Scalper V2: {status}\nShadow: {_enabled(r.shadow_mode)}; "
            f"LIVE: {_enabled(r.live_enabled)}\n"
            f"Время работы: {int(r.uptime_seconds)} с\n"
            f"Снимки рынка: {s.market.snapshot_count}\n"
            f"Решения: {d.total}; теневые входы: {d.entries}\n"
            f"Активные runtime: {_count(t.active_count)}; завершённые: {t.completed}\n"
            f"Средний R: {t.average_r:.3f}; суммарный R: {t.total_r:.3f}\n{freshness}"
        )
    elif command == "scalper_active":
        if t.active_count is None:
            text = "Текущие runtime-сделки недоступны."
        elif t.active_count == 0:
            text = "Активных теневых сделок нет."
        else:
            lines = [
                f"Активные runtime-сделки: {t.active_count}; показано: {len(t.active)}"
            ]
            for trade in t.active:
                entry_time = (
                    trade.entry_time.isoformat()
                    if trade.entry_time
                    else "ещё не исполнен"
                )
                lines.append(
                    f"{trade.symbol} {trade.direction} · {trade.stage}\n"
                    f"ID: {trade.trade_id}\nScore: {trade.score:.1f}; "
                    f"вход: {trade.entry:g}; стоп: {trade.stop:g}\n"
                    f"TP1: {trade.tp1:g}; TP2: {trade.tp2:g}; R: {trade.result_r:.3f}\n"
                    f"Вход по времени: {entry_time}"
                )
            text = "\n\n".join(lines)
    elif command == "scalper_stats":
        text = (
            f"Статистика Scalper\nРешения: {d.total}; "
            f"заблокировано: {d.blocked}; входы: {d.entries}\n"
            f"Завершено: {t.completed}; Win rate: {t.win_rate:.2f}%\n"
            f"Средний R: {t.average_r:.3f}; суммарный R: {t.total_r:.3f}\n"
            f"Expired: {t.expired_rate:.2f}%; TP1: {t.tp1_rate:.2f}%; "
            f"TP2: {t.tp2_rate:.2f}%; стоп: {t.stop_rate:.2f}%"
        )
    elif command == "scalper_blocks":
        text = "Причины блокировки Scalper\n" + (
            "\n".join(f"{b.count} · {b.reason}" for b in s.blockers) or "Данных нет."
        )
    elif command == "scalper_experiments":
        text = "Эксперименты Scalper (только чтение)\n" + "\n".join(
            f"{e.name}: {_enabled(e.enabled)}; ошибок: {e.errors}"
            + (f"; {e.warning}" if e.warning else "")
            for e in s.experiments
        )
    elif command == "scalper_health":
        text = (
            f"Здоровье Scalper: {status}\nГотовность: {'да' if r.ready else 'нет'}\n"
            f"Ошибки pipeline: {r.pipeline_errors}; "
            f"переподключения: {r.reconnect_attempts}\n"
            f"Карантин исторических сделок: {r.quarantine_count}\n"
            f"{r.quarantine_warning or 'Предупреждений карантина нет.'}\n"
            f"Последняя ошибка: {r.last_error or 'нет'}\n"
            "Предыдущая ошибка публикации: "
            f"{s.resources.previous_publish_error or 'нет'}\n{freshness}"
        )
    elif command == "scalper_version":
        text = (
            f"Scalper Control Plane\nСхема: {s.schema_version}\n"
            f"Код/deploy SHA: {s.version.code_sha}\nОбновлено: {generated}"
        )
    else:
        text = "Недавние события пока не опубликованы Control Plane."
    return split_messages(text)

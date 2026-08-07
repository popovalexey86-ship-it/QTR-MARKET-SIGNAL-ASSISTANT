from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.inplay.early_discovery_audit_analyzer import (
    DIRECTION_RU,
    STAGE_RU,
    analyze,
    build_episodes,
    main,
    read_audit,
    run,
)

NOW = datetime(2026, 8, 1, 12, tzinfo=UTC)


def row(
    minutes: int,
    *,
    symbol: str = "BTCUSDT",
    stage: str = "EARLY_ATTENTION",
    direction: str = "UP",
    price: float = 100.0,
    rank: int | None = 10,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "scanned_at": (NOW + timedelta(minutes=minutes)).isoformat(),
        "market_direction": direction,
        "discovery_stage": stage,
        "discovery_score": 65.0,
        "entry_readiness_score": 70.0,
        "last_price": price,
        "spread_pct": 0.03,
        "price_change_24h_pct": 2.0,
        "distance_from_breakout_atr": 0.7,
        "is_in_current_top20": rank is not None and rank <= 20,
        "rank_in_current_inplay_universe": rank,
        "confirmations": ["fresh 5m breakout"],
        "warnings": [],
    }


def write_jsonl(path: Path, rows: list[dict[str, object] | str]) -> str:
    content = "".join(
        f"{item}\n" if isinstance(item, str) else json.dumps(item) + "\n"
        for item in rows
    )
    path.write_text(content, encoding="utf-8")
    return content


def test_reads_valid_jsonl_and_skips_damaged_and_empty_lines(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(path, [row(0), "{broken", ""])

    result = read_audit(path)

    assert result.valid_lines == 1
    assert result.damaged_lines == 1
    assert result.empty_lines == 1
    assert tuple(result.by_symbol) == ("BTCUSDT",)


def test_empty_audit_is_analyzed_without_failure(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    path.write_text("", encoding="utf-8")

    result, files = run(path, tmp_path / "analysis")

    assert result.read.valid_lines == 0
    assert result.episodes == ()
    assert files.report.exists()
    assert files.episodes.read_bytes().startswith(b"\xef\xbb\xbf")


def test_groups_scans_by_actual_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(
        path,
        [row(0), row(0, symbol="ETHUSDT"), row(5), row(5, symbol="ETHUSDT")],
    )

    result = analyze(path)

    assert result.scan_statistics["scan_count"] == 2
    assert result.scan_statistics["minimum_symbols"] == 2
    assert result.scan_statistics["median_interval_minutes"] == 5.0


def test_scan_statistics_count_disappearance_and_reappearance(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(
        path,
        [
            row(0),
            row(0, symbol="ETHUSDT"),
            row(5),
            row(10),
            row(10, symbol="ETHUSDT"),
        ],
    )

    statistics = analyze(path).scan_statistics

    assert statistics["complete_scan_count"] == 3
    assert statistics["disappearance_events"] == 1
    assert statistics["symbols_reappeared"] == 1


def test_builds_episode_and_merges_repeated_ready_snapshots(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(
        path,
        [
            row(0, stage="QUIET", direction="NEUTRAL"),
            row(5, stage="EARLY_ATTENTION"),
            row(10, stage="SETUP_FORMING"),
            row(15, stage="READY_CANDIDATE"),
            row(20, stage="READY_CANDIDATE"),
            row(25, stage="READY_CANDIDATE"),
        ],
    )

    episodes = build_episodes(read_audit(path).by_symbol)

    assert len(episodes) == 1
    assert episodes[0].ready_count == 3
    assert episodes[0].maximum_ready_streak == 3
    assert episodes[0].first_ready_at == NOW + timedelta(minutes=15)


def test_episode_breaks_after_thirty_minute_absence(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(path, [row(0), row(5), row(40), row(45)])

    episodes = build_episodes(read_audit(path).by_symbol)

    assert len(episodes) == 2


def test_episode_breaks_on_up_down_direction_change(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(path, [row(0), row(5), row(10, direction="DOWN")])

    episodes = build_episodes(read_audit(path).by_symbol)

    assert len(episodes) == 2
    assert [item.direction for item in episodes] == ["UP", "DOWN"]


def test_directed_returns_for_up_and_down(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(
        path,
        [
            row(0, stage="READY_CANDIDATE", price=100),
            row(15, stage="SETUP_FORMING", price=101),
            row(
                0,
                symbol="ETHUSDT",
                stage="READY_CANDIDATE",
                direction="DOWN",
                price=100,
            ),
            row(
                15, symbol="ETHUSDT", stage="SETUP_FORMING", direction="DOWN", price=98
            ),
        ],
    )

    result = analyze(path)
    outcomes = {item.symbol: item.horizon_results[15] for item in result.episodes}

    assert outcomes["BTCUSDT"] is not None
    assert outcomes["BTCUSDT"].directed_return_pct == pytest.approx(1.0)
    assert outcomes["ETHUSDT"] is not None
    assert outcomes["ETHUSDT"].directed_return_pct == pytest.approx(2.0)


def test_all_horizons_use_only_subsequent_audit_prices(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(
        path,
        [
            row(0, stage="READY_CANDIDATE", price=100),
            row(15, price=101),
            row(30, price=102),
            row(60, price=103),
            row(180, price=104),
        ],
    )

    episode = analyze(path).episodes[0]

    expected = {15: 1.0, 30: 2.0, 60: 3.0, 180: 4.0}
    for minutes, value in expected.items():
        outcome = episode.horizon_results[minutes]
        assert outcome is not None
        assert outcome.directed_return_pct == pytest.approx(value)


def test_missing_future_data_and_production_rank_are_preserved(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    write_jsonl(path, [row(0, stage="READY_CANDIDATE", rank=None)])

    result = analyze(path)
    episode = result.episodes[0]

    assert episode.best_production_rank is None
    assert all(value is None for value in episode.horizon_results.values())


def test_russian_stage_and_direction_mappings_are_complete() -> None:
    assert STAGE_RU["READY_CANDIDATE"] == "ГОТОВ К НАБЛЮДЕНИЮ"
    assert STAGE_RU["DO_NOT_CHASE"] == "НЕ ДОГОНЯТЬ"
    assert DIRECTION_RU["UP"] == "ВВЕРХ"
    assert DIRECTION_RU["NEUTRAL"] == "НЕЙТРАЛЬНО"


def test_offline_run_writes_bom_semicolon_csv_without_mutating_input(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "audit.jsonl"
    original = write_jsonl(
        input_path,
        [row(0, stage="READY_CANDIDATE"), row(15, price=101)],
    )
    output = tmp_path / "analysis"

    _, files = run(input_path, output)

    assert input_path.read_text(encoding="utf-8") == original
    csv_bytes = files.episodes.read_bytes()
    assert csv_bytes.startswith(b"\xef\xbb\xbf")
    header = files.episodes.read_text(encoding="utf-8-sig").splitlines()[0]
    assert ";" in header
    assert "Направление" in header
    assert files.report.exists()
    assert files.metrics.exists()
    assert files.best.exists()
    assert files.worst.exists()
    assert files.recommendations.exists()


def test_cli_completes_without_token_or_network(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    del capsys
    input_path = tmp_path / "audit.jsonl"
    write_jsonl(input_path, [row(0, stage="READY_CANDIDATE"), row(15, price=101)])
    output = tmp_path / "analysis"

    exit_code = main(["--input", str(input_path), "--output", str(output)])

    assert exit_code == 0
    assert (output / "итоговый_отчёт.md").exists()


def test_cli_reports_missing_input_in_russian(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "analysis"

    with pytest.raises(SystemExit) as error:
        main(
            [
                "--input",
                str(tmp_path / "missing.jsonl"),
                "--output",
                str(output),
            ]
        )

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert "Файл аудита не найден" in captured.err


def test_report_exposes_component_limit_and_safety_counts(tmp_path: Path) -> None:
    input_path = tmp_path / "audit.jsonl"
    risky = row(0, stage="READY_CANDIDATE")
    risky["price_change_24h_pct"] = 31.0
    risky["spread_pct"] = 0.21
    risky["distance_from_breakout_atr"] = 2.1
    write_jsonl(input_path, [risky, row(60, price=101)])

    result, files = run(input_path, tmp_path / "analysis")
    report = files.report.read_text(encoding="utf-8")

    assert result.safety_violations["изменение_24ч_не_менее_30"] == 1
    assert result.safety_violations["расстояние_более_2_ATR"] == 1
    assert "отдельных балльных вкладов компонентов" in report
    assert "Проверка защитных ограничений" in report

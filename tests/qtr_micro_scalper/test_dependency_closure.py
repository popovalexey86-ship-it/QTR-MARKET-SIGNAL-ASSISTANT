from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from market_signal_assistant.settings import QtrScalperV2LiveSettings

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "market_signal_assistant" / "qtr_micro_scalper"


def test_scalper_v2_settings_defaults_are_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCALPER_V2_ENABLED", raising=False)
    monkeypatch.delenv("QTR_SCALPER_V2_LIVE_ENABLED", raising=False)
    monkeypatch.delenv("QTR_SCALPER_V2_SHADOW_MODE", raising=False)
    settings = QtrScalperV2LiveSettings.from_environment()
    assert not settings.enabled
    assert settings.shadow_mode


def test_primary_enabled_flag_and_legacy_live_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCALPER_V2_ENABLED", raising=False)
    monkeypatch.setenv("QTR_SCALPER_V2_LIVE_ENABLED", "true")
    assert QtrScalperV2LiveSettings.from_environment().enabled

    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "false")
    assert not QtrScalperV2LiveSettings.from_environment().enabled


def test_enabled_live_data_requires_shadow_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "false")
    with pytest.raises(ValueError, match="shadow mode only"):
        QtrScalperV2LiveSettings.from_environment()


def test_first_party_import_closure_is_explicit() -> None:
    external: set[tuple[str, str, tuple[str, ...]]] = set()
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module is None:
                continue
            if not node.module.startswith("market_signal_assistant"):
                continue
            if node.module.startswith("market_signal_assistant.qtr_micro_scalper"):
                continue
            external.add(
                (
                    path.relative_to(PACKAGE).as_posix(),
                    node.module,
                    tuple(alias.name for alias in node.names),
                )
            )
    assert external == {
        (
            "service.py",
            "market_signal_assistant.settings",
            ("QtrScalperV2LiveSettings",),
        )
    }


def test_package_imports_in_fresh_interpreter_from_checkout() -> None:
    source = ROOT / "src"
    code = (
        "import sys; "
        f"sys.path.insert(0, {str(source)!r}); "
        "import market_signal_assistant.qtr_micro_scalper"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

import ast
from pathlib import Path


def test_detection_layer_cannot_import_events_or_outcomes() -> None:
    root = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "market_signal_assistant"
        / "watchdog"
    )
    detection_modules = (
        "models.py",
        "features.py",
        "detectors.py",
        "aggregation.py",
        "state_machine.py",
        "engine.py",
    )
    forbidden = {
        "market_signal_assistant.watchdog.events",
        "market_signal_assistant.watchdog.outcomes",
    }

    for name in detection_modules:
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        assert not any(
            imported == boundary or imported.startswith(f"{boundary}.")
            for imported in imports
            for boundary in forbidden
        ), f"{name} imports the future-outcome layer"

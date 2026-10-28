from datetime import UTC, datetime

import pytest


@pytest.fixture(autouse=True)
def shadow_process_identity(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    if "test_qtr_micro_distance_shadow" not in request.node.nodeid:
        return
    from market_signal_assistant.qtr_micro import (
        distance_shadow_accounting as accounting,
    )

    monkeypatch.setattr(accounting, "_current_invocation_id", lambda: "1" * 32)
    monkeypatch.setattr(
        accounting,
        "_run_created_at",
        lambda: datetime(2026, 9, 25, tzinfo=UTC).isoformat(),
    )

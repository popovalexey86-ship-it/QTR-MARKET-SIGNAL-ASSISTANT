import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

from market_signal_assistant.application.models import MarketSummary, ScreeningReport
from market_signal_assistant.telegram.bot import execute_command
from market_signal_assistant.web.app import create_app

NOW = datetime(2026, 7, 31, tzinfo=UTC)
REPORT = ScreeningReport(NOW, (), (), (), MarketSummary(1, 1, 0, 0, 0, 1))


class Service:
    def screen(self, request: object) -> ScreeningReport:
        del request
        return REPORT


def test_web_and_telegram_use_equivalent_screening_report() -> None:
    service = Service()
    captured: dict[str, object] = {}

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        captured["status"] = status
        captured["headers"] = headers

    payload = json.dumps(
        {
            "instruments": ["BTCUSDT:crypto"],
            "interval": "1h",
            "minimum_score": 45,
            "minimum_confidence": 0,
            "include_derivatives": True,
            "maximum_results": 10,
        }
    ).encode()
    body = b"".join(
        create_app(service_factory=lambda: service)(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/api/screen",
                "CONTENT_LENGTH": str(len(payload)),
                "wsgi.input": BytesIO(payload),
            },
            start_response,
        )
    )
    web_view: dict[str, Any] = json.loads(body)
    telegram = execute_command(
        "/screen BTCUSDT interval=1h min_score=45",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=service,
    )
    assert telegram.report is REPORT
    assert telegram.view is not None
    telegram_json = json.loads(json.dumps(telegram.view.as_dict()))
    assert telegram_json == web_view

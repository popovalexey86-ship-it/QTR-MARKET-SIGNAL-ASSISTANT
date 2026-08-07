import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any
from unittest.mock import Mock

from market_signal_assistant.application.models import (
    InstrumentFailure,
    MarketSummary,
    ScreeningReport,
)
from market_signal_assistant.models import AssetClass, Instrument
from market_signal_assistant.web.app import create_app

NOW = datetime(2026, 7, 31, tzinfo=UTC)
EMPTY_REPORT = ScreeningReport(
    NOW, (), (), (), MarketSummary(1, 1, 0, 0, 0, 1)
)


class Service:
    def __init__(self, report: ScreeningReport = EMPTY_REPORT) -> None:
        self.report = report
        self.requests: list[object] = []

    def screen(self, request: object) -> ScreeningReport:
        self.requests.append(request)
        return self.report


def call(
    app: Any,
    method: str,
    path: str,
    payload: object | None = None,
    *,
    raw_body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    body = (
        raw_body
        if raw_body is not None
        else b"" if payload is None else json.dumps(payload).encode()
    )
    captured: dict[str, object] = {}

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        captured["status"] = status
        captured["headers"] = dict(headers)

    response = app(
        {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "CONTENT_LENGTH": str(len(body)),
            "wsgi.input": BytesIO(body),
        },
        start_response,
    )
    return (
        int(str(captured["status"]).split()[0]),
        captured["headers"],  # type: ignore[return-value]
        b"".join(response),
    )


def test_health_does_not_build_service_or_open_network() -> None:
    factory = Mock(side_effect=AssertionError("service/network created"))
    status, _, body = call(create_app(service_factory=factory), "GET", "/health")
    assert status == 200
    assert json.loads(body) == {"status": "ok"}
    factory.assert_not_called()


def test_instruments_are_local_and_dashboard_is_available() -> None:
    app = create_app(service_factory=Mock(side_effect=AssertionError))
    status, _, body = call(app, "GET", "/api/instruments")
    assert status == 200
    assert "BTCUSDT" in body.decode()
    status, headers, body = call(app, "GET", "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    html = body.decode("utf-8")
    assert "Информационный помощник по рынку" in html
    assert "Техническая сила сигнала" in html
    assert "Итоговый балл" in html
    assert "Уверенность" in html
    assert "Ошибки анализа" in html


def test_dashboard_renders_only_ranked_results_with_safe_dom_updates() -> None:
    app = create_app(service_factory=Mock(side_effect=AssertionError))
    status, _, body = call(app, "GET", "/")
    html = body.decode()
    assert status == 200
    assert "data.ranked_signals" in html
    assert "data.successful_results" not in html
    assert "innerHTML" not in html
    assert "textContent" in html


def test_screen_calls_shared_service_and_returns_report() -> None:
    service = Service()
    status, _, body = call(
        create_app(service_factory=lambda: service),
        "POST",
        "/api/screen",
        {
            "instruments": ["BTCUSDT:crypto"],
            "interval": "1h",
            "minimum_score": 60,
            "minimum_confidence": 50,
            "include_derivatives": True,
            "maximum_results": 5,
        },
    )
    result = json.loads(body)
    assert status == 200
    assert result["market_summary"]["neutral"] == 1
    assert len(service.requests) == 1


def test_screen_validation_error_does_not_call_service() -> None:
    service = Service()
    status, _, body = call(
        create_app(service_factory=lambda: service),
        "POST",
        "/api/screen",
        {"instruments": [], "interval": "2h"},
    )
    assert status == 400
    assert "error" in json.loads(body)
    assert service.requests == []


def test_screen_rejects_malformed_and_empty_json_without_calling_service() -> None:
    service = Service()
    app = create_app(service_factory=lambda: service)
    malformed_status, _, _ = call(
        app, "POST", "/api/screen", raw_body=b"{broken"
    )
    empty_status, _, _ = call(app, "POST", "/api/screen", raw_body=b"")
    assert malformed_status == 400
    assert empty_status == 400
    _, _, malformed_body = call(
        app, "POST", "/api/screen", raw_body=b"{broken"
    )
    assert json.loads(malformed_body)["error"] == "Некорректный JSON-запрос."
    assert service.requests == []


def test_known_route_with_unsupported_method_returns_405_and_allow() -> None:
    app = create_app(service_factory=Mock(side_effect=AssertionError))
    status, headers, body = call(app, "POST", "/health")
    assert status == 405
    assert headers["Allow"] == "GET"
    assert json.loads(body) == {"error": "Метод не поддерживается."}


def test_screen_returns_per_instrument_error() -> None:
    failure = InstrumentFailure(
        Instrument("ETHUSDT", AssetClass.CRYPTO),
        "technical",
        "RuntimeError",
        "provider unavailable",
    )
    service = Service(
        ScreeningReport(NOW, (), (failure,), (), MarketSummary(1, 0, 1, 0, 0, 0))
    )
    status, _, body = call(
        create_app(service_factory=lambda: service),
        "POST",
        "/api/screen",
        {"instruments": ["ETHUSDT:crypto"], "interval": "1h"},
    )
    payload = json.loads(body)
    assert status == 200
    assert payload["failed_instruments"][0]["symbol"] == "ETHUSDT"
    assert payload["failed_instruments"][0]["stage"] == "технический анализ"
    assert payload["failed_instruments"][0]["message"] == (
        "Рыночные данные недоступны."
    )


def test_hostile_failure_is_data_not_dashboard_markup() -> None:
    hostile = '<img src=x onerror=alert(1)>'
    failure = InstrumentFailure(
        Instrument("ETHUSDT", AssetClass.CRYPTO),
        "technical",
        "ProviderError",
        hostile,
    )
    service = Service(
        ScreeningReport(NOW, (), (failure,), (), MarketSummary(1, 0, 1, 0, 0, 0))
    )
    app = create_app(service_factory=lambda: service)
    status, _, body = call(
        app,
        "POST",
        "/api/screen",
        {"instruments": ["ETHUSDT:crypto"], "interval": "1h"},
    )
    assert status == 200
    assert json.loads(body)["failed_instruments"][0]["message"] == (
        "Рыночные данные недоступны."
    )

    _, _, dashboard = call(app, "GET", "/")
    html = dashboard.decode()
    assert hostile not in html
    assert "innerHTML" not in html

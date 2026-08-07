from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Protocol, cast
from wsgiref.simple_server import make_server

from market_signal_assistant.application.models import ScreeningReport, ScreeningRequest
from market_signal_assistant.application.presentation import present_report
from market_signal_assistant.catalog import MARKET_INSTRUMENTS
from market_signal_assistant.localized_argparse import RussianArgumentParser
from market_signal_assistant.models import AssetClass, Instrument
from market_signal_assistant.web.dashboard import DASHBOARD_HTML


class ScreeningService(Protocol):
    def screen(self, request: ScreeningRequest) -> ScreeningReport: ...


ServiceFactory = Callable[[], ScreeningService]
StartResponse = Callable[[str, list[tuple[str, str]]], None]

INSTRUMENTS = tuple(
    {"symbol": item.symbol, "asset_class": item.asset_class.value}
    for item in MARKET_INSTRUMENTS
)
ROUTE_METHODS = {
    "/": ("GET",),
    "/health": ("GET",),
    "/api/instruments": ("GET",),
    "/api/screen": ("POST",),
}


class WebApplication:
    def __init__(self, service_factory: ServiceFactory) -> None:
        self._service_factory = service_factory

    def __call__(
        self, environ: Mapping[str, Any], start_response: StartResponse
    ) -> Iterable[bytes]:
        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        path = str(environ.get("PATH_INFO", "/"))
        if method == "GET" and path == "/health":
            return _json_response(start_response, 200, {"status": "ok"})
        if method == "GET" and path == "/api/instruments":
            return _json_response(start_response, 200, INSTRUMENTS)
        if method == "GET" and path == "/":
            return _response(start_response, 200, DASHBOARD_HTML, "text/html")
        if method == "POST" and path == "/api/screen":
            try:
                payload = _read_json(environ)
                request = screening_request_from_payload(payload)
            except json.JSONDecodeError:
                return _json_response(
                    start_response, 400, {"error": "Некорректный JSON-запрос."}
                )
            except (ValueError, TypeError, KeyError):
                return _json_response(
                    start_response,
                    400,
                    {"error": "Некорректные параметры сканирования."},
                )
            report = self._service_factory().screen(request)
            return _json_response(start_response, 200, present_report(report).as_dict())
        if path in ROUTE_METHODS:
            return _json_response(
                start_response,
                405,
                {"error": "Метод не поддерживается."},
                headers=[("Allow", ", ".join(ROUTE_METHODS[path]))],
            )
        return _json_response(start_response, 404, {"error": "Ресурс не найден."})


def create_app(service_factory: ServiceFactory | None = None) -> WebApplication:
    return WebApplication(service_factory or _default_service_factory)


def screening_request_from_payload(payload: Mapping[str, Any]) -> ScreeningRequest:
    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, list):
        raise ValueError("instruments must be a list.")
    instruments = tuple(_instrument(item) for item in raw_instruments)
    return ScreeningRequest(
        instruments=instruments,
        interval=str(payload.get("interval", "1h")),
        minimum_score=payload.get("minimum_score", 45.0),
        minimum_confidence=payload.get("minimum_confidence", 0.0),
        include_derivatives=payload.get("include_derivatives", False),
        maximum_results=payload.get("maximum_results", 10),
    )


def _instrument(value: object) -> Instrument:
    if not isinstance(value, str):
        raise ValueError("instrument must use SYMBOL:asset_class format.")
    try:
        symbol, asset_class = value.rsplit(":", 1)
        return Instrument(symbol, AssetClass(asset_class.lower()))
    except ValueError:
        raise ValueError("instrument must use SYMBOL:asset_class format.") from None


def _read_json(environ: Mapping[str, Any]) -> Mapping[str, Any]:
    length = int(environ.get("CONTENT_LENGTH") or 0)
    stream = environ.get("wsgi.input")
    if stream is None or not hasattr(stream, "read"):
        raise ValueError("Request body is unavailable.")
    payload = json.loads(stream.read(length).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")
    return payload


def _json_response(
    start_response: StartResponse,
    status: int,
    payload: object,
    *,
    headers: list[tuple[str, str]] | None = None,
) -> list[bytes]:
    return _response(
        start_response,
        status,
        json.dumps(payload, ensure_ascii=False),
        "application/json",
        headers=headers,
    )


def _response(
    start_response: StartResponse,
    status: int,
    body: str,
    content_type: str,
    *,
    headers: list[tuple[str, str]] | None = None,
) -> list[bytes]:
    encoded = body.encode("utf-8")
    reason = {
        200: "OK",
        400: "Bad Request",
        404: "Not Found",
        405: "Method Not Allowed",
    }[status]
    response_headers = [
        ("Content-Type", f"{content_type}; charset=utf-8"),
        ("Content-Length", str(len(encoded))),
    ]
    response_headers.extend(headers or ())
    start_response(
        f"{status} {reason}",
        response_headers,
    )
    return [encoded]


def _default_service_factory() -> ScreeningService:
    from market_signal_assistant.composition import build_screening_service

    service, _ = build_screening_service()
    return service


def main(argv: Sequence[str] | None = None) -> None:
    parser = RussianArgumentParser(
        description="Веб-панель информационного помощника по рынку."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    from market_signal_assistant.composition import build_screening_service
    from market_signal_assistant.settings import LiveDerivativesSettings

    service, derivatives = build_screening_service()
    live_settings = LiveDerivativesSettings.from_environment()
    if live_settings.enabled:
        derivatives.stream.start(list(live_settings.symbols))
    try:
        app = create_app(service_factory=lambda: service)
        with make_server(args.host, args.port, cast(Any, app)) as server:
            print(f"Market Signal dashboard: http://{args.host}:{args.port}")
            server.serve_forever()
    finally:
        derivatives.stream.stop()


if __name__ == "__main__":
    main()

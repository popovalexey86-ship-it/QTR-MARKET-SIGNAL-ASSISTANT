from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.application.models import (
    SUPPORTED_INTERVALS,
    ScreeningRequest,
)
from market_signal_assistant.catalog import CRYPTO_PRESET, MARKETS_PRESET
from market_signal_assistant.models import AssetClass, Instrument

SUPPORTED_COMMANDS = frozenset(
    {"start", "help", "screen", "crypto", "markets", "status", "inplay", "news"}
)


@dataclass(frozen=True, slots=True)
class ParsedCommand:
    name: str
    request: ScreeningRequest | None = None


def parse_command(text: str) -> ParsedCommand:
    tokens = text.strip().split()
    if not tokens or not tokens[0].startswith("/"):
        raise ValueError("ожидается команда, начинающаяся с символа /.")
    name = tokens[0][1:].split("@", 1)[0].lower()
    if name not in SUPPORTED_COMMANDS:
        raise ValueError("команда не поддерживается. Используйте /help.")
    if name in {"start", "help", "status", "inplay", "news"}:
        if len(tokens) != 1:
            raise ValueError(f"команда /{name} не принимает параметры.")
        return ParsedCommand(name)
    if name == "crypto":
        return ParsedCommand(
            name,
            ScreeningRequest(
                instruments=CRYPTO_PRESET,
                interval="1h",
                include_derivatives=True,
                maximum_results=10,
            ),
        )
    if name == "markets":
        return ParsedCommand(
            name,
            ScreeningRequest(
                instruments=MARKETS_PRESET,
                interval="1h",
                maximum_results=10,
            ),
        )
    return ParsedCommand(name, _parse_screen(tokens[1:]))


def _parse_screen(tokens: list[str]) -> ScreeningRequest:
    symbols: list[str] = []
    options: dict[str, str] = {}
    for token in tokens:
        if "=" in token:
            key, value = token.split("=", 1)
            options[key.lower()] = value
        else:
            symbols.append(token)
    if not symbols:
        raise ValueError(
            "укажите хотя бы один инструмент. "
            "Например: /screen BTCUSDT ETHUSDT"
        )
    unknown = set(options) - {
        "interval",
        "min_score",
        "min_confidence",
        "derivatives",
        "max_results",
    }
    if unknown:
        raise ValueError(f"неизвестный параметр: {sorted(unknown)[0]}.")
    return _request(
        tuple(symbols),
        interval=options.get("interval", "1h"),
        minimum_score=_float(options.get("min_score", "45"), "min_score"),
        minimum_confidence=_float(
            options.get("min_confidence", "0"), "min_confidence"
        ),
        include_derivatives=_boolean(
            options.get("derivatives", "true"), "derivatives"
        ),
        maximum_results=_integer(options.get("max_results", "10"), "max_results"),
    )


def _request(
    symbols: tuple[str, ...],
    *,
    interval: str,
    minimum_score: float = 45.0,
    minimum_confidence: float = 0.0,
    include_derivatives: bool = False,
    maximum_results: int = 10,
) -> ScreeningRequest:
    if interval not in SUPPORTED_INTERVALS:
        raise ValueError(
            "неподдерживаемый интервал. Допустимы: 5m, 15m, 1h, 4h, 1d."
        )
    try:
        return ScreeningRequest(
            instruments=tuple(_instrument(symbol) for symbol in symbols),
            interval=interval,
            minimum_score=minimum_score,
            minimum_confidence=minimum_confidence,
            include_derivatives=include_derivatives,
            maximum_results=maximum_results,
        )
    except ValueError as error:
        if "Duplicate" in str(error):
            raise ValueError("инструменты не должны повторяться.") from None
        raise


def _instrument(value: str) -> Instrument:
    if ":" in value:
        symbol, raw_class = value.rsplit(":", 1)
        try:
            return Instrument(symbol, AssetClass(raw_class.lower()))
        except ValueError:
            raise ValueError(f"некорректный инструмент: {value}.") from None
    return Instrument(value, AssetClass.CRYPTO)


def _float(value: str, name: str) -> float:
    label = {
        "min_score": "минимальный балл",
        "min_confidence": "минимальная уверенность",
    }.get(name, "значение")
    try:
        parsed = float(value)
    except ValueError:
        raise ValueError(f"{label} должен быть числом.") from None
    if not 0 <= parsed <= 100:
        suffix = (
            "должна быть от 0 до 100."
            if name == "min_confidence"
            else "должен быть от 0 до 100."
        )
        raise ValueError(f"{label} {suffix}")
    return parsed


def _integer(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise ValueError("максимальное число результатов должно быть целым.") from None
    if parsed <= 0:
        raise ValueError("максимальное число результатов должно быть положительным.")
    return parsed


def _boolean(value: str, name: str) -> bool:
    normalized = value.lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"параметр {name} должен быть true или false.")

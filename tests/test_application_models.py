from dataclasses import FrozenInstanceError

import pytest

from market_signal_assistant.application.models import ScreeningRequest
from market_signal_assistant.models import AssetClass, Instrument


def request(**changes: object) -> ScreeningRequest:
    values: dict[str, object] = {
        "instruments": (Instrument("BTCUSDT", AssetClass.CRYPTO),),
        "interval": "1h",
        "minimum_score": 60.0,
        "minimum_confidence": 55.0,
        "include_derivatives": True,
        "maximum_results": 10,
    }
    values.update(changes)
    return ScreeningRequest(**values)  # type: ignore[arg-type]


def test_request_rejects_empty_instruments() -> None:
    with pytest.raises(ValueError, match="instrument"):
        request(instruments=())


def test_request_rejects_duplicates_after_normalization() -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        request(
            instruments=(
                Instrument(" btcusdt ", AssetClass.CRYPTO),
                Instrument("BTCUSDT", AssetClass.CRYPTO),
            )
        )


def test_request_normalizes_symbols() -> None:
    result = request(
        instruments=(Instrument(" btcusdt ", AssetClass.CRYPTO),)
    )
    assert result.instruments[0].symbol == "BTCUSDT"


@pytest.mark.parametrize("interval", ["", "2h", "60", "1H"])
def test_request_rejects_unsupported_interval(interval: str) -> None:
    with pytest.raises(ValueError, match="interval"):
        request(interval=interval)


@pytest.mark.parametrize("field", ["minimum_score", "minimum_confidence"])
@pytest.mark.parametrize("value", [-0.1, 100.1, float("nan"), float("inf"), True])
def test_request_rejects_invalid_score_ranges(field: str, value: object) -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        request(**{field: value})


@pytest.mark.parametrize("value", [0, -1, 1.5, True])
def test_request_rejects_invalid_maximum_results(value: object) -> None:
    with pytest.raises(ValueError, match="maximum_results"):
        request(maximum_results=value)


def test_request_is_immutable() -> None:
    result = request()
    with pytest.raises(FrozenInstanceError):
        result.interval = "5m"  # type: ignore[misc]


def test_request_rejects_non_boolean_derivatives_flag() -> None:
    with pytest.raises(ValueError, match="boolean"):
        request(include_derivatives="false")

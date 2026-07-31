from unittest.mock import Mock

from market_signal_assistant.composition import build_derivatives_components


def test_import_and_bootstrap_do_not_open_network() -> None:
    getter = Mock(side_effect=AssertionError("REST network opened"))
    websocket_factory = Mock(side_effect=AssertionError("WebSocket opened"))
    components = build_derivatives_components(
        getter=getter,
        websocket_factory=websocket_factory,
    )
    getter.assert_not_called()
    websocket_factory.assert_not_called()
    assert components.stream.running is False

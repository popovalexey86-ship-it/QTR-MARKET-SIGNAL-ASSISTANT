from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from market_signal_assistant.derivatives.intelligence import (
    DerivativesIntelligence,
)
from market_signal_assistant.providers import JsonGetter, public_json_get
from market_signal_assistant.providers.bybit_derivatives import (
    BybitDerivativesProvider,
)
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
    BybitLiquidationStream,
    WebSocketFactory,
)
from market_signal_assistant.signals.fusion import SignalFusion


@dataclass(frozen=True, slots=True)
class DerivativesComponents:
    provider: BybitDerivativesProvider
    intelligence: DerivativesIntelligence
    accumulator: BybitLiquidationAccumulator
    stream: BybitLiquidationStream
    fusion: SignalFusion


def build_derivatives_components(
    *,
    getter: JsonGetter = public_json_get,
    websocket_factory: WebSocketFactory | None = None,
    clock: Callable[[], datetime] | None = None,
    testnet: bool = False,
) -> DerivativesComponents:
    """Compose informational derivatives services without network access."""
    accumulator = BybitLiquidationAccumulator(clock=clock)
    return DerivativesComponents(
        provider=BybitDerivativesProvider(
            accumulator,
            getter=getter,
            clock=clock,
        ),
        intelligence=DerivativesIntelligence(),
        accumulator=accumulator,
        stream=BybitLiquidationStream(
            accumulator,
            testnet=testnet,
            websocket_factory=websocket_factory,
        ),
        fusion=SignalFusion(),
    )

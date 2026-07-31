from __future__ import annotations

from typing import Protocol

from market_signal_assistant.derivatives.models import DerivativesSnapshot


class DerivativesDataError(RuntimeError):
    """A safe provider-level derivatives data failure."""


class DerivativesProvider(Protocol):
    @property
    def name(self) -> str: ...

    def collect(self, symbol: str) -> DerivativesSnapshot: ...

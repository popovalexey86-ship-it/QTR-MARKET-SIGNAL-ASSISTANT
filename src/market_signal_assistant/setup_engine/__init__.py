"""Теневой классификатор торговых конструкций без исполнения сделок."""

from market_signal_assistant.setup_engine.adapters import (
    input_from_early_discovery_v2,
)
from market_signal_assistant.setup_engine.analyzer import (
    analyze_setup,
    classification_candidates,
)
from market_signal_assistant.setup_engine.audit import JsonlSetupAuditStore
from market_signal_assistant.setup_engine.engine import SetupEngine
from market_signal_assistant.setup_engine.models import (
    SETUP_CLASSIFICATION_PRIORITY,
    SetupAnalysisInput,
    SetupAnalysisResult,
    SetupDirection,
    SetupState,
    SetupType,
    TradeEligibility,
)
from market_signal_assistant.setup_engine.settings import SetupEngineSettings

__all__ = [
    "SETUP_CLASSIFICATION_PRIORITY",
    "JsonlSetupAuditStore",
    "SetupAnalysisInput",
    "SetupAnalysisResult",
    "SetupDirection",
    "SetupEngine",
    "SetupEngineSettings",
    "SetupState",
    "SetupType",
    "TradeEligibility",
    "analyze_setup",
    "classification_candidates",
    "input_from_early_discovery_v2",
]

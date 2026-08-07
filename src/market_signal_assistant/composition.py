from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from market_signal_assistant.application.service import (
    MarketScreeningService,
    SignalAnalyzer,
)
from market_signal_assistant.derivatives.intelligence import (
    DerivativesIntelligence,
)
from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.inplay.audit import (
    DEFAULT_INPLAY_DETECTION_STATE_PATH,
    DEFAULT_INPLAY_TIMING_AUDIT_PATH,
    InPlayTimingAuditor,
    JsonInPlayDetectionStore,
    JsonlInPlayTimingAuditStore,
)
from market_signal_assistant.inplay.early_discovery import (
    DEFAULT_EARLY_DISCOVERY_AUDIT_PATH,
    EarlyDiscoveryService,
    JsonlEarlyDiscoveryAuditStore,
)
from market_signal_assistant.inplay.early_discovery_v2 import (
    DEFAULT_V2_AUDIT_PATH,
    DEFAULT_V2_STATE_PATH,
    EarlyDiscoveryV2Config,
    EarlyDiscoveryV2Service,
    JsonEarlyDiscoveryV2StateStore,
    JsonlEarlyDiscoveryV2AuditStore,
)
from market_signal_assistant.inplay.listings import JsonListingStore, ListingTracker
from market_signal_assistant.inplay.notifications import (
    DEFAULT_INPLAY_NOTIFICATION_STATE_PATH,
    InPlayNotificationService,
    JsonInPlayNotificationStore,
)
from market_signal_assistant.inplay.service import INPLAY_MIN_SCORE, InPlayService
from market_signal_assistant.news.bybit_provider import BybitAnnouncementProvider
from market_signal_assistant.news.classifier import NewsClassifier
from market_signal_assistant.news.notifications import (
    DEFAULT_NEWS_NOTIFICATION_STATE_PATH,
    JsonNewsNotificationStore,
    NewsNotificationService,
)
from market_signal_assistant.news.service import NewsService
from market_signal_assistant.providers import (
    BybitPublicProvider,
    JsonGetter,
    MarketDataProvider,
    RoutingMarketDataProvider,
    public_json_get,
)
from market_signal_assistant.providers.bybit_derivatives import (
    BybitDerivativesProvider,
)
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
    BybitLiquidationStream,
    WebSocketFactory,
)
from market_signal_assistant.settings import (
    EarlyDiscoverySettings,
    EarlyDiscoveryV2Settings,
    InPlayTimingAuditSettings,
    NewsSettings,
)
from market_signal_assistant.signals.fusion import SignalFusion


@dataclass(frozen=True, slots=True)
class DerivativesComponents:
    provider: BybitDerivativesProvider
    intelligence: DerivativesIntelligence
    accumulator: BybitLiquidationAccumulator
    stream: BybitLiquidationStream
    fusion: SignalFusion


def build_screening_service(
    *,
    getter: JsonGetter = public_json_get,
    websocket_factory: WebSocketFactory | None = None,
    clock: Callable[[], datetime] | None = None,
    testnet: bool = False,
    technical_provider: MarketDataProvider | None = None,
    technical_analyzer: SignalAnalyzer | None = None,
    candle_limit: int = 250,
) -> tuple[MarketScreeningService, DerivativesComponents]:
    """Build the shared application service without opening the network."""
    derivatives = build_derivatives_components(
        getter=getter,
        websocket_factory=websocket_factory,
        clock=clock,
        testnet=testnet,
    )
    service = MarketScreeningService(
        provider=technical_provider or RoutingMarketDataProvider(),
        analyzer=technical_analyzer or SignalEngine(min_score=1.0, min_confirmations=1),
        derivatives_provider=derivatives.provider,
        derivatives_intelligence=derivatives.intelligence,
        fusion=derivatives.fusion,
        liquidations_active=lambda: derivatives.stream.running,
        clock=clock,
        candle_limit=candle_limit,
    )
    return service, derivatives


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


def build_inplay_service(
    *,
    getter: JsonGetter = public_json_get,
    clock: Callable[[], datetime] | None = None,
    state_path: Path = Path("data/inplay_listings.json"),
    audit_path: Path = DEFAULT_INPLAY_TIMING_AUDIT_PATH,
    detection_state_path: Path = DEFAULT_INPLAY_DETECTION_STATE_PATH,
    audit_settings: InPlayTimingAuditSettings | None = None,
    derivatives: DerivativesComponents | None = None,
) -> InPlayService:
    """Build the manual IN PLAY use case without opening the network or state."""
    components = derivatives or build_derivatives_components(
        getter=getter,
        clock=clock,
    )
    provider = BybitPublicProvider(getter=getter)
    resolved_audit = audit_settings or InPlayTimingAuditSettings.from_environment()
    timing_auditor = (
        InPlayTimingAuditor(
            JsonlInPlayTimingAuditStore(audit_path),
            JsonInPlayDetectionStore(detection_state_path),
            episode_score=resolved_audit.episode_score,
            qualification_score=INPLAY_MIN_SCORE,
            episode_reset=timedelta(minutes=resolved_audit.episode_reset_minutes),
        )
        if resolved_audit.enabled
        else None
    )
    return InPlayService(
        catalog_provider=provider,
        market_provider=provider,
        analyzer=SignalEngine(min_score=1.0, min_confirmations=1),
        listing_tracker=ListingTracker(JsonListingStore(state_path)),
        derivatives_provider=components.provider,
        derivatives_intelligence=components.intelligence,
        fusion=components.fusion,
        clock=clock,
        timing_auditor=timing_auditor,
    )


def build_inplay_notification_service(
    *,
    state_path: Path = DEFAULT_INPLAY_NOTIFICATION_STATE_PATH,
) -> InPlayNotificationService:
    """Build notification deduplication without reading or creating state."""
    return InPlayNotificationService(JsonInPlayNotificationStore(state_path))


def build_early_discovery_service(
    settings: EarlyDiscoverySettings,
    *,
    inplay_evaluator: InPlayService,
    getter: JsonGetter = public_json_get,
    clock: Callable[[], datetime] | None = None,
    audit_path: Path = DEFAULT_EARLY_DISCOVERY_AUDIT_PATH,
) -> EarlyDiscoveryService:
    """Build silent Early Discovery diagnostics without opening the network."""
    del settings
    provider = BybitPublicProvider(getter=getter)
    return EarlyDiscoveryService(
        catalog_provider=provider,
        market_provider=provider,
        audit_store=JsonlEarlyDiscoveryAuditStore(audit_path),
        inplay_evaluator=inplay_evaluator,
        clock=clock,
    )


def build_early_discovery_v2_service(
    settings: EarlyDiscoveryV2Settings,
    *,
    inplay_evaluator: InPlayService | None = None,
    getter: JsonGetter = public_json_get,
    clock: Callable[[], datetime] | None = None,
    audit_path: Path = DEFAULT_V2_AUDIT_PATH,
    state_path: Path = DEFAULT_V2_STATE_PATH,
    v1_audit_path: Path = DEFAULT_EARLY_DISCOVERY_AUDIT_PATH,
) -> EarlyDiscoveryV2Service:
    """Build standalone V2 diagnostics without opening network connections."""
    provider = BybitPublicProvider(getter=getter)
    evaluator = inplay_evaluator or build_inplay_service(getter=getter, clock=clock)
    v1_settings = EarlyDiscoverySettings.from_environment()
    v1_audit = (
        JsonlEarlyDiscoveryAuditStore(v1_audit_path) if v1_settings.enabled else None
    )
    return EarlyDiscoveryV2Service(
        catalog_provider=provider,
        market_provider=provider,
        audit_store=JsonlEarlyDiscoveryV2AuditStore(audit_path),
        state_store=JsonEarlyDiscoveryV2StateStore(state_path),
        config=EarlyDiscoveryV2Config(
            required_ready_scans=settings.required_ready_scans,
            forming_scans=settings.forming_scans,
            episode_gap_minutes=settings.episode_gap_minutes,
        ),
        inplay_evaluator=evaluator,
        v1_audit_store=v1_audit,
        clock=clock,
    )


def build_news_service(
    settings: NewsSettings,
    *,
    getter: JsonGetter = public_json_get,
    clock: Callable[[], datetime] | None = None,
) -> NewsService:
    """Build manual official-news use case without opening the network."""
    return NewsService(
        BybitAnnouncementProvider(
            getter=getter,
            base_url=settings.base_url,
        ),
        NewsClassifier(),
        lookback_hours=settings.lookback_hours,
        clock=clock,
    )


def build_news_notification_service(
    settings: NewsSettings,
    *,
    state_path: Path = DEFAULT_NEWS_NOTIFICATION_STATE_PATH,
) -> NewsNotificationService:
    """Build future notification state handling without reading state or polling."""
    return NewsNotificationService(
        JsonNewsNotificationStore(state_path),
        retention_days=settings.notification_retention_days,
        lookback_hours=settings.lookback_hours,
    )

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from urllib.parse import urlsplit


class TelegramSettings:
    """Runtime settings whose secret cannot be serialized as a dataclass."""

    __slots__ = ("_bot_token", "allowed_chat_ids", "allow_all")

    def __init__(
        self,
        bot_token: str,
        allowed_chat_ids: frozenset[int] = frozenset(),
        allow_all: bool = False,
    ) -> None:
        normalized_token = bot_token.strip()
        if not normalized_token:
            raise ValueError("Переменная TELEGRAM_BOT_TOKEN обязательна.")
        self._bot_token = normalized_token
        self.allowed_chat_ids = allowed_chat_ids
        self.allow_all = allow_all

    @property
    def bot_token(self) -> str:
        return self._bot_token

    def __repr__(self) -> str:
        return (
            "TelegramSettings(bot_token=<redacted>, "
            f"allowed_chat_ids={self.allowed_chat_ids!r}, "
            f"allow_all={self.allow_all!r})"
        )

    @classmethod
    def from_environment(cls) -> TelegramSettings:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        raw_ids = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
        try:
            allowed = frozenset(
                int(value.strip()) for value in raw_ids.split(",") if value.strip()
            )
        except ValueError:
            raise ValueError(
                "TELEGRAM_ALLOWED_CHAT_IDS должна содержать целые числа через запятую."
            ) from None
        allow_all = _environment_bool("TELEGRAM_ALLOW_ALL")
        return cls(token, allowed, allow_all)


@dataclass(frozen=True, slots=True)
class LiveDerivativesSettings:
    enabled: bool = False
    symbols: tuple[str, ...] = ()

    @classmethod
    def from_environment(cls) -> LiveDerivativesSettings:
        enabled = os.getenv("DERIVATIVES_LIVE_ENABLED", "false").lower() == "true"
        symbols = tuple(
            value.strip().upper()
            for value in os.getenv("DERIVATIVES_LIVE_SYMBOLS", "").split(",")
            if value.strip()
        )
        if enabled and not symbols:
            raise ValueError(
                "DERIVATIVES_LIVE_SYMBOLS обязательна при включённом онлайн-контексте."
            )
        return cls(enabled, symbols)


@dataclass(frozen=True, slots=True)
class InPlayAutoSettings:
    enabled: bool = False
    interval_minutes: int = 15

    def __post_init__(self) -> None:
        if self.interval_minutes < 5:
            raise ValueError("INPLAY_SCAN_INTERVAL_MINUTES должна быть не меньше 5.")

    @classmethod
    def from_environment(cls) -> InPlayAutoSettings:
        enabled = _environment_bool("INPLAY_AUTO_ENABLED")
        raw_interval = os.getenv("INPLAY_SCAN_INTERVAL_MINUTES", "15").strip()
        try:
            interval = int(raw_interval)
        except ValueError:
            raise ValueError(
                "INPLAY_SCAN_INTERVAL_MINUTES должна быть целым числом."
            ) from None
        return cls(enabled=enabled, interval_minutes=interval)


@dataclass(frozen=True, slots=True)
class InPlayTimingAuditSettings:
    enabled: bool = False
    auto_enabled: bool = False
    interval_minutes: int = 5
    episode_score: float = 40.0
    episode_reset_minutes: int = 60

    def __post_init__(self) -> None:
        if not 5 <= self.interval_minutes <= 60:
            raise ValueError(
                "INPLAY_TIMING_AUDIT_INTERVAL_MINUTES interval должен быть "
                "от 5 до 60 минут."
            )
        if not 0.0 <= self.episode_score <= 100.0:
            raise ValueError(
                "INPLAY_AUDIT_EPISODE_SCORE score должен быть от 0 до 100."
            )
        if not 15 <= self.episode_reset_minutes <= 1440:
            raise ValueError(
                "INPLAY_AUDIT_EPISODE_RESET_MINUTES reset должен быть "
                "от 15 до 1440 минут."
            )

    @classmethod
    def from_environment(cls) -> InPlayTimingAuditSettings:
        try:
            interval = int(
                os.getenv("INPLAY_TIMING_AUDIT_INTERVAL_MINUTES", "5").strip()
            )
            episode_score = float(os.getenv("INPLAY_AUDIT_EPISODE_SCORE", "40").strip())
            reset = int(os.getenv("INPLAY_AUDIT_EPISODE_RESET_MINUTES", "60").strip())
        except ValueError:
            raise ValueError(
                "Настройки IN PLAY timing audit должны быть числовыми."
            ) from None
        return cls(
            enabled=_environment_bool("INPLAY_TIMING_AUDIT_ENABLED"),
            auto_enabled=_environment_bool("INPLAY_TIMING_AUDIT_AUTO_ENABLED"),
            interval_minutes=interval,
            episode_score=episode_score,
            episode_reset_minutes=reset,
        )


@dataclass(frozen=True, slots=True)
class EarlyDiscoverySettings:
    enabled: bool = False
    interval_minutes: int = 5

    def __post_init__(self) -> None:
        if not 5 <= self.interval_minutes <= 60:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_INTERVAL_MINUTES interval должен быть "
                "от 5 до 60 минут."
            )

    @classmethod
    def from_environment(cls) -> EarlyDiscoverySettings:
        raw_interval = os.getenv(
            "INPLAY_EARLY_DISCOVERY_INTERVAL_MINUTES",
            "5",
        ).strip()
        try:
            interval = int(raw_interval)
        except ValueError:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_INTERVAL_MINUTES должна быть целым числом."
            ) from None
        return cls(
            enabled=_environment_bool("INPLAY_EARLY_DISCOVERY_ENABLED"),
            interval_minutes=interval,
        )


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryV2Settings:
    enabled: bool = False
    interval_minutes: int = 5
    required_ready_scans: int = 3
    forming_scans: int = 2
    episode_gap_minutes: int = 30

    def __post_init__(self) -> None:
        if not 5 <= self.interval_minutes <= 60:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_V2_INTERVAL_MINUTES должна быть "
                "от 5 до 60 минут."
            )
        if self.forming_scans < 1:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_V2_FORMING_SCANS должна быть положительной."
            )
        if self.required_ready_scans <= self.forming_scans:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_V2_REQUIRED_READY_SCANS должна быть "
                "больше FORMING_SCANS."
            )
        if not 5 <= self.episode_gap_minutes <= 1440:
            raise ValueError(
                "INPLAY_EARLY_DISCOVERY_V2_EPISODE_GAP_MINUTES должна быть "
                "от 5 до 1440 минут."
            )

    @classmethod
    def from_environment(cls) -> EarlyDiscoveryV2Settings:
        try:
            interval = int(
                os.getenv("INPLAY_EARLY_DISCOVERY_V2_INTERVAL_MINUTES", "5").strip()
            )
            required = int(
                os.getenv("INPLAY_EARLY_DISCOVERY_V2_REQUIRED_READY_SCANS", "3").strip()
            )
            forming = int(
                os.getenv("INPLAY_EARLY_DISCOVERY_V2_FORMING_SCANS", "2").strip()
            )
            gap = int(
                os.getenv("INPLAY_EARLY_DISCOVERY_V2_EPISODE_GAP_MINUTES", "30").strip()
            )
        except ValueError:
            raise ValueError(
                "Числовые настройки раннего обнаружения V2 должны быть целыми."
            ) from None
        return cls(
            enabled=_environment_bool("INPLAY_EARLY_DISCOVERY_V2_ENABLED"),
            interval_minutes=interval,
            required_ready_scans=required,
            forming_scans=forming,
            episode_gap_minutes=gap,
        )


@dataclass(frozen=True, slots=True)
class QtrSetupTelegramSettings:
    """Opt-in Telegram delivery for Setup Engine results."""

    enabled: bool = False
    minimum_quality: float = 90.0
    maximum_distance_atr: float = 1.2

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.minimum_quality)
            or not 0 <= self.minimum_quality <= 100
        ):
            raise ValueError(
                "QTR_SCANNER_TELEGRAM_MIN_QUALITY должна быть от 0 до 100."
            )
        if (
            not math.isfinite(self.maximum_distance_atr)
            or self.maximum_distance_atr <= 0
        ):
            raise ValueError(
                "QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR должна быть положительной."
            )

    @classmethod
    def from_environment(cls) -> QtrSetupTelegramSettings:
        try:
            minimum_quality = float(
                os.getenv("QTR_SCANNER_TELEGRAM_MIN_QUALITY", "90").strip()
            )
            maximum_distance_atr = float(
                os.getenv("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", "1.2").strip()
            )
        except ValueError:
            raise ValueError(
                "Настройки качества QTR Scanner Telegram должны быть числами."
            ) from None
        return cls(
            enabled=_environment_bool("QTR_SETUP_TELEGRAM_ENABLED"),
            minimum_quality=minimum_quality,
            maximum_distance_atr=maximum_distance_atr,
        )


@dataclass(frozen=True, slots=True)
class NewsAutoSettings:
    enabled: bool = False
    interval_minutes: int = 60

    def __post_init__(self) -> None:
        if not 15 <= self.interval_minutes <= 1440:
            raise ValueError("NEWS_SCAN_INTERVAL_MINUTES должна быть от 15 до 1440.")

    @classmethod
    def from_environment(cls) -> NewsAutoSettings:
        enabled = _environment_bool("NEWS_AUTO_ENABLED")
        raw_interval = os.getenv("NEWS_SCAN_INTERVAL_MINUTES", "60").strip()
        try:
            interval = int(raw_interval)
        except ValueError:
            raise ValueError(
                "NEWS_SCAN_INTERVAL_MINUTES должна быть целым числом."
            ) from None
        return cls(enabled=enabled, interval_minutes=interval)


@dataclass(frozen=True, slots=True)
class NewsSettings:
    enabled: bool = True
    lookback_hours: int = 24
    base_url: str = "https://api.bybit.com"
    notification_retention_days: int = 30

    def __post_init__(self) -> None:
        if not 1 <= self.lookback_hours <= 168:
            raise ValueError("NEWS_LOOKBACK_HOURS должна быть от 1 до 168.")
        if not 7 <= self.notification_retention_days <= 365:
            raise ValueError(
                "NEWS_NOTIFICATION_RETENTION_DAYS должна быть от 7 до 365."
            )
        normalized_url = self.base_url.strip().rstrip("/")
        parsed = urlsplit(normalized_url)
        if parsed.scheme != "https" or not parsed.netloc or "testnet" in parsed.netloc:
            raise ValueError(
                "BYBIT_PUBLIC_BASE_URL должна указывать на production HTTPS endpoint."
            )
        object.__setattr__(self, "base_url", normalized_url)

    @classmethod
    def from_environment(cls) -> NewsSettings:
        enabled = _environment_bool("NEWS_ENABLED", default=True)
        raw_lookback = os.getenv("NEWS_LOOKBACK_HOURS", "24").strip()
        try:
            lookback = int(raw_lookback)
        except ValueError:
            raise ValueError("NEWS_LOOKBACK_HOURS должна быть целым числом.") from None
        base_url = os.getenv(
            "BYBIT_PUBLIC_BASE_URL",
            "https://api.bybit.com",
        )
        raw_retention = os.getenv(
            "NEWS_NOTIFICATION_RETENTION_DAYS",
            "30",
        ).strip()
        try:
            retention = int(raw_retention)
        except ValueError:
            raise ValueError(
                "NEWS_NOTIFICATION_RETENTION_DAYS должна быть целым числом."
            ) from None
        return cls(enabled, lookback, base_url, retention)


def _environment_bool(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name, "true" if default else "false").strip().lower()
    if value == "true":
        return True
    if value in {"", "false"}:
        return False
    raise ValueError(f"{name} должна иметь значение 'true' или 'false'.")

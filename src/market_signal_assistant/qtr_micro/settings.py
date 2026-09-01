from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlsplit

DEMO_API_HOST = "api-demo.bybit.com"
DEMO_BASE_URL = f"https://{DEMO_API_HOST}"


@dataclass(frozen=True, slots=True, repr=False)
class QtrMicroSettings:
    enabled: bool = False
    mode: str = "demo"
    api_key: str = ""
    api_secret: str = ""
    base_url: str = DEMO_BASE_URL
    max_signal_age_seconds: int = 60
    max_entry_distance_atr: float = 0.25
    base_risk_pct: float = 0.5
    max_risk_pct: float = 1.0
    base_leverage: int = 5
    max_leverage: int = 10
    max_notional_usdt: float = 100_000.0
    max_notional_equity_pct: float = 100.0
    max_estimated_fees_r_pct: float = 20.0
    taker_fee_rate: float = 0.00055
    actual_risk_tolerance_pct: float = 10.0
    fill_confirmation_timeout_seconds: float = 20.0
    fill_poll_interval_seconds: float = 1.0
    stop_atr_buffer: float = 0.15
    tp1_r: float = 1.0
    tp1_close_pct: float = 40.0
    tp2_r: float = 2.0
    tp2_close_pct: float = 30.0
    runner_initial_r: float = 3.0
    runner_max_r: float = 5.0
    after_tp1_stop_mode: str = "breakeven"
    progress_check_minutes: int = 15
    normal_max_hold_minutes: int = 45
    runner_max_hold_minutes: int = 90
    max_open_positions: int = 2
    max_consecutive_losses: int = 3
    loss_pause_minutes: int = 30
    daily_loss_limit_pct: float = 2.0
    kill_switch: bool = False

    def __post_init__(self) -> None:
        if self.mode != "demo":
            raise ValueError("QTR_MICRO_MODE поддерживает только значение demo.")
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != DEMO_API_HOST
            or parsed.netloc != DEMO_API_HOST
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("QTR Micro разрешает только https://api-demo.bybit.com.")
        if not 1 <= self.max_signal_age_seconds <= 3600:
            raise ValueError("QTR_MICRO_MAX_SIGNAL_AGE_SECONDS вне диапазона.")
        if not 0 < self.max_entry_distance_atr <= 2:
            raise ValueError("QTR_MICRO_MAX_ENTRY_DISTANCE_ATR вне диапазона.")
        for name, value in (
            ("QTR_MICRO_BASE_RISK_PCT", self.base_risk_pct),
            ("QTR_MICRO_MAX_RISK_PCT", self.max_risk_pct),
        ):
            if not 0.1 <= value <= 1.0:
                raise ValueError(f"{name} должен быть от 0.1 до 1.0.")
        if self.base_risk_pct > self.max_risk_pct:
            raise ValueError("Base risk не может превышать max risk.")
        if not 1 <= self.base_leverage <= self.max_leverage <= 10:
            raise ValueError("QTR Micro leverage должен быть от x1 до x10.")
        if self.max_notional_usdt <= 0:
            raise ValueError("QTR_MICRO_MAX_NOTIONAL_USDT должен быть положительным.")
        if not 1 <= self.max_notional_equity_pct <= 1000:
            raise ValueError("QTR_MICRO_MAX_NOTIONAL_EQUITY_PCT вне диапазона.")
        if not 0 < self.max_estimated_fees_r_pct <= 100:
            raise ValueError("QTR_MICRO_MAX_ESTIMATED_FEES_R_PCT вне диапазона.")
        if not 0 < self.taker_fee_rate <= 0.01:
            raise ValueError("QTR_MICRO_TAKER_FEE_RATE вне диапазона.")
        if not 0 <= self.actual_risk_tolerance_pct <= 100:
            raise ValueError("QTR_MICRO_ACTUAL_RISK_TOLERANCE_PCT вне диапазона.")
        if not 15 <= self.fill_confirmation_timeout_seconds <= 30:
            raise ValueError(
                "QTR_MICRO_FILL_CONFIRMATION_TIMEOUT_SECONDS вне диапазона."
            )
        if not 0.5 <= self.fill_poll_interval_seconds <= 2:
            raise ValueError("QTR_MICRO_FILL_POLL_INTERVAL_SECONDS вне диапазона.")
        if not 0.05 <= self.stop_atr_buffer <= 0.30:
            raise ValueError("QTR_MICRO_STOP_ATR_BUFFER должен быть 0.05-0.30 ATR.")
        if self.tp1_close_pct <= 0 or self.tp2_close_pct <= 0:
            raise ValueError("Partial close percentages должны быть положительными.")
        if self.tp1_close_pct + self.tp2_close_pct >= 100:
            raise ValueError("Для runner должна оставаться положительная доля.")
        if not 0 < self.tp1_r < self.tp2_r <= self.runner_initial_r:
            raise ValueError("TP/runner R levels заданы в неверном порядке.")
        if self.runner_initial_r > self.runner_max_r:
            raise ValueError("Runner initial R не может превышать max R.")
        if self.after_tp1_stop_mode != "breakeven":
            raise ValueError("V1 поддерживает только breakeven после TP1.")
        if not (
            1
            <= self.progress_check_minutes
            < self.normal_max_hold_minutes
            < self.runner_max_hold_minutes
        ):
            raise ValueError("Micro time limits заданы в неверном порядке.")
        if self.max_open_positions < 1:
            raise ValueError("QTR_MICRO_MAX_OPEN_POSITIONS должен быть положительным.")
        if self.max_consecutive_losses < 1 or self.loss_pause_minutes < 1:
            raise ValueError("Loss safety settings должны быть положительными.")
        if not 0 < self.daily_loss_limit_pct <= 10:
            raise ValueError("QTR_MICRO_DAILY_LOSS_LIMIT_PCT вне диапазона.")

    def __repr__(self) -> str:
        return (
            "QtrMicroSettings("
            f"enabled={self.enabled!r}, mode={self.mode!r}, "
            "api_key=<redacted>, api_secret=<redacted>, "
            f"base_url={self.base_url!r})"
        )

    @property
    def credentials_present(self) -> bool:
        return bool(self.api_key.strip() and self.api_secret.strip())

    @classmethod
    def from_environment(cls) -> QtrMicroSettings:
        return cls(
            enabled=_bool("QTR_MICRO_ENABLED"),
            mode=os.getenv("QTR_MICRO_MODE", "demo").strip().lower(),
            api_key=os.getenv("BYBIT_DEMO_API_KEY", "").strip(),
            api_secret=os.getenv("BYBIT_DEMO_API_SECRET", "").strip(),
            base_url=(
                os.getenv("BYBIT_DEMO_BASE_URL", DEMO_BASE_URL).strip().rstrip("/")
            ),
            max_signal_age_seconds=_int("QTR_MICRO_MAX_SIGNAL_AGE_SECONDS", 60),
            max_entry_distance_atr=_float("QTR_MICRO_MAX_ENTRY_DISTANCE_ATR", 0.25),
            base_risk_pct=_float("QTR_MICRO_BASE_RISK_PCT", 0.5),
            max_risk_pct=_float("QTR_MICRO_MAX_RISK_PCT", 1.0),
            base_leverage=_int("QTR_MICRO_BASE_LEVERAGE", 5),
            max_leverage=_int("QTR_MICRO_MAX_LEVERAGE", 10),
            max_notional_usdt=_float("QTR_MICRO_MAX_NOTIONAL_USDT", 100_000),
            max_notional_equity_pct=_float("QTR_MICRO_MAX_NOTIONAL_EQUITY_PCT", 100),
            max_estimated_fees_r_pct=_float("QTR_MICRO_MAX_ESTIMATED_FEES_R_PCT", 20),
            taker_fee_rate=_float("QTR_MICRO_TAKER_FEE_RATE", 0.00055),
            actual_risk_tolerance_pct=_float("QTR_MICRO_ACTUAL_RISK_TOLERANCE_PCT", 10),
            fill_confirmation_timeout_seconds=_float(
                "QTR_MICRO_FILL_CONFIRMATION_TIMEOUT_SECONDS", 20
            ),
            fill_poll_interval_seconds=_float(
                "QTR_MICRO_FILL_POLL_INTERVAL_SECONDS", 1
            ),
            stop_atr_buffer=_float("QTR_MICRO_STOP_ATR_BUFFER", 0.15),
            tp1_r=_float("QTR_MICRO_TP1_R", 1.0),
            tp1_close_pct=_float("QTR_MICRO_TP1_CLOSE_PCT", 40),
            tp2_r=_float("QTR_MICRO_TP2_R", 2.0),
            tp2_close_pct=_float("QTR_MICRO_TP2_CLOSE_PCT", 30),
            runner_initial_r=_float("QTR_MICRO_RUNNER_INITIAL_R", 3.0),
            runner_max_r=_float("QTR_MICRO_RUNNER_MAX_R", 5.0),
            after_tp1_stop_mode=os.getenv("QTR_MICRO_AFTER_TP1_STOP_MODE", "breakeven")
            .strip()
            .lower(),
            progress_check_minutes=_int("QTR_MICRO_PROGRESS_CHECK_MINUTES", 15),
            normal_max_hold_minutes=_int("QTR_MICRO_NORMAL_MAX_HOLD_MINUTES", 45),
            runner_max_hold_minutes=_int("QTR_MICRO_RUNNER_MAX_HOLD_MINUTES", 90),
            max_open_positions=_int("QTR_MICRO_MAX_OPEN_POSITIONS", 2),
            max_consecutive_losses=_int("QTR_MICRO_MAX_CONSECUTIVE_LOSSES", 3),
            loss_pause_minutes=_int("QTR_MICRO_LOSS_PAUSE_MINUTES", 30),
            daily_loss_limit_pct=_float("QTR_MICRO_DAILY_LOSS_LIMIT_PCT", 2.0),
            kill_switch=_bool("QTR_MICRO_KILL_SWITCH"),
        )


def _bool(name: str) -> bool:
    value = os.getenv(name, "false").strip().lower()
    if value in {"", "false"}:
        return False
    if value == "true":
        return True
    raise ValueError(f"{name} должен иметь значение true или false.")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        raise ValueError(f"{name} должен быть целым числом.") from None


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except ValueError:
        raise ValueError(f"{name} должен быть числом.") from None

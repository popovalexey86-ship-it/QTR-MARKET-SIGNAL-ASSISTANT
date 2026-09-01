from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from market_signal_assistant.qtr_micro.client import DemoTradingClient
from market_signal_assistant.qtr_micro.engine import QtrMicroEntryEngine
from market_signal_assistant.qtr_micro.execution import QtrMicroExecutionService
from market_signal_assistant.qtr_micro.journal import (
    JsonlQtrMicroDecisionAudit,
    JsonlQtrMicroTradeJournal,
)
from market_signal_assistant.qtr_micro.models import (
    EntryPlan,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    MicroState,
    PreflightResult,
)
from market_signal_assistant.qtr_micro.preflight import QtrMicroPreflight
from market_signal_assistant.qtr_micro.runtime_audit import (
    JsonlQtrMicroRuntimeAudit,
    QtrMicroRuntimeAuditRecord,
    QtrMicroRuntimeEvent,
)
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.models import SetupDirection, SetupState
from market_signal_assistant.telegram.qtr_micro import (
    format_micro_closed,
    format_micro_entry,
    format_micro_tp,
)

_LOGGER = logging.getLogger(__name__)
MicroSender = Callable[[int, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class QtrMicroRuntimeStatus:
    enabled: bool
    demo_api_ready: bool
    blocked_reason: str | None
    open_positions: int
    daily_pnl: float
    kill_switch: bool


@dataclass(frozen=True, slots=True)
class PostEntryManagementFlags:
    setup_cancelled: bool
    opposite_structure: bool
    structure_degraded: bool


def post_entry_management_flags(
    position: MicroPosition,
    candidate: QtrSetupCandidate | None,
) -> PostEntryManagementFlags:
    """Map current setup evidence without treating degradation as hard cancel."""

    if candidate is None:
        return PostEntryManagementFlags(False, False, False)
    result = candidate.result
    current_failure = bool(result.current_breakout_failure)
    expected_direction = (
        SetupDirection.UP
        if position.direction.value == "LONG"
        else SetupDirection.DOWN
    )
    return PostEntryManagementFlags(
        setup_cancelled=(
            result.setup_state is SetupState.CANCELLED and not current_failure
        ),
        opposite_structure=bool(
            result.structure_confirmation
            and result.direction
            not in {expected_direction, SetupDirection.NEUTRAL}
        ),
        structure_degraded=current_failure,
    )


class QtrMicroRuntime:
    """Explicit Telegram-owned Demo lifecycle; no task or network at import."""

    def __init__(
        self,
        *,
        settings: QtrMicroSettings,
        client: DemoTradingClient | None,
        state_store: JsonQtrMicroStateStore,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
        decision_audit: JsonlQtrMicroDecisionAudit | None = None,
        runtime_audit: JsonlQtrMicroRuntimeAudit | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._state_store = state_store
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._decision_audit = decision_audit or JsonlQtrMicroDecisionAudit()
        self._runtime_audit = runtime_audit or JsonlQtrMicroRuntimeAudit()
        self._engine = QtrMicroEntryEngine(settings)
        self._execution = (
            QtrMicroExecutionService(
                settings=settings,
                client=client,
                state_store=state_store,
                engine=self._engine,
                journal=JsonlQtrMicroTradeJournal(),
                runtime_audit=self._runtime_audit,
            )
            if client is not None
            else None
        )
        self._preflight = QtrMicroPreflight(
            settings,
            client,
            state_store=state_store,
            clock=self._clock,
        )
        self._preflight_result = PreflightResult(False, "Preflight ещё не выполнен.")
        self._lock = asyncio.Lock()

    @property
    def status(self) -> QtrMicroRuntimeStatus:
        now = self._clock()
        state = self._state_store.load(
            today=now.date(), trading_enabled=self._settings.enabled
        )
        open_count = sum(
            item.stage not in {MicroStage.CLOSED, MicroStage.BLOCKED}
            for item in state.positions.values()
        )
        return QtrMicroRuntimeStatus(
            enabled=self._settings.enabled,
            demo_api_ready=self._preflight_result.ready,
            blocked_reason=self._preflight_result.reason or state.blocked_reason,
            open_positions=open_count,
            daily_pnl=state.realised_daily_pnl,
            kill_switch=self._settings.kill_switch,
        )

    async def initialize(self) -> PreflightResult:
        async with self._lock:
            result = await asyncio.to_thread(self._preflight.run, None)
            self._preflight_result = result
            if not result.ready:
                _LOGGER.warning("QTR Micro Demo заблокирован: %s", result.reason)
                return result
            assert self._execution is not None
            state = self._state_store.load(
                today=self._clock().date(), trading_enabled=True
            )
            if state.day_start_equity <= 0 and result.equity is not None:
                self._state_store.save(
                    replace(
                        state,
                        trading_enabled=True,
                        day_start_equity=result.equity,
                        updated_at=self._clock(),
                    )
                )
            await asyncio.to_thread(self._execution.reconcile, self._clock())
            return result

    async def handle_candidates(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        send: MicroSender,
    ) -> None:
        if not self._preflight_result.ready:
            return
        if self._client is None or self._execution is None:
            return
        async with self._lock:
            now = self._clock()
            assert self._execution is not None
            await asyncio.to_thread(self._execution.reconcile, now)
            try:
                remote_positions, equity = await asyncio.gather(
                    asyncio.to_thread(self._client.list_positions),
                    asyncio.to_thread(self._client.wallet_equity),
                )
            except Exception as error:
                _LOGGER.warning(
                    "QTR Micro cycle заблокирован до получения свежего Demo state "
                    "(%s).",
                    type(error).__name__,
                )
                return
            remote_symbols = {item.symbol for item in remote_positions}
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            if state.trading_day != now.date():
                state = replace(
                    state,
                    updated_at=now,
                    trading_day=now.date(),
                    day_start_equity=equity,
                    realised_daily_pnl=0.0,
                    consecutive_losses=0,
                    loss_pause_until=None,
                )
                self._state_store.save(state)
            await self._confirm_pending(state, send, now)
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            by_symbol = {item.result.symbol: item for item in candidates}
            await self._manage_open(state, by_symbol, send, now)
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            if self._settings.kill_switch:
                return
            if not state.trading_enabled or state.blocked_reason is not None:
                return
            for candidate in candidates:
                if candidate.result.symbol in remote_symbols:
                    continue
                if any(
                    item.setup_episode_id == candidate.episode_id
                    for item in state.positions.values()
                ):
                    continue
                try:
                    position_mode = await asyncio.to_thread(
                        self._client.position_mode, candidate.result.symbol
                    )
                    if position_mode != "ONE_WAY":
                        _LOGGER.warning(
                            "QTR Micro entry пропущен для %s: "
                            "неподдерживаемый position mode %s.",
                            candidate.result.symbol,
                            position_mode,
                        )
                        continue
                    rules = await asyncio.to_thread(
                        self._client.instrument_rules, candidate.result.symbol
                    )
                    decision = self._engine.prepare_entry(
                        candidate,
                        now=now,
                        equity=equity,
                        rules=rules,
                        state=state,
                        preflight=self._preflight_result,
                    )
                    if decision.plan is None:
                        self._decision_audit.append_skip(
                            decided_at=now,
                            symbol=candidate.result.symbol,
                            episode_id=candidate.episode_id,
                            decision=decision,
                        )
                        _LOGGER.info(
                            "QTR Micro entry пропущен для %s: %s (%s).",
                            candidate.result.symbol,
                            decision.skip_detail
                            or (
                                decision.skip_reason.value
                                if decision.skip_reason is not None
                                else "неизвестная причина"
                            ),
                            (
                                decision.instrument_status.value
                                if decision.instrument_status is not None
                                else "n/a"
                            ),
                        )
                        continue
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.ENTRY_REVALIDATION_STARTED,
                        decision.plan,
                        now,
                    )
                    fresh_price = await asyncio.to_thread(
                        self._client.current_market_price,
                        candidate.result.symbol,
                    )
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.FRESH_PRICE_LOADED,
                        decision.plan,
                        now,
                        detail=f"price={fresh_price:.12g}",
                    )
                    revalidated = self._engine.revalidate_entry(
                        candidate,
                        decision.plan,
                        current_price=fresh_price,
                        now=now,
                        equity=equity,
                        rules=rules,
                        state=state,
                        preflight=self._preflight_result,
                    )
                    if revalidated.plan is None:
                        self._append_entry_audit(
                            QtrMicroRuntimeEvent.ENTRY_REVALIDATION_REJECTED,
                            decision.plan,
                            now,
                            detail=(
                                revalidated.skip_reason.value
                                if revalidated.skip_reason is not None
                                else "unknown"
                            ),
                        )
                        self._decision_audit.append_skip(
                            decided_at=now,
                            symbol=candidate.result.symbol,
                            episode_id=candidate.episode_id,
                            decision=revalidated,
                        )
                        continue
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.ENTRY_REVALIDATION_PASSED,
                        revalidated.plan,
                        now,
                    )
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.SIZE_RECALCULATED,
                        revalidated.plan,
                        now,
                        detail=(
                            f"qty={revalidated.plan.qty:.12g}; "
                            f"notional={revalidated.plan.notional:.8f}; "
                            f"fees_r_pct={revalidated.plan.estimated_fees_r_pct:.4f}"
                        ),
                    )
                    confirmed = await asyncio.to_thread(
                        self._execution.submit_and_confirm_entry,
                        revalidated.plan,
                        now,
                        rules,
                    )
                    if confirmed.stage is MicroStage.OPEN:
                        await self._broadcast(
                            send, format_micro_entry(_plan_from_position(confirmed))
                        )
                except Exception as error:
                    _LOGGER.warning(
                        "QTR Micro entry пропущен для %s (%s): %s.",
                        candidate.result.symbol,
                        type(error).__name__,
                        str(error),
                    )

    def _append_entry_audit(
        self,
        event: QtrMicroRuntimeEvent,
        plan: EntryPlan,
        occurred_at: datetime,
        *,
        detail: str | None = None,
    ) -> None:
        self._runtime_audit.append(
            QtrMicroRuntimeAuditRecord(
                occurred_at=occurred_at,
                event=event,
                trade_id=plan.trade_id,
                symbol=plan.symbol,
                stage=MicroStage.PREPARED.value,
                detail=detail,
            )
        )

    async def _confirm_pending(
        self, state: MicroState, send: MicroSender, now: datetime
    ) -> None:
        assert self._execution is not None
        assert self._client is not None
        for position in state.positions.values():
            if position.stage is not MicroStage.ENTRY_ACKNOWLEDGED:
                continue
            confirmed = await asyncio.to_thread(
                self._execution.confirm_entry,
                position.trade_id,
                now,
                rules=await asyncio.to_thread(
                    self._client.instrument_rules, position.symbol
                ),
            )
            if confirmed is None or confirmed.stage is not MicroStage.OPEN:
                continue
            plan = _plan_from_position(confirmed)
            await self._broadcast(send, format_micro_entry(plan))

    async def _manage_open(
        self,
        state: MicroState,
        candidates: dict[str, QtrSetupCandidate],
        send: MicroSender,
        now: datetime,
    ) -> None:
        assert self._execution is not None
        assert self._client is not None
        for position in state.positions.values():
            if position.stage not in {
                MicroStage.OPEN,
                MicroStage.TP1_FILLED,
                MicroStage.TP2_FILLED,
                MicroStage.RUNNER,
                MicroStage.EXIT_ACKNOWLEDGED,
            }:
                continue
            candidate = candidates.get(position.symbol)
            current_price = (
                candidate.result.current_price if candidate is not None else None
            )
            if current_price is None:
                try:
                    current_price = await asyncio.to_thread(
                        self._client.current_market_price, position.symbol
                    )
                except Exception as error:
                    _LOGGER.warning(
                        "QTR Micro management пропущен для %s: свежая цена "
                        "недоступна (%s).",
                        position.symbol,
                        type(error).__name__,
                    )
                    continue
            assert current_price is not None
            management_flags = post_entry_management_flags(position, candidate)
            decision = await asyncio.to_thread(
                self._execution.manage_position,
                position.trade_id,
                current_price=current_price,
                now=now,
                setup_cancelled=management_flags.setup_cancelled,
                opposite_structure=management_flags.opposite_structure,
                structure_degraded=management_flags.structure_degraded,
            )
            if decision.action in {MicroExitReason.TP1, MicroExitReason.TP2}:
                result_r = 1.0 if decision.action is MicroExitReason.TP1 else 2.0
                await self._broadcast(
                    send, format_micro_tp(position.symbol, decision.action, result_r)
                )
            elif decision.action is not None:
                await self._broadcast(
                    send,
                    format_micro_closed(
                        position,
                        reason=decision.action.value,
                        pnl=0.0,
                        result_r=0.0,
                        hold_minutes=_hold_minutes(position, now),
                    ),
                )

    async def _broadcast(self, send: MicroSender, text: str) -> None:
        for chat_id in sorted(self._allowed_chat_ids):
            await send(chat_id, text)


def _plan_from_position(position: MicroPosition) -> EntryPlan:
    if position.average_fill is None:
        raise ValueError("Filled Micro position has no average fill.")
    return EntryPlan(
        trade_id=position.trade_id,
        setup_episode_id=position.setup_episode_id,
        symbol=position.symbol,
        direction=position.direction,
        setup_type=position.setup_type,
        setup_confidence=position.setup_confidence,
        signal_at=position.signal_at,
        signal_price=position.signal_price,
        entry_price=position.average_fill,
        stop_price=position.structural_stop,
        risk_pct=position.risk_pct,
        risk_amount=position.risk_amount,
        qty=position.initial_qty,
        leverage=position.leverage,
        tp1_price=position.tp1_price,
        tp1_qty=position.tp1_qty,
        tp2_price=position.tp2_price,
        tp2_qty=position.tp2_qty,
        runner_target_price=position.runner_target_price,
        runner_qty=position.runner_qty,
        initial_r=position.initial_r,
        order_link_id=position.entry_order_link_id,
    )


def _hold_minutes(position: MicroPosition, now: datetime) -> int:
    opened_at = position.opened_at
    if opened_at is None:
        return 0
    return max(0, int((now - opened_at).total_seconds() // 60))

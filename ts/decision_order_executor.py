from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from core.logger import get_logger, log_info, log_warning
from trading.order_service import OrderService
from trading.trade_telegram_notifier import TradeTelegramNotifier
from trading.trade_store import (
    append_trade_event,
    clear_trade_runtime_state,
    create_trade,
    get_trade_by_id,
    get_open_trade_for_instrument,
    mark_entry_filled,
    mark_entry_submitted,
    mark_exit_filled_and_close,
    mark_exit_submitted,
    mark_trade_error,
    upsert_trade_runtime_state,
)

logger = get_logger(__name__)


@dataclass(slots=True)
class DecisionOrderState:
    position_side: Optional[str] = None
    position_qty: int = 0

    current_trade_id: Optional[int] = None
    entry_hour_start_ts: Optional[int] = None
    last_entry_attempt_hour_start_ts: Optional[int] = None


class DecisionOrderExecutor:
    def __init__(
            self,
            *,
            settings,
            order_service: OrderService,
            instrument_code: str = "MNQ",
    ):
        self.settings = settings
        self.order_service = order_service
        self.instrument_code = instrument_code

        self.trade_db_path = settings.trade_db_path

        self.enabled = settings.trading_enable_order_execution
        self.quantity = settings.trading_order_quantity
        self.exit_bar_index = settings.trading_exit_bar_index
        self.order_ref_prefix = settings.trading_order_ref_prefix
        self.time_in_force = settings.trading_order_time_in_force
        self.accept_timeout = settings.trading_accept_timeout_seconds
        self.done_timeout = settings.trading_done_timeout_seconds

        self.telegram_notifier = TradeTelegramNotifier(
            settings=settings,
            instrument_code=instrument_code,
        )

        self.state = DecisionOrderState()

    def hydrate_recovered_state(
            self,
            *,
            current_trade_id,
            position_side,
            position_qty,
            entry_hour_start_ts,
    ):
        # Загружаем восстановленное состояние из recovery/reconcile слоя.
        self.state = DecisionOrderState(
            position_side=position_side,
            position_qty=position_qty,
            current_trade_id=current_trade_id,
            entry_hour_start_ts=entry_hour_start_ts,
            last_entry_attempt_hour_start_ts=entry_hour_start_ts,
        )

    def reset_in_memory_state(self):
        # Полностью очищаем внутреннее торговое состояние executor.
        self.state = DecisionOrderState()

    async def close(self):
        await self.telegram_notifier.close()

    async def on_snapshot(self, *, snapshot, active_futures, pearson_live_runtime=None):
        if snapshot is None:
            return

        self._sync_runtime_state(snapshot)

        if not self.enabled:
            return

        await self._maybe_exit_position(snapshot=snapshot, active_futures=active_futures)
        await self._maybe_enter_position(
            snapshot=snapshot,
            active_futures=active_futures,
            pearson_live_runtime=pearson_live_runtime,
        )

        self._sync_runtime_state(snapshot)

    async def _maybe_enter_position(self, *, snapshot, active_futures, pearson_live_runtime=None):
        if self.state.position_side is not None:
            return

        if not snapshot.decision_calculated:
            return

        if snapshot.decision_result is None:
            return

        decision = snapshot.decision_result["decision"]

        if decision not in ("LONG", "SHORT"):
            return

        if self.state.last_entry_attempt_hour_start_ts == snapshot.hour_start_ts:
            return

        self.state.last_entry_attempt_hour_start_ts = snapshot.hour_start_ts

        local_symbol = self._get_active_local_symbol(active_futures)
        order_ref = (
            f"{self.order_ref_prefix}_"
            f"{self.instrument_code}_"
            f"{snapshot.hour_start_ts}_"
            f"ENTRY_{decision}"
        )

        trade_id = None

        try:
            trade_id = self._create_trade_from_snapshot(
                snapshot=snapshot,
                decision=decision,
                contract_local_symbol=local_symbol,
            )

            self.state.current_trade_id = trade_id
            self.state.entry_hour_start_ts = snapshot.hour_start_ts

            self._append_event(
                trade_id=trade_id,
                event_type="SIGNAL_ACCEPTED",
                snapshot=snapshot,
                message=f"Получен торговый сигнал {decision}",
                payload={
                    "decision_result": snapshot.decision_result,
                    "forecast_summary": snapshot.forecast_summary,
                },
            )

            self._sync_runtime_state(snapshot)

            contract = await self.order_service.future(local_symbol=local_symbol)

            if decision == "LONG":
                placement = await self.order_service.buy_market(
                    contract=contract,
                    quantity=self.quantity,
                    order_ref=order_ref,
                    time_in_force=self.time_in_force,
                    wait="done",
                    accept_timeout=self.accept_timeout,
                    done_timeout=self.done_timeout,
                )
                self.state.position_side = "LONG"

            else:
                placement = await self.order_service.sell_market(
                    contract=contract,
                    quantity=self.quantity,
                    order_ref=order_ref,
                    time_in_force=self.time_in_force,
                    wait="done",
                    accept_timeout=self.accept_timeout,
                    done_timeout=self.done_timeout,
                )
                self.state.position_side = "SHORT"

            self.state.position_qty = self.quantity

            entry_order_id = placement.receipt.order_id
            entry_perm_id = self._extract_perm_id(placement)
            entry_submitted_ts = int(placement.receipt.placed_at_utc.timestamp())
            entry_submitted_time = self._format_utc_ts(entry_submitted_ts)

            mark_entry_submitted(
                self.trade_db_path,
                trade_id=trade_id,
                status="ENTRY_SUBMITTED",
                entry_submitted_ts=entry_submitted_ts,
                entry_submitted_time=entry_submitted_time,
                entry_order_id=entry_order_id,
                entry_perm_id=entry_perm_id,
            )

            self._append_event(
                trade_id=trade_id,
                event_type="ENTRY_SUBMITTED",
                snapshot=snapshot,
                message=f"Отправлен ордер на вход {decision}",
                payload={
                    "order_ref": order_ref,
                    "entry_order_id": entry_order_id,
                    "entry_perm_id": entry_perm_id,
                },
            )

            entry_filled_ts = self._extract_done_ts(placement)
            entry_filled_time = self._format_utc_ts(entry_filled_ts)

            mark_entry_filled(
                self.trade_db_path,
                trade_id=trade_id,
                status="OPEN",
                entry_filled_ts=entry_filled_ts,
                entry_filled_time=entry_filled_time,
                entry_avg_fill_price=placement.avg_fill_price,
                commissions_total=placement.total_commission,
            )

            self._append_event(
                trade_id=trade_id,
                event_type="ENTRY_FILLED",
                snapshot=snapshot,
                message=f"Вход исполнен {decision}",
                payload={
                    "avg_fill_price": placement.avg_fill_price,
                    "fills_count": placement.fills_count,
                    "total_commission": placement.total_commission,
                    "realized_pnl": placement.realized_pnl,
                },
            )

            log_info(
                logger,
                (
                    f"TRADE ENTRY | "
                    f"trade_id={trade_id} | "
                    f"decision={decision} | "
                    f"hour_ct={snapshot.hour_start_ct} | "
                    f"bar_index={snapshot.current_bar_index} | "
                    f"qty={self.quantity} | "
                    f"avg_fill_price={placement.avg_fill_price}"
                ),
                to_telegram=True,
            )

            try:
                await self.telegram_notifier.send_entry_message(
                    snapshot=snapshot,
                    pearson_live_runtime=pearson_live_runtime,
                    trade_id=trade_id,
                    local_symbol=local_symbol,
                    side=decision,
                    quantity=self.quantity,
                    placement=placement,
                )
            except Exception as notify_exc:
                log_warning(
                    logger,
                    f"Ошибка отправки telegram-сообщения об открытии сделки: {notify_exc}",
                    to_telegram=False,
                )

        except Exception as exc:
            if trade_id is not None:
                mark_trade_error(
                    self.trade_db_path,
                    trade_id=trade_id,
                    status="ERROR",
                    error_text=str(exc),
                )

                self._append_event(
                    trade_id=trade_id,
                    event_type="ENTRY_ERROR",
                    snapshot=snapshot,
                    message="Не удалось открыть позицию",
                    payload={"error": str(exc)},
                )

                clear_trade_runtime_state(self.trade_db_path, self.instrument_code)

            self.state.current_trade_id = None
            self.state.position_side = None
            self.state.position_qty = 0
            self.state.entry_hour_start_ts = None

            log_warning(
                logger,
                (
                    f"Не удалось открыть позицию. "
                    f"decision={decision}, hour_ct={snapshot.hour_start_ct}, "
                    f"bar_index={snapshot.current_bar_index}, error={exc}"
                ),
                to_telegram=True,
            )

    async def _maybe_exit_position(self, *, snapshot, active_futures):
        if self.state.position_side is None:
            return

        if self.state.entry_hour_start_ts is None:
            return

        need_exit = False

        if snapshot.hour_start_ts == self.state.entry_hour_start_ts:
            if snapshot.current_bar_index is not None and snapshot.current_bar_index >= self.exit_bar_index:
                need_exit = True

        if snapshot.hour_start_ts != self.state.entry_hour_start_ts:
            need_exit = True

        if not need_exit:
            return

        local_symbol = self._get_active_local_symbol(active_futures)
        exit_side = "SELL" if self.state.position_side == "LONG" else "BUY"
        order_ref = (
            f"{self.order_ref_prefix}_"
            f"{self.instrument_code}_"
            f"{self.state.entry_hour_start_ts}_"
            f"EXIT_{self.state.position_side}"
        )

        trade_id = self.state.current_trade_id

        if trade_id is None:
            open_trade = get_open_trade_for_instrument(self.trade_db_path, self.instrument_code)
            if open_trade is not None:
                trade_id = open_trade["trade_id"]
                self.state.current_trade_id = trade_id

        try:
            contract = await self.order_service.future(local_symbol=local_symbol)

            if exit_side == "SELL":
                placement = await self.order_service.sell_market(
                    contract=contract,
                    quantity=self.state.position_qty,
                    order_ref=order_ref,
                    time_in_force=self.time_in_force,
                    wait="done",
                    accept_timeout=self.accept_timeout,
                    done_timeout=self.done_timeout,
                )
            else:
                placement = await self.order_service.buy_market(
                    contract=contract,
                    quantity=self.state.position_qty,
                    order_ref=order_ref,
                    time_in_force=self.time_in_force,
                    wait="done",
                    accept_timeout=self.accept_timeout,
                    done_timeout=self.done_timeout,
                )

            if trade_id is not None:
                exit_order_id = placement.receipt.order_id
                exit_perm_id = self._extract_perm_id(placement)
                exit_submitted_ts = int(placement.receipt.placed_at_utc.timestamp())
                exit_submitted_time = self._format_utc_ts(exit_submitted_ts)

                mark_exit_submitted(
                    self.trade_db_path,
                    trade_id=trade_id,
                    status="EXIT_SUBMITTED",
                    exit_submitted_ts=exit_submitted_ts,
                    exit_submitted_time=exit_submitted_time,
                    exit_order_id=exit_order_id,
                    exit_perm_id=exit_perm_id,
                )

                self._append_event(
                    trade_id=trade_id,
                    event_type="EXIT_SUBMITTED",
                    snapshot=snapshot,
                    message=f"Отправлен ордер на выход {exit_side}",
                    payload={
                        "order_ref": order_ref,
                        "exit_order_id": exit_order_id,
                        "exit_perm_id": exit_perm_id,
                    },
                )

                trade_row = get_trade_by_id(self.trade_db_path, trade_id)
                previous_commissions = 0.0
                if trade_row is not None and trade_row["commissions_total"] is not None:
                    previous_commissions = float(trade_row["commissions_total"])

                total_commissions = previous_commissions + float(placement.total_commission or 0.0)

                exit_filled_ts = self._extract_done_ts(placement)
                exit_filled_time = self._format_utc_ts(exit_filled_ts)

                mark_exit_filled_and_close(
                    self.trade_db_path,
                    trade_id=trade_id,
                    exit_filled_ts=exit_filled_ts,
                    exit_filled_time=exit_filled_time,
                    exit_avg_fill_price=placement.avg_fill_price,
                    commissions_total=total_commissions,
                    realized_pnl=placement.realized_pnl,
                )

                self._append_event(
                    trade_id=trade_id,
                    event_type="EXIT_FILLED",
                    snapshot=snapshot,
                    message=f"Выход исполнен {exit_side}",
                    payload={
                        "avg_fill_price": placement.avg_fill_price,
                        "fills_count": placement.fills_count,
                        "total_commission": placement.total_commission,
                        "realized_pnl": placement.realized_pnl,
                    },
                )

            log_info(
                logger,
                (
                    f"TRADE EXIT | "
                    f"trade_id={trade_id} | "
                    f"entry_side={self.state.position_side} | "
                    f"exit_side={exit_side} | "
                    f"hour_ct={snapshot.hour_start_ct} | "
                    f"bar_index={snapshot.current_bar_index} | "
                    f"qty={self.state.position_qty} | "
                    f"avg_fill_price={placement.avg_fill_price}"
                ),
                to_telegram=True,
            )

            try:
                await self.telegram_notifier.send_exit_message(
                    snapshot=snapshot,
                    trade_id=trade_id,
                    entry_side=self.state.position_side,
                    exit_side=exit_side,
                    quantity=self.state.position_qty,
                    placement=placement,
                )
            except Exception as notify_exc:
                log_warning(
                    logger,
                    f"Ошибка отправки telegram-сообщения о закрытии сделки: {notify_exc}",
                    to_telegram=False,
                )

            clear_trade_runtime_state(self.trade_db_path, self.instrument_code)

            self.state = DecisionOrderState(
                last_entry_attempt_hour_start_ts=snapshot.hour_start_ts
            )

        except Exception as exc:
            if trade_id is not None:
                mark_trade_error(
                    self.trade_db_path,
                    trade_id=trade_id,
                    status="EXIT_ERROR",
                    error_text=str(exc),
                )

                self._append_event(
                    trade_id=trade_id,
                    event_type="EXIT_ERROR",
                    snapshot=snapshot,
                    message="Не удалось закрыть позицию",
                    payload={"error": str(exc)},
                )

            log_warning(
                logger,
                (
                    f"Не удалось закрыть позицию. "
                    f"entry_side={self.state.position_side}, "
                    f"hour_ct={snapshot.hour_start_ct}, "
                    f"bar_index={snapshot.current_bar_index}, error={exc}"
                ),
                to_telegram=True,
            )

    def _create_trade_from_snapshot(self, *, snapshot, decision, contract_local_symbol):
        best_similarity_score = None

        if snapshot.ranked_similarity_candidates:
            best_similarity_score = snapshot.ranked_similarity_candidates[0]["final_score"]

        forecast_direction = self._build_forecast_direction(snapshot.forecast_summary)

        forecast_candidate_count = None
        forecast_positive_ratio = None
        forecast_negative_ratio = None
        forecast_mean_final_move = None
        forecast_median_final_move = None

        if snapshot.forecast_summary is not None:
            forecast_candidate_count = snapshot.forecast_summary["candidate_count"]
            forecast_positive_ratio = snapshot.forecast_summary["positive_ratio"]
            forecast_negative_ratio = snapshot.forecast_summary["negative_ratio"]
            forecast_mean_final_move = snapshot.forecast_summary["mean_final_move"]
            forecast_median_final_move = snapshot.forecast_summary["median_final_move"]

        trade_id = create_trade(
            self.trade_db_path,
            instrument_code=self.instrument_code,
            contract_local_symbol=contract_local_symbol,
            side=decision,
            quantity=self.quantity,
            status="NEW",
            signal_hour_start_ts=snapshot.hour_start_ts,
            signal_hour_start_ts_ct=snapshot.hour_start_ts_ct,
            signal_hour_start_ct=snapshot.hour_start_ct,
            signal_bar_index=snapshot.current_bar_index,
            signal_bar_time_ts=snapshot.last_bar_time_ts,
            signal_bar_time_ts_ct=None,
            signal_bar_time_ct=self._format_utc_ts(snapshot.last_bar_time_ts),
            decision=decision,
            decision_reason=snapshot.decision_result["reason"],
            best_similarity_score=best_similarity_score,
            forecast_direction=forecast_direction,
            forecast_candidate_count=forecast_candidate_count,
            forecast_positive_ratio=forecast_positive_ratio,
            forecast_negative_ratio=forecast_negative_ratio,
            forecast_mean_final_move=forecast_mean_final_move,
            forecast_median_final_move=forecast_median_final_move,
            decision_payload=snapshot.decision_result,
            forecast_summary=snapshot.forecast_summary,
        )

        return trade_id

    def _append_event(self, *, trade_id, event_type, snapshot, message=None, payload=None):
        event_ts = snapshot.last_bar_time_ts
        event_time = self._format_utc_ts(event_ts)

        append_trade_event(
            self.trade_db_path,
            trade_id=trade_id,
            instrument_code=self.instrument_code,
            event_type=event_type,
            event_time_ts=event_ts,
            event_time=event_time,
            message=message,
            payload=payload,
        )

    def _sync_runtime_state(self, snapshot):
        last_decision = None
        last_decision_reason = None

        if snapshot.decision_result is not None:
            last_decision = snapshot.decision_result["decision"]
            last_decision_reason = snapshot.decision_result["reason"]

        entry_hour_start_ts_ct = None
        entry_hour_start_ct = None

        if self.state.entry_hour_start_ts == snapshot.hour_start_ts:
            entry_hour_start_ts_ct = snapshot.hour_start_ts_ct
            entry_hour_start_ct = snapshot.hour_start_ct

        upsert_trade_runtime_state(
            self.trade_db_path,
            instrument_code=self.instrument_code,
            current_trade_id=self.state.current_trade_id,
            position_side=self.state.position_side,
            position_qty=self.state.position_qty,
            entry_hour_start_ts=self.state.entry_hour_start_ts,
            entry_hour_start_ts_ct=entry_hour_start_ts_ct,
            entry_hour_start_ct=entry_hour_start_ct,
            broker_position_qty=self.state.position_qty,
            broker_avg_cost=None,
            last_decision=last_decision,
            last_decision_reason=last_decision_reason,
            last_snapshot_time_ts=snapshot.last_bar_time_ts,
            last_snapshot_time=self._format_utc_ts(snapshot.last_bar_time_ts),
        )

    def _extract_perm_id(self, placement):
        trade = placement.receipt.trade
        order = getattr(trade, "order", None)

        if order is None:
            return None

        perm_id = getattr(order, "permId", None)

        if perm_id in (None, 0):
            return None

        return int(perm_id)

    def _extract_done_ts(self, placement):
        if placement.done is not None:
            return int(placement.done.checked_at_utc.timestamp())

        return int(placement.receipt.placed_at_utc.timestamp())

    def _build_forecast_direction(self, forecast_summary):
        if forecast_summary is None:
            return None

        mean_final_move = forecast_summary["mean_final_move"]
        median_final_move = forecast_summary["median_final_move"]

        if mean_final_move > 0.0 and median_final_move > 0.0:
            return "UP"

        if mean_final_move < 0.0 and median_final_move < 0.0:
            return "DOWN"

        return "MIXED"

    def _format_utc_ts(self, ts):
        if ts is None:
            return None

        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _get_active_local_symbol(self, active_futures):
        if self.instrument_code not in active_futures:
            raise ValueError(
                f"Для инструмента {self.instrument_code} нет активного localSymbol в active_futures"
            )

        local_symbol = active_futures[self.instrument_code]

        if not local_symbol:
            raise ValueError(
                f"Пустой localSymbol для инструмента {self.instrument_code}"
            )

        return local_symbol

import asyncio
from dataclasses import dataclass
from typing import Optional

from core.logger import get_logger, log_info, log_warning
from trading.order_service import OrderService

logger = get_logger(__name__)


@dataclass(slots=True)
class DecisionOrderState:
    # Состояние простого торгового контура.
    #
    # Первый вариант специально делаем максимально прямым:
    # - не больше одного входа в час;
    # - не больше одной позиции одновременно;
    # - выход принудительно за 10 секунд до конца часа;
    # - если новый час уже начался, а позиция ещё есть, пытаемся закрыть её сразу.
    position_side: Optional[str] = None
    position_qty: int = 0

    entry_hour_start_ts: Optional[int] = None
    last_entry_attempt_hour_start_ts: Optional[int] = None


class DecisionOrderExecutor:
    # Преобразует decision layer в реальные заявки через OrderService.
    #
    # На текущем этапе логика простая:
    # - если decision = LONG -> BUY market;
    # - если decision = SHORT -> SELL market;
    # - выход за 10 секунд до конца часа market-ордером;
    # - одновременно держим не больше одной позиции.
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

        self.enabled = settings.trading_enable_order_execution
        self.quantity = settings.trading_order_quantity
        self.exit_bar_index = settings.trading_exit_bar_index
        self.order_ref_prefix = settings.trading_order_ref_prefix
        self.time_in_force = settings.trading_order_time_in_force
        self.accept_timeout = settings.trading_accept_timeout_seconds
        self.done_timeout = settings.trading_done_timeout_seconds

        self.state = DecisionOrderState()
        self._lock = asyncio.Lock()

    async def on_snapshot(self, *, snapshot, active_futures):
        # Главная точка входа.
        #
        # Вызывается после расчёта очередного snapshot live-логики.
        if snapshot is None:
            return

        if not self.enabled:
            return

        async with self._lock:
            await self._maybe_exit_position(snapshot=snapshot, active_futures=active_futures)
            await self._maybe_enter_position(snapshot=snapshot, active_futures=active_futures)

    async def _maybe_enter_position(self, *, snapshot, active_futures):
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

        try:
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
            self.state.entry_hour_start_ts = snapshot.hour_start_ts

            log_info(
                logger,
                (
                    f"TRADE ENTRY | "
                    f"decision={decision} | "
                    f"hour_ct={snapshot.hour_start_ct} | "
                    f"bar_index={snapshot.current_bar_index} | "
                    f"qty={self.quantity} | "
                    f"avg_fill_price={placement.avg_fill_price}"
                ),
                to_telegram=True,
            )

        except Exception as exc:
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

        # Штатный выход внутри того же часа.
        if snapshot.hour_start_ts == self.state.entry_hour_start_ts:
            if snapshot.current_bar_index is not None and snapshot.current_bar_index >= self.exit_bar_index:
                need_exit = True

        # Аварийный выход, если уже начался новый час, а позиция ещё висит.
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

            log_info(
                logger,
                (
                    f"TRADE EXIT | "
                    f"entry_side={self.state.position_side} | "
                    f"exit_side={exit_side} | "
                    f"hour_ct={snapshot.hour_start_ct} | "
                    f"bar_index={snapshot.current_bar_index} | "
                    f"qty={self.state.position_qty} | "
                    f"avg_fill_price={placement.avg_fill_price}"
                ),
                to_telegram=True,
            )

            self.state = DecisionOrderState(
                last_entry_attempt_hour_start_ts=snapshot.hour_start_ts
            )

        except Exception as exc:
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
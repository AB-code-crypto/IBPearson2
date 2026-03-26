import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping, Optional, Sequence

from ib_async import (
    IB,
    Contract,
    Trade,
    Order,
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder,
)

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


# =========================
# DTOs (тонкий контракт)
# =========================

@dataclass(slots=True)
class PlaceOrderRequest:
    contract: Contract
    order: Order
    order_ref: str


@dataclass(slots=True)
class PlaceOrderReceipt:
    order_id: int
    order_ref: str
    placed_at_utc: datetime
    trade: Trade


@dataclass(slots=True)
class CancelOrderReceipt:
    order_id: int
    cancel_requested_at_utc: datetime


@dataclass(slots=True)
class BracketOrders:
    parent: Order
    take_profit: Optional[Order]
    stop_loss: Optional[Order]


# =========================
# IB Order API (адаптер)
# =========================

class IBOrderApi:
    """
    Тонкий API-адаптер для IB (ib_async).

    Принципы:
      - Только сборка ордеров + placeOrder/cancelOrder.
      - Никаких ожиданий статусов, ретраев, бизнес-правил, рисков и т.д.
      - Разрешённая "логика" здесь — это техническая сериализация параметров
        (например, преобразование ttl_seconds -> goodTillDate для GTD).

    Этот слой должен быть максимально стабильным и легко тестируемым.
    """

    def __init__(self, ib: IB, logger: Optional[logging.Logger] = None) -> None:
        self._ib = ib
        self._log = logger or log

    @property
    def ib(self) -> IB:
        return self._ib

    # -------------------------
    # Low-level primitives
    # -------------------------

    def next_order_id(self) -> int:
        """
        Зарезервировать следующий orderId у клиента IB.

        Используется для bracket/OCA, когда важно заранее выставить parentId
        и transmit-флаги до отправки.
        """
        return int(self._ib.client.getReqId())

    async def place_order(self, contract: Contract, order: Order, *, order_ref: str) -> PlaceOrderReceipt:
        """
        Отправить ордер в IB.

        Возвращает receipt отправки. "Принят/отклонён/исполнен" определяется
        отдельным слоем по событиям orderStatus/errorEvent.

        Важно: этот метод асинхронный, но не ждёт брокера — он лишь отправляет
        запрос и отдаёт управление event loop.
        """
        if not order_ref:
            raise ValueError("order_ref must be a non-empty string")

        order.orderRef = order_ref

        placed_at = datetime.now(timezone.utc)
        trade: Trade = self._ib.placeOrder(contract, order)

        # Отдать управление event loop (placeOrder может быть очень быстрым, но сеть/сокет — не гарантируется)
        await asyncio.sleep(0)

        order_id = int(trade.order.orderId)

        return PlaceOrderReceipt(
            order_id=order_id,
            order_ref=order_ref,
            placed_at_utc=placed_at,
            trade=trade,
        )

    async def cancel_order(self, order_id: int) -> CancelOrderReceipt:
        """
        Запросить отмену ордера по orderId.

        Примечание: используем низкоуровневый клиент, т.к. cancelOrder в IB-клиенте
        адресуется именно по orderId.
        """
        self._ib.client.cancelOrder(int(order_id))
        await asyncio.sleep(0)
        return CancelOrderReceipt(
            order_id=int(order_id),
            cancel_requested_at_utc=datetime.now(timezone.utc),
        )

    async def cancel_orders(self, order_ids: Sequence[int]) -> list[CancelOrderReceipt]:
        receipts: list[CancelOrderReceipt] = []
        for oid in order_ids:
            receipts.append(await self.cancel_order(int(oid)))
        return receipts

    # -------------------------
    # Builders: TIF / GTD / kwargs
    # -------------------------

    def _apply_order_kwargs(self, order: Order, order_kwargs: Optional[Mapping[str, Any]]) -> None:
        if not order_kwargs:
            return

        for key, value in order_kwargs.items():
            # Неизвестные поля должны "взрываться" сразу (никаких тихих опечаток).
            if not hasattr(order, key):
                raise AttributeError(f"IB Order has no attribute {key!r}")
            setattr(order, key, value)

    def _format_good_till_date_utc(self, dt: datetime) -> str:
        """
        Формат goodTillDate согласно IB: `YYYYMMDD-HH:MM:SS` (UTC).
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y%m%d-%H:%M:%S")

    def _apply_time_in_force(
            self,
            order: Order,
            *,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
    ) -> None:
        """
        Техническая установка TIF/GTD.

        - Если задан ttl_seconds или good_till -> tif=GTD + goodTillDate.
        - Иначе, если задан time_in_force -> order.tif = time_in_force.
        """
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")

        if ttl_seconds is not None and good_till is not None:
            raise ValueError("Use either ttl_seconds or good_till, not both")

        if ttl_seconds is not None:
            good_till = datetime.now(timezone.utc) + timedelta(seconds=int(ttl_seconds))

        if good_till is not None:
            order.tif = "GTD"
            order.goodTillDate = self._format_good_till_date_utc(good_till)
            return

        if time_in_force is not None:
            order.tif = str(time_in_force)

    # -------------------------
    # Builders: core order types
    # -------------------------

    def build_market(
            self,
            *,
            action: Side,
            quantity: int,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = MarketOrder(action, int(quantity))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = LimitOrder(action, int(quantity), float(limit_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopOrder(action, int(quantity), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop_limit(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopLimitOrder(action, int(quantity), float(limit_price), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_trailing_stop(
            self,
            *,
            action: Side,
            quantity: int,
            trailing_percent: Optional[float] = None,
            trailing_amount: Optional[float] = None,
            trail_stop_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        """
        Trailing Stop (orderType="TRAIL").

        В используемой версии ib_async нет отдельного TrailingStopOrder, поэтому собираем через базовый Order.
        Тонкие параметры (как именно биржа/маршрут интерпретирует auxPrice/trailStopPrice)
        можно уточнять/переопределять через order_kwargs.
        """
        if trailing_percent is not None and trailing_amount is not None:
            raise ValueError("Use either trailing_percent or trailing_amount, not both")

        o = Order()
        o.action = action
        o.totalQuantity = int(quantity)
        o.orderType = "TRAIL"

        if trailing_percent is not None:
            o.trailingPercent = float(trailing_percent)

        if trailing_amount is not None:
            # Для TRAIL это часто auxPrice (trail amount).
            o.auxPrice = float(trailing_amount)

        if trail_stop_price is not None:
            o.trailStopPrice = float(trail_stop_price)

        self._apply_time_in_force(o, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(o, order_kwargs)
        return o

    # -------------------------
    # Builders: OCA / Bracket
    # -------------------------

    def apply_oca_group(
            self,
            orders: Sequence[Order],
            *,
            oca_group: str,
            oca_type: int = 1,
    ) -> None:
        """
        Присвоить группе ордеров OCA-параметры (one-cancels-all).

        ocaType:
          1 = CANCEL_WITH_BLOCK (классическое "один исполнился — остальные снять")
        """
        if not oca_group:
            raise ValueError("oca_group must be a non-empty string")

        for o in orders:
            o.ocaGroup = oca_group
            o.ocaType = int(oca_type)

    def build_bracket_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            take_profit_price: Optional[float] = None,
            stop_loss_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
            take_profit_order_kwargs: Optional[Mapping[str, Any]] = None,
            stop_loss_order_kwargs: Optional[Mapping[str, Any]] = None,
            oca_group: Optional[str] = None,
            oca_type: int = 1,
    ) -> BracketOrders:
        """
        Bracket на базе LIMIT (parent) + TP (limit) + SL (stop).
        """
        parent = self.build_limit(
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            time_in_force=time_in_force,
            ttl_seconds=ttl_seconds,
            good_till=good_till,
            order_kwargs=order_kwargs,
        )

        parent.transmit = False

        tp: Optional[Order] = None
        sl: Optional[Order] = None

        exit_action: Side = "SELL" if action == "BUY" else "BUY"
        children: list[Order] = []

        if take_profit_price is not None:
            tp = self.build_limit(
                action=exit_action,
                quantity=quantity,
                limit_price=float(take_profit_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=take_profit_order_kwargs,
            )
            tp.transmit = False
            children.append(tp)

        if stop_loss_price is not None:
            sl = self.build_stop(
                action=exit_action,
                quantity=quantity,
                stop_price=float(stop_loss_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=stop_loss_order_kwargs,
            )
            sl.transmit = False
            children.append(sl)

        if len(children) >= 2:
            if oca_group is None:
                oca_group = f"OCA_BRACKET_{self.next_order_id()}"
            self.apply_oca_group(children, oca_group=oca_group, oca_type=oca_type)

        if children:
            children[-1].transmit = True
        else:
            parent.transmit = True

        return BracketOrders(parent=parent, take_profit=tp, stop_loss=sl)

    # -------------------------
    # Convenience: placement helpers (STILL NO waiting)
    # -------------------------

    async def place_simple(
            self,
            *,
            contract: Contract,
            order: Order,
            order_ref: str,
    ) -> tuple[PlaceOrderRequest, PlaceOrderReceipt]:
        req = PlaceOrderRequest(contract=contract, order=order, order_ref=order_ref)
        rec = await self.place_order(contract, order, order_ref=order_ref)
        return req, rec

    def assign_bracket_ids(self, bracket: BracketOrders) -> None:
        parent_id = self.next_order_id()
        bracket.parent.orderId = int(parent_id)

        children: list[Order] = []
        if bracket.take_profit is not None:
            children.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            children.append(bracket.stop_loss)

        for child in children:
            child_id = self.next_order_id()
            child.orderId = int(child_id)
            child.parentId = int(parent_id)

    async def place_bracket(
            self,
            *,
            contract: Contract,
            bracket: BracketOrders,
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        self.assign_bracket_ids(bracket)

        orders: list[Order] = [bracket.parent]
        if bracket.take_profit is not None:
            orders.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            orders.append(bracket.stop_loss)

        receipts: list[PlaceOrderReceipt] = []
        for o in orders:
            receipts.append(await self.place_order(contract, o, order_ref=order_ref))
        return receipts

    async def place_oca_group(
            self,
            *,
            contract: Contract,
            orders: Sequence[Order],
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        receipts: list[PlaceOrderReceipt] = []
        for o in orders:
            receipts.append(await self.place_order(contract, o, order_ref=order_ref))
        return receipts

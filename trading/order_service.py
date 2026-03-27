import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from ib_async import CommissionReport, Contract, Fill, Forex, Future, IB, Stock, Trade

from trading.ib_order_api import (
    BracketOrders,
    CancelOrderReceipt,
    IBOrderApi,
    PlaceOrderReceipt,
)
from trading.order_monitor import AcceptanceResult, DoneResult, IBError, OrderMonitor


log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]
WaitMode = Literal["none", "accept", "done"]


class OrderRejectedError(RuntimeError):
    def __init__(self, *, order_id: int, status: str, error: Optional[IBError]) -> None:
        msg = f"Order rejected: order_id={order_id}, status={status}"
        if error is not None:
            msg += f", ib_code={error.code}, ib_msg={error.message}"
        super().__init__(msg)
        self.order_id = order_id
        self.status = status
        self.error = error


class OrderTimeoutError(TimeoutError):
    def __init__(self, *, order_id: int, stage: str, status: str) -> None:
        super().__init__(f"Order timeout at stage={stage}: order_id={order_id}, status={status}")
        self.order_id = order_id
        self.stage = stage
        self.status = status


@dataclass(slots=True)
class TradeFillInfo:
    """
    Детализация отдельного исполнения (fill) — для логов, аналитики и отладки.
    Совместимо по смыслу с TradeFillInfo из trade_engine.py.
    """

    exec_id: str
    time: Optional[datetime]
    price: float
    size: float
    commission: Optional[float]
    realized_pnl: Optional[float]


@dataclass(slots=True)
class OrderPlacement:
    """Нормализованный результат постановки (и, опционально, ожидания) ордера."""

    receipt: PlaceOrderReceipt
    acceptance: Optional[AcceptanceResult] = None
    done: Optional[DoneResult] = None
    fills: list[TradeFillInfo] = field(default_factory=list)
    fills_count: int = 0
    total_commission: float = 0.0
    realized_pnl: float = 0.0
    avg_fill_price: Optional[float] = None


class OrderService:
    """
    Верхний слой:
    - формирует/квалифицирует контракты,
    - строит ордера через IBOrderApi,
    - отправляет,
    - получает фидбек (accept/done) через OrderMonitor,
    - принимает базовые решения: reject/timeout -> исключение (по умолчанию).

    ВАЖНО:
    - Сервис НЕ запускает сетевой цикл IB. Это делает ваш IBConnect.run_forever().
    """

    def __init__(self, ib: IB, *, api: Optional[IBOrderApi] = None, monitor: Optional[OrderMonitor] = None) -> None:
        self._ib = ib
        self._api = api or IBOrderApi(ib)
        self._monitor = monitor or OrderMonitor(ib)

    @property
    def ib(self) -> IB:
        return self._ib

    @property
    def api(self) -> IBOrderApi:
        return self._api

    @property
    def monitor(self) -> OrderMonitor:
        return self._monitor

    # --------
    # Contract factory / resolver
    # --------
    async def qualify(self, contract: Contract) -> Contract:
        """Гарантируем, что у контракта есть conId и он корректно разрешён в IB."""
        if getattr(contract, "conId", 0):
            return contract
        res = await self._ib.qualifyContractsAsync(contract)
        if not res:
            raise RuntimeError(f"qualifyContractsAsync returned empty for contract={contract!r}")
        return res[0]

    async def future(self, *, local_symbol: str, exchange: str = "CME", currency: str = "USD") -> Contract:
        return await self.qualify(Future(localSymbol=local_symbol, exchange=exchange, currency=currency))

    async def stock(self, *, symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        return await self.qualify(Stock(symbol=symbol, exchange=exchange, currency=currency))

    async def forex(self, *, pair: str) -> Contract:
        return await self.qualify(Forex(pair))

    # --------
    # Core placement
    # --------
    async def place(
        self,
        *,
        contract: Contract,
        order,
        order_ref: str,
        wait: WaitMode = "accept",
        accept_timeout: float = 5.0,
        done_timeout: float = 60.0,
        poll_interval: float = 0.10,
    ) -> OrderPlacement:
        """
        Универсальная постановка "любого" ордера (order может быть Order или наследник).

        wait:
        - none: только отправка (receipt)
        - accept: ждём подтверждение постановки
        - done: ждём завершение
        """
        contract_q = await self.qualify(contract)
        receipt = await self._api.place_order(contract_q, order, order_ref=order_ref)
        placement = OrderPlacement(receipt=receipt)

        if wait == "none":
            return placement

        placement.acceptance = await self._wait_for_accept(
            receipt.trade,
            timeout=accept_timeout,
            poll_interval=poll_interval,
        )
        self._raise_for_unaccepted(placement.acceptance)

        if wait == "done":
            placement.done = await self._wait_for_done(
                receipt.trade,
                timeout=done_timeout,
                poll_interval=poll_interval,
            )
            self._raise_for_undone(placement.done)
            self._hydrate_fill_statistics(placement, receipt.trade.fills)

        return placement

    # --------
    # Convenience wrappers for common orders
    # --------
    async def buy_market(
        self,
        *,
        contract: Contract,
        quantity: int,
        order_ref: str,
        time_in_force: str = "DAY",
        wait: WaitMode = "done",
        accept_timeout: float = 5.0,
        done_timeout: float = 60.0,
    ) -> OrderPlacement:
        order = self._build_market_order(action="BUY", quantity=quantity, time_in_force=time_in_force)
        return await self.place(
            contract=contract,
            order=order,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def sell_market(
        self,
        *,
        contract: Contract,
        quantity: int,
        order_ref: str,
        time_in_force: str = "DAY",
        wait: WaitMode = "done",
        accept_timeout: float = 5.0,
        done_timeout: float = 60.0,
    ) -> OrderPlacement:
        order = self._build_market_order(action="SELL", quantity=quantity, time_in_force=time_in_force)
        return await self.place(
            contract=contract,
            order=order,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def buy_limit(
        self,
        *,
        contract: Contract,
        quantity: int,
        limit_price: float,
        order_ref: str,
        ttl_seconds: Optional[int] = None,
        time_in_force: str = "DAY",
        wait: WaitMode = "accept",
    ) -> OrderPlacement:
        order = self._api.build_limit(
            action="BUY",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=order, order_ref=order_ref, wait=wait)

    async def sell_limit(
        self,
        *,
        contract: Contract,
        quantity: int,
        limit_price: float,
        order_ref: str,
        ttl_seconds: Optional[int] = None,
        time_in_force: str = "DAY",
        wait: WaitMode = "accept",
    ) -> OrderPlacement:
        order = self._api.build_limit(
            action="SELL",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=order, order_ref=order_ref, wait=wait)

    async def place_bracket_limit(
        self,
        *,
        contract: Contract,
        action: Side,
        quantity: int,
        limit_price: float,
        take_profit_price: Optional[float],
        stop_loss_price: Optional[float],
        order_ref: str,
        ttl_seconds: Optional[int] = None,
        time_in_force: str = "DAY",
        accept_timeout: float = 5.0,
        atomic: bool = True,
    ) -> list[OrderPlacement]:
        """
        Сценарий: parent LMT + (TP LMT) + (SL STP), TP/SL в OCA.
        atomic=True: если какой-то ордер не принят, отменяем остальные из этой связки.
        """
        bracket: BracketOrders = self._api.build_bracket_limit(
            action=action,
            quantity=int(quantity),
            limit_price=float(limit_price),
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        self._api.assign_bracket_ids(bracket)
        receipts = await self._api.place_bracket(
            contract=await self.qualify(contract),
            bracket=bracket,
            order_ref=order_ref,
        )
        return await self._collect_atomic_acceptance_results(
            receipts,
            accept_timeout=accept_timeout,
            atomic=atomic,
        )

    async def place_oca_orders(
        self,
        *,
        contract: Contract,
        orders: list,
        oca_group: str,
        oca_type: int = 1,
        order_ref: str,
        accept_timeout: float = 5.0,
        atomic: bool = True,
    ) -> list[OrderPlacement]:
        """
        Поставить набор ордеров в OCA-группу (классический механизм IB для OCO).
        atomic=True: если какой-то ордер не принят, отменяем остальные.
        """
        self._api.apply_oca_group(orders, oca_group=oca_group, oca_type=int(oca_type))
        receipts = await self._api.place_oca_group(
            contract=await self.qualify(contract),
            orders=orders,
            order_ref=order_ref,
        )
        return await self._collect_atomic_acceptance_results(
            receipts,
            accept_timeout=accept_timeout,
            atomic=atomic,
        )

    async def place_oco_orders(
        self,
        *,
        contract: Contract,
        orders: list,
        oco_group: str,
        order_ref: str,
        oca_type: int = 1,
        accept_timeout: float = 5.0,
        atomic: bool = True,
    ) -> list[OrderPlacement]:
        """Алиас к place_oca_orders (в терминологии IB это OCA, но по смыслу — OCO)."""
        return await self.place_oca_orders(
            contract=contract,
            orders=orders,
            oca_group=oco_group,
            oca_type=oca_type,
            order_ref=order_ref,
            accept_timeout=accept_timeout,
            atomic=atomic,
        )

    # --------
    # Cancel helpers (ручное управление открытыми ордерами)
    # --------
    async def cancel_order_id(self, order_id: int) -> CancelOrderReceipt:
        """Отправить запрос отмены ордера по orderId (без ожидания статуса)."""
        return await self._api.cancel_order(int(order_id))

    async def cancel_order_ids(self, order_ids: list[int]) -> list[CancelOrderReceipt]:
        """Отправить запросы отмены для набора orderId (без ожидания статуса)."""
        return await self._api.cancel_orders([int(x) for x in order_ids])

    @staticmethod
    def _is_limitish_order(order) -> bool:
        """
        Практическое определение "лимитного" ордера: всё, что несёт limit-компонент,
        либо имеет поле lmtPrice.
        """
        order_type = str(getattr(order, "orderType", "") or "").upper()
        if order_type in {"LMT", "STP LMT", "LIT", "LOC", "LOO", "TRAIL LIMIT"}:
            return True
        return getattr(order, "lmtPrice", None) is not None

    def open_order_ids(self, *, only_limitish: bool = False, order_ref: Optional[str] = None) -> list[int]:
        """
        Вернуть список orderId по текущим openTrades().
        only_limitish=True: вернёт только "лимитные".
        order_ref: если задан, фильтруем по order.orderRef.
        """
        ids: list[int] = []
        for trade in self._iter_open_trades():
            order = trade.order
            order_id = int(getattr(order, "orderId", 0) or 0)
            if not order_id:
                continue
            if order_ref is not None and str(getattr(order, "orderRef", "") or "") != str(order_ref):
                continue
            if only_limitish and not self._is_limitish_order(order):
                continue
            ids.append(order_id)
        return sorted(set(ids))

    async def cancel_open_limit_orders(self, *, order_ref: Optional[str] = None) -> list[CancelOrderReceipt]:
        """
        Отменить все текущие "лимитные" ордера (по openTrades()).
        По умолчанию отменяет все лимитные ордера, видимые для этого IB clientId.
        """
        ids = self.open_order_ids(only_limitish=True, order_ref=order_ref)
        if not ids:
            return []
        return await self.cancel_order_ids(ids)

    async def cancel_all_open_orders(self, *, order_ref: Optional[str] = None) -> list[CancelOrderReceipt]:
        """Отменить все открытые ордера (по openTrades())."""
        ids = self.open_order_ids(only_limitish=False, order_ref=order_ref)
        if not ids:
            return []
        return await self.cancel_order_ids(ids)

    async def global_cancel(self) -> None:
        """Отправить IB reqGlobalCancel() (глобальная отмена всех открытых ордеров в аккаунте)."""
        self._ib.reqGlobalCancel()
        await asyncio.sleep(0)

    # --------
    # Private helpers
    # --------
    def _build_market_order(self, *, action: Side, quantity: int, time_in_force: str):
        order = self._api.build_market(action=action, quantity=int(quantity))
        order.tif = time_in_force
        order.goodAfterTime = ""
        order.goodTillDate = ""
        return order

    async def _wait_for_accept(self, trade: Trade, *, timeout: float, poll_interval: float) -> AcceptanceResult:
        return await self._monitor.wait_for_accept(
            trade,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    async def _wait_for_done(self, trade: Trade, *, timeout: float, poll_interval: float) -> DoneResult:
        return await self._monitor.wait_for_done(
            trade,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    @staticmethod
    def _raise_for_unaccepted(acceptance: AcceptanceResult) -> None:
        if acceptance.accepted:
            return
        if acceptance.timed_out:
            raise OrderTimeoutError(order_id=acceptance.order_id, stage="accept", status=acceptance.status)
        raise OrderRejectedError(
            order_id=acceptance.order_id,
            status=acceptance.status,
            error=acceptance.error,
        )

    @staticmethod
    def _raise_for_undone(done: DoneResult) -> None:
        if done.done:
            return
        raise OrderTimeoutError(order_id=done.order_id, stage="done", status=done.status)

    def _hydrate_fill_statistics(self, placement: OrderPlacement, fills: list[Fill]) -> None:
        placement.fills = self._collect_fill_infos(list(fills))
        placement.fills_count = len(fills)
        placement.total_commission, placement.realized_pnl = self._aggregate_commission_and_pnl(list(fills))
        placement.avg_fill_price = self._avg_fill_price(list(fills))

    async def _collect_atomic_acceptance_results(
        self,
        receipts: list[PlaceOrderReceipt],
        *,
        accept_timeout: float,
        atomic: bool,
    ) -> list[OrderPlacement]:
        results: list[OrderPlacement] = []
        for receipt in receipts:
            acceptance = await self._monitor.wait_for_accept(receipt.trade, timeout=accept_timeout)
            results.append(OrderPlacement(receipt=receipt, acceptance=acceptance))

        if atomic:
            self._raise_for_atomic_failures(results)
        return results

    async def _cancel_partial_group(self, results: list[OrderPlacement]) -> None:
        for placement in results:
            await self._api.cancel_order(placement.receipt.order_id)

    async def _raise_for_atomic_failures(self, results: list[OrderPlacement]) -> None:
        bad = [placement for placement in results if not (placement.acceptance and placement.acceptance.accepted)]
        if not bad:
            return
        await self._cancel_partial_group(results)
        first_bad = bad[0]
        acceptance = first_bad.acceptance
        if acceptance and acceptance.timed_out:
            raise OrderTimeoutError(order_id=acceptance.order_id, stage="accept", status=acceptance.status)
        raise OrderRejectedError(
            order_id=first_bad.receipt.order_id,
            status=acceptance.status if acceptance else "",
            error=acceptance.error if acceptance else None,
        )

    def _iter_open_trades(self):
        return list(self._ib.openTrades())

    # --------
    # Helpers: fills aggregation
    # --------
    @staticmethod
    def _collect_fill_infos(fills: list[Fill]) -> list[TradeFillInfo]:
        """
        Преобразовать ib_async Fill -> TradeFillInfo (детализация исполнений).

        Важно:
        - CommissionReport может приходить не сразу, поэтому поля commission/realized_pnl
          могут быть None.
        """
        result: list[TradeFillInfo] = []
        for fill in fills:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            commission_report: Optional[CommissionReport] = getattr(fill, "commissionReport", None)
            result.append(
                TradeFillInfo(
                    exec_id=str(getattr(execution, "execId", "")),
                    time=getattr(execution, "time", None),
                    price=float(getattr(execution, "price", 0.0) or 0.0),
                    size=float(getattr(execution, "shares", 0.0) or 0.0),
                    commission=(
                        float(commission_report.commission)
                        if commission_report is not None and commission_report.commission is not None
                        else None
                    ),
                    realized_pnl=(
                        float(commission_report.realizedPNL)
                        if commission_report is not None and commission_report.realizedPNL is not None
                        else None
                    ),
                )
            )
        return result

    @staticmethod
    def _aggregate_commission_and_pnl(fills: list[Fill]) -> tuple[float, float]:
        total_commission = 0.0
        realized_pnl = 0.0
        for fill in fills:
            commission_report: Optional[CommissionReport] = getattr(fill, "commissionReport", None)
            if commission_report is None:
                continue
            if commission_report.commission:
                total_commission += float(commission_report.commission)
            if commission_report.realizedPNL:
                realized_pnl += float(commission_report.realizedPNL)
        return total_commission, realized_pnl

    @staticmethod
    def _avg_fill_price(fills: list[Fill]) -> Optional[float]:
        if not fills:
            return None
        total_qty = 0.0
        total_notional = 0.0
        for fill in fills:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            shares = float(getattr(execution, "shares", 0.0) or 0.0)
            price = float(getattr(execution, "price", 0.0) or 0.0)
            total_qty += shares
            total_notional += shares * price
        if total_qty <= 0:
            return None
        return total_notional / total_qty

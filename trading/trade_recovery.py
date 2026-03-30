import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from core.logger import get_logger, log_info, log_warning
from trading.trade_store import (
    append_trade_event,
    clear_trade_runtime_state,
    create_trade,
    get_open_trade_for_instrument,
    mark_trade_error,
    upsert_trade_runtime_state,
)


logger = get_logger(__name__)


def _utc_now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def _format_utc_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _extract_broker_position_for_local_symbol(ib, local_symbol: str) -> dict[str, Any]:
    position_qty = 0
    avg_cost = None
    for item in ib.positions():
        contract = getattr(item, "contract", None)
        item_local_symbol = getattr(contract, "localSymbol", None)
        if item_local_symbol != local_symbol:
            continue
        qty = int(item.position)
        position_qty += qty
        avg_cost = float(item.avgCost)
    return {
        "position_qty": position_qty,
        "avg_cost": avg_cost,
    }


def _extract_broker_open_orders_for_local_symbol(ib, local_symbol: str) -> list[dict[str, Any]]:
    result = []
    for trade in ib.openTrades():
        contract = getattr(trade, "contract", None)
        item_local_symbol = getattr(contract, "localSymbol", None)
        if item_local_symbol != local_symbol:
            continue

        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        result.append(
            {
                "trade": trade,
                "order_id": getattr(order, "orderId", None),
                "perm_id": getattr(order, "permId", None),
                "action": getattr(order, "action", None),
                "order_type": getattr(order, "orderType", None),
                "total_quantity": getattr(order, "totalQuantity", None),
                "status": getattr(order_status, "status", None),
            }
        )
    return result


def _position_side_from_qty(position_qty: int) -> Optional[str]:
    if position_qty > 0:
        return "LONG"
    if position_qty < 0:
        return "SHORT"
    return None


def _serialize_open_orders(broker_open_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "order_id": item["order_id"],
            "perm_id": item["perm_id"],
            "action": item["action"],
            "order_type": item["order_type"],
            "total_quantity": item["total_quantity"],
            "status": item["status"],
        }
        for item in broker_open_orders
    ]


def _cancel_open_orders_if_enabled(settings, ib, local_symbol: str, broker_open_orders: list[dict[str, Any]]) -> dict[str, Any]:
    """Отменяем открытые ордера по localSymbol, если включена политика автоснятия."""
    if not broker_open_orders:
        return {
            "cancel_enabled": settings.trade_recovery_cancel_open_orders,
            "cancel_attempted": False,
            "cancelled_order_ids": [],
            "message": "Открытых ордеров нет",
        }

    if not settings.trade_recovery_cancel_open_orders:
        return {
            "cancel_enabled": False,
            "cancel_attempted": False,
            "cancelled_order_ids": [],
            "message": "Автоснятие открытых ордеров выключено",
        }

    cancelled_order_ids: list[int] = []
    for item in broker_open_orders:
        trade = item["trade"]
        order = getattr(trade, "order", None)
        if order is None:
            continue
        ib.cancelOrder(order)
        if item["order_id"] is not None:
            cancelled_order_ids.append(int(item["order_id"]))

    return {
        "cancel_enabled": True,
        "cancel_attempted": True,
        "cancelled_order_ids": cancelled_order_ids,
        "message": f"Отправлены cancelOrder для {len(cancelled_order_ids)} ордеров",
    }


def _load_runtime_state_safe(settings, instrument_code: str):
    runtime_state = None
    try:
        from trading.trade_store import load_trade_runtime_state

        runtime_state = load_trade_runtime_state(settings.trade_db_path, instrument_code)
    except Exception:
        runtime_state = None
    return runtime_state


def _reset_executor(decision_order_executor) -> None:
    if decision_order_executor is not None:
        decision_order_executor.reset_in_memory_state()


def _hydrate_executor(decision_order_executor, *, current_trade_id: int, position_side: str, position_qty: int, entry_hour_start_ts: int) -> None:
    if decision_order_executor is not None:
        decision_order_executor.hydrate_recovered_state(
            current_trade_id=current_trade_id,
            position_side=position_side,
            position_qty=position_qty,
            entry_hour_start_ts=entry_hour_start_ts,
        )


def _clear_runtime_and_executor(settings, instrument_code: str, decision_order_executor) -> None:
    clear_trade_runtime_state(settings.trade_db_path, instrument_code)
    _reset_executor(decision_order_executor)


def _append_recovery_event(
    settings,
    *,
    trade_id,
    instrument_code: str,
    event_type: str,
    event_time_ts: int,
    event_time: str,
    message: str,
    payload: dict[str, Any],
) -> None:
    append_trade_event(
        settings.trade_db_path,
        trade_id=trade_id,
        instrument_code=instrument_code,
        event_type=event_type,
        event_time_ts=event_time_ts,
        event_time=event_time,
        message=message,
        payload=payload,
    )


def _build_recovery_summary(
    *,
    instrument_code: str,
    local_symbol: str,
    runtime_state,
    open_trade,
    broker_position: dict[str, Any],
    broker_open_orders: list[dict[str, Any]],
    cancel_result,
    action: str,
    message: str,
) -> dict[str, Any]:
    return {
        "instrument_code": instrument_code,
        "local_symbol": local_symbol,
        "runtime_state": runtime_state,
        "open_trade": open_trade,
        "broker_position": broker_position,
        "broker_open_orders": broker_open_orders,
        "cancel_result": cancel_result,
        "action": action,
        "message": message,
    }


def _summary_with_context(
    *,
    context: dict[str, Any],
    open_trade,
    cancel_result,
    action: str,
    message: str,
) -> dict[str, Any]:
    return _build_recovery_summary(
        instrument_code=context["instrument_code"],
        local_symbol=context["local_symbol"],
        runtime_state=context["runtime_state"],
        open_trade=open_trade,
        broker_position=context["broker_position"],
        broker_open_orders=context["broker_open_orders"],
        cancel_result=cancel_result,
        action=action,
        message=message,
    )


def _build_context(*, settings, ib, instrument_code: str, active_futures: dict[str, str]) -> dict[str, Any]:
    if instrument_code not in active_futures:
        raise ValueError(f"Нет active future для {instrument_code}")

    local_symbol = active_futures[instrument_code]
    runtime_state = _load_runtime_state_safe(settings, instrument_code)
    open_trade = get_open_trade_for_instrument(settings.trade_db_path, instrument_code)
    broker_position = _extract_broker_position_for_local_symbol(ib, local_symbol)
    broker_open_orders = _extract_broker_open_orders_for_local_symbol(ib, local_symbol)
    now_ts = _utc_now_ts()
    now_text = _format_utc_ts(now_ts)

    return {
        "instrument_code": instrument_code,
        "local_symbol": local_symbol,
        "runtime_state": runtime_state,
        "open_trade": open_trade,
        "broker_position": broker_position,
        "broker_open_orders": broker_open_orders,
        "now_ts": now_ts,
        "now_text": now_text,
        "broker_qty": broker_position["position_qty"],
        "broker_side": _position_side_from_qty(broker_position["position_qty"]),
        "broker_abs_qty": abs(broker_position["position_qty"]),
    }


def _handle_empty_everywhere(*, settings, context: dict[str, Any], decision_order_executor):
    cancel_result = _cancel_open_orders_if_enabled(
        settings=settings,
        ib=context["ib"],
        local_symbol=context["local_symbol"],
        broker_open_orders=context["broker_open_orders"],
    )
    if context["broker_open_orders"]:
        _append_recovery_event(
            settings,
            trade_id=None,
            instrument_code=context["instrument_code"],
            event_type="RECOVERY_BROKER_OPEN_ORDERS_WITHOUT_POSITION",
            event_time_ts=context["now_ts"],
            event_time=context["now_text"],
            message="У брокера найдены открытые ордера без позиции и без локальной сделки",
            payload={
                "local_symbol": context["local_symbol"],
                "open_orders": _serialize_open_orders(context["broker_open_orders"]),
                "cancel_result": cancel_result,
            },
        )

    _clear_runtime_and_executor(settings, context["instrument_code"], decision_order_executor)
    return _summary_with_context(
        context=context,
        open_trade=context["open_trade"],
        cancel_result=cancel_result,
        action="CLEAR_EMPTY",
        message="Локально и у брокера позиции нет",
    )


def _handle_local_trade_without_broker_position(*, settings, context: dict[str, Any], decision_order_executor):
    open_trade = context["open_trade"]
    trade_id = open_trade["trade_id"]
    cancel_result = _cancel_open_orders_if_enabled(
        settings=settings,
        ib=context["ib"],
        local_symbol=context["local_symbol"],
        broker_open_orders=context["broker_open_orders"],
    )

    mark_trade_error(
        settings.trade_db_path,
        trade_id=trade_id,
        status="EXTERNALLY_CLOSED",
        error_text=(
            "Startup/periodic reconcile: локальная сделка завершена вне робота "
            "(у брокера позиции нет)"
        ),
    )
    _append_recovery_event(
        settings,
        trade_id=trade_id,
        instrument_code=context["instrument_code"],
        event_type="RECOVERY_DESYNC_NO_BROKER_POSITION",
        event_time_ts=context["now_ts"],
        event_time=context["now_text"],
        message="Локально сделка была открыта, но у брокера позиция отсутствует",
        payload={
            "local_symbol": context["local_symbol"],
            "broker_open_orders": _serialize_open_orders(context["broker_open_orders"]),
            "cancel_result": cancel_result,
        },
    )

    _clear_runtime_and_executor(settings, context["instrument_code"], decision_order_executor)
    return _summary_with_context(
        context=context,
        open_trade=open_trade,
        cancel_result=cancel_result,
        action="MARK_EXTERNALLY_CLOSED_AND_CLEAR",
        message="Локальная сделка завершена вне робота: у брокера позиции нет",
    )


def _handle_broker_position_without_local_trade(*, settings, context: dict[str, Any], decision_order_executor):
    trade_id = create_trade(
        settings.trade_db_path,
        instrument_code=context["instrument_code"],
        contract_local_symbol=context["local_symbol"],
        side=context["broker_side"],
        quantity=context["broker_abs_qty"],
        status="OPEN",
        signal_hour_start_ts=0,
        signal_hour_start_ts_ct=None,
        signal_hour_start_ct="RECOVERED",
        signal_bar_index=None,
        signal_bar_time_ts=context["now_ts"],
        signal_bar_time_ts_ct=None,
        signal_bar_time_ct=context["now_text"],
        decision="RECOVERED",
        decision_reason="Позиция подхвачена из брокера при reconcile",
        best_similarity_score=None,
        forecast_direction=None,
        forecast_candidate_count=None,
        forecast_positive_ratio=None,
        forecast_negative_ratio=None,
        forecast_mean_final_move=None,
        forecast_median_final_move=None,
        decision_payload={
            "decision": "RECOVERED",
            "reason": "Позиция подхвачена из брокера при reconcile",
        },
        forecast_summary=None,
    )

    _append_recovery_event(
        settings,
        trade_id=trade_id,
        instrument_code=context["instrument_code"],
        event_type="RECOVERED_FROM_BROKER",
        event_time_ts=context["now_ts"],
        event_time=context["now_text"],
        message="Позиция была найдена у брокера и подхвачена в локальную БД",
        payload={
            "local_symbol": context["local_symbol"],
            "broker_position_qty": context["broker_qty"],
            "broker_avg_cost": context["broker_position"]["avg_cost"],
            "broker_open_orders": _serialize_open_orders(context["broker_open_orders"]),
        },
    )

    upsert_trade_runtime_state(
        settings.trade_db_path,
        instrument_code=context["instrument_code"],
        current_trade_id=trade_id,
        position_side=context["broker_side"],
        position_qty=context["broker_abs_qty"],
        entry_hour_start_ts=0,
        entry_hour_start_ts_ct=None,
        entry_hour_start_ct="RECOVERED",
        broker_position_qty=context["broker_qty"],
        broker_avg_cost=context["broker_position"]["avg_cost"],
        last_decision="RECOVERED",
        last_decision_reason="Позиция подхвачена из брокера при reconcile",
        last_snapshot_time_ts=context["now_ts"],
        last_snapshot_time=context["now_text"],
    )

    _hydrate_executor(
        decision_order_executor,
        current_trade_id=trade_id,
        position_side=context["broker_side"],
        position_qty=context["broker_abs_qty"],
        entry_hour_start_ts=0,
    )

    return _summary_with_context(
        context=context,
        open_trade=None,
        cancel_result=None,
        action="CREATE_RECOVERED_TRADE",
        message="Позиция брокера подхвачена в локальную БД",
    )


def _handle_attach_existing_trade(*, settings, context: dict[str, Any], decision_order_executor):
    open_trade = context["open_trade"]
    trade_id = open_trade["trade_id"]
    local_side = open_trade["side"]
    local_qty = int(open_trade["quantity"])
    mismatch = (local_side != context["broker_side"]) or (local_qty != context["broker_abs_qty"])

    if mismatch:
        event_type = "RECOVERY_POSITION_MISMATCH"
        message = "Локальное состояние сделки не совпало с брокерской позицией, берём брокера как источник истины"
        payload = {
            "local_side": local_side,
            "local_qty": local_qty,
            "broker_side": context["broker_side"],
            "broker_qty": context["broker_abs_qty"],
            "broker_open_orders": _serialize_open_orders(context["broker_open_orders"]),
        }
    else:
        event_type = "RECOVERY_ATTACH_OPEN_TRADE"
        message = "Открытая локальная сделка подтверждена брокером и подхвачена"
        payload = {
            "broker_open_orders": _serialize_open_orders(context["broker_open_orders"]),
        }

    _append_recovery_event(
        settings,
        trade_id=trade_id,
        instrument_code=context["instrument_code"],
        event_type=event_type,
        event_time_ts=context["now_ts"],
        event_time=context["now_text"],
        message=message,
        payload=payload,
    )

    entry_hour_start_ts = open_trade["signal_hour_start_ts"]
    upsert_trade_runtime_state(
        settings.trade_db_path,
        instrument_code=context["instrument_code"],
        current_trade_id=trade_id,
        position_side=context["broker_side"],
        position_qty=context["broker_abs_qty"],
        entry_hour_start_ts=entry_hour_start_ts,
        entry_hour_start_ts_ct=open_trade.get("signal_hour_start_ts_ct"),
        entry_hour_start_ct=open_trade.get("signal_hour_start_ct"),
        broker_position_qty=context["broker_qty"],
        broker_avg_cost=context["broker_position"]["avg_cost"],
        last_decision=open_trade.get("decision"),
        last_decision_reason=open_trade.get("decision_reason"),
        last_snapshot_time_ts=context["now_ts"],
        last_snapshot_time=context["now_text"],
    )

    _hydrate_executor(
        decision_order_executor,
        current_trade_id=trade_id,
        position_side=context["broker_side"],
        position_qty=context["broker_abs_qty"],
        entry_hour_start_ts=entry_hour_start_ts,
    )

    return _summary_with_context(
        context=context,
        open_trade=open_trade,
        cancel_result=None,
        action="ATTACH_EXISTING_TRADE",
        message="Локальная открытая сделка синхронизирована с брокером",
    )


def reconcile_trade_state_once(
    *,
    settings,
    ib,
    instrument_code,
    active_futures,
    decision_order_executor=None,
):
    context = _build_context(
        settings=settings,
        ib=ib,
        instrument_code=instrument_code,
        active_futures=active_futures,
    )
    context["ib"] = ib

    if context["open_trade"] is None and context["broker_qty"] == 0:
        return _handle_empty_everywhere(
            settings=settings,
            context=context,
            decision_order_executor=decision_order_executor,
        )

    if context["open_trade"] is not None and context["broker_qty"] == 0:
        return _handle_local_trade_without_broker_position(
            settings=settings,
            context=context,
            decision_order_executor=decision_order_executor,
        )

    if context["open_trade"] is None and context["broker_qty"] != 0:
        return _handle_broker_position_without_local_trade(
            settings=settings,
            context=context,
            decision_order_executor=decision_order_executor,
        )

    if context["open_trade"] is not None and context["broker_qty"] != 0:
        return _handle_attach_existing_trade(
            settings=settings,
            context=context,
            decision_order_executor=decision_order_executor,
        )

    raise RuntimeError("Непредусмотренная ветка reconcile")


def build_recovery_signature(summary: dict[str, Any]) -> tuple:
    cancelled_ids = ()
    if summary["cancel_result"] is not None:
        cancelled_ids = tuple(summary["cancel_result"]["cancelled_order_ids"])

    return (
        summary["action"],
        summary["broker_position"]["position_qty"],
        len(summary["broker_open_orders"]),
        summary["open_trade"]["trade_id"] if summary["open_trade"] else None,
        cancelled_ids,
    )


async def trade_reconcile_task(
    *,
    ib,
    settings,
    active_futures,
    decision_order_executor=None,
    instrument_code="MNQ",
    interval_seconds=30.0,
    initial_signature=None,
):
    log_info(
        logger,
        f"Запуск фоновой синхронизации trade state для {instrument_code}",
        to_telegram=False,
    )
    last_signature = initial_signature

    try:
        while True:
            await asyncio.sleep(interval_seconds)

            try:
                if ib.isConnected():
                    summary = reconcile_trade_state_once(
                        settings=settings,
                        ib=ib,
                        instrument_code=instrument_code,
                        active_futures=active_futures,
                        decision_order_executor=decision_order_executor,
                    )
                    signature = build_recovery_signature(summary)

                    if signature != last_signature:
                        cancel_text = "cancel=NONE"
                        if summary["cancel_result"] is not None:
                            cancel_text = (
                                f"cancel_attempted={summary['cancel_result']['cancel_attempted']} | "
                                f"cancelled={summary['cancel_result']['cancelled_order_ids']}"
                            )

                        log_info(
                            logger,
                            (
                                f"TRADE RECOVERY | "
                                f"instrument={instrument_code} | "
                                f"action={summary['action']} | "
                                f"message={summary['message']} | "
                                f"broker_qty={summary['broker_position']['position_qty']} | "
                                f"open_orders={len(summary['broker_open_orders'])} | "
                                f"{cancel_text}"
                            ),
                            to_telegram=True,
                        )
                        last_signature = signature
            except Exception as exc:
                log_warning(logger, f"Ошибка в trade_reconcile_task: {exc}", to_telegram=True)
    except asyncio.CancelledError:
        log_info(
            logger,
            f"Фоновая синхронизация trade state для {instrument_code} остановлена",
            to_telegram=False,
        )
        raise

import asyncio
from datetime import datetime, timezone

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


def _utc_now_ts():
    return int(datetime.now(tz=timezone.utc).timestamp())


def _format_utc_ts(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _extract_broker_position_for_local_symbol(ib, local_symbol):
    # Читаем текущую позицию брокера по localSymbol.
    #
    # В ib_async positions() относится к current state и синхронизируется
    # с TWS/IBG, а openTrades() даёт открытые заявки. Для корректного
    # восстановления на коннекте в TWS/IBG должна быть включена опция
    # "Download open orders on connection". Support: ib_async docs. 
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


def _extract_broker_open_orders_for_local_symbol(ib, local_symbol):
    # Читаем открытые заявки брокера по localSymbol.
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
                "order_id": getattr(order, "orderId", None),
                "perm_id": getattr(order, "permId", None),
                "action": getattr(order, "action", None),
                "order_type": getattr(order, "orderType", None),
                "total_quantity": getattr(order, "totalQuantity", None),
                "status": getattr(order_status, "status", None),
            }
        )

    return result


def _position_side_from_qty(position_qty):
    if position_qty > 0:
        return "LONG"

    if position_qty < 0:
        return "SHORT"

    return None


def _build_recovery_summary(
        *,
        instrument_code,
        local_symbol,
        runtime_state,
        open_trade,
        broker_position,
        broker_open_orders,
        action,
        message,
):
    return {
        "instrument_code": instrument_code,
        "local_symbol": local_symbol,
        "runtime_state": runtime_state,
        "open_trade": open_trade,
        "broker_position": broker_position,
        "broker_open_orders": broker_open_orders,
        "action": action,
        "message": message,
    }


def reconcile_trade_state_once(
        *,
        settings,
        ib,
        instrument_code,
        active_futures,
        decision_order_executor=None,
):
    # Один проход reconciliation между:
    # - trade_db runtime/open trade;
    # - фактической позицией брокера;
    # - открытыми заявками брокера.
    #
    # Ключевая идея:
    # - если брокер пуст и локально пусто -> чистое состояние;
    # - если локально сделка есть, а у брокера позиции нет -> помечаем рассинхрон;
    # - если у брокера позиция есть, а локально сделки нет -> создаём RECOVERED trade;
    # - если и локально, и у брокера позиция есть -> подхватываем и гидратим executor.
    if instrument_code not in active_futures:
        raise ValueError(f"Нет active future для {instrument_code}")

    local_symbol = active_futures[instrument_code]

    runtime_state = None
    try:
        from trading.trade_store import load_trade_runtime_state
        runtime_state = load_trade_runtime_state(settings.trade_db_path, instrument_code)
    except Exception:
        runtime_state = None

    open_trade = get_open_trade_for_instrument(settings.trade_db_path, instrument_code)
    broker_position = _extract_broker_position_for_local_symbol(ib, local_symbol)
    broker_open_orders = _extract_broker_open_orders_for_local_symbol(ib, local_symbol)

    broker_qty = broker_position["position_qty"]
    broker_side = _position_side_from_qty(broker_qty)
    broker_abs_qty = abs(broker_qty)

    now_ts = _utc_now_ts()
    now_text = _format_utc_ts(now_ts)

    # Случай 1: везде пусто.
    if open_trade is None and broker_qty == 0:
        clear_trade_runtime_state(settings.trade_db_path, instrument_code)

        if decision_order_executor is not None:
            decision_order_executor.reset_in_memory_state()

        return _build_recovery_summary(
            instrument_code=instrument_code,
            local_symbol=local_symbol,
            runtime_state=runtime_state,
            open_trade=open_trade,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
            action="CLEAR_EMPTY",
            message="Локально и у брокера позиции нет",
        )

    # Случай 2: локально есть открытая сделка, а у брокера позиции нет.
    if open_trade is not None and broker_qty == 0:
        trade_id = open_trade["trade_id"]

        mark_trade_error(
            settings.trade_db_path,
            trade_id=trade_id,
            status="ERROR",
            error_text="Startup/periodic reconcile: локально сделка открыта, но у брокера позиции нет",
        )

        append_trade_event(
            settings.trade_db_path,
            trade_id=trade_id,
            instrument_code=instrument_code,
            event_type="RECOVERY_DESYNC_NO_BROKER_POSITION",
            event_time_ts=now_ts,
            event_time=now_text,
            message="Локально сделка была открыта, но у брокера позиция отсутствует",
            payload={
                "local_symbol": local_symbol,
                "broker_open_orders": broker_open_orders,
            },
        )

        clear_trade_runtime_state(settings.trade_db_path, instrument_code)

        if decision_order_executor is not None:
            decision_order_executor.reset_in_memory_state()

        return _build_recovery_summary(
            instrument_code=instrument_code,
            local_symbol=local_symbol,
            runtime_state=runtime_state,
            open_trade=open_trade,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
            action="MARK_ERROR_AND_CLEAR",
            message="Локальная сделка помечена как ERROR: у брокера позиции нет",
        )

    # Случай 3: у брокера позиция есть, а локально сделки нет.
    if open_trade is None and broker_qty != 0:
        # В этом случае создаём recovered-сделку и подхватываем позицию.
        #
        # Поскольку доверенного signal-hour у нас нет, entry_hour_start_ts ставим 0.
        # Тогда executor на первом же snapshot воспримет позицию как "старую"
        # и сможет закрыть её немедленно по своей штатной логике, если нужно.
        trade_id = create_trade(
            settings.trade_db_path,
            instrument_code=instrument_code,
            contract_local_symbol=local_symbol,
            side=broker_side,
            quantity=broker_abs_qty,
            status="OPEN",
            signal_hour_start_ts=0,
            signal_hour_start_ts_ct=None,
            signal_hour_start_ct="RECOVERED",
            signal_bar_index=None,
            signal_bar_time_ts=now_ts,
            signal_bar_time_ts_ct=None,
            signal_bar_time_ct=now_text,
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

        append_trade_event(
            settings.trade_db_path,
            trade_id=trade_id,
            instrument_code=instrument_code,
            event_type="RECOVERED_FROM_BROKER",
            event_time_ts=now_ts,
            event_time=now_text,
            message="Позиция была найдена у брокера и подхвачена в локальную БД",
            payload={
                "local_symbol": local_symbol,
                "broker_position_qty": broker_qty,
                "broker_avg_cost": broker_position["avg_cost"],
                "broker_open_orders": broker_open_orders,
            },
        )

        upsert_trade_runtime_state(
            settings.trade_db_path,
            instrument_code=instrument_code,
            current_trade_id=trade_id,
            position_side=broker_side,
            position_qty=broker_abs_qty,
            entry_hour_start_ts=0,
            entry_hour_start_ts_ct=None,
            entry_hour_start_ct="RECOVERED",
            broker_position_qty=broker_qty,
            broker_avg_cost=broker_position["avg_cost"],
            last_decision="RECOVERED",
            last_decision_reason="Позиция подхвачена из брокера при reconcile",
            last_snapshot_time_ts=now_ts,
            last_snapshot_time=now_text,
        )

        if decision_order_executor is not None:
            decision_order_executor.hydrate_recovered_state(
                current_trade_id=trade_id,
                position_side=broker_side,
                position_qty=broker_abs_qty,
                entry_hour_start_ts=0,
            )

        return _build_recovery_summary(
            instrument_code=instrument_code,
            local_symbol=local_symbol,
            runtime_state=runtime_state,
            open_trade=None,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
            action="CREATE_RECOVERED_TRADE",
            message="Позиция брокера подхвачена в локальную БД",
        )

    # Случай 4: и локально, и у брокера позиция есть.
    if open_trade is not None and broker_qty != 0:
        trade_id = open_trade["trade_id"]
        local_side = open_trade["side"]
        local_qty = int(open_trade["quantity"])

        mismatch = (local_side != broker_side) or (local_qty != broker_abs_qty)

        if mismatch:
            append_trade_event(
                settings.trade_db_path,
                trade_id=trade_id,
                instrument_code=instrument_code,
                event_type="RECOVERY_POSITION_MISMATCH",
                event_time_ts=now_ts,
                event_time=now_text,
                message="Локальное состояние сделки не совпало с брокерской позицией, берём брокера как источник истины",
                payload={
                    "local_side": local_side,
                    "local_qty": local_qty,
                    "broker_side": broker_side,
                    "broker_qty": broker_abs_qty,
                    "broker_open_orders": broker_open_orders,
                },
            )
        else:
            append_trade_event(
                settings.trade_db_path,
                trade_id=trade_id,
                instrument_code=instrument_code,
                event_type="RECOVERY_ATTACH_OPEN_TRADE",
                event_time_ts=now_ts,
                event_time=now_text,
                message="Открытая локальная сделка подтверждена брокером и подхвачена",
                payload={
                    "broker_open_orders": broker_open_orders,
                },
            )

        entry_hour_start_ts = open_trade["signal_hour_start_ts"]

        upsert_trade_runtime_state(
            settings.trade_db_path,
            instrument_code=instrument_code,
            current_trade_id=trade_id,
            position_side=broker_side,
            position_qty=broker_abs_qty,
            entry_hour_start_ts=entry_hour_start_ts,
            entry_hour_start_ts_ct=open_trade.get("signal_hour_start_ts_ct"),
            entry_hour_start_ct=open_trade.get("signal_hour_start_ct"),
            broker_position_qty=broker_qty,
            broker_avg_cost=broker_position["avg_cost"],
            last_decision=open_trade.get("decision"),
            last_decision_reason=open_trade.get("decision_reason"),
            last_snapshot_time_ts=now_ts,
            last_snapshot_time=now_text,
        )

        if decision_order_executor is not None:
            decision_order_executor.hydrate_recovered_state(
                current_trade_id=trade_id,
                position_side=broker_side,
                position_qty=broker_abs_qty,
                entry_hour_start_ts=entry_hour_start_ts,
            )

        return _build_recovery_summary(
            instrument_code=instrument_code,
            local_symbol=local_symbol,
            runtime_state=runtime_state,
            open_trade=open_trade,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
            action="ATTACH_EXISTING_TRADE",
            message="Локальная открытая сделка синхронизирована с брокером",
        )

    raise RuntimeError("Непредусмотренная ветка reconcile")


async def trade_reconcile_task(
        *,
        ib,
        settings,
        active_futures,
        decision_order_executor=None,
        instrument_code="MNQ",
        interval_seconds=30.0,
):
    # Фоновая периодическая синхронизация trade_db <-> broker.
    #
    # Нужна, чтобы:
    # - подхватить состояние после рестарта;
    # - после восстановления соединения снова привести локальное состояние
    #   к реальности у брокера;
    # - не зависеть только от памяти текущего процесса.
    log_info(
        logger,
        f"Запуск фоновой синхронизации trade state для {instrument_code}",
        to_telegram=False,
    )

    last_signature = None

    try:
        while True:
            try:
                if ib.isConnected():
                    summary = reconcile_trade_state_once(
                        settings=settings,
                        ib=ib,
                        instrument_code=instrument_code,
                        active_futures=active_futures,
                        decision_order_executor=decision_order_executor,
                    )

                    signature = (
                        summary["action"],
                        summary["broker_position"]["position_qty"],
                        len(summary["broker_open_orders"]),
                        summary["open_trade"]["trade_id"] if summary["open_trade"] else None,
                    )

                    if signature != last_signature:
                        log_info(
                            logger,
                            (
                                f"TRADE RECOVERY | "
                                f"instrument={instrument_code} | "
                                f"action={summary['action']} | "
                                f"message={summary['message']} | "
                                f"broker_qty={summary['broker_position']['position_qty']} | "
                                f"open_orders={len(summary['broker_open_orders'])}"
                            ),
                            to_telegram=True,
                        )
                        last_signature = signature

            except Exception as exc:
                log_warning(
                    logger,
                    f"Ошибка в trade_reconcile_task: {exc}",
                    to_telegram=True,
                )

            await asyncio.sleep(interval_seconds)

    except asyncio.CancelledError:
        log_info(
            logger,
            f"Фоновая синхронизация trade state для {instrument_code} остановлена",
            to_telegram=False,
        )
        raise

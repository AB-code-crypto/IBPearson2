import asyncio
from typing import Any, Dict, Optional

from config import settings_live as settings
from core.active_futures import build_active_futures
from core.db_initializer import initialize_databases
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    get_ib_server_time_text,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.load_history import load_history_task
from core.load_realtime import load_realtime_task
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.telegram_sender import TelegramSender
from trading.order_service import OrderService
from trading.trade_performance_summary import trade_performance_summary_task
from trading.trade_recovery import build_recovery_signature, reconcile_trade_state_once, trade_reconcile_task
from ts.decision_order_executor import DecisionOrderExecutor
from ts.pearson_live import PearsonLiveRuntime
from ts.prepared_task import prepared_db_sync_task, run_prepared_sync_once

# Настраиваем логирование один раз при старте приложения.
setup_logging()
logger = get_logger(__name__)

# Создаём TelegramSender и подключаем его к логгеру.
telegram_sender = TelegramSender(settings)
setup_telegram_logging(telegram_sender)


def _build_recent_backfill_state() -> Dict[str, Any]:
    """Создаём начальное состояние backfill после старта/reconnect."""
    return {
        "first_bid_ts": None,
        "first_ask_ts": None,
        "last_backfill_completed_sync_ts": None,
        "backfill_task": None,
    }


def _log_connection_details(*, server_time_text: str, active_futures: dict) -> None:
    """Печатаем параметры подключения и активные контракты на старте."""
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}")
    log_info(logger, f"Время сервера IB: {server_time_text}")
    log_info(logger, f"Активные фьючерсы на старте: {active_futures}", to_telegram=False)


async def _bootstrap_strategy_runtime() -> PearsonLiveRuntime:
    """Разовый bootstrap аналитического контура перед запуском realtime."""
    # До запуска фоновых realtime-задач создаём нужные БД и таблицы.
    await initialize_databases(settings)

    # Перед запуском realtime один раз синхронизируем prepared DB.
    # Это убирает гонку, когда live-runtime стартует раньше,
    # чем prepared DB успеет добрать последний закрытый час.
    await run_prepared_sync_once(
        settings=settings,
        instrument_code="MNQ",
        lookback_days=31,
    )

    return PearsonLiveRuntime(
        settings=settings,
        instrument_code="MNQ",
    )


def _build_decision_executor(*, ib) -> DecisionOrderExecutor:
    """Создаём торговый сервис и исполнитель решений."""
    order_service = OrderService(ib)
    return DecisionOrderExecutor(
        settings=settings,
        order_service=order_service,
        instrument_code="MNQ",
    )


def _run_startup_recovery(*, ib, active_futures: dict, decision_order_executor: DecisionOrderExecutor) -> dict:
    """Один раз на старте сверяем локальное состояние с брокером."""
    recovery_summary = reconcile_trade_state_once(
        settings=settings,
        ib=ib,
        instrument_code="MNQ",
        active_futures=active_futures,
        decision_order_executor=decision_order_executor,
    )

    log_info(
        logger,
        (
            "TRADE RECOVERY STARTUP | "
            f"action={recovery_summary['action']} | "
            f"message={recovery_summary['message']} | "
            f"broker_qty={recovery_summary['broker_position']['position_qty']} | "
            f"open_orders={len(recovery_summary['broker_open_orders'])}"
        ),
        to_telegram=True,
    )

    if settings.trading_enable_order_execution:
        log_info(logger, "Торговое исполнение включено", to_telegram=True)
    else:
        log_info(logger, "Торговое исполнение выключено", to_telegram=False)

    return recovery_summary


def _start_background_tasks(
        *,
        ib,
        ib_health,
        active_futures: dict,
        pearson_live_runtime: PearsonLiveRuntime,
        decision_order_executor: DecisionOrderExecutor,
        recent_backfill_state: dict,
        trade_reconcile_initial_signature=None,
) -> dict[str, asyncio.Task]:
    """Запускаем все фоновые задачи приложения и возвращаем их по именам."""
    return {
        "monitor": asyncio.create_task(
            monitor_ib_connection(ib, settings, ib_health),
            name="monitor_ib_connection",
        ),
        "heartbeat": asyncio.create_task(
            heartbeat_ib_connection(ib, ib_health),
            name="heartbeat_ib_connection",
        ),
        "realtime": asyncio.create_task(
            load_realtime_task(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                active_futures=active_futures,
                recent_backfill_state=recent_backfill_state,
                pearson_live_runtime=pearson_live_runtime,
                decision_order_executor=decision_order_executor,
            ),
            name="load_realtime_task",
        ),
        "prepared_sync": asyncio.create_task(
            prepared_db_sync_task(
                settings=settings,
                instrument_code="MNQ",
                lookback_days=31,
                run_immediately=False,
            ),
            name="prepared_db_sync_task",
        ),
        "trade_reconcile": asyncio.create_task(
            trade_reconcile_task(
                ib=ib,
                settings=settings,
                active_futures=active_futures,
                decision_order_executor=decision_order_executor,
                instrument_code="MNQ",
                interval_seconds=30.0,
                initial_signature=trade_reconcile_initial_signature,
            ),
            name="trade_reconcile_task",
        ),
        "trade_summary": asyncio.create_task(
            trade_performance_summary_task(
                settings=settings,
                instrument_code="MNQ",
                poll_interval_seconds=20.0,
            ),
            name="trade_performance_summary_task",
        ),
    }


async def _run_initial_history_load(*, ib, ib_health) -> None:
    """Разовый добор истории до запуска realtime."""
    history_task = asyncio.create_task(
        load_history_task(ib, ib_health, settings),
        name="load_history_task",
    )
    await history_task


async def _cancel_and_await(task: Optional[asyncio.Task]) -> None:
    """Аккуратно останавливаем задачу, игнорируя штатную CancelledError."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _shutdown_background_tasks(tasks: dict[str, asyncio.Task]) -> None:
    """Останавливаем фоновые задачи в контролируемом порядке."""
    shutdown_order = (
        "prepared_sync",
        "trade_reconcile",
        "trade_summary",
        "realtime",
        "heartbeat",
        "monitor",
    )
    for task_name in shutdown_order:
        await _cancel_and_await(tasks.get(task_name))


async def _shutdown_app(*, ib, decision_order_executor: Optional[DecisionOrderExecutor], shutdown_message: str, tasks: dict[str, asyncio.Task]) -> None:
    """Общий shutdown-путь приложения."""
    await _shutdown_background_tasks(tasks)

    if decision_order_executor is not None:
        try:
            await decision_order_executor.close()
        except Exception:
            pass

    try:
        await disconnect_ib(ib)
        log_info(logger, "Соединение с IB закрыто", to_telegram=False)
    except Exception:
        pass

    disable_telegram_logging()
    log_info(logger, shutdown_message)
    await wait_telegram_logging()


async def main():
    shutdown_message = "Робот завершает работу"
    tasks: dict[str, asyncio.Task] = {}
    decision_order_executor: Optional[DecisionOrderExecutor] = None
    recent_backfill_state = _build_recent_backfill_state()
    startup_recovery_signature = None

    ib, ib_health = await connect_ib(settings)

    try:
        server_time_text = await get_ib_server_time_text(ib)

        # Ролловер внутри процесса пока не автоматизируем:
        # при смене квартального контракта робот будет перезапускаться вручную.
        active_futures = build_active_futures(server_time_text)
        _log_connection_details(
            server_time_text=server_time_text,
            active_futures=active_futures,
        )

        await _run_initial_history_load(ib=ib, ib_health=ib_health)
        pearson_live_runtime = await _bootstrap_strategy_runtime()
        decision_order_executor = _build_decision_executor(ib=ib)

        startup_recovery_summary = _run_startup_recovery(
            ib=ib,
            active_futures=active_futures,
            decision_order_executor=decision_order_executor,
        )
        startup_recovery_signature = build_recovery_signature(startup_recovery_summary)

        tasks = _start_background_tasks(
            ib=ib,
            ib_health=ib_health,
            active_futures=active_futures,
            pearson_live_runtime=pearson_live_runtime,
            decision_order_executor=decision_order_executor,
            recent_backfill_state=recent_backfill_state,
            trade_reconcile_initial_signature=startup_recovery_signature,
        )

        await asyncio.gather(
            tasks["realtime"],
            tasks["prepared_sync"],
            tasks["trade_reconcile"],
        )
    except asyncio.CancelledError:
        shutdown_message = "Робот остановлен пользователем"
        raise
    finally:
        await _shutdown_app(
            ib=ib,
            decision_order_executor=decision_order_executor,
            shutdown_message=shutdown_message,
            tasks=tasks,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info(logger, "Робот остановлен пользователем")

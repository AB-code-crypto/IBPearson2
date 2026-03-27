import asyncio

from config import settings_live as settings
from core.active_futures import build_active_futures
from core.load_realtime import load_realtime_task
from core.telegram_sender import TelegramSender
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    heartbeat_ib_connection,
    monitor_ib_connection,
    get_ib_server_time_text,
)
from core.load_history import load_history_task
from core.db_initializer import initialize_databases
from core.logger import (
    setup_logging,
    setup_telegram_logging,
    disable_telegram_logging,
    wait_telegram_logging,
    get_logger,
    log_info,
)
from ts.decision_order_executor import DecisionOrderExecutor
from ts.prepared_task import prepared_db_sync_task, run_prepared_sync_once
from ts.pearson_live import PearsonLiveRuntime
from trading.order_service import OrderService
from trading.trade_recovery import reconcile_trade_state_once, trade_reconcile_task

# Настраиваем логирование один раз при старте приложения.
setup_logging()

logger = get_logger(__name__)

# Создаём TelegramSender и подключаем его к логгеру.
telegram_sender = TelegramSender(settings)
setup_telegram_logging(telegram_sender)


async def main():
    # На старте ждём, пока соединение с IB будет установлено.
    ib, ib_health = await connect_ib(settings)

    # Фоновые задачи.
    monitor_task = None
    heartbeat_task = None
    history_task = None
    realtime_task = None
    prepared_sync_task_handle = None
    trade_reconcile_task_handle = None

    # Runtime стратегии и торгового контура.
    pearson_live_runtime = None
    order_service = None
    decision_order_executor = None

    # Простое состояние разового добора последнего часа после старта realtime
    # и после последующих реконнектов.
    recent_backfill_state = {
        "first_bid_ts": None,
        "first_ask_ts": None,
        "last_backfill_completed_sync_ts": None,
        "backfill_task": None,
    }

    # Текст итогового сообщения при остановке.
    shutdown_message = "Робот завершает работу"

    try:
        # Печатаем параметры текущего подключения.
        log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
        log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
        log_info(logger, f"Client ID: {settings.ib_client_id}")

        # Запрашиваем и печатаем время сервера IB.
        server_time_text = await get_ib_server_time_text(ib)
        log_info(logger, f"Время сервера IB: {server_time_text}")

        # Один раз при старте определяем активные фьючерсы.
        # Ролловер внутри процесса пока не автоматизируем:
        # при смене квартального контракта робот будет перезапускаться вручную.
        active_futures = build_active_futures(server_time_text)
        log_info(logger, f"Активные фьючерсы на старте: {active_futures}", to_telegram=False)

        # До запуска фоновых задач создаём нужные БД и таблицы.
        #
        # Если структура хранилищ ещё не подготовлена, историческому загрузчику и
        # прочим задачам дальше идти рано. Поэтому инициализацию делаем заранее.
        await initialize_databases(settings)

        # Запускаем фоновую задачу мониторинга соединения.
        monitor_task = asyncio.create_task(monitor_ib_connection(ib, settings, ib_health))

        # Запускаем heartbeat-задачу.
        heartbeat_task = asyncio.create_task(heartbeat_ib_connection(ib, ib_health))

        # Запускаем задачу первичной загрузки истории.
        history_task = asyncio.create_task(load_history_task(ib, ib_health, settings))

        # Ждём завершения загрузки истории.
        # Если таска упадёт, main тоже упадёт громко.
        await history_task

        # Перед запуском realtime один раз синхронизируем prepared DB.
        # Это убирает гонку, когда live-runtime стартует раньше,
        # чем prepared DB успеет добрать последний закрытый час.
        await run_prepared_sync_once(
            settings=settings,
            instrument_code="MNQ",
            lookback_days=31,
        )

        # Создаём live-runtime стратегии.
        pearson_live_runtime = PearsonLiveRuntime(
            settings=settings,
            instrument_code="MNQ",
        )

        # Создаём торговый сервис и исполнитель решений.
        order_service = OrderService(ib)
        decision_order_executor = DecisionOrderExecutor(
            settings=settings,
            order_service=order_service,
            instrument_code="MNQ",
        )

        # Один раз на старте приводим локальное торговое состояние
        # к фактическому состоянию у брокера.
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
                f"TRADE RECOVERY STARTUP | "
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

        # Потом переходим на реальные котировки.
        realtime_task = asyncio.create_task(
            load_realtime_task(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                active_futures=active_futures,
                recent_backfill_state=recent_backfill_state,
                pearson_live_runtime=pearson_live_runtime,
                decision_order_executor=decision_order_executor,
            )
        )

        prepared_sync_task_handle = asyncio.create_task(
            prepared_db_sync_task(
                settings=settings,
                instrument_code="MNQ",
                lookback_days=31,
                run_immediately=False,
            )
        )

        trade_reconcile_task_handle = asyncio.create_task(
            trade_reconcile_task(
                ib=ib,
                settings=settings,
                active_futures=active_futures,
                decision_order_executor=decision_order_executor,
                instrument_code="MNQ",
                interval_seconds=30.0,
            )
        )

        await asyncio.gather(
            realtime_task,
            prepared_sync_task_handle,
            trade_reconcile_task_handle,
        )

    except asyncio.CancelledError:
        shutdown_message = "Робот остановлен пользователем"
        raise

    finally:
        if prepared_sync_task_handle is not None:
            prepared_sync_task_handle.cancel()
            try:
                await prepared_sync_task_handle
            except asyncio.CancelledError:
                pass

        if trade_reconcile_task_handle is not None:
            trade_reconcile_task_handle.cancel()
            try:
                await trade_reconcile_task_handle
            except asyncio.CancelledError:
                pass

        if realtime_task is not None:
            realtime_task.cancel()
            try:
                await realtime_task
            except asyncio.CancelledError:
                pass

        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        if monitor_task is not None:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        try:
            await disconnect_ib(ib)
            log_info(logger, "Соединение с IB закрыто", to_telegram=False)
        except Exception:
            pass

        disable_telegram_logging()
        log_info(logger, shutdown_message)
        await wait_telegram_logging()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info(logger, "Робот остановлен пользователем")

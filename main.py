import asyncio

from config import settings_live as settings
from core.telegram_sender import TelegramSender
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    heartbeat_ib_connection,
    monitor_ib_connection,
    get_ib_server_time_text,
)
from core.logger import (
    setup_logging,
    setup_telegram_logging,
    disable_telegram_logging,
    wait_telegram_logging,
    get_logger,
    log_info,
)

# Настраиваем логирование один раз при старте приложения.
setup_logging()

# Логгер этого модуля.
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

        # Запускаем фоновую задачу мониторинга соединения.
        monitor_task = asyncio.create_task(monitor_ib_connection(ib, settings, ib_health))

        # Запускаем heartbeat-задачу.
        heartbeat_task = asyncio.create_task(heartbeat_ib_connection(ib, ib_health))

        # Пока других сервисов у нас ещё нет,
        # просто держим main() живым бесконечно.
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        # Это штатный сценарий при ручной остановке робота.
        shutdown_message = "Робот остановлен пользователем"
        raise

    finally:
        # Останавливаем heartbeat.
        if heartbeat_task is not None:
            heartbeat_task.cancel()

            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        # Останавливаем монитор.
        if monitor_task is not None:
            monitor_task.cancel()

            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        # Закрываем соединение с IB.
        disconnect_ib(ib)

        # Больше не создаём новые telegram-задачи через logger-обёртки.
        disable_telegram_logging()

        # Это сообщение идёт только в консоль.
        logger.info("Соединение с IB закрыто")

        # Дожидаемся уже созданных задач отправки в Telegram.
        await wait_telegram_logging()

        # Явно отправляем финальное сообщение в Telegram.
        # Это не logger-обёртка, а прямой awaited-вызов, поэтому
        # он корректно отработает до закрытия HTTP-сессии.
        await telegram_sender.send_text(f"{shutdown_message}. Соединение с IB закрыто")

        # И только после этого закрываем HTTP-сессию Telegram.
        await telegram_sender.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Робот остановлен пользователем")

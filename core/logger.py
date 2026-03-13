import asyncio
import logging
import sys

# Имя главного логгера проекта.
PROJECT_LOGGER_NAME = "robot"

# Здесь хранится объект TelegramSender.
telegram_sender = None

# Флаг: можно ли сейчас создавать новые задачи отправки в Telegram.
telegram_logging_enabled = True

# Все созданные фоновые задачи отправки в Telegram.
telegram_tasks = set()


def setup_logging():
    # Создаём отдельный логгер проекта.
    logger = logging.getLogger(PROJECT_LOGGER_NAME)

    # Уровень логирования.
    logger.setLevel(logging.INFO)

    # Очищаем старые обработчики, чтобы в PyCharm не было дублей.
    logger.handlers.clear()

    # Очень важно:
    # не передавать сообщения выше в root logger,
    # иначе туда снова начнут примешиваться сторонние логи.
    logger.propagate = False

    # Пишем логи в стандартный вывод.
    handler = logging.StreamHandler(sys.stdout)

    # Формат:
    # дата-время | уровень | модуль.функция:строка | сообщение
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Глушим внутренние логи библиотеки ib_async,
    # чтобы в консоль шли только наши сообщения.
    ib_async_logger = logging.getLogger("ib_async")
    ib_async_logger.handlers.clear()
    ib_async_logger.addHandler(logging.NullHandler())
    ib_async_logger.propagate = False


def setup_telegram_logging(sender):
    # Подключаем TelegramSender к логгеру.
    global telegram_sender
    global telegram_logging_enabled

    telegram_sender = sender
    telegram_logging_enabled = True


def disable_telegram_logging():
    # Запрещаем создавать новые telegram-задачи.
    global telegram_logging_enabled
    telegram_logging_enabled = False


async def wait_telegram_logging():
    # Дожидаемся завершения уже созданных telegram-задач.
    if not telegram_tasks:
        return

    tasks = list(telegram_tasks)
    await asyncio.gather(*tasks, return_exceptions=True)


def get_logger(module_name):
    # Возвращаем дочерний логгер для конкретного модуля проекта.
    return logging.getLogger(f"{PROJECT_LOGGER_NAME}.{module_name}")


def _on_telegram_task_done(task):
    # Удаляем завершённую задачу из набора.
    telegram_tasks.discard(task)


def _send_to_telegram(message, to_telegram=True):
    # Если для конкретного сообщения отправка в Telegram отключена,
    # просто ничего не делаем.
    if not to_telegram:
        return

    # Если TelegramSender не подключён, просто ничего не делаем.
    if telegram_sender is None:
        return

    # Если telegram-логирование уже выключено, новые задачи не создаём.
    if not telegram_logging_enabled:
        return

    # Отправку в Telegram делаем fire-and-forget.
    # Но задачу сохраняем, чтобы при shutdown можно было дождаться её завершения.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Если активного event loop нет, просто пропускаем отправку.
        return

    task = loop.create_task(telegram_sender.send_text(message))
    telegram_tasks.add(task)
    task.add_done_callback(_on_telegram_task_done)


def log_info(logger, message, to_telegram=True):
    # stacklevel=2 нужен, чтобы в логах показывалось место вызова
    # этой функции, а не сама helper-функция log_info().
    logger.info(message, stacklevel=2)
    _send_to_telegram(message, to_telegram=to_telegram)


def log_warning(logger, message, to_telegram=True):
    logger.warning(message, stacklevel=2)
    _send_to_telegram(message, to_telegram=to_telegram)


def log_error(logger, message, to_telegram=True):
    logger.error(message, stacklevel=2)
    _send_to_telegram(message, to_telegram=to_telegram)

import asyncio
import time
from dataclasses import dataclass, field

from ib_async import IB

from core.logger import get_logger, log_info, log_warning

logger = get_logger(__name__)

# Пауза между попытками подключения / переподключения.
RECONNECT_DELAY_SECONDS = 5

# Как часто монитор проверяет, живо ли текущее соединение.
CONNECTION_CHECK_INTERVAL_SECONDS = 1

# Раз в сколько секунд слать heartbeat-сообщение.
HEARTBEAT_INTERVAL_SECONDS = 20


@dataclass
class IbConnectionHealth:
    # Состояние связи TWS / IB Gateway с backend IB.
    ib_backend_ok: bool = True

    # Набор market data farm, которые сейчас считаются недоступными.
    # Если набор пустой, значит market data farm в норме.
    market_data_down_farms: set[str] = field(default_factory=set)

    # Набор historical data farm (HMDS), которые сейчас считаются недоступными.
    # Если набор пустой, значит HMDS в норме.
    hmds_down_farms: set[str] = field(default_factory=set)

    @property
    def market_data_ok(self):
        # Market data считаем исправными, если нет ни одной "падающей" farm.
        return len(self.market_data_down_farms) == 0

    @property
    def hmds_ok(self):
        # HMDS считаем исправными, если нет ни одной "падающей" farm.
        return len(self.hmds_down_farms) == 0


def normalize_ib_message(text):
    # Приводим системные сообщения IB к читаемому виду.
    # Иногда TWS присылает уже нормальный текст,
    # а иногда строку с escaped unicode-последовательностями вида \u041d...
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def extract_ib_farm_name(error_string):
    # Для сообщений 2103 / 2104 / 2105 / 2106 название farm обычно идёт после ":".
    # Например:
    # "Соединение с базой рыночных данных исправно:cashfarm"
    # "Нарушено соединение с базой данных HMDS:euhmds"
    text = str(error_string).strip()

    if ":" not in text:
        return "unknown"

    farm_name = text.rsplit(":", 1)[-1].strip()

    if not farm_name:
        return "unknown"

    return farm_name


def build_farms_text(farms):
    # Красиво собираем список farm в одну строку для логов.
    if not farms:
        return "-"

    return ", ".join(sorted(farms))


def _build_ib_health_text(ib_health):
    # Строка со сводным состоянием здоровья соединения.
    return (
        f"ib_backend_ok={ib_health.ib_backend_ok}, "
        f"market_data_ok={ib_health.market_data_ok}, "
        f"market_data_down_farms={build_farms_text(ib_health.market_data_down_farms)}, "
        f"hmds_ok={ib_health.hmds_ok}, "
        f"hmds_down_farms={build_farms_text(ib_health.hmds_down_farms)}"
    )


def _reset_ib_health_for_new_connect(ib_health):
    # При новом локальном подключении начинаем с "чистого" состояния.
    # Если TWS во время синхронизации пришлёт системные коды проблем,
    # обработчик errorEvent тут же обновит эти флаги и наборы farm.
    ib_health.ib_backend_ok = True
    ib_health.market_data_down_farms.clear()
    ib_health.hmds_down_farms.clear()


def _register_ib_health_handlers(ib, ib_health):
    # Подписываемся на системные сообщения TWS / IB Gateway.
    # Через них будем поддерживать состояние backend, market data farm и HMDS.
    def on_ib_error(req_id, error_code, error_string, contract):
        # Сразу декодируем текст сообщения, чтобы в логах был нормальный русский текст.
        error_string = normalize_ib_message(error_string)

        # 1100 = TWS потерял связь с backend IB.
        # 2110 = Connectivity between TWS and server is broken.
        if error_code in (1100, 2110):
            if ib_health.ib_backend_ok:
                log_warning(
                    logger,
                    f"IB backend недоступен: code={error_code}, message={error_string}",
                )

            ib_health.ib_backend_ok = False
            return

        # 1101 / 1102 = связь TWS с backend IB восстановлена.
        if error_code in (1101, 1102):
            was_bad = not ib_health.ib_backend_ok
            ib_health.ib_backend_ok = True

            if was_bad:
                log_info(
                    logger,
                    f"IB backend снова доступен: code={error_code}, message={error_string}",
                )
            return

        # 2103 = market data farm disconnected.
        if error_code == 2103:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name not in ib_health.market_data_down_farms:
                log_warning(
                    logger,
                    f"Market data farm недоступен: code={error_code}, farm={farm_name}, message={error_string}", to_telegram=False
                )

            ib_health.market_data_down_farms.add(farm_name)
            return

        # 2104 = market data farm connection is OK.
        if error_code == 2104:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name in ib_health.market_data_down_farms:
                ib_health.market_data_down_farms.discard(farm_name)

                log_info(
                    logger,
                    f"Market data farm снова доступен: code={error_code}, farm={farm_name}, message={error_string}",
                )
            return

        # 2105 = historical data farm disconnected.
        if error_code == 2105:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name not in ib_health.hmds_down_farms:
                log_warning(
                    logger,
                    f"HMDS недоступен: code={error_code}, farm={farm_name}, message={error_string}", to_telegram=False
                )

            ib_health.hmds_down_farms.add(farm_name)
            return

        # 2106 = historical data farm connection is OK.
        if error_code == 2106:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name in ib_health.hmds_down_farms:
                ib_health.hmds_down_farms.discard(farm_name)

                log_info(
                    logger,
                    f"HMDS снова доступен: code={error_code}, farm={farm_name}, message={error_string}",
                )
            return

    ib.errorEvent += on_ib_error


async def connect_ib(settings):
    # Создаём объект клиента IB.
    ib = IB()

    # Создаём объект со служебным состоянием здоровья соединения.
    ib_health = IbConnectionHealth()

    # Подписываемся на системные сообщения TWS один раз.
    # Эти подписки останутся на том же объекте ib и после реконнектов.
    _register_ib_health_handlers(ib, ib_health)

    # Засекаем момент начала попыток подключения.
    connect_started_at = time.monotonic()

    # Счётчик попыток подключения.
    connect_attempt = 0

    # Последний текст ошибки, чтобы не дублировать одно и то же слишком часто.
    last_error_text = None

    log_info(logger, "Подключаюсь к IB...")

    # Бесконечно пытаемся подключиться, пока соединение не будет установлено
    # или пока пользователь сам не остановит робота.
    while True:
        connect_attempt += 1

        try:
            # Перед новой попыткой подключения приводим состояние "здоровья" в базовое.
            _reset_ib_health_for_new_connect(ib_health)

            # Пытаемся открыть соединение с TWS / IB Gateway.
            await ib.connectAsync(
                host=settings.ib_host,
                port=settings.ib_port,
                clientId=settings.ib_client_id,
            )

            # Дополнительная жёсткая проверка:
            # если connectAsync завершился без исключения,
            # но соединение реально не активно, считаем это ошибкой.
            if not ib.isConnected():
                raise RuntimeError("IB вернул connectAsync без активного соединения")

            # Считаем, сколько секунд заняло подключение.
            connect_duration = int(time.monotonic() - connect_started_at)

            log_info(
                logger,
                f"Соединение с IB установлено после {connect_attempt} попыток за {connect_duration} сек",
            )

            # Возвращаем и сам объект IB, и объект состояния здоровья.
            return ib, ib_health

        except asyncio.CancelledError:
            # Если задачу отменили извне, обязательно пробрасываем отмену дальше.
            raise

        except Exception as e:
            error_text = str(e)

            # При первой неудаче печатаем поясняющее сообщение.
            if connect_attempt == 1:
                log_warning(logger, "Не удалось подключиться к IB при старте")
                log_warning(logger, "Ожидаю доступности TWS/IB Gateway...")

            # Печатаем:
            # - первую ошибку,
            # - каждую 5-ю попытку,
            # - либо если текст ошибки изменился.
            if connect_attempt == 1 or connect_attempt % 5 == 0 or error_text != last_error_text:
                log_warning(logger, f"Попытка подключения #{connect_attempt} не удалась: {error_text}")

            last_error_text = error_text

            # Ждём и пробуем снова.
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def disconnect_ib(ib):
    # Закрываем соединение только если оно активно.
    if ib.isConnected():
        ib.disconnect()


async def get_ib_server_time_text(ib):
    # Запрашиваем текущее серверное время у IB и возвращаем готовую строку.
    current_time = await ib.reqCurrentTimeAsync()
    return str(current_time).split("+")[0]


async def monitor_ib_connection(ib, settings, ib_health):
    # Запоминаем, было ли соединение активным на момент старта монитора.
    was_connected = ib.isConnected()

    # Момент начала переподключения после потери связи.
    reconnect_started_at = None

    # Счётчик попыток переподключения.
    reconnect_attempt = 0

    # Последний текст ошибки переподключения.
    last_error_text = None

    # Монитор работает постоянно, пока жив робот.
    while True:
        # Если соединение активно:
        if ib.isConnected():
            # Если раньше связи не было, а теперь появилась,
            # значит соединение восстановлено.
            if not was_connected:
                reconnect_duration = int(time.monotonic() - reconnect_started_at)

                log_info(
                    logger,
                    f"Соединение с IB восстановлено после {reconnect_attempt} попыток за {reconnect_duration} сек",
                )

                # После восстановления сразу пробуем запросить серверное время.
                try:
                    server_time_text = await get_ib_server_time_text(ib)
                    log_info(logger, f"Время сервера IB: {server_time_text}")
                except Exception as e:
                    log_warning(logger, f"Не удалось получить время сервера IB после восстановления: {e}")

                # Сбрасываем служебные переменные после успешного восстановления.
                reconnect_started_at = None
                reconnect_attempt = 0
                last_error_text = None

            # Обновляем флаг: соединение сейчас активно.
            was_connected = True

            # Даём циклу немного поспать, чтобы не крутиться слишком часто.
            await asyncio.sleep(CONNECTION_CHECK_INTERVAL_SECONDS)
            continue

        # Если мы попали сюда, значит прямо сейчас локального API-соединения нет.
        if was_connected:
            log_warning(logger, "Соединение с IB потеряно")
            log_warning(logger, "Запускаю переподключение к IB...")

            was_connected = False
            reconnect_started_at = time.monotonic()
            reconnect_attempt = 0
            last_error_text = None

        reconnect_attempt += 1

        try:
            # Перед новой попыткой реконнекта приводим состояние "здоровья"
            # в базовое состояние нового локального подключения.
            _reset_ib_health_for_new_connect(ib_health)

            # Пытаемся восстановить соединение.
            await ib.connectAsync(
                host=settings.ib_host,
                port=settings.ib_port,
                clientId=settings.ib_client_id,
            )

            # Если подключились успешно, сразу идём на новую итерацию,
            # чтобы без лишней паузы зафиксировать восстановление.
            if ib.isConnected():
                continue

        except asyncio.CancelledError:
            # Корректно пробрасываем отмену задачи наружу.
            raise

        except Exception as e:
            error_text = str(e)

            # Печатаем:
            # - первую ошибку,
            # - каждую 5-ю попытку,
            # - либо если текст ошибки изменился.
            if reconnect_attempt == 1 or reconnect_attempt % 5 == 0 or error_text != last_error_text:
                log_warning(logger, f"Попытка переподключения #{reconnect_attempt} не удалась: {error_text}")

            last_error_text = error_text

        # Если переподключение не удалось, ждём и пробуем снова.
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


async def heartbeat_ib_connection(ib, ib_health):
    # Бесконечная фоновая задача.
    # Раз в 2 минуты проверяет не только локальный API-сокет,
    # но и реальное состояние TWS / IB по backend, market data farm и HMDS.
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

        # Если локального API-соединения сейчас нет,
        # heartbeat не пишем — монитор и так уже выводит нужные сообщения.
        if not ib.isConnected():
            continue

        # Пытаемся получить живое серверное время у IB.
        server_time_text = ""
        server_time_ok = False

        try:
            server_time_text = await get_ib_server_time_text(ib)
            server_time_ok = True
        except Exception as e:
            server_time_text = f"не получено ({e})"

        # Полностью "здоровым" считаем состояние только если:
        # - локальный API-сокет жив,
        # - серверное время успешно получено,
        # - backend IB доступен,
        # - нет проблемных market data farm,
        # - нет проблемных HMDS farm.
        if (
                server_time_ok
                and ib_health.ib_backend_ok
                and ib_health.market_data_ok
                and ib_health.hmds_ok
        ):
            log_info(
                logger,
                f"Робот работает в штатном режиме. API-соединение с TWS активно. "
                f"Время сервера IB: {server_time_text}",
            )
            continue

        # Если хотя бы один признак не в норме — пишем warning,
        # а не "всё хорошо".
        log_warning(
            logger,
            f"Робот работает, но состояние IB нештатное. "
            f"Время сервера IB: {server_time_text}. "
            f"{_build_ib_health_text(ib_health)}",
        )

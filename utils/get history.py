'''
тестовый скрипт для ручной проверки historical requests к IB по выбранному контракту и интервалу. Последовательно запрашивает разные whatToShow,
печатает параметры запроса и полученные бары, а также считает пустой результат и ряд кодов ошибок IB фатальными для теста.
'''
import asyncio
import logging
from datetime import datetime, timezone

from ib_async import IB, Contract

from contracts import Instrument

# ==========================================================
# НАСТРОЙКИ ПОДКЛЮЧЕНИЯ
# ==========================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7496
IB_CLIENT_ID = 201

# ==========================================================
# НАСТРОЙКИ ТЕСТА
# ==========================================================

# Берём именно этот контракт из contracts.py
INSTRUMENT_CODE = "MNQ"
CONTRACT_LOCAL_SYMBOL = "MNQH6"

# Что хотим проверить
WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK"]

# Размер бара.
# Для первого теста берём 5 секунд, чтобы потом было проще сравнивать с realtime 5-sec bars.
BAR_SIZE_SETTING = "5 secs"

# Брать ли только regular trading hours
USE_RTH = False

# Интервал истории.
# Задаём его в UTC.
START_UTC = "2026-03-12 17:00:00"
END_UTC = "2026-03-12 17:05:00"

# Печатать все бары или только начало/конец выборки
PRINT_ALL_BARS = False

# Сколько баров показывать с начала и с конца, если PRINT_ALL_BARS = False
HEAD_BARS = 3
TAIL_BARS = 10

# ==========================================================
# НАСТРОЙКИ СТРОГОСТИ ПРОВЕРОК
# ==========================================================

# Для тестового скрипта пустой результат считаем ошибкой.
FAIL_IF_NO_BARS = True

# Сколько секунд дать IB на доставку errorEvent после historical request.
ERROR_FLUSH_DELAY_SECONDS = 0.2

# Коды ошибок IB, которые в этом скрипте считаем фатальными для historical data.
FATAL_HISTORICAL_ERROR_CODES = {
    162,  # Historical market data service error
    165,  # Historical market data service query message
    166,  # HMDS Expired Contract Violation
    200,  # No security definition found / contract problem
    321,  # Validation error / request problem
    366,  # No historical data query found
}


# ==========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

def normalize_ib_message(text):
    # Приводим сообщения IB к читаемому виду.
    # Иногда текст уже нормальный, а иногда приходит как escaped unicode.
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def parse_utc(text):
    # Преобразуем строку вида "2026-03-13 08:00:00" в aware-datetime UTC.
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def build_duration_str(start_dt, end_dt):
    # IB historical bars запрашиваются как:
    # endDateTime + durationStr
    # Поэтому из start/end считаем durationStr.
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("END_UTC должно быть строго больше START_UTC")

    # Для первого теста с 5-секундными барами лучше держать окно не больше 1 часа.
    # Так меньше шанс уткнуться в ограничения IB по small bars.
    if BAR_SIZE_SETTING == "5 secs" and total_seconds > 3600:
        raise ValueError(
            "Для первого теста с 5-секундными барами держи окно не больше 1 часа. "
            "Если нужно больше — потом сделаем загрузку кусками."
        )

    return f"{total_seconds} S"


def build_end_datetime_str(end_dt):
    # Формат, который понимает IB historical request.
    return end_dt.strftime("%Y%m%d %H:%M:%S UTC")


def get_contract_row():
    # Находим нужный контракт по localSymbol в реестре contracts.py.
    instrument_row = Instrument[INSTRUMENT_CODE]

    for row in instrument_row["contracts"]:
        if row["localSymbol"] == CONTRACT_LOCAL_SYMBOL:
            return instrument_row, row

    raise ValueError(
        f"Контракт {CONTRACT_LOCAL_SYMBOL} не найден в Instrument['{INSTRUMENT_CODE}']['contracts']"
    )


def build_ib_contract():
    # Собираем полный IB Contract из данных contracts.py.
    instrument_row, contract_row = get_contract_row()

    return Contract(
        secType=instrument_row["secType"],
        symbol=INSTRUMENT_CODE,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        tradingClass=instrument_row["tradingClass"],
        multiplier=str(instrument_row["multiplier"]),
        conId=contract_row["conId"],
        localSymbol=contract_row["localSymbol"],
        lastTradeDateOrContractMonth=contract_row["lastTradeDateOrContractMonth"],
    )


def format_server_time(dt):
    # Красивый вывод серверного времени IB.
    return str(dt).split("+")[0]


def print_request_info(contract, start_dt, end_dt, duration_str):
    # Печатаем параметры теста, чтобы сразу видеть, что именно запрашиваем.
    print("=" * 100)
    print("ПАРАМЕТРЫ ИСТОРИЧЕСКОГО ЗАПРОСА")
    print("=" * 100)
    print(f"symbol                      : {INSTRUMENT_CODE}")
    print(f"localSymbol                 : {contract.localSymbol}")
    print(f"conId                       : {contract.conId}")
    print(f"lastTradeDateOrContractMonth: {contract.lastTradeDateOrContractMonth}")
    print(f"exchange                    : {contract.exchange}")
    print(f"currency                    : {contract.currency}")
    print(f"tradingClass                : {contract.tradingClass}")
    print(f"multiplier                  : {contract.multiplier}")
    print(f"start_utc                   : {start_dt}")
    print(f"end_utc                     : {end_dt}")
    print(f"durationStr                 : {duration_str}")
    print(f"barSizeSetting              : {BAR_SIZE_SETTING}")
    print(f"useRTH                      : {USE_RTH}")
    print(f"whatToShow list             : {WHAT_TO_SHOW_LIST}")
    print()


def print_bar(bar, index):
    # Печатаем один historical bar.
    print(
        f"{index:04d} | "
        f"date={str(bar.date).split('+')[0]} | "
        f"open={bar.open} | "
        f"high={bar.high} | "
        f"low={bar.low} | "
        f"close={bar.close} | "
        f"volume={bar.volume} | "
        f"average={bar.average} | "
        f"barCount={bar.barCount}"
    )


def print_bars_result(what_to_show, bars):
    # Печатаем результат по одному whatToShow.
    print("=" * 100)
    print(f"whatToShow = {what_to_show}")
    print(f"bars count  = {len(bars)}")
    print("=" * 100)

    if PRINT_ALL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    # Если баров немного, печатаем все.
    if len(bars) <= HEAD_BARS + TAIL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    # Иначе печатаем начало и конец, чтобы глазами быстро посмотреть структуру.
    # print("--- НАЧАЛО ---")
    for i, bar in enumerate(bars[:HEAD_BARS], start=1):
        print_bar(bar, i)

    # print("...")
    #
    # print("--- КОНЕЦ ---")
    # tail_start_index = len(bars) - TAIL_BARS + 1
    # for i, bar in enumerate(bars[-TAIL_BARS:], start=tail_start_index):
    #     print_bar(bar, i)

    # print()


def raise_if_historical_errors(error_records, what_to_show):
    # Если IB прислал фатальные ошибки по historical data — падаем сразу.
    if not error_records:
        return

    lines = [f"[{what_to_show}] IB вернул ошибки historical data:"]

    for code, message in error_records:
        lines.append(f"code={code}, message={message}")

    raise RuntimeError("\n".join(lines))


# ==========================================================
# ОСНОВНОЙ КОД
# ==========================================================

async def main():
    # Готовим временной интервал.
    start_dt = parse_utc(START_UTC)
    end_dt = parse_utc(END_UTC)

    duration_str = build_duration_str(start_dt, end_dt)
    end_datetime_str = build_end_datetime_str(end_dt)

    # Собираем контракт из contracts.py.
    contract = build_ib_contract()

    # Печатаем параметры запроса.
    print_request_info(contract, start_dt, end_dt, duration_str)

    # Глушим внутренние логи ib_async, чтобы не было дублирования сырого текста ошибок.
    ib_async_logger = logging.getLogger("ib_async")
    ib_async_logger.handlers.clear()
    ib_async_logger.addHandler(logging.NullHandler())
    ib_async_logger.propagate = False

    ib = IB()

    # Здесь будем собирать IB errorEvent по текущему запросу.
    current_request_errors = []

    def on_ib_error(req_id, error_code, error_string, contract_obj):
        # В тестовом скрипте собираем только фатальные ошибки,
        # связанные с historical data.
        if error_code not in FATAL_HISTORICAL_ERROR_CODES:
            return

        message = normalize_ib_message(error_string)
        current_request_errors.append((error_code, message))

    # Подписываемся на ошибки один раз на весь жизненный цикл объекта IB.
    ib.errorEvent += on_ib_error

    try:
        # Подключаемся к TWS / IB Gateway.
        await ib.connectAsync(
            host=IB_HOST,
            port=IB_PORT,
            clientId=IB_CLIENT_ID,
        )

        if not ib.isConnected():
            raise RuntimeError("Не удалось установить соединение с IB")

        print("Соединение с IB установлено")

        # Получаем время сервера IB и валидируем,
        # что не пытаемся запрашивать будущее.
        server_time = await ib.reqCurrentTimeAsync()
        print(f"Время сервера IB: {format_server_time(server_time)}")
        print()

        if end_dt > server_time.astimezone(timezone.utc):
            raise ValueError(
                f"END_UTC={end_dt} лежит в будущем относительно времени сервера IB={server_time}"
            )

        # Последовательно проверяем все whatToShow.
        # Последовательно — чтобы не плодить параллельные historical requests.
        for what_to_show in WHAT_TO_SHOW_LIST:
            # Перед каждым запросом очищаем список ошибок.
            current_request_errors.clear()

            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_datetime_str,
                durationStr=duration_str,
                barSizeSetting=BAR_SIZE_SETTING,
                whatToShow=what_to_show,
                useRTH=USE_RTH,
                formatDate=2,
                keepUpToDate=False,
            )

            # Даём event loop короткий шанс доставить возможный errorEvent.
            await asyncio.sleep(ERROR_FLUSH_DELAY_SECONDS)

            # Если IB прислал ошибку — падаем сразу.
            raise_if_historical_errors(current_request_errors, what_to_show)

            # Для тестового скрипта отсутствие баров тоже считаем ошибкой.
            if FAIL_IF_NO_BARS and len(bars) == 0:
                raise RuntimeError(
                    f"[{what_to_show}] IB не вернул ни одного historical bar"
                )

            print_bars_result(what_to_show, bars)

            # Небольшая пауза между запросами, чтобы вести себя аккуратно по pacing.
            await asyncio.sleep(1)

    finally:
        try:
            ib.errorEvent -= on_ib_error
        except Exception:
            pass

        if ib.isConnected():
            ib.disconnect()
            print("Соединение с IB закрыто")


if __name__ == "__main__":
    asyncio.run(main())

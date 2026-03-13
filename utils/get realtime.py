import asyncio
from datetime import datetime, timezone

from ib_async import IB, Contract

from contracts import Instrument


# ==========================================================
# НАСТРОЙКИ ПОДКЛЮЧЕНИЯ
# ==========================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7496
IB_CLIENT_ID = 202


# ==========================================================
# НАСТРОЙКИ ТЕСТА
# ==========================================================

# Берём контракт из contracts.py
INSTRUMENT_CODE = "MNQ"
CONTRACT_LOCAL_SYMBOL = "MNQH6"

# Для realtime bars у IB имеет смысл проверять только эти режимы.
# BID_ASK для realtime bars не используем.
WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK"]

# Для realtime bars размер у IB фиксирован: только 5 секунд.
BAR_SIZE = 5

# Regular Trading Hours или вся сессия.
USE_RTH = False

# Сколько секунд держать подписку на каждый режим.
# 15 секунд обычно даёт около 3 баров.
SECONDS_PER_MODE = 15

# Печатать ли время бара в UTC.
PRINT_UTC_TIME = True


# ==========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

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


def format_bar_time(bar):
    # У realtime bars поле time в текущем поведении ib_async
    # приходит уже как datetime, а не как Unix timestamp.
    # Здесь явно поддерживаем только datetime.
    value = bar.time

    if not isinstance(value, datetime):
        raise TypeError(
            f"Ожидался datetime в bar.time, получено: {type(value).__name__}"
        )

    # Если datetime без tzinfo, считаем его как есть и просто печатаем.
    if value.tzinfo is None:
        if PRINT_UTC_TIME:
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")

    if PRINT_UTC_TIME:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def print_contract_info(contract):
    # Печатаем параметры теста.
    print("=" * 100)
    print("ПАРАМЕТРЫ REALTIME-ТЕСТА")
    print("=" * 100)
    print(f"symbol                      : {INSTRUMENT_CODE}")
    print(f"localSymbol                 : {contract.localSymbol}")
    print(f"conId                       : {contract.conId}")
    print(f"lastTradeDateOrContractMonth: {contract.lastTradeDateOrContractMonth}")
    print(f"exchange                    : {contract.exchange}")
    print(f"currency                    : {contract.currency}")
    print(f"tradingClass                : {contract.tradingClass}")
    print(f"multiplier                  : {contract.multiplier}")
    print(f"barSize                     : {BAR_SIZE}")
    print(f"useRTH                      : {USE_RTH}")
    print(f"seconds per mode            : {SECONDS_PER_MODE}")
    print(f"whatToShow list             : {WHAT_TO_SHOW_LIST}")
    print()


def print_bar(bar, what_to_show, index):
    # Печатаем один realtime bar.
    # ВАЖНО:
    # - у RealTimeBar поле открытия называется open_
    # - time уже приходит как datetime
    print(
        f"[{what_to_show:<8}] "
        f"#{index:03d} | "
        f"time={format_bar_time(bar)} | "
        f"open={bar.open_} | "
        f"high={bar.high} | "
        f"low={bar.low} | "
        f"close={bar.close} | "
        f"volume={bar.volume} | "
        f"wap={bar.wap} | "
        f"count={bar.count}"
    )


async def print_new_bars_for_period(bars, what_to_show, seconds_to_watch):
    # Не полагаемся на callback updateEvent.
    # Просто раз в секунду смотрим, появились ли новые бары в списке.
    printed_count = 0

    for _ in range(seconds_to_watch):
        current_count = len(bars)

        if current_count > printed_count:
            new_bars = bars[printed_count:current_count]

            for i, bar in enumerate(new_bars, start=printed_count + 1):
                print_bar(bar, what_to_show, i)

            printed_count = current_count

        await asyncio.sleep(1)

    return printed_count


# ==========================================================
# ОСНОВНОЙ КОД
# ==========================================================

async def main():
    contract = build_ib_contract()
    print_contract_info(contract)

    ib = IB()

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

        # Печатаем серверное время, чтобы понимать,
        # в какой момент рынка мы тестируем realtime.
        server_time = await ib.reqCurrentTimeAsync()
        print(f"Время сервера IB: {format_server_time(server_time)}")
        print()

        # Последовательно проверяем каждый режим whatToShow.
        for what_to_show in WHAT_TO_SHOW_LIST:
            print("=" * 100)
            print(f"СТАРТ ПОДПИСКИ: whatToShow = {what_to_show}")
            print("=" * 100)

            bars = ib.reqRealTimeBars(
                contract,
                BAR_SIZE,
                what_to_show,
                USE_RTH,
                [],
            )

            try:
                printed_count = await print_new_bars_for_period(
                    bars,
                    what_to_show,
                    SECONDS_PER_MODE,
                )

                print(f"[{what_to_show}] Получено баров   : {len(bars)}")
                print(f"[{what_to_show}] Напечатано баров : {printed_count}")

            finally:
                # Даже если тест упал, подписку всё равно снимаем.
                ib.cancelRealTimeBars(bars)

                print(f"СТОП ПОДПИСКИ: whatToShow = {what_to_show}")
                print()

                # Небольшая пауза между режимами.
                await asyncio.sleep(2)

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("Соединение с IB закрыто")


if __name__ == "__main__":
    asyncio.run(main())
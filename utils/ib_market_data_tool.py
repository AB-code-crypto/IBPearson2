"""
Единый ручной инструмент для проверки работы с рыночными данными Interactive Brokers.

Зачем нужен
-----------
Этот файл объединяет несколько разрозненных вспомогательных скриптов, которые раньше
жили отдельно и частично дублировали один и тот же код подключения к IB,
построения фьючерсного контракта и печати результатов.

Скрипт предназначен не для боевой загрузки истории и не для работы робота,
а именно для ручной диагностики и разовых проверок:
- найти conId и проверить, как IB распознаёт конкретный фьючерс;
- проверить historical request по выбранному контракту и интервалу;
- получить historical bars в более прикладном компактном виде;
- проверить realtime bars по разным режимам whatToShow.

Какие режимы есть
-----------------
Через переменную MODE вверху файла можно выбрать один из режимов:

1. contract_lookup
   Поиск контракта в IB по localSymbol и дополнительным уточняющим полям.
   Нужен, когда надо найти conId, проверить expiry, exchange, tradingClass,
   multiplier и убедиться, что IB вообще видит нужный контракт.

2. historical_probe
   Диагностический режим historical data.
   Делает запросы по списку WHAT_TO_SHOW_LIST, печатает параметры запроса,
   количество баров и сами бары. Также считает ряд типовых IB historical ошибок
   фатальными для теста.

3. historical_fetch
   Более спокойный режим historical data.
   Делает один запрос по выбранному WHAT_TO_SHOW и возвращает результат как
   компактный список баров, который удобно глазами проверить или использовать
   как небольшой ручной fetch для отладки.

4. realtime_probe
   Подписывается на reqRealTimeBars по выбранным режимам whatToShow и в течение
   заданного количества секунд печатает новые бары по мере их появления.

Общий принцип работы
--------------------
Для исторических и realtime режимов контракт берётся из contracts.py по паре:
- INSTRUMENT_CODE
- CONTRACT_LOCAL_SYMBOL

Для режима contract_lookup контракт, наоборот, строится вручную из lookup-полей,
чтобы можно было искать и старые, и неоднозначные фьючерсы даже без готового
conId в contracts.py.

Что этот скрипт не делает
-------------------------
- не пишет данные в price DB;
- не обновляет prepared DB;
- не запускает торговую логику;
- не используется внутри основного робота;
- не предназначен для массовой исторической закачки продакшен-уровня.

Это именно ручной инженерный инструмент для диагностики IB.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

from ib_async import IB, Contract

from contracts import Instrument

# ==========================================================
# ОБЩИЙ РЕЖИМ РАБОТЫ
# ==========================================================
# Варианты:
# - "contract_lookup"
# - "historical_probe"
# - "historical_fetch"
# - "realtime_probe"
MODE = "historical_probe"

# ==========================================================
# НАСТРОЙКИ ПОДКЛЮЧЕНИЯ К IB
# ==========================================================
IB_HOST = "127.0.0.1"
IB_PORT = 7496
IB_CLIENT_ID = 201

# ==========================================================
# НАСТРОЙКИ ОБЩЕГО КОНТРАКТА ДЛЯ HISTORICAL/REALTIME РЕЖИМОВ
# ==========================================================
INSTRUMENT_CODE = "MNQ"
CONTRACT_LOCAL_SYMBOL = "MNQH6"

# ==========================================================
# НАСТРОЙКИ CONTRACT LOOKUP
# ==========================================================
LOOKUP_LOCAL_SYMBOL = "MNQM4"
LOOKUP_SEC_TYPE = "FUT"
LOOKUP_INCLUDE_EXPIRED = True
LOOKUP_SYMBOL = ""
LOOKUP_EXCHANGE = "CME"
LOOKUP_CURRENCY = ""
LOOKUP_TRADING_CLASS = ""
LOOKUP_MULTIPLIER = ""
LOOKUP_EXPIRY = ""
LOOKUP_PRINT_JSON = False

# ==========================================================
# НАСТРОЙКИ HISTORICAL РЕЖИМОВ
# ==========================================================
# Интервал задаётся в UTC.
HISTORICAL_START_UTC = "2026-03-12 17:00:00"
HISTORICAL_END_UTC = "2026-03-12 17:05:00"

HISTORICAL_BAR_SIZE_SETTING = "5 secs"
HISTORICAL_USE_RTH = False

# Для historical_probe проверяем несколько whatToShow подряд.
HISTORICAL_PROBE_WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK"]

# Для historical_fetch берём один whatToShow.
HISTORICAL_FETCH_WHAT_TO_SHOW = "MIDPOINT"

# Печатать все бары или только начало/конец.
PRINT_ALL_HISTORICAL_BARS = False
HEAD_BARS = 3
TAIL_BARS = 10

# Для probe пустой результат считаем ошибкой.
FAIL_IF_NO_BARS = True

# Сколько секунд дать IB на доставку errorEvent после historical request.
ERROR_FLUSH_DELAY_SECONDS = 0.2

# Коды ошибок IB, которые считаем фатальными для historical request.
FATAL_HISTORICAL_ERROR_CODES = {
    162,
    165,
    166,
    200,
    321,
    366,
}

# ==========================================================
# НАСТРОЙКИ REALTIME РЕЖИМА
# ==========================================================
REALTIME_WHAT_TO_SHOW_LIST = ["TRADES", "MIDPOINT", "BID", "ASK"]
REALTIME_BAR_SIZE = 5
REALTIME_USE_RTH = False
SECONDS_PER_MODE = 15
PRINT_UTC_TIME = True

# ==========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

def parse_utc(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def normalize_ib_message(text):
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def format_server_time(dt):
    return str(dt).split("+")[0]


def format_utc(dt, for_ib=False):
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt, bar_size_setting):
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("HISTORICAL_END_UTC должно быть строго больше HISTORICAL_START_UTC")

    if bar_size_setting == "5 secs" and total_seconds > 3600:
        raise ValueError(
            "Для ручного теста с 5-секундными барами держи окно не больше 1 часа."
        )

    return f"{total_seconds} S"


def get_contract_row_from_registry(instrument_code, contract_local_symbol):
    instrument_row = Instrument[instrument_code]

    for row in instrument_row["contracts"]:
        if row["localSymbol"] == contract_local_symbol:
            return instrument_row, row

    raise ValueError(
        f"Контракт {contract_local_symbol} не найден в Instrument['{instrument_code}']['contracts']"
    )


def build_registry_contract(instrument_code, contract_local_symbol):
    instrument_row, contract_row = get_contract_row_from_registry(
        instrument_code=instrument_code,
        contract_local_symbol=contract_local_symbol,
    )

    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        tradingClass=instrument_row["tradingClass"],
        multiplier=str(instrument_row["multiplier"]),
        conId=contract_row["conId"],
        localSymbol=contract_row["localSymbol"],
        lastTradeDateOrContractMonth=contract_row["lastTradeDateOrContractMonth"],
    )


def build_lookup_contract():
    kwargs = {
        "secType": LOOKUP_SEC_TYPE,
        "localSymbol": LOOKUP_LOCAL_SYMBOL,
        "includeExpired": LOOKUP_INCLUDE_EXPIRED,
    }

    if LOOKUP_SYMBOL:
        kwargs["symbol"] = LOOKUP_SYMBOL

    if LOOKUP_EXCHANGE:
        kwargs["exchange"] = LOOKUP_EXCHANGE

    if LOOKUP_CURRENCY:
        kwargs["currency"] = LOOKUP_CURRENCY

    if LOOKUP_TRADING_CLASS:
        kwargs["tradingClass"] = LOOKUP_TRADING_CLASS

    if LOOKUP_MULTIPLIER:
        kwargs["multiplier"] = LOOKUP_MULTIPLIER

    if LOOKUP_EXPIRY:
        kwargs["lastTradeDateOrContractMonth"] = LOOKUP_EXPIRY

    return Contract(**kwargs)


def contract_to_dict(contract):
    return {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "tradingClass": getattr(contract, "tradingClass", None),
        "multiplier": getattr(contract, "multiplier", None),
        "exchange": getattr(contract, "exchange", None),
        "primaryExchange": getattr(contract, "primaryExchange", None),
        "currency": getattr(contract, "currency", None),
    }


def print_contract(contract, index=None):
    if index is not None:
        print(f"Инструмент #{index}")

    print(f"  conId       : {getattr(contract, 'conId', None)}")
    print(f"  symbol      : {getattr(contract, 'symbol', None)}")
    print(f"  localSymbol : {getattr(contract, 'localSymbol', None)}")
    print(f"  expiry      : {getattr(contract, 'lastTradeDateOrContractMonth', None)}")
    print(f"  tradingClass: {getattr(contract, 'tradingClass', None)}")
    print(f"  multiplier  : {getattr(contract, 'multiplier', None)}")
    print(f"  exchange    : {getattr(contract, 'exchange', None)}")
    print(f"  primaryExch : {getattr(contract, 'primaryExchange', None)}")
    print(f"  currency    : {getattr(contract, 'currency', None)}")


def print_registry_contract_info(contract, bar_size=None, use_rth=None, what_to_show_list=None):
    print("=" * 100)
    print("ПАРАМЕТРЫ ТЕСТА")
    print("=" * 100)
    print(f"symbol                      : {INSTRUMENT_CODE}")
    print(f"localSymbol                 : {contract.localSymbol}")
    print(f"conId                       : {contract.conId}")
    print(f"lastTradeDateOrContractMonth: {contract.lastTradeDateOrContractMonth}")
    print(f"exchange                    : {contract.exchange}")
    print(f"currency                    : {contract.currency}")
    print(f"tradingClass                : {contract.tradingClass}")
    print(f"multiplier                  : {contract.multiplier}")

    if bar_size is not None:
        print(f"barSize                      : {bar_size}")

    if use_rth is not None:
        print(f"useRTH                       : {use_rth}")

    if what_to_show_list is not None:
        print(f"whatToShow list              : {what_to_show_list}")

    print()


def print_historical_request_info(contract, start_dt, end_dt, duration_str, what_to_show_list):
    print_registry_contract_info(
        contract=contract,
        bar_size=HISTORICAL_BAR_SIZE_SETTING,
        use_rth=HISTORICAL_USE_RTH,
        what_to_show_list=what_to_show_list,
    )
    print(f"start_utc                   : {start_dt}")
    print(f"end_utc                     : {end_dt}")
    print(f"durationStr                 : {duration_str}")
    print()


def print_bar(bar, index):
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


def print_historical_bars_result(what_to_show, bars):
    print("=" * 100)
    print(f"whatToShow = {what_to_show}")
    print(f"bars count  = {len(bars)}")
    print("=" * 100)

    if PRINT_ALL_HISTORICAL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    if len(bars) <= HEAD_BARS + TAIL_BARS:
        for i, bar in enumerate(bars, start=1):
            print_bar(bar, i)
        print()
        return

    for i, bar in enumerate(bars[:HEAD_BARS], start=1):
        print_bar(bar, i)

    print("...")

    tail_start_index = len(bars) - TAIL_BARS + 1
    for i, bar in enumerate(bars[-TAIL_BARS:], start=tail_start_index):
        print_bar(bar, i)

    print()


def raise_if_historical_errors(error_records, what_to_show):
    if not error_records:
        return

    lines = [f"[{what_to_show}] IB вернул ошибки historical data:"]

    for code, message in error_records:
        lines.append(f"code={code}, message={message}")

    raise RuntimeError("\n".join(lines))


def format_realtime_bar_time(bar):
    value = bar.time

    if not isinstance(value, datetime):
        raise TypeError(
            f"Ожидался datetime в bar.time, получено: {type(value).__name__}"
        )

    if value.tzinfo is None:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if PRINT_UTC_TIME:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def print_realtime_bar(bar, what_to_show, index):
    print(
        f"[{what_to_show:<8}] "
        f"#{index:03d} | "
        f"time={format_realtime_bar_time(bar)} | "
        f"open={bar.open_} | "
        f"high={bar.high} | "
        f"low={bar.low} | "
        f"close={bar.close} | "
        f"volume={bar.volume} | "
        f"wap={bar.wap} | "
        f"count={bar.count}"
    )


def historical_bars_to_dicts(bars):
    result = []

    for bar in bars:
        bar_date = bar.date

        if isinstance(bar_date, datetime):
            if bar_date.tzinfo is None:
                dt_utc = bar_date.replace(tzinfo=timezone.utc)
            else:
                dt_utc = bar_date.astimezone(timezone.utc)
        else:
            ts_utc = int(bar_date)
            dt_utc = datetime.fromtimestamp(ts_utc, tz=timezone.utc)

        result.append(
            {
                "ts_utc": int(dt_utc.timestamp()),
                "dt_utc": dt_utc,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": float(bar.volume),
                "barCount": bar.barCount,
                "wap": bar.average,
            }
        )

    return result


async def connect_ib_async():
    ib_async_logger = logging.getLogger("ib_async")
    ib_async_logger.handlers.clear()
    ib_async_logger.addHandler(logging.NullHandler())
    ib_async_logger.propagate = False

    ib = IB()

    await ib.connectAsync(
        host=IB_HOST,
        port=IB_PORT,
        clientId=IB_CLIENT_ID,
    )

    if not ib.isConnected():
        raise RuntimeError("Не удалось установить соединение с IB")

    return ib


async def request_historical_once(
    ib,
    contract,
    start_dt,
    end_dt,
    what_to_show,
    bar_size_setting,
    use_rth,
):
    duration_str = build_duration_str(start_dt, end_dt, bar_size_setting)

    return await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=format_utc(end_dt, for_ib=True),
        durationStr=duration_str,
        barSizeSetting=bar_size_setting,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=False,
    )


async def print_new_realtime_bars_for_period(bars, what_to_show, seconds_to_watch):
    printed_count = 0

    for _ in range(seconds_to_watch):
        current_count = len(bars)

        if current_count > printed_count:
            new_bars = bars[printed_count:current_count]

            for i, bar in enumerate(new_bars, start=printed_count + 1):
                print_realtime_bar(bar, what_to_show, i)

            printed_count = current_count

        await asyncio.sleep(1)

    return printed_count

# ==========================================================
# РЕЖИМЫ
# ==========================================================

async def run_contract_lookup_mode(ib):
    contract = build_lookup_contract()
    details = await ib.reqContractDetailsAsync(contract)

    if not details:
        print("[NOT FOUND] Ничего не найдено.")
        print()
        print("Текущие параметры поиска:")
        print(f"  LOOKUP_LOCAL_SYMBOL={LOOKUP_LOCAL_SYMBOL}")
        print(f"  LOOKUP_SEC_TYPE={LOOKUP_SEC_TYPE}")
        print(f"  LOOKUP_INCLUDE_EXPIRED={LOOKUP_INCLUDE_EXPIRED}")
        print(f"  LOOKUP_SYMBOL={LOOKUP_SYMBOL}")
        print(f"  LOOKUP_EXCHANGE={LOOKUP_EXCHANGE}")
        print(f"  LOOKUP_CURRENCY={LOOKUP_CURRENCY}")
        print(f"  LOOKUP_TRADING_CLASS={LOOKUP_TRADING_CLASS}")
        print(f"  LOOKUP_MULTIPLIER={LOOKUP_MULTIPLIER}")
        print(f"  LOOKUP_EXPIRY={LOOKUP_EXPIRY}")
        return

    contracts = [item.contract for item in details]

    if LOOKUP_PRINT_JSON:
        data = [contract_to_dict(c) for c in contracts]
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    print(f"Найдено инструментов: {len(contracts)}")
    print()

    for i, c in enumerate(contracts, start=1):
        print_contract(c, index=i)
        print()

    if len(contracts) == 1:
        print("Найден единственный контракт.")
    else:
        print("Выше выведены все найденные варианты. Можно выбрать нужный conId вручную.")


async def run_historical_probe_mode(ib):
    start_dt = parse_utc(HISTORICAL_START_UTC)
    end_dt = parse_utc(HISTORICAL_END_UTC)
    duration_str = build_duration_str(start_dt, end_dt, HISTORICAL_BAR_SIZE_SETTING)
    contract = build_registry_contract(INSTRUMENT_CODE, CONTRACT_LOCAL_SYMBOL)

    print_historical_request_info(
        contract=contract,
        start_dt=start_dt,
        end_dt=end_dt,
        duration_str=duration_str,
        what_to_show_list=HISTORICAL_PROBE_WHAT_TO_SHOW_LIST,
    )

    current_request_errors = []

    def on_ib_error(req_id, error_code, error_string, contract_obj):
        if error_code not in FATAL_HISTORICAL_ERROR_CODES:
            return

        message = normalize_ib_message(error_string)
        current_request_errors.append((error_code, message))

    ib.errorEvent += on_ib_error

    try:
        server_time = await ib.reqCurrentTimeAsync()
        print(f"Соединение с IB установлено")
        print(f"Время сервера IB: {format_server_time(server_time)}")
        print()

        if end_dt > server_time.astimezone(timezone.utc):
            raise ValueError(
                f"HISTORICAL_END_UTC={end_dt} лежит в будущем относительно времени сервера IB={server_time}"
            )

        for what_to_show in HISTORICAL_PROBE_WHAT_TO_SHOW_LIST:
            current_request_errors.clear()

            bars = await request_historical_once(
                ib=ib,
                contract=contract,
                start_dt=start_dt,
                end_dt=end_dt,
                what_to_show=what_to_show,
                bar_size_setting=HISTORICAL_BAR_SIZE_SETTING,
                use_rth=HISTORICAL_USE_RTH,
            )

            await asyncio.sleep(ERROR_FLUSH_DELAY_SECONDS)
            raise_if_historical_errors(current_request_errors, what_to_show)

            if FAIL_IF_NO_BARS and len(bars) == 0:
                raise RuntimeError(
                    f"[{what_to_show}] IB не вернул ни одного historical bar"
                )

            print_historical_bars_result(what_to_show, bars)
            await asyncio.sleep(1)

    finally:
        try:
            ib.errorEvent -= on_ib_error
        except Exception:
            pass


async def run_historical_fetch_mode(ib):
    start_dt = parse_utc(HISTORICAL_START_UTC)
    end_dt = parse_utc(HISTORICAL_END_UTC)
    duration_str = build_duration_str(start_dt, end_dt, HISTORICAL_BAR_SIZE_SETTING)
    contract = build_registry_contract(INSTRUMENT_CODE, CONTRACT_LOCAL_SYMBOL)

    print_historical_request_info(
        contract=contract,
        start_dt=start_dt,
        end_dt=end_dt,
        duration_str=duration_str,
        what_to_show_list=[HISTORICAL_FETCH_WHAT_TO_SHOW],
    )

    server_time = await ib.reqCurrentTimeAsync()
    print(f"Соединение с IB установлено")
    print(f"Время сервера IB: {format_server_time(server_time)}")
    print()

    if end_dt > server_time.astimezone(timezone.utc):
        raise ValueError(
            f"HISTORICAL_END_UTC={end_dt} лежит в будущем относительно времени сервера IB={server_time}"
        )

    bars = await request_historical_once(
        ib=ib,
        contract=contract,
        start_dt=start_dt,
        end_dt=end_dt,
        what_to_show=HISTORICAL_FETCH_WHAT_TO_SHOW,
        bar_size_setting=HISTORICAL_BAR_SIZE_SETTING,
        use_rth=HISTORICAL_USE_RTH,
    )

    rows = historical_bars_to_dicts(bars)

    print(f"Загружено баров: {len(rows)}")

    if not rows:
        print("Пустой результат.")
        return

    print("Первые 3 бара:")
    for row in rows[:3]:
        print(row)

    if len(rows) > 3:
        print("Последние 3 бара:")
        for row in rows[-3:]:
            print(row)


async def run_realtime_probe_mode(ib):
    contract = build_registry_contract(INSTRUMENT_CODE, CONTRACT_LOCAL_SYMBOL)

    print_registry_contract_info(
        contract=contract,
        bar_size=REALTIME_BAR_SIZE,
        use_rth=REALTIME_USE_RTH,
        what_to_show_list=REALTIME_WHAT_TO_SHOW_LIST,
    )

    server_time = await ib.reqCurrentTimeAsync()
    print("Соединение с IB установлено")
    print(f"Время сервера IB: {format_server_time(server_time)}")
    print()

    for what_to_show in REALTIME_WHAT_TO_SHOW_LIST:
        print("=" * 100)
        print(f"СТАРТ ПОДПИСКИ: whatToShow = {what_to_show}")
        print("=" * 100)

        bars = ib.reqRealTimeBars(
            contract,
            REALTIME_BAR_SIZE,
            what_to_show,
            REALTIME_USE_RTH,
            [],
        )

        try:
            printed_count = await print_new_realtime_bars_for_period(
                bars,
                what_to_show,
                SECONDS_PER_MODE,
            )

            print(f"[{what_to_show}] Получено баров   : {len(bars)}")
            print(f"[{what_to_show}] Напечатано баров : {printed_count}")

        finally:
            ib.cancelRealTimeBars(bars)
            print(f"СТОП ПОДПИСКИ: whatToShow = {what_to_show}")
            print()
            await asyncio.sleep(2)


async def main():
    if MODE not in {
        "contract_lookup",
        "historical_probe",
        "historical_fetch",
        "realtime_probe",
    }:
        raise ValueError(f"Неподдерживаемый MODE: {MODE}")

    ib = await connect_ib_async()

    try:
        if MODE == "contract_lookup":
            await run_contract_lookup_mode(ib)
        elif MODE == "historical_probe":
            await run_historical_probe_mode(ib)
        elif MODE == "historical_fetch":
            await run_historical_fetch_mode(ib)
        elif MODE == "realtime_probe":
            await run_realtime_probe_mode(ib)
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("Соединение с IB закрыто")


if __name__ == "__main__":
    asyncio.run(main())

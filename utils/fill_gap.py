import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from ib_async import Contract

from config import settings_for_gap as settings
from contracts import Instrument
from core.db_sql import create_quotes_table_sql, upsert_quotes_sql
from core.ib_connector import connect_ib, disconnect_ib
from core.logger import setup_logging, get_logger

# ==============================
# Настройки скрипта
# ==============================

# Путь к ценовой БД.
DB_PATH = settings.price_db_path

# Код инструмента из contracts.py.
INSTRUMENT_CODE = "MNQ"

# Какой именно фьючерс качать.
# Здесь указываем localSymbol из contracts.py.
CONTRACT_LOCAL_SYMBOL = "MNQM6"

# Начало интервала в UTC.
# Скрипт всегда качает ровно один час от этой точки в будущее.
START_UTC = "2026-03-20 12:00:00"

# Длина закачиваемого окна фиксирована и равна одному часу.
DURATION_SECONDS = 3600

# Пауза между BID и ASK запросами.
HISTORICAL_REQUEST_DELAY_SECONDS = 11

# Защитный таймаут одного запроса.
HISTORICAL_REQUEST_TIMEOUT_SECONDS = 90

setup_logging()
logger = get_logger(__name__)


def parse_utc_datetime(text):
    # Парсим строку времени в UTC.
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def format_utc(dt, for_ib=False):
    # Универсальный форматтер времени.
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt):
    # IB принимает durationStr, а не start/end напрямую.
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть строго больше начала")

    return f"{total_seconds} S"


def build_table_name(instrument_code, bar_size_setting):
    # Имя таблицы по той же схеме, что и в основном проекте.
    suffix = (
        bar_size_setting
        .replace(" ", "")
        .replace("secs", "s")
        .replace("sec", "s")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("mins", "m")
        .replace("min", "m")
    )

    return f"{instrument_code}_{suffix}"


def get_instrument_row(instrument_code):
    # Получаем описание инструмента из реестра.
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    return Instrument[instrument_code]


def get_contract_row(instrument_row, contract_local_symbol):
    # Ищем нужный контракт по localSymbol.
    if "contracts" not in instrument_row:
        raise ValueError("У инструмента нет списка contracts")

    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == contract_local_symbol:
            return contract_row

    raise ValueError(f"Контракт не найден в contracts.py: {contract_local_symbol}")


def build_futures_contract(instrument_code, instrument_row, contract_row):
    # Собираем контракт IB из данных contracts.py.
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


def build_quote_rows(bid_bars, ask_bars, contract_name):
    # Преобразуем BID/ASK бары в строки для SQLite.
    rows_by_ts = {}

    for bar in ask_bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())

        if bar_time_ts not in rows_by_ts:
            rows_by_ts[bar_time_ts] = {
                "bar_time_ts": bar_time_ts,
                "bar_time": format_utc(dt),
                "contract": contract_name,
                "ask_open": None,
                "bid_open": None,
                "ask_high": None,
                "bid_high": None,
                "ask_low": None,
                "bid_low": None,
                "ask_close": None,
                "bid_close": None,
                "volume": bar.volume,
                "average": bar.average,
                "bar_count": bar.barCount,
            }

        rows_by_ts[bar_time_ts]["ask_open"] = bar.open
        rows_by_ts[bar_time_ts]["ask_high"] = bar.high
        rows_by_ts[bar_time_ts]["ask_low"] = bar.low
        rows_by_ts[bar_time_ts]["ask_close"] = bar.close

    for bar in bid_bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())

        if bar_time_ts not in rows_by_ts:
            rows_by_ts[bar_time_ts] = {
                "bar_time_ts": bar_time_ts,
                "bar_time": format_utc(dt),
                "contract": contract_name,
                "ask_open": None,
                "bid_open": None,
                "ask_high": None,
                "bid_high": None,
                "ask_low": None,
                "bid_low": None,
                "ask_close": None,
                "bid_close": None,
                "volume": bar.volume,
                "average": bar.average,
                "bar_count": bar.barCount,
            }

        rows_by_ts[bar_time_ts]["bid_open"] = bar.open
        rows_by_ts[bar_time_ts]["bid_high"] = bar.high
        rows_by_ts[bar_time_ts]["bid_low"] = bar.low
        rows_by_ts[bar_time_ts]["bid_close"] = bar.close

    rows = []

    for bar_time_ts in sorted(rows_by_ts.keys()):
        row = rows_by_ts[bar_time_ts]
        rows.append(
            (
                row["bar_time_ts"],
                row["bar_time"],
                row["contract"],
                row["ask_open"],
                row["bid_open"],
                row["ask_high"],
                row["bid_high"],
                row["ask_low"],
                row["bid_low"],
                row["ask_close"],
                row["bid_close"],
                row["volume"],
                row["average"],
                row["bar_count"],
            )
        )

    return rows


def write_quote_rows_to_sqlite(db_path, table_name, rows):
    # Пишем строки в ценовую БД через UPSERT.
    create_sql = create_quotes_table_sql(table_name)
    upsert_sql = upsert_quotes_sql(table_name)

    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")

        conn.execute(create_sql)
        conn.executemany(upsert_sql, rows)
        conn.commit()

    finally:
        conn.close()


async def get_history(ib, contract, start_dt, end_dt, bar_size_setting, what_to_show, use_rth):
    # Один простой запрос исторических данных без сложной обвязки.
    return await asyncio.wait_for(
        ib.reqHistoricalDataAsync(
            contract,
            endDateTime=format_utc(end_dt, for_ib=True),
            durationStr=build_duration_str(start_dt, end_dt),
            barSizeSetting=bar_size_setting,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=2,
            keepUpToDate=False,
        ),
        timeout=HISTORICAL_REQUEST_TIMEOUT_SECONDS,
    )


async def main():
    instrument_row = get_instrument_row(INSTRUMENT_CODE)

    if instrument_row["secType"] != "FUT":
        raise ValueError(f"Скрипт рассчитан только на FUT. Получено: {instrument_row['secType']}")

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Скрипт рассчитан только на 5-секундные данные. Получено: {instrument_row['barSizeSetting']}"
        )

    contract_row = get_contract_row(instrument_row, CONTRACT_LOCAL_SYMBOL)
    contract = build_futures_contract(INSTRUMENT_CODE, instrument_row, contract_row)

    start_dt = parse_utc_datetime(START_UTC)
    end_dt = start_dt + timedelta(seconds=DURATION_SECONDS)

    table_name = build_table_name(INSTRUMENT_CODE, instrument_row["barSizeSetting"])

    logger.info(
        f"Старт разовой закачки: instrument={INSTRUMENT_CODE}, contract={CONTRACT_LOCAL_SYMBOL}, "
        f"interval={format_utc(start_dt)} -> {format_utc(end_dt)}, table={table_name}, db={DB_PATH}"
    )

    ib, _ = await connect_ib(settings)

    try:
        bid_bars = await get_history(
            ib=ib,
            contract=contract,
            start_dt=start_dt,
            end_dt=end_dt,
            bar_size_setting=instrument_row["barSizeSetting"],
            what_to_show="BID",
            use_rth=instrument_row["useRTH"],
        )

        await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

        ask_bars = await get_history(
            ib=ib,
            contract=contract,
            start_dt=start_dt,
            end_dt=end_dt,
            bar_size_setting=instrument_row["barSizeSetting"],
            what_to_show="ASK",
            use_rth=instrument_row["useRTH"],
        )

        rows = build_quote_rows(
            bid_bars=bid_bars,
            ask_bars=ask_bars,
            contract_name=CONTRACT_LOCAL_SYMBOL,
        )

        await asyncio.to_thread(
            write_quote_rows_to_sqlite,
            DB_PATH,
            table_name,
            rows,
        )

        logger.info(
            f"Разовая закачка завершена: contract={CONTRACT_LOCAL_SYMBOL}, "
            f"interval={format_utc(start_dt)} -> {format_utc(end_dt)}, rows={len(rows)}"
        )

    finally:
        disconnect_ib(ib)
        logger.info("Соединение с IB закрыто")


if __name__ == "__main__":
    asyncio.run(main())

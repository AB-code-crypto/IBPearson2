import asyncio
import sqlite3
from datetime import datetime, timezone

from ib_async import Contract

from contracts import Instrument
from core.db_sql import (
    get_create_quotes_table_sql,
    get_upsert_quotes_sql,
    get_create_ohlc_table_sql,
    get_upsert_ohlc_sql,
)
from core.logger import get_logger, log_info


logger = get_logger(__name__)


# Пауза после каждого historical request.
# Это даёт спокойный темп и помогает не упираться в pacing limits IB.
HISTORICAL_REQUEST_DELAY_SECONDS = 11


def format_utc(dt, for_ib=False):
    # Один форматтер:
    # - для записи времени в БД
    # - для endDateTime в historical request
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt):
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть больше начала")

    return f"{total_seconds} S"


def build_table_name(instrument_code, bar_size_setting):
    # Простое имя таблицы: инструмент + таймфрейм без пробелов.
    # Примеры:
    # "5 secs" -> "5s"
    # "1 hour" -> "1h"
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


def build_futures_contract(instrument_code, instrument_row, contract_row):
    # Контракт фьючерса целиком собираем из contracts.py.
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


def build_index_contract(instrument_code, instrument_row):
    # Контракт индекса собираем из contracts.py.
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        conId=instrument_row["conId"],
    )


def iter_chunks(start_ts, end_ts, chunk_seconds):
    # Идём полуоткрытыми интервалами [start, end)
    current_start_ts = start_ts

    while current_start_ts < end_ts:
        current_end_ts = min(current_start_ts + chunk_seconds, end_ts)
        yield current_start_ts, current_end_ts
        current_start_ts = current_end_ts


def get_chunk_seconds(bar_size_setting, start_ts, end_ts):
    # Для 5-секундных баров качаем по часу.
    if bar_size_setting == "5 secs":
        return 3600

    # Для остальных пока грузим одним куском.
    return end_ts - start_ts


def build_quote_rows(bid_bars, ask_bars, contract_name):
    # Склеиваем BID и ASK в памяти по bar_time_ts.
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
                "volume": None,
                "average": None,
                "bar_count": None,
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
                "volume": None,
                "average": None,
                "bar_count": None,
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


def build_ohlc_rows(bars, contract_name):
    # Обычный одиночный OHLC-поток, например VIX/TRADES.
    rows = []

    for bar in bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())

        rows.append(
            (
                bar_time_ts,
                format_utc(dt),
                contract_name,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.average,
                bar.barCount,
            )
        )

    return rows


def write_quote_rows_to_sqlite(db_path, table_name, rows):
    create_sql = get_create_quotes_table_sql(table_name)
    upsert_sql = get_upsert_quotes_sql(table_name)

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


def write_ohlc_rows_to_sqlite(db_path, table_name, rows):
    create_sql = get_create_ohlc_table_sql(table_name)
    upsert_sql = get_upsert_ohlc_sql(table_name)

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


def get_last_bar_time_ts(db_path, table_name):
    # Берём время последнего бара из таблицы.
    # Если таблицы ещё нет или она пустая — возвращаем None.
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.execute(f"SELECT MAX(bar_time_ts) FROM {table_name}")
        row = cursor.fetchone()

        if row is None or row[0] is None:
            return None

        return int(row[0])

    except sqlite3.OperationalError:
        return None

    finally:
        conn.close()


async def load_history_bid_ask_once(
    ib,
    contract,
    db_path,
    table_name,
    start_dt,
    end_dt,
    bar_size_setting,
    use_rth,
):
    # Простая разовая загрузка BID и ASK истории,
    # склейка в памяти и запись в БД.
    bid_bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=format_utc(end_dt, for_ib=True),
        durationStr=build_duration_str(start_dt, end_dt),
        barSizeSetting=bar_size_setting,
        whatToShow="BID",
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=False,
    )

    # Пауза после первого historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    ask_bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=format_utc(end_dt, for_ib=True),
        durationStr=build_duration_str(start_dt, end_dt),
        barSizeSetting=bar_size_setting,
        whatToShow="ASK",
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=False,
    )

    # Пауза после второго historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    rows = build_quote_rows(
        bid_bars=bid_bars,
        ask_bars=ask_bars,
        contract_name=contract.localSymbol,
    )

    await asyncio.to_thread(
        write_quote_rows_to_sqlite,
        db_path,
        table_name,
        rows,
    )

    return len(rows)


async def load_history_single_stream_once(
    ib,
    contract,
    db_path,
    table_name,
    start_dt,
    end_dt,
    bar_size_setting,
    what_to_show,
    use_rth,
    contract_name,
):
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=format_utc(end_dt, for_ib=True),
        durationStr=build_duration_str(start_dt, end_dt),
        barSizeSetting=bar_size_setting,
        whatToShow=what_to_show,
        useRTH=use_rth,
        formatDate=2,
        keepUpToDate=False,
    )

    # Пауза после historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    rows = build_ohlc_rows(
        bars=bars,
        contract_name=contract_name,
    )

    await asyncio.to_thread(
        write_ohlc_rows_to_sqlite,
        db_path,
        table_name,
        rows,
    )

    return len(rows)


async def load_history_task(ib, settings):
    # Таска полной первичной загрузки истории по Registry из contracts.py.
    # Для фьючерсов сейчас жёстко качаем BID + ASK.
    # Для индекса используем whatToShow из contracts.py.
    log_info(logger, "Запускаю задачу первичной загрузки истории", to_telegram=True)

    total_rows_written = 0

    for instrument_code, instrument_row in Instrument.items():
        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        last_bar_time_ts = await asyncio.to_thread(
            get_last_bar_time_ts,
            settings.price_db_path,
            table_name,
        )

        if last_bar_time_ts is None:
            log_info(
                logger,
                f"Начинаю загрузку истории: instrument={instrument_code}, table={table_name}, с самого начала",
                to_telegram=False,
            )
        else:
            log_info(
                logger,
                f"Продолжаю загрузку истории: instrument={instrument_code}, table={table_name}, "
                f"с bar_time_ts={last_bar_time_ts}",
                to_telegram=False,
            )

        # Фьючерсы качаем по всем контрактам из contracts.py.
        if instrument_row["secType"] == "FUT":
            for contract_row in instrument_row["contracts"]:
                contract = build_futures_contract(
                    instrument_code=instrument_code,
                    instrument_row=instrument_row,
                    contract_row=contract_row,
                )

                start_ts = contract_row["active_from_ts_utc"]
                end_ts = contract_row["active_to_ts_utc"]

                if last_bar_time_ts is not None:
                    start_ts = max(start_ts, last_bar_time_ts)

                if start_ts >= end_ts:
                    log_info(
                        logger,
                        f"Пропускаю контракт {contract.localSymbol}, история уже есть",
                        to_telegram=False,
                    )
                    continue

                chunk_seconds = get_chunk_seconds(
                    instrument_row["barSizeSetting"],
                    start_ts,
                    end_ts,
                )

                for chunk_start_ts, chunk_end_ts in iter_chunks(
                    start_ts,
                    end_ts,
                    chunk_seconds,
                ):
                    chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
                    chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

                    rows_written = await load_history_bid_ask_once(
                        ib=ib,
                        contract=contract,
                        db_path=settings.price_db_path,
                        table_name=table_name,
                        start_dt=chunk_start_dt,
                        end_dt=chunk_end_dt,
                        bar_size_setting=instrument_row["barSizeSetting"],
                        use_rth=instrument_row["useRTH"],
                    )

                    total_rows_written += rows_written

                    log_info(
                        logger,
                        f"История загружена: {instrument_code} {contract.localSymbol} "
                        f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} "
                        f"rows={rows_written}",
                        to_telegram=False,
                    )

            continue

        # Индексы и одиночные потоки.
        contract = build_index_contract(
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )

        start_ts = instrument_row["active_from_ts_utc"]

        server_time = await ib.reqCurrentTimeAsync()
        end_ts = int(server_time.astimezone(timezone.utc).timestamp())

        if last_bar_time_ts is not None:
            start_ts = max(start_ts, last_bar_time_ts)

        if start_ts >= end_ts:
            log_info(
                logger,
                f"Пропускаю {instrument_code}, история уже есть",
                to_telegram=False,
            )
            continue

        chunk_seconds = get_chunk_seconds(
            instrument_row["barSizeSetting"],
            start_ts,
            end_ts,
        )

        for chunk_start_ts, chunk_end_ts in iter_chunks(
            start_ts,
            end_ts,
            chunk_seconds,
        ):
            chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
            chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

            rows_written = await load_history_single_stream_once(
                ib=ib,
                contract=contract,
                db_path=settings.price_db_path,
                table_name=table_name,
                start_dt=chunk_start_dt,
                end_dt=chunk_end_dt,
                bar_size_setting=instrument_row["barSizeSetting"],
                what_to_show=instrument_row["whatToShow"],
                use_rth=instrument_row["useRTH"],
                contract_name=instrument_code,
            )

            total_rows_written += rows_written

            log_info(
                logger,
                f"История загружена: {instrument_code} "
                f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} "
                f"rows={rows_written}",
                to_telegram=False,
            )

    log_info(
        logger,
        f"Первичная загрузка истории завершена. Всего записано строк: {total_rows_written}",
        to_telegram=True,
    )
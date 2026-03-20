import asyncio
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from config import settings_for_gap as settings
from contracts import Instrument
from core.db_sql import get_upsert_quotes_ask_sql, get_upsert_quotes_bid_sql
from core.ib_connector import connect_ib, disconnect_ib
from core.load_history import (
    HISTORICAL_REQUEST_DELAY_SECONDS,
    build_futures_contract,
    build_table_name,
    format_utc,
    format_utc_ts,
    get_chunk_seconds,
    iter_chunks,
    request_historical_data_with_reconnect,
    validate_price_value,
)
from core.logger import setup_logging, get_logger, log_info, log_warning


# ============================================================================
# НАСТРОЙКИ СКРИПТА
# ============================================================================
INSTRUMENT_CODE = "MNQ"
EXPECTED_STEP_SECONDS = 5
# Даже если целевой интервал совсем узкий, у IB исторические BID/ASK-запросы
# на 5 секунд часто отрабатывают плохо или возвращают пусто.
# Поэтому для исторического запроса всегда берём минимум 1 минуту,
# а потом уже отфильтровываем только нужные бары целевого интервала.
MIN_HISTORICAL_SIDE_REQUEST_SECONDS = 60
setup_logging()
logger = get_logger(__name__)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def resolve_db_path(db_path_text):
    # Относительный путь из config.py считаем относительно корня проекта.
    db_path = Path(db_path_text)

    if db_path.is_absolute():
        return db_path

    project_root = Path(__file__).resolve().parent.parent
    return project_root / db_path


def get_instrument_row(instrument_code):
    # Получаем инструмент из contracts.py и сразу валидируем, что это FUT.
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] != "FUT":
        raise ValueError(
            f"Скрипт рассчитан только на FUT. Получено: {instrument_row['secType']}"
        )

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Скрипт рассчитан только на 5-секундные данные. "
            f"Получено: {instrument_row['barSizeSetting']}"
        )

    return instrument_row


def get_contract_row(instrument_row, contract_local_symbol):
    # Ищем нужный контракт по localSymbol.
    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == contract_local_symbol:
            return contract_row

    raise ValueError(f"Контракт не найден в contracts.py: {contract_local_symbol}")


def ensure_table_exists(conn, table_name):
    # Проверяем, что нужная таблица есть в БД.
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
    row = conn.execute(sql, (table_name,)).fetchone()

    if row is None:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе")


def fetch_history_bounds(conn, table_name):
    # Получаем границы доступной истории в таблице.
    sql = f"SELECT MIN(bar_time_ts), MAX(bar_time_ts) FROM {table_name}"
    row = conn.execute(sql).fetchone()
    return row[0], row[1]


def fetch_problem_rows(conn, table_name):
    # Читаем все строки, где отсутствует хотя бы одна ASK-цена
    # или хотя бы одна BID-цена.
    sql = f"""
        SELECT
            bar_time_ts,
            contract,

            CASE
                WHEN ask_open IS NULL
                  OR ask_high IS NULL
                  OR ask_low IS NULL
                  OR ask_close IS NULL
                THEN 1 ELSE 0
            END AS ask_missing,

            CASE
                WHEN bid_open IS NULL
                  OR bid_high IS NULL
                  OR bid_low IS NULL
                  OR bid_close IS NULL
                THEN 1 ELSE 0
            END AS bid_missing
        FROM {table_name}
        WHERE
            ask_open IS NULL
            OR ask_high IS NULL
            OR ask_low IS NULL
            OR ask_close IS NULL
            OR bid_open IS NULL
            OR bid_high IS NULL
            OR bid_low IS NULL
            OR bid_close IS NULL
        ORDER BY contract ASC, bar_time_ts ASC
    """
    return conn.execute(sql).fetchall()


def build_side_intervals(problem_rows, what_to_show):
    # Группируем подряд идущие проблемные бары в интервалы.
    #
    # Интервалы полуоткрытые:
    # [start_ts, end_ts_exclusive)
    if what_to_show not in ("ASK", "BID"):
        raise ValueError(f"Неподдерживаемая сторона: {what_to_show}")

    intervals = []

    current_contract = None
    current_start_ts = None
    current_prev_ts = None

    for row in problem_rows:
        bar_time_ts = row[0]
        contract_local_symbol = row[1]
        ask_missing = bool(row[2])
        bid_missing = bool(row[3])

        if what_to_show == "ASK":
            side_missing = ask_missing
        else:
            side_missing = bid_missing

        if not side_missing:
            continue

        if current_start_ts is None:
            current_contract = contract_local_symbol
            current_start_ts = bar_time_ts
            current_prev_ts = bar_time_ts
            continue

        is_same_contract = contract_local_symbol == current_contract
        is_next_bar = bar_time_ts == current_prev_ts + EXPECTED_STEP_SECONDS

        if is_same_contract and is_next_bar:
            current_prev_ts = bar_time_ts
            continue

        intervals.append(
            {
                "contract_local_symbol": current_contract,
                "what_to_show": what_to_show,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "bars_count": (
                    (current_prev_ts - current_start_ts) // EXPECTED_STEP_SECONDS
                ) + 1,
            }
        )

        current_contract = contract_local_symbol
        current_start_ts = bar_time_ts
        current_prev_ts = bar_time_ts

    if current_start_ts is not None:
        intervals.append(
            {
                "contract_local_symbol": current_contract,
                "what_to_show": what_to_show,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "bars_count": (
                    (current_prev_ts - current_start_ts) // EXPECTED_STEP_SECONDS
                ) + 1,
            }
        )

    return intervals


def build_single_side_quote_rows(bars, contract_name, what_to_show):
    # Строим строки для записи только одной стороны quote-таблицы.
    #
    # Формат строки совпадает с side-specific UPSERT из db_sql.py:
    # ASK -> (bar_time_ts, bar_time, contract, ask_open, ask_high, ask_low, ask_close)
    # BID -> (bar_time_ts, bar_time, contract, bid_open, bid_high, bid_low, bid_close)
    rows = []

    for bar in bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())
        bar_time = format_utc(dt)

        rows.append(
            (
                bar_time_ts,
                bar_time,
                contract_name,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
            )
        )

    return rows


def write_single_side_quote_rows_to_sqlite(db_path, table_name, rows, what_to_show):
    # Пишем только одну сторону BID/ASK в уже существующую quote-таблицу.
    #
    # Ничего не создаём и ничего не проверяем — считаем, что инфраструктура уже готова.
    if not rows:
        return

    if what_to_show == "ASK":
        upsert_sql = get_upsert_quotes_ask_sql(table_name)
    elif what_to_show == "BID":
        upsert_sql = get_upsert_quotes_bid_sql(table_name)
    else:
        raise ValueError(f"Неподдерживаемая сторона: {what_to_show}")

    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.executemany(upsert_sql, rows)
        conn.commit()
    finally:
        conn.close()


async def load_quote_side_chunk_once(
    ib,
    ib_health,
    db_path,
    table_name,
    contract,
    start_dt,
    end_dt,
    bar_size_setting,
    what_to_show,
    use_rth,
):
    # Одна атомарная закачка только ASK или только BID для одного chunk.
    #
    # Важный момент:
    # если целевой интервал слишком узкий, например 5 секунд,
    # то у IB historical request на такой диапазон может вернуть пустой результат.
    # Поэтому сам запрос к IB расширяем минимум до 1 минуты влево,
    # но в БД потом пишем только бары, которые попали в исходный
    # целевой полуоткрытый интервал [start_dt, end_dt).
    while True:
        target_duration_seconds = int((end_dt - start_dt).total_seconds())

        request_start_dt = start_dt
        if target_duration_seconds < MIN_HISTORICAL_SIDE_REQUEST_SECONDS:
            request_start_dt = end_dt - timedelta(seconds=MIN_HISTORICAL_SIDE_REQUEST_SECONDS)

        bars = await request_historical_data_with_reconnect(
            ib=ib,
            ib_health=ib_health,
            contract=contract,
            end_dt=end_dt,
            start_dt=request_start_dt,
            bar_size_setting=bar_size_setting,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )

        await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

        # Из расширенного historical response оставляем только те бары,
        # которые реально относятся к целевому интервалу этого chunk.
        filtered_bars = []

        for bar in bars:
            bar_dt = bar.date.astimezone(timezone.utc)

            if start_dt <= bar_dt < end_dt:
                filtered_bars.append(bar)

        interval_text = f"{format_utc(start_dt)} -> {format_utc(end_dt)}"
        validation_error = None

        for index, bar in enumerate(filtered_bars):
            validation_error = validate_price_value(
                value=bar.open,
                field_name="open",
                stream_name=what_to_show,
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.high,
                field_name="high",
                stream_name=what_to_show,
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.low,
                field_name="low",
                stream_name=what_to_show,
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.close,
                field_name="close",
                stream_name=what_to_show,
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

        if validation_error is not None:
            log_warning(
                logger,
                f"Фьючерс {contract.localSymbol}: historical request вернул некорректные цены "
                f"{what_to_show}. {validation_error}. Повторяю этот же chunk",
                to_telegram=False,
            )
            continue

        rows = build_single_side_quote_rows(
            bars=filtered_bars,
            contract_name=contract.localSymbol,
            what_to_show=what_to_show,
        )

        await asyncio.to_thread(
            write_single_side_quote_rows_to_sqlite,
            db_path,
            table_name,
            rows,
            what_to_show,
        )

        return len(rows)


async def fill_side_interval(
    ib,
    ib_health,
    db_path,
    table_name,
    contract,
    bar_size_setting,
    use_rth,
    what_to_show,
    start_ts,
    end_ts_exclusive,
):
    # Докачиваем один полуоткрытый интервал только по одной стороне.
    if end_ts_exclusive <= start_ts:
        return 0

    chunk_seconds = get_chunk_seconds("FUT", bar_size_setting)
    total_rows_written = 0

    for chunk_start_ts, chunk_end_ts in iter_chunks(
        start_ts,
        end_ts_exclusive,
        chunk_seconds,
    ):
        chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
        chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: запрашиваю fill-null-price-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} ({what_to_show})",
            to_telegram=False,
        )

        rows_written = await load_quote_side_chunk_once(
            ib=ib,
            ib_health=ib_health,
            db_path=db_path,
            table_name=table_name,
            contract=contract,
            start_dt=chunk_start_dt,
            end_dt=chunk_end_dt,
            bar_size_setting=bar_size_setting,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )

        total_rows_written += rows_written

        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: загружен fill-null-price-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)}, "
            f"rows={rows_written}, side={what_to_show}",
            to_telegram=False,
        )

    return total_rows_written


def print_intervals(intervals):
    # Печатаем интервалы в консоль в человекочитаемом виде.
    if not intervals:
        print("Интервалов для докачки не найдено.")
        return

    for index, interval in enumerate(intervals, start=1):
        print(
            f"[{index}] "
            f"contract={interval['contract_local_symbol']} | "
            f"side={interval['what_to_show']} | "
            f"{format_utc_ts(interval['start_ts'])} -> "
            f"{format_utc_ts(interval['end_ts_exclusive'])} | "
            f"баров={interval['bars_count']}"
        )


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================
async def main():
    instrument_row = get_instrument_row(INSTRUMENT_CODE)
    table_name = build_table_name(INSTRUMENT_CODE, instrument_row["barSizeSetting"])
    db_path = resolve_db_path(settings.price_db_path)

    print(f"База данных: {db_path}")
    print(f"Таблица: {table_name}")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print("Режим: поиск строк с NULL в BID/ASK ценах и их докачка")

    conn = sqlite3.connect(db_path)

    try:
        ensure_table_exists(conn, table_name)

        history_min_ts, history_max_ts = fetch_history_bounds(conn, table_name)

        if history_min_ts is None or history_max_ts is None:
            print("Таблица существует, но данных в ней нет.")
            return

        print(
            "Доступная история в таблице: "
            f"{format_utc_ts(history_min_ts)} -> {format_utc_ts(history_max_ts)}"
        )

        problem_rows = fetch_problem_rows(conn, table_name)

    finally:
        conn.close()

    print(f"Найдено проблемных строк: {len(problem_rows)}")

    if not problem_rows:
        print("Свечек с отсутствующими ASK/BID ценами не найдено.")
        return

    ask_intervals = build_side_intervals(problem_rows, "ASK")
    bid_intervals = build_side_intervals(problem_rows, "BID")

    all_intervals = ask_intervals + bid_intervals
    all_intervals.sort(
        key=lambda item: (
            item["contract_local_symbol"],
            item["start_ts"],
            item["what_to_show"],
        )
    )

    print(f"Найдено ASK-интервалов для докачки: {len(ask_intervals)}")
    print(f"Найдено BID-интервалов для докачки: {len(bid_intervals)}")
    print(f"Всего интервалов для докачки: {len(all_intervals)}")
    print_intervals(all_intervals)

    ib = None
    ib_health = None
    total_rows_written = 0

    try:
        ib, ib_health = await connect_ib(settings)

        for interval in all_intervals:
            contract_row = get_contract_row(
                instrument_row=instrument_row,
                contract_local_symbol=interval["contract_local_symbol"],
            )
            contract = build_futures_contract(
                instrument_code=INSTRUMENT_CODE,
                instrument_row=instrument_row,
                contract_row=contract_row,
            )

            log_info(
                logger,
                f"Начинаю докачку интервала: "
                f"contract={interval['contract_local_symbol']}, "
                f"side={interval['what_to_show']}, "
                f"interval={format_utc_ts(interval['start_ts'])} -> "
                f"{format_utc_ts(interval['end_ts_exclusive'])}, "
                f"bars={interval['bars_count']}",
                to_telegram=False,
            )

            rows_written = await fill_side_interval(
                ib=ib,
                ib_health=ib_health,
                db_path=db_path,
                table_name=table_name,
                contract=contract,
                bar_size_setting=instrument_row["barSizeSetting"],
                use_rth=instrument_row["useRTH"],
                what_to_show=interval["what_to_show"],
                start_ts=interval["start_ts"],
                end_ts_exclusive=interval["end_ts_exclusive"],
            )

            total_rows_written += rows_written

        print(f"Докачка завершена. Всего записано строк: {total_rows_written}")

    finally:
        if ib is not None:
            disconnect_ib(ib)
            log_info(logger, "Соединение с IB закрыто", to_telegram=False)


if __name__ == "__main__":
    asyncio.run(main())
import sqlite3
from datetime import datetime, timezone

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from core.db_sql import (
    get_delete_prepared_hour_sql,
    get_insert_prepared_quote_sql,
)


# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Час указываем явно в UTC.
# Это начало исторического часа, который хотим построить в prepared DB.
HOUR_START_TEXT = "2026-03-20 15:00:00"


def parse_utc_hour_start_text(hour_start_text):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в UTC timestamp начала часа.
    #
    # Здесь ожидаем именно начало часа:
    # minute == 0, second == 0
    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"HOUR_START_TEXT должен указывать ровно на начало часа, получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def get_table_name(instrument_code):
    # Собираем имя таблицы по тем же правилам, что и в основном коде.
    instrument_row = Instrument[instrument_code]

    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )


def load_price_rows_for_one_hour(price_conn, table_name, hour_start_ts):
    # Читаем из ценовой БД все 5-секундные бары за один час.
    #
    # Берём только поля, нужные для подготовки y / sum_y / sum_y2.
    hour_end_ts = hour_start_ts + 3600

    sql = f"""
    SELECT
        bar_time_ts,
        bar_time,
        contract,
        ask_open,
        bid_open,
        ask_close,
        bid_close
    FROM {table_name}
    WHERE bar_time_ts >= ?
      AND bar_time_ts < ?
    ORDER BY bar_time_ts
    ;
    """

    cursor = price_conn.execute(sql, (hour_start_ts, hour_end_ts))
    rows = cursor.fetchall()

    return rows


def validate_price_rows(rows, hour_start_ts):
    # Проверяем, что час пригоден для переноса в prepared DB.
    #
    # Требования:
    # - ровно 720 баров;
    # - timestamps идут подряд с шагом 5 секунд;
    # - ask_open, bid_open, ask_close, bid_close не NULL;
    # - contract одинаковый во всём часе.
    if len(rows) != 720:
        raise ValueError(
            f"Ожидалось 720 баров за час, получено: {len(rows)}"
        )

    first_contract = rows[0]["contract"]

    for bar_index, row in enumerate(rows):
        expected_ts = hour_start_ts + bar_index * 5

        if row["bar_time_ts"] != expected_ts:
            raise ValueError(
                f"Нарушена непрерывность часа: bar_index={bar_index}, "
                f"ожидался ts={expected_ts}, получен ts={row['bar_time_ts']}"
            )

        if row["contract"] != first_contract:
            raise ValueError(
                f"Внутри часа найдено смешение контрактов: "
                f"ожидался {first_contract}, получен {row['contract']} на bar_index={bar_index}"
            )

        if row["ask_open"] is None:
            raise ValueError(f"ask_open is NULL на bar_index={bar_index}")

        if row["bid_open"] is None:
            raise ValueError(f"bid_open is NULL на bar_index={bar_index}")

        if row["ask_close"] is None:
            raise ValueError(f"ask_close is NULL на bar_index={bar_index}")

        if row["bid_close"] is None:
            raise ValueError(f"bid_close is NULL на bar_index={bar_index}")


def build_prepared_rows(rows, hour_start_ts, hour_start_text):
    # Строим 720 строк для prepared DB:
    # - y
    # - sum_y
    # - sum_y2
    #
    # Формула:
    # y_i = mid_close_i / mid_open_0 - 1
    #
    # где:
    # mid_open_0  = (ask_open_0  + bid_open_0)  / 2
    # mid_close_i = (ask_close_i + bid_close_i) / 2
    first_row = rows[0]

    mid_open_0 = (first_row["ask_open"] + first_row["bid_open"]) / 2.0

    if mid_open_0 == 0:
        raise ValueError("mid_open_0 == 0, деление невозможно")

    hour_slot = datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).hour
    contract = first_row["contract"]

    prepared_rows = []
    sum_y = 0.0
    sum_y2 = 0.0

    for bar_index, row in enumerate(rows):
        mid_close_i = (row["ask_close"] + row["bid_close"]) / 2.0
        y = (mid_close_i / mid_open_0) - 1.0

        sum_y += y
        sum_y2 += y * y

        prepared_rows.append(
            (
                hour_start_ts,
                hour_start_text,
                hour_slot,
                contract,
                bar_index,
                y,
                sum_y,
                sum_y2,
            )
        )

    return prepared_rows


def save_prepared_rows(prepared_conn, table_name, hour_start_ts, prepared_rows):
    # Полностью пересобираем один час в prepared DB:
    # 1) удаляем старые строки этого часа
    # 2) вставляем заново все 720 строк
    #
    # Всё делаем в одной транзакции.
    delete_sql = get_delete_prepared_hour_sql(table_name)
    insert_sql = get_insert_prepared_quote_sql(table_name)

    prepared_conn.execute("BEGIN")

    try:
        prepared_conn.execute(delete_sql, (hour_start_ts,))
        prepared_conn.executemany(insert_sql, prepared_rows)
        prepared_conn.commit()
    except Exception:
        prepared_conn.rollback()
        raise


def main():
    hour_start_ts = parse_utc_hour_start_text(HOUR_START_TEXT)
    table_name = get_table_name(INSTRUMENT_CODE)

    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"Час UTC: {HOUR_START_TEXT}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print("")

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row

        price_conn.execute("PRAGMA busy_timeout=5000;")
        prepared_conn.execute("PRAGMA busy_timeout=5000;")

        rows = load_price_rows_for_one_hour(price_conn, table_name, hour_start_ts)

        print(f"Прочитано баров из price DB: {len(rows)}")

        validate_price_rows(rows, hour_start_ts)

        prepared_rows = build_prepared_rows(
            rows=rows,
            hour_start_ts=hour_start_ts,
            hour_start_text=HOUR_START_TEXT,
        )

        save_prepared_rows(
            prepared_conn=prepared_conn,
            table_name=table_name,
            hour_start_ts=hour_start_ts,
            prepared_rows=prepared_rows,
        )

        print(f"Записано строк в prepared DB: {len(prepared_rows)}")

        first_prepared_row = prepared_rows[0]
        last_prepared_row = prepared_rows[-1]

        print("")
        print("Проверка результата:")
        print(f"  Первый bar_index: {first_prepared_row[4]}")
        print(f"  Первый y:         {first_prepared_row[5]}")
        print(f"  Первый sum_y:     {first_prepared_row[6]}")
        print(f"  Первый sum_y2:    {first_prepared_row[7]}")
        print("")
        print(f"  Последний bar_index: {last_prepared_row[4]}")
        print(f"  Последний y:         {last_prepared_row[5]}")
        print(f"  Последний sum_y:     {last_prepared_row[6]}")
        print(f"  Последний sum_y2:    {last_prepared_row[7]}")
        print("")
        print("Готово.")

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()
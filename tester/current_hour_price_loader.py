import sqlite3
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows


def open_price_connection(price_db_path: str | Path):
    conn = sqlite3.connect(f"file:{Path(price_db_path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def load_current_hour_price_rows(
        price_conn,
        table_name: str,
        current_hour_start_ts: int,
):
    """
    Низкоуровневая загрузка одного полного текущего часа из price DB.

    current_hour_start_ts:
    - это старт часа по UTC-оси;
    - именно по bar_time_ts идёт чтение из price DB.

    Что делает:
    - одним запросом читает все бары часа;
    - прогоняет их через ту же валидацию, что используется при prepared-builder;
    - возвращает список sqlite3.Row.
    """

    rows = load_price_rows_for_one_hour(
        price_conn=price_conn,
        table_name=table_name,
        hour_start_ts=current_hour_start_ts,
    )

    validate_price_rows(
        rows=rows,
        hour_start_ts=current_hour_start_ts,
    )

    return rows


if __name__ == "__main__":
    price_db_path = settings_live.price_db_path

    instrument_code = "MNQ"
    current_hour_start_ts = 1775635200  # UTC start of hour

    instrument_row = Instrument[instrument_code]
    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    conn = open_price_connection(price_db_path)

    try:
        rows = load_current_hour_price_rows(
            price_conn=conn,
            table_name=table_name,
            current_hour_start_ts=current_hour_start_ts,
        )

        print(f"instrument_code = {instrument_code}")
        print(f"table_name = {table_name}")
        print(f"current_hour_start_ts = {current_hour_start_ts}")
        print(f"rows_count = {len(rows)}")

        if rows:
            first_row = rows[0]
            last_row = rows[-1]

            print("first row:")
            print(f"  bar_time_ts = {first_row['bar_time_ts']}")
            print(f"  bar_time = {first_row['bar_time']}")
            print(f"  bar_time_ts_ct = {first_row['bar_time_ts_ct']}")
            print(f"  bar_time_ct = {first_row['bar_time_ct']}")
            print(f"  contract = {first_row['contract']}")
            print(f"  ask_open = {first_row['ask_open']}")
            print(f"  bid_open = {first_row['bid_open']}")
            print(f"  ask_close = {first_row['ask_close']}")
            print(f"  bid_close = {first_row['bid_close']}")

            print("last row:")
            print(f"  bar_time_ts = {last_row['bar_time_ts']}")
            print(f"  bar_time = {last_row['bar_time']}")
            print(f"  bar_time_ts_ct = {last_row['bar_time_ts_ct']}")
            print(f"  bar_time_ct = {last_row['bar_time_ct']}")
            print(f"  contract = {last_row['contract']}")
            print(f"  ask_open = {last_row['ask_open']}")
            print(f"  bid_open = {last_row['bid_open']}")
            print(f"  ask_close = {last_row['ask_close']}")
            print(f"  bid_close = {last_row['bid_close']}")
    finally:
        conn.close()

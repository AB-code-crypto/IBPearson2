import sqlite3
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from core.db_sql import select_prepared_rows_by_slots_sql
from ts.prepared_reader import build_prepared_hour_payload
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS


def load_prepared_candidate_hours(
        prepared_conn,
        table_name: str,
        current_hour_start_ts_ct: int,
        strategy_params=DEFAULT_STRATEGY_PARAMS,
) -> list[dict]:
    """
    Низкоуровневая bulk-загрузка historical prepared-candidates
    для рассматриваемого текущего часа.

    Вход:
    - prepared_conn: открытое sqlite3-соединение к prepared DB
    - table_name: prepared-таблица нужного инструмента
    - current_hour_start_ts_ct: старт текущего рассматриваемого часа по CT-оси

    Что делает:
    - сам вычисляет current_hour_slot_ct из current_hour_start_ts_ct
    - сам получает allowed_hour_slots_ct через strategy_params
    - одним SQL-запросом читает все historical hours по этим slot-ам
    - берёт только часы строго раньше current_hour_start_ts_ct
    - группирует строки по hour_start_ts
    - возвращает список prepared-hour payload
    """

    current_hour_slot_ct = (current_hour_start_ts_ct // 3600) % 24
    allowed_hour_slots_ct = strategy_params.resolve_allowed_hour_slots(current_hour_slot_ct)

    if not allowed_hour_slots_ct:
        return []

    sql = select_prepared_rows_by_slots_sql(
        table_name=table_name,
        slot_count=len(allowed_hour_slots_ct),
    )

    params = [
        *allowed_hour_slots_ct,
        current_hour_start_ts_ct,
        current_hour_start_ts_ct,
    ]

    rows = prepared_conn.execute(sql, params).fetchall()

    hours = []
    current_hour_start_ts = None
    current_rows = []

    for row in rows:
        row_hour_start_ts = row["hour_start_ts"]

        if current_hour_start_ts is None:
            current_hour_start_ts = row_hour_start_ts

        if row_hour_start_ts != current_hour_start_ts:
            hours.append(build_prepared_hour_payload(current_rows))
            current_hour_start_ts = row_hour_start_ts
            current_rows = []

        current_rows.append(row)

    if current_rows:
        hours.append(build_prepared_hour_payload(current_rows))

    return hours


def open_prepared_connection(prepared_db_path: str | Path):
    conn = sqlite3.connect(f"file:{Path(prepared_db_path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


if __name__ == "__main__":
    prepared_db_path = settings_live.prepared_db_path

    instrument_code = "MNQ"
    current_hour_start_ts_ct = 1775617200

    instrument_row = Instrument[instrument_code]
    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    conn = open_prepared_connection(prepared_db_path)

    try:
        prepared_hours = load_prepared_candidate_hours(
            prepared_conn=conn,
            table_name=table_name,
            current_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        current_hour_slot_ct = (current_hour_start_ts_ct // 3600) % 24
        allowed_hour_slots_ct = DEFAULT_STRATEGY_PARAMS.resolve_allowed_hour_slots(
            current_hour_slot_ct
        )

        print(f"instrument_code = {instrument_code}")
        print(f"table_name = {table_name}")
        print(f"current_hour_start_ts_ct = {current_hour_start_ts_ct}")
        print(f"current_hour_slot_ct = {current_hour_slot_ct}")
        print(f"allowed_hour_slots_ct = {allowed_hour_slots_ct}")
        print(f"candidate_hours_count = {len(prepared_hours)}")

        if prepared_hours:
            first_item = prepared_hours[0]
            print("first candidate:")
            print(f"  hour_start_ts = {first_item['hour_start_ts']}")
            print(f"  hour_start_ts_ct = {first_item['hour_start_ts_ct']}")
            print(f"  hour_start_ct = {first_item['hour_start_ct']}")
            print(f"  hour_slot_ct = {first_item['hour_slot_ct']}")
            print(f"  y_len = {len(first_item['y'])}")
            print(f"  y[:5] = {first_item['y'][:5]}")
    finally:
        conn.close()

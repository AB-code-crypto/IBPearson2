from core.db_sql import (
    select_prepared_rows_by_slots_sql,
)


def build_prepared_hour_payload(rows):
    # Преобразуем строки одного prepared-часа
    # в удобную Python-структуру.
    #
    # Возвращаем dict:
    # {
    #     "hour_start_ts": ...,
    #     "hour_start": ...,
    #     "hour_slot": ...,
    #     "contract": ...,
    #     "y": [...],
    #     "sum_y": [...],
    #     "sum_y2": [...],
    # }
    if not rows:
        raise ValueError("Нельзя построить prepared hour payload: rows пустой")

    first_row = rows[0]

    payload = {
        "hour_start_ts": first_row["hour_start_ts"],
        "hour_start_ts_ct": first_row["hour_start_ts_ct"],
        "hour_start_ct": first_row["hour_start_ct"],
        "hour_slot_ct": first_row["hour_slot_ct"],
        "contract": first_row["contract"],
        "y": [],
        "sum_y": [],
        "sum_y2": [],
    }

    for row in rows:
        payload["y"].append(row["y"])
        payload["sum_y"].append(row["sum_y"])
        payload["sum_y2"].append(row["sum_y2"])

    return payload


def load_prepared_hours_by_slots(prepared_conn, table_name, hour_slots_ct, before_hour_start_ts_ct=None):
    # Загружаем все prepared-часы для списка CT-slot одним SQL-запросом
    # и группируем их в Python по hour_start_ts.
    if not hour_slots_ct:
        return []

    sql = select_prepared_rows_by_slots_sql(
        table_name=table_name,
        slot_count=len(hour_slots_ct),
    )

    params = [*hour_slots_ct, before_hour_start_ts_ct, before_hour_start_ts_ct]

    cursor = prepared_conn.execute(sql, params)
    rows = cursor.fetchall()

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

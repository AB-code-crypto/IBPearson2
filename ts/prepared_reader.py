from core.db_sql import (
    select_prepared_hour_rows_sql,
    select_prepared_hour_headers_by_slot_sql,
    select_prepared_rows_by_slot_sql,
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
        "hour_start": first_row["hour_start"],
        "hour_slot": first_row["hour_slot"],
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


def load_prepared_hour_rows(prepared_conn, table_name, hour_start_ts):
    # Загружаем все строки одного prepared-часа.
    sql = select_prepared_hour_rows_sql(table_name)

    cursor = prepared_conn.execute(sql, (hour_start_ts,))
    rows = cursor.fetchall()

    return rows


def load_prepared_hour(prepared_conn, table_name, hour_start_ts):
    # Загружаем один prepared-час целиком и возвращаем
    # готовую Python-структуру.
    rows = load_prepared_hour_rows(
        prepared_conn=prepared_conn,
        table_name=table_name,
        hour_start_ts=hour_start_ts,
    )

    payload = build_prepared_hour_payload(rows)

    return payload


def load_prepared_hour_headers_by_slot(prepared_conn, table_name, hour_slot, before_hour_start_ts=None):
    # Возвращаем список заголовков prepared-часов для указанного hour_slot.
    #
    # before_hour_start_ts удобно использовать в runtime:
    # например, брать только часы строго раньше текущего часа.
    sql = select_prepared_hour_headers_by_slot_sql(table_name)

    cursor = prepared_conn.execute(
        sql,
        (hour_slot, before_hour_start_ts, before_hour_start_ts),
    )

    rows = cursor.fetchall()

    headers = []

    for row in rows:
        headers.append(
            {
                "hour_start_ts": row["hour_start_ts"],
                "hour_start": row["hour_start"],
                "hour_slot": row["hour_slot"],
                "contract": row["contract"],
            }
        )

    return headers


def load_prepared_hours_by_slot(prepared_conn, table_name, hour_slot, before_hour_start_ts=None):
    # Загружаем все prepared-часы указанного hour_slot одним SQL-запросом
    # и группируем их в Python по hour_start_ts.
    sql = select_prepared_rows_by_slot_sql(table_name)

    cursor = prepared_conn.execute(
        sql,
        (hour_slot, before_hour_start_ts, before_hour_start_ts),
    )

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

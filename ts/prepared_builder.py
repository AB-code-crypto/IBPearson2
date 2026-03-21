from datetime import datetime, timezone

from core.db_sql import (
    count_prepared_hour_rows_sql,
    delete_prepared_hour_sql,
    insert_prepared_quote_sql,
)


def hour_start_text_from_ts(hour_start_ts):
    # Преобразуем UTC timestamp начала часа в текстовый формат,
    # который используем в prepared DB.
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def hour_slot_from_ts(hour_start_ts):
    # Номер часа суток в UTC: 0..23
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).hour


def load_price_rows_for_one_hour(price_conn, table_name, hour_start_ts):
    # Читаем из ценовой БД все 5-секундные бары одного часа.
    #
    # Берём только те поля, которые реально нужны для подготовки
    # исторического часа в prepared DB.
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
    # Проверяем, что исторический час пригоден для подготовки.
    #
    # Жёсткие требования:
    # - ровно 720 баров;
    # - каждый бар стоит на ожидаемом timestamp с шагом 5 секунд;
    # - внутри часа один и тот же contract;
    # - ask_open, bid_open, ask_close, bid_close не NULL.
    if len(rows) != 720:
        raise ValueError(
            f"Ожидалось 720 баров за час, получено: {len(rows)}"
        )

    first_contract = rows[0]["contract"]

    for bar_index, row in enumerate(rows):
        expected_ts = hour_start_ts + bar_index * 5

        if row["bar_time_ts"] != expected_ts:
            raise ValueError(
                f"Нарушена непрерывность часа: "
                f"bar_index={bar_index}, ожидался ts={expected_ts}, "
                f"получен ts={row['bar_time_ts']}"
            )

        if row["contract"] != first_contract:
            raise ValueError(
                f"Внутри часа найдено смешение контрактов: "
                f"ожидался {first_contract}, получен {row['contract']} "
                f"на bar_index={bar_index}"
            )

        if row["ask_open"] is None:
            raise ValueError(f"ask_open is NULL на bar_index={bar_index}")

        if row["bid_open"] is None:
            raise ValueError(f"bid_open is NULL на bar_index={bar_index}")

        if row["ask_close"] is None:
            raise ValueError(f"ask_close is NULL на bar_index={bar_index}")

        if row["bid_close"] is None:
            raise ValueError(f"bid_close is NULL на bar_index={bar_index}")


def build_prepared_rows(rows, hour_start_ts):
    # Строим 720 строк для prepared DB.
    #
    # Формула:
    # y_i = mid_close_i / mid_open_0 - 1
    #
    # где:
    # mid_open_0  = (ask_open_0  + bid_open_0)  / 2
    # mid_close_i = (ask_close_i + bid_close_i) / 2
    #
    # На выходе получаем строки для prepared-таблицы:
    # (
    #     hour_start_ts,
    #     hour_start,
    #     hour_slot,
    #     contract,
    #     bar_index,
    #     y,
    #     sum_y,
    #     sum_y2,
    # )
    if not rows:
        raise ValueError("Нельзя построить prepared_rows: список rows пустой")

    first_row = rows[0]

    mid_open_0 = (first_row["ask_open"] + first_row["bid_open"]) / 2.0

    if mid_open_0 == 0:
        raise ValueError("mid_open_0 == 0, деление невозможно")

    hour_start_text = hour_start_text_from_ts(hour_start_ts)
    hour_slot = hour_slot_from_ts(hour_start_ts)
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


def prepared_hour_row_count(prepared_conn, table_name, hour_start_ts):
    # Возвращаем, сколько строк уже лежит в prepared DB
    # по указанному историческому часу.
    sql = count_prepared_hour_rows_sql(table_name)

    cursor = prepared_conn.execute(sql, (hour_start_ts,))
    row_count = cursor.fetchone()[0]

    return row_count


def insert_prepared_rows(prepared_conn, table_name, prepared_rows):
    # Вставляем новый prepared-час без предварительного удаления.
    #
    # Используется в сценариях:
    # - массовое заполнение по истории;
    # - будущая задача по завершению часа.
    #
    # Ожидаем, что prepared_rows уже подготовлен и в нём ровно 720 строк.
    if not prepared_rows:
        raise ValueError("prepared_rows пустой")

    insert_sql = insert_prepared_quote_sql(table_name)

    prepared_conn.execute("BEGIN")

    try:
        prepared_conn.executemany(insert_sql, prepared_rows)
        prepared_conn.commit()
    except Exception:
        prepared_conn.rollback()
        raise


def replace_prepared_hour(prepared_conn, table_name, hour_start_ts, prepared_rows):
    # Полностью пересобираем один исторический час в prepared DB:
    # 1) удаляем старые строки этого часа
    # 2) вставляем заново все 720 строк
    #
    # Это сервисный режим для разовой утилиты build_prepared_hour.py
    if not prepared_rows:
        raise ValueError("prepared_rows пустой")

    delete_sql = delete_prepared_hour_sql(table_name)
    insert_sql = insert_prepared_quote_sql(table_name)

    prepared_conn.execute("BEGIN")

    try:
        prepared_conn.execute(delete_sql, (hour_start_ts,))
        prepared_conn.executemany(insert_sql, prepared_rows)
        prepared_conn.commit()
    except Exception:
        prepared_conn.rollback()
        raise


def build_prepared_rows_for_one_hour(price_conn, table_name, hour_start_ts):
    # Полный низкоуровневый конвейер для одного исторического часа:
    # - загрузить час из price DB
    # - проверить валидность
    # - построить prepared_rows
    #
    # Возвращает готовые 720 строк для prepared DB.
    rows = load_price_rows_for_one_hour(
        price_conn=price_conn,
        table_name=table_name,
        hour_start_ts=hour_start_ts,
    )

    validate_price_rows(
        rows=rows,
        hour_start_ts=hour_start_ts,
    )

    prepared_rows = build_prepared_rows(
        rows=rows,
        hour_start_ts=hour_start_ts,
    )

    return prepared_rows

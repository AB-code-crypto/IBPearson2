from datetime import datetime, timezone

from core.db_sql import (
    count_prepared_hour_rows_sql,
    delete_prepared_hour_sql,
    insert_prepared_quote_sql,
)

BAR_INTERVAL_SECONDS = 5
ANALYSIS_WINDOW_SECONDS = 3600
ANALYSIS_WINDOW_BAR_COUNT = ANALYSIS_WINDOW_SECONDS // BAR_INTERVAL_SECONDS


def analysis_window_start_text_from_ts(analysis_window_start_ts):
    # UTC-текст старта окна анализа для логов и диагностики.
    return datetime.fromtimestamp(
        analysis_window_start_ts,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S")


def hour_slot_ct_from_ts_ct(analysis_window_start_ts_ct):
    # Номер часа суток на локальной CT-оси проекта: 0..23.
    # Для окна 08:30..09:30 hour_slot_ct = 8.
    return (analysis_window_start_ts_ct // 3600) % 24


def load_price_rows_for_analysis_window(price_conn, table_name, analysis_window_start_ts):
    # Читаем из ценовой БД все 5-секундные бары одного 60-минутного окна анализа.
    #
    # Окно может стартовать как в HH:00, так и в HH:30.
    # Берём только поля, которые нужны для подготовки historical candidate.
    analysis_window_end_ts = analysis_window_start_ts + ANALYSIS_WINDOW_SECONDS

    sql = f"""
    SELECT
        bar_time_ts,
        bar_time,
        bar_time_ts_ct,
        bar_time_ct,
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

    cursor = price_conn.execute(sql, (analysis_window_start_ts, analysis_window_end_ts))
    return cursor.fetchall()


def validate_analysis_window_price_rows(rows, analysis_window_start_ts):
    # Проверяем, что historical analysis window пригодно для prepared DB.
    #
    # Жёсткие требования:
    # - ровно 720 баров;
    # - каждый бар стоит на ожидаемом timestamp с шагом 5 секунд;
    # - внутри окна один и тот же contract;
    # - ask_open, bid_open, ask_close, bid_close не NULL.
    if len(rows) != ANALYSIS_WINDOW_BAR_COUNT:
        raise ValueError(
            f"Ожидалось {ANALYSIS_WINDOW_BAR_COUNT} баров за окно анализа, "
            f"получено: {len(rows)}"
        )

    first_contract = rows[0]["contract"]

    for bar_index, row in enumerate(rows):
        expected_ts = analysis_window_start_ts + bar_index * BAR_INTERVAL_SECONDS

        if row["bar_time_ts"] != expected_ts:
            raise ValueError(
                f"Нарушена непрерывность окна анализа: "
                f"bar_index={bar_index}, ожидался ts={expected_ts}, "
                f"получен ts={row['bar_time_ts']}"
            )

        if row["contract"] != first_contract:
            raise ValueError(
                f"Внутри окна анализа найдено смешение контрактов: "
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


def build_prepared_rows(rows, analysis_window_start_ts):
    # Строим 720 строк для prepared DB.
    #
    # Формула:
    # y_i = mid_close_i / mid_open_0 - 1
    #
    # В prepared DB храним:
    # - hour_start_ts     : UTC start окна анализа;
    # - hour_start_ts_ct  : CT-axis start окна анализа;
    # - hour_start_ct     : человекочитаемый CT start окна анализа;
    # - hour_slot_ct      : номер часа суток в CT для start окна анализа;
    # - y / sum_y / sum_y2.
    #
    # Названия колонок hour_* оставлены как текущая схема БД.
    # По смыслу это теперь start 60-минутного analysis window.
    if not rows:
        raise ValueError("Нельзя построить prepared_rows: список rows пустой")

    first_row = rows[0]
    mid_open_0 = (first_row["ask_open"] + first_row["bid_open"]) / 2.0

    if mid_open_0 == 0:
        raise ValueError("mid_open_0 == 0, деление невозможно")

    analysis_window_start_ts_ct = first_row["bar_time_ts_ct"]
    analysis_window_start_ct = first_row["bar_time_ct"]
    hour_slot_ct = hour_slot_ct_from_ts_ct(analysis_window_start_ts_ct)
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
                analysis_window_start_ts,
                analysis_window_start_ts_ct,
                analysis_window_start_ct,
                hour_slot_ct,
                contract,
                bar_index,
                y,
                sum_y,
                sum_y2,
            )
        )

    return prepared_rows


def prepared_analysis_window_row_count(prepared_conn, table_name, analysis_window_start_ts):
    # Возвращает, сколько строк уже лежит в prepared DB
    # по указанному start окна анализа.
    sql = count_prepared_hour_rows_sql(table_name)

    cursor = prepared_conn.execute(sql, (analysis_window_start_ts,))
    return cursor.fetchone()[0]


def insert_prepared_rows(prepared_conn, table_name, prepared_rows):
    # Вставляем новое prepared-окно без предварительного удаления.
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


def replace_prepared_analysis_window(
        prepared_conn,
        table_name,
        analysis_window_start_ts,
        prepared_rows,
):
    # Полностью пересобираем одно prepared-окно в prepared DB:
    # 1) удаляем старые строки этого окна;
    # 2) вставляем заново все 720 строк.
    if not prepared_rows:
        raise ValueError("prepared_rows пустой")

    delete_sql = delete_prepared_hour_sql(table_name)
    insert_sql = insert_prepared_quote_sql(table_name)

    prepared_conn.execute("BEGIN")

    try:
        prepared_conn.execute(delete_sql, (analysis_window_start_ts,))
        prepared_conn.executemany(insert_sql, prepared_rows)
        prepared_conn.commit()
    except Exception:
        prepared_conn.rollback()
        raise


def build_prepared_rows_for_analysis_window(price_conn, table_name, analysis_window_start_ts):
    # Полный низкоуровневый конвейер для одного historical analysis window:
    # - загрузить 60 минут из price DB;
    # - проверить валидность;
    # - построить prepared_rows.
    #
    # Возвращает готовые 720 строк для prepared DB.
    rows = load_price_rows_for_analysis_window(
        price_conn=price_conn,
        table_name=table_name,
        analysis_window_start_ts=analysis_window_start_ts,
    )

    validate_analysis_window_price_rows(
        rows=rows,
        analysis_window_start_ts=analysis_window_start_ts,
    )

    return build_prepared_rows(
        rows=rows,
        analysis_window_start_ts=analysis_window_start_ts,
    )

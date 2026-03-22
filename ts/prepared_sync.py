import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import (
    build_prepared_rows_for_one_hour,
    hour_start_text_from_ts,
    insert_prepared_rows,
    prepared_hour_row_count,
)


@dataclass
class PreparedSyncStats:
    # Краткая сводка одного прохода синхронизации prepared DB.
    instrument_code: str
    table_name: str
    window_start_ts: Optional[int]
    window_end_ts: Optional[int]
    candidate_hours: int
    inserted_hours: int
    skipped_existing_hours: int
    skipped_invalid_hours: int



def floor_to_hour_ts(ts):
    # Округляем Unix timestamp вниз до точного начала часа.
    return (ts // 3600) * 3600



def load_candidate_hour_starts(price_conn, table_name, start_hour_ts=None, end_hour_ts=None):
    # Загружаем список всех candidate hour_start_ts, где в price DB
    # есть хотя бы один бар.
    #
    # start_hour_ts и end_hour_ts трактуются как границы по bar_time_ts:
    # - start_hour_ts включительно
    # - end_hour_ts не включительно
    #
    # Такой подход удобен и для ручной массовой подготовки, и для фоновой
    # синхронизации последних закрытых часов.
    sql = f"""
    SELECT DISTINCT
        CAST(bar_time_ts / 3600 AS INTEGER) * 3600 AS hour_start_ts
    FROM {table_name}
    WHERE (? IS NULL OR bar_time_ts >= ?)
      AND (? IS NULL OR bar_time_ts < ?)
    ORDER BY hour_start_ts
    ;
    """

    cursor = price_conn.execute(
        sql,
        (
            start_hour_ts, start_hour_ts,
            end_hour_ts, end_hour_ts,
        )
    )

    rows = cursor.fetchall()

    return [row[0] for row in rows]



def build_recent_closed_hours_window(now_ts, lookback_days):
    # Строим окно последних закрытых часов.
    #
    # Важное правило:
    # текущий открытый час в prepared DB не готовим.
    #
    # Поэтому правая граница окна - это начало текущего часа UTC.
    # Она не включается в SQL-фильтр.
    current_hour_start_ts = floor_to_hour_ts(now_ts)
    start_hour_ts = current_hour_start_ts - (lookback_days * 24 * 3600)
    end_hour_ts = current_hour_start_ts

    return start_hour_ts, end_hour_ts



def sync_prepared_hours_for_range(
    settings,
    instrument_code,
    start_hour_ts=None,
    end_hour_ts=None,
    verbose=False,
):
    # Массовая синхронизация prepared DB по заданному окну.
    #
    # Логика очень простая:
    # 1) находим candidate-часы в price DB;
    # 2) если часа уже нетронуто и полностью нет в prepared DB - строим его;
    # 3) если час уже есть целиком - пропускаем;
    # 4) если час невалиден - пропускаем.
    instrument_row = Instrument[instrument_code]

    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row

        price_conn.execute("PRAGMA busy_timeout=5000;")
        prepared_conn.execute("PRAGMA busy_timeout=5000;")

        candidate_hour_starts = load_candidate_hour_starts(
            price_conn=price_conn,
            table_name=table_name,
            start_hour_ts=start_hour_ts,
            end_hour_ts=end_hour_ts,
        )

        inserted_hours = 0
        skipped_existing_hours = 0
        skipped_invalid_hours = 0

        total_hours = len(candidate_hour_starts)

        for index, hour_start_ts in enumerate(candidate_hour_starts, start=1):
            hour_start_text = hour_start_text_from_ts(hour_start_ts)

            existing_row_count = prepared_hour_row_count(
                prepared_conn=prepared_conn,
                table_name=table_name,
                hour_start_ts=hour_start_ts,
            )

            if existing_row_count == 720:
                skipped_existing_hours += 1

                if verbose:
                    print(
                        f"[{index}/{total_hours}] "
                        f"{hour_start_text} UTC -> уже есть в prepared DB, пропускаю"
                    )

                continue

            if existing_row_count != 0:
                raise ValueError(
                    f"В prepared DB найден частично записанный час: "
                    f"{hour_start_text} UTC, row_count={existing_row_count}. "
                    f"Ожидалось либо 0, либо 720."
                )

            try:
                prepared_rows = build_prepared_rows_for_one_hour(
                    price_conn=price_conn,
                    table_name=table_name,
                    hour_start_ts=hour_start_ts,
                )
            except ValueError as exc:
                skipped_invalid_hours += 1

                if verbose:
                    print(
                        f"[{index}/{total_hours}] "
                        f"{hour_start_text} UTC -> пропускаю, час невалиден: {exc}"
                    )

                continue

            insert_prepared_rows(
                prepared_conn=prepared_conn,
                table_name=table_name,
                prepared_rows=prepared_rows,
            )

            inserted_hours += 1

            if verbose:
                print(
                    f"[{index}/{total_hours}] "
                    f"{hour_start_text} UTC -> вставлено 720 строк"
                )

        return PreparedSyncStats(
            instrument_code=instrument_code,
            table_name=table_name,
            window_start_ts=start_hour_ts,
            window_end_ts=end_hour_ts,
            candidate_hours=total_hours,
            inserted_hours=inserted_hours,
            skipped_existing_hours=skipped_existing_hours,
            skipped_invalid_hours=skipped_invalid_hours,
        )

    finally:
        price_conn.close()
        prepared_conn.close()



def sync_recent_prepared_hours(
    settings,
    instrument_code,
    lookback_days=31,
    now_ts=None,
    verbose=False,
):
    # Удобная обёртка для боевого режима.
    #
    # Берём только последние lookback_days дней и только уже закрытые часы.
    if now_ts is None:
        now_ts = int(datetime.now(timezone.utc).timestamp())

    start_hour_ts, end_hour_ts = build_recent_closed_hours_window(
        now_ts=now_ts,
        lookback_days=lookback_days,
    )

    return sync_prepared_hours_for_range(
        settings=settings,
        instrument_code=instrument_code,
        start_hour_ts=start_hour_ts,
        end_hour_ts=end_hour_ts,
        verbose=verbose,
    )

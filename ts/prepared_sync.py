import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import (
    analysis_window_start_text_from_ts,
    build_prepared_rows_for_analysis_window,
    insert_prepared_rows,
    prepared_analysis_window_row_count,
)

HALF_HOUR_SECONDS = 1800
ANALYSIS_WINDOW_SECONDS = 3600
PREPARED_WINDOW_ROW_COUNT = 720


@dataclass
class PreparedSyncStats:
    # Краткая сводка одного прохода синхронизации prepared DB.
    instrument_code: str
    table_name: str
    window_start_ts: Optional[int]
    window_end_ts: Optional[int]
    candidate_windows: int
    inserted_windows: int
    skipped_existing_windows: int
    skipped_invalid_windows: int


def floor_to_half_hour_ts(ts):
    # Округляем Unix timestamp вниз до точной 30-минутной границы.
    return (ts // HALF_HOUR_SECONDS) * HALF_HOUR_SECONDS


def load_candidate_analysis_window_starts(
        price_conn,
        table_name,
        start_analysis_window_ts=None,
        end_analysis_window_ts=None,
):
    # Загружаем список всех candidate analysis_window_start_ts,
    # где в price DB есть хотя бы один бар соответствующего 30-минутного bucket.
    #
    # Дальше каждое окно жёстко валидируется на наличие полных 720 баров.
    #
    # start_analysis_window_ts и end_analysis_window_ts задают диапазон именно
    # для start окна анализа:
    # - start включительно;
    # - end не включительно.
    sql = f"""
    SELECT DISTINCT
        CAST(bar_time_ts / {HALF_HOUR_SECONDS} AS INTEGER) * {HALF_HOUR_SECONDS}
            AS analysis_window_start_ts
    FROM {table_name}
    WHERE (? IS NULL OR bar_time_ts >= ?)
      AND (? IS NULL OR bar_time_ts < ?)
    ORDER BY analysis_window_start_ts
    ;
    """

    cursor = price_conn.execute(
        sql,
        (
            start_analysis_window_ts,
            start_analysis_window_ts,
            end_analysis_window_ts,
            end_analysis_window_ts,
        )
    )

    rows = cursor.fetchall()
    return [row[0] for row in rows]


def build_recent_closed_analysis_windows_range(now_ts, lookback_days):
    # Строим диапазон последних закрытых 60-минутных окон анализа.
    #
    # Важное правило:
    # текущее ещё не закрытое analysis window в prepared DB не готовим.
    #
    # Последнее закрытое окно анализа имеет start:
    #   floor_to_half_hour(now_ts) - 3600
    #
    # Так как правая граница диапазона exclusive, используем:
    #   floor_to_half_hour(now_ts) - 1800
    current_half_hour_start_ts = floor_to_half_hour_ts(now_ts)
    end_analysis_window_ts = current_half_hour_start_ts - HALF_HOUR_SECONDS
    start_analysis_window_ts = end_analysis_window_ts - (lookback_days * 24 * 3600)

    return start_analysis_window_ts, end_analysis_window_ts


def sync_prepared_analysis_windows_for_range(
        settings,
        instrument_code,
        start_analysis_window_ts=None,
        end_analysis_window_ts=None,
        verbose=False,
):
    # Массовая синхронизация prepared DB по заданному диапазону 60-минутных окон.
    #
    # Окна стартуют каждые 30 минут:
    # - HH:00..HH+1:00;
    # - HH:30..HH+1:30.
    #
    # Логика:
    # 1) находим candidate analysis windows в price DB;
    # 2) если окна ещё нет в prepared DB — строим его;
    # 3) если окно уже есть целиком — пропускаем;
    # 4) если окно невалидно — пропускаем.
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

        candidate_window_starts = load_candidate_analysis_window_starts(
            price_conn=price_conn,
            table_name=table_name,
            start_analysis_window_ts=start_analysis_window_ts,
            end_analysis_window_ts=end_analysis_window_ts,
        )

        inserted_windows = 0
        skipped_existing_windows = 0
        skipped_invalid_windows = 0

        total_windows = len(candidate_window_starts)

        for index, analysis_window_start_ts in enumerate(candidate_window_starts, start=1):
            analysis_window_start_text = analysis_window_start_text_from_ts(
                analysis_window_start_ts
            )

            existing_row_count = prepared_analysis_window_row_count(
                prepared_conn=prepared_conn,
                table_name=table_name,
                analysis_window_start_ts=analysis_window_start_ts,
            )

            if existing_row_count == PREPARED_WINDOW_ROW_COUNT:
                skipped_existing_windows += 1

                if verbose:
                    print(
                        f"[{index}/{total_windows}] "
                        f"{analysis_window_start_text} UTC -> уже есть в prepared DB, пропускаю"
                    )

                continue

            if existing_row_count != 0:
                raise ValueError(
                    f"В prepared DB найдено частично записанное окно анализа: "
                    f"{analysis_window_start_text} UTC, row_count={existing_row_count}. "
                    f"Ожидалось либо 0, либо {PREPARED_WINDOW_ROW_COUNT}."
                )

            try:
                prepared_rows = build_prepared_rows_for_analysis_window(
                    price_conn=price_conn,
                    table_name=table_name,
                    analysis_window_start_ts=analysis_window_start_ts,
                )
            except ValueError as exc:
                skipped_invalid_windows += 1

                if verbose:
                    print(
                        f"[{index}/{total_windows}] "
                        f"{analysis_window_start_text} UTC -> пропускаю, окно невалидно: {exc}"
                    )

                continue

            insert_prepared_rows(
                prepared_conn=prepared_conn,
                table_name=table_name,
                prepared_rows=prepared_rows,
            )

            inserted_windows += 1

            if verbose:
                print(
                    f"[{index}/{total_windows}] "
                    f"{analysis_window_start_text} UTC -> вставлено "
                    f"{PREPARED_WINDOW_ROW_COUNT} строк"
                )

        return PreparedSyncStats(
            instrument_code=instrument_code,
            table_name=table_name,
            window_start_ts=start_analysis_window_ts,
            window_end_ts=end_analysis_window_ts,
            candidate_windows=total_windows,
            inserted_windows=inserted_windows,
            skipped_existing_windows=skipped_existing_windows,
            skipped_invalid_windows=skipped_invalid_windows,
        )

    finally:
        price_conn.close()
        prepared_conn.close()


def sync_recent_prepared_analysis_windows(
        settings,
        instrument_code,
        lookback_days=31,
        now_ts=None,
        verbose=False,
):
    # Удобная обёртка для боевого режима.
    #
    # Берём последние lookback_days дней и только уже закрытые 60-минутные
    # analysis windows со стартом на HH:00 или HH:30.
    if now_ts is None:
        now_ts = int(datetime.now(timezone.utc).timestamp())

    start_analysis_window_ts, end_analysis_window_ts = build_recent_closed_analysis_windows_range(
        now_ts=now_ts,
        lookback_days=lookback_days,
    )

    return sync_prepared_analysis_windows_for_range(
        settings=settings,
        instrument_code=instrument_code,
        start_analysis_window_ts=start_analysis_window_ts,
        end_analysis_window_ts=end_analysis_window_ts,
        verbose=verbose,
    )

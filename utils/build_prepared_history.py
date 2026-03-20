import sqlite3
from datetime import datetime, timezone

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from core.prepared_builder import (
    build_prepared_rows_for_one_hour,
    hour_start_text_from_ts,
    insert_prepared_rows,
    prepared_hour_row_count,
)


# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Оба ограничения необязательные.
# Если None - ограничение не применяется.
#
# Это начало часа в UTC.
START_HOUR_TEXT = None
END_HOUR_TEXT = None


def parse_optional_utc_hour_start_text(hour_start_text):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в UTC timestamp начала часа.
    #
    # Если передан None, возвращаем None.
    if hour_start_text is None:
        return None

    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"Ожидалось точное начало часа, получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def load_candidate_hour_starts(price_conn, table_name, start_hour_ts, end_hour_ts):
    # Загружаем список всех candidate hour_start_ts, где в ценовой БД
    # есть хотя бы один бар.
    #
    # Берём DISTINCT по началу часа, вычисленному из bar_time_ts.
    # Это лучше, чем слепо идти по каждому часу от min до max:
    # мы сразу пропускаем полностью пустые часы.
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


def main():
    instrument_row = Instrument[INSTRUMENT_CODE]

    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    start_hour_ts = parse_optional_utc_hour_start_text(START_HOUR_TEXT)
    end_hour_ts = parse_optional_utc_hour_start_text(END_HOUR_TEXT)

    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print(f"START_HOUR_TEXT: {START_HOUR_TEXT}")
    print(f"END_HOUR_TEXT:   {END_HOUR_TEXT}")
    print("")

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

        total_hours = len(candidate_hour_starts)

        print(f"Найдено candidate-часов в price DB: {total_hours}")
        print("")

        inserted_hours = 0
        skipped_existing_hours = 0
        skipped_invalid_hours = 0

        for index, hour_start_ts in enumerate(candidate_hour_starts, start=1):
            hour_start_text = hour_start_text_from_ts(hour_start_ts)

            existing_row_count = prepared_hour_row_count(
                prepared_conn=prepared_conn,
                table_name=table_name,
                hour_start_ts=hour_start_ts,
            )

            if existing_row_count == 720:
                skipped_existing_hours += 1
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

            print(
                f"[{index}/{total_hours}] "
                f"{hour_start_text} UTC -> вставлено 720 строк"
            )

        print("")
        print("ИТОГ:")
        print(f"  candidate-часов:          {total_hours}")
        print(f"  вставлено новых часов:    {inserted_hours}")
        print(f"  уже существовало часов:   {skipped_existing_hours}")
        print(f"  невалидных часов:         {skipped_invalid_hours}")
        print("")

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()
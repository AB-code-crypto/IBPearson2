'''
ручной скрипт для пересборки одного конкретного исторического часа в prepared DB. Берёт выбранный час из price DB, строит prepared-строки через
build_prepared_rows_for_one_hour, затем полностью заменяет этот час в prepared DB и печатает краткую проверку результата.
'''

import sqlite3
from datetime import datetime, timezone

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import (
    build_prepared_rows_for_one_hour,
    replace_prepared_hour,
)

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Начало исторического часа в UTC.
HOUR_START_TEXT = "2026-03-19 14:00:00"


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


def main():
    instrument_row = Instrument[INSTRUMENT_CODE]

    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    hour_start_ts = parse_utc_hour_start_text(HOUR_START_TEXT)

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

        prepared_rows = build_prepared_rows_for_one_hour(
            price_conn=price_conn,
            table_name=table_name,
            hour_start_ts=hour_start_ts,
        )

        replace_prepared_hour(
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

import sqlite3
from datetime import datetime, timezone

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_time import resolve_allowed_hour_slots

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"
HOUR_SLOT = 14

# Если нужно, можно ограничить только историей строго раньше этого часа.
# Формат: "YYYY-MM-DD HH:MM:SS"
# Если None - ограничение не применяется.
BEFORE_HOUR_START_TEXT = None


def parse_optional_utc_hour_start_text(hour_start_text):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в UTC timestamp начала часа.
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


def print_hour_summary(prefix, hour_payload):
    # Короткая печать одного prepared-часа.
    print(f"{prefix}:")
    print(f"  hour_start_ts: {hour_payload['hour_start_ts']}")
    print(f"  hour_start:    {hour_payload['hour_start']} UTC")
    print(f"  hour_slot:     {hour_payload['hour_slot']}")
    print(f"  contract:      {hour_payload['contract']}")
    print(f"  len(y):        {len(hour_payload['y'])}")
    print(f"  len(sum_y):    {len(hour_payload['sum_y'])}")
    print(f"  len(sum_y2):   {len(hour_payload['sum_y2'])}")

    print("  y[0:3]:")
    for value in hour_payload["y"][0:3]:
        print(f"    {value}")

    print("  y[-3:]:")
    for value in hour_payload["y"][-3:]:
        print(f"    {value}")

    print("  sum_y[0:3]:")
    for value in hour_payload["sum_y"][0:3]:
        print(f"    {value}")

    print("  sum_y[-3:]:")
    for value in hour_payload["sum_y"][-3:]:
        print(f"    {value}")

    print("  sum_y2[0:3]:")
    for value in hour_payload["sum_y2"][0:3]:
        print(f"    {value}")

    print("  sum_y2[-3:]:")
    for value in hour_payload["sum_y2"][-3:]:
        print(f"    {value}")

    print("")


def main():
    instrument_row = Instrument[INSTRUMENT_CODE]

    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    before_hour_start_ts = parse_optional_utc_hour_start_text(BEFORE_HOUR_START_TEXT)
    allowed_hour_slots = resolve_allowed_hour_slots(HOUR_SLOT)

    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print(f"Текущий hour_slot: {HOUR_SLOT}")
    print(f"Разрешённые hour_slot: {allowed_hour_slots}")
    print(f"BEFORE_HOUR_START_TEXT: {BEFORE_HOUR_START_TEXT}")
    print("")

    conn = sqlite3.connect(settings.prepared_db_path)

    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000;")

        hours = load_prepared_hours_by_slots(
            prepared_conn=conn,
            table_name=table_name,
            hour_slots=allowed_hour_slots,
            before_hour_start_ts=before_hour_start_ts,
        )

        print(f"Найдено prepared-часов: {len(hours)}")
        print("")

        if not hours:
            print("По заданной группе hour_slot ничего не найдено.")
            return

        first_hour = hours[0]
        last_hour = hours[-1]

        print_hour_summary("Первый prepared-час", first_hour)
        print_hour_summary("Последний prepared-час", last_hour)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

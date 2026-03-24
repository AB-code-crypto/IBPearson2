"""
ручной скрипт для массового заполнения prepared DB по заданному диапазону часов.
Работает через общий sync-механизм: ищет candidate-часы в price DB, вставляет
только отсутствующие prepared-часы, пропускает уже существующие и отдельно
считает невалидные часы.

Prepared DB теперь хранит CT-поля часа:
- hour_start_ts_ct
- hour_start_ct
- hour_slot_ct

При этом диапазон разового запуска здесь по-прежнему задаётся в UTC,
потому что технический якорь prepared-часа остаётся hour_start_ts.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import settings_live as settings
from ts.prepared_sync import sync_prepared_hours_for_range

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Оба ограничения необязательные.
# Если None - ограничение не применяется.
#
# Здесь указываем именно начало часа в UTC.
START_HOUR_TEXT = None
END_HOUR_TEXT = None

CHICAGO_TZ = ZoneInfo("America/Chicago")


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


def format_utc_hour_start_ts(hour_start_ts):
    # Преобразуем UTC timestamp начала часа в читаемый UTC-текст.
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_ct_hour_start_ts_from_utc(hour_start_ts):
    # Преобразуем UTC timestamp начала часа в читаемый CT-текст.
    dt_utc = datetime.fromtimestamp(hour_start_ts, tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CHICAGO_TZ)
    return dt_ct.strftime("%Y-%m-%d %H:%M:%S")


def print_range_line(label, hour_start_ts):
    # Печатаем одну границу диапазона сразу в UTC и CT.
    if hour_start_ts is None:
        print(f"{label}: None")
        return

    print(
        f"{label}: "
        f"UTC={format_utc_hour_start_ts(hour_start_ts)} | "
        f"CT={format_ct_hour_start_ts_from_utc(hour_start_ts)}"
    )


def main():
    start_hour_ts = parse_optional_utc_hour_start_text(START_HOUR_TEXT)
    end_hour_ts = parse_optional_utc_hour_start_text(END_HOUR_TEXT)

    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print_range_line("START_HOUR_TEXT", start_hour_ts)
    print_range_line("END_HOUR_TEXT", end_hour_ts)
    print("")

    stats = sync_prepared_hours_for_range(
        settings=settings,
        instrument_code=INSTRUMENT_CODE,
        start_hour_ts=start_hour_ts,
        end_hour_ts=end_hour_ts,
        verbose=True,
    )

    print("")
    print("ИТОГ:")
    print(f"  candidate-часов:          {stats.candidate_hours}")
    print(f"  вставлено новых часов:    {stats.inserted_hours}")
    print(f"  уже существовало часов:   {stats.skipped_existing_hours}")
    print(f"  невалидных часов:         {stats.skipped_invalid_hours}")
    print("")


if __name__ == "__main__":
    main()
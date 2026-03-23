'''
ручной скрипт для массового заполнения prepared DB по заданному диапазону часов. Работает через общий sync-механизм: ищет candidate-часы в price DB, вставляет
только отсутствующие prepared-часы, пропускает уже существующие и отдельно считает невалидные часы.
'''
from datetime import datetime, timezone

from config import settings_live as settings
from ts.prepared_sync import sync_prepared_hours_for_range

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


def main():
    start_hour_ts = parse_optional_utc_hour_start_text(START_HOUR_TEXT)
    end_hour_ts = parse_optional_utc_hour_start_text(END_HOUR_TEXT)

    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print(f"START_HOUR_TEXT: {START_HOUR_TEXT}")
    print(f"END_HOUR_TEXT:   {END_HOUR_TEXT}")
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

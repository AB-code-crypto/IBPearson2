"""
Единый служебный скрипт для работы с prepared DB.

Prepared DB теперь хранит не "часы" в старом смысле, а 60-минутные
analysis windows, которые могут стартовать каждые 30 минут:

- HH:00 .. HH+1:00
- HH:30 .. HH+1:30

Каждое окно анализа по-прежнему содержит 720 строк по 5 секунд.
Названия колонок в БД остаются hour_* по текущей схеме, но по смыслу
это start 60-минутного analysis window.

Режимы
------

1. inspect_reader
   Проверяет чтение prepared DB через load_prepared_hours_by_slots(...).

   Можно выбрать:
   - CT-hour slot;
   - offset старта analysis window:
       0    -> только HH:00 windows;
       1800 -> только HH:30 windows;
       None -> все offsets;
   - ограничение "строго раньше" заданного момента на CT-оси.

2. sync_range
   Массово заполняет prepared DB по диапазону analysis-window starts.

   Работает через sync_prepared_analysis_windows_for_range(...):
   - находит candidate analysis windows в price DB;
   - для каждого окна проверяет, есть ли уже 720 строк в prepared DB;
   - строит отсутствующие окна;
   - пропускает уже готовые;
   - отдельно считает невалидные окна.

   Этот режим нужен для полной пересборки prepared DB после удаления старой БД.

3. rebuild_one_window
   Точечно пересобирает одно 60-минутное analysis window.

   Старт окна задаётся в UTC и может быть только на HH:00 или HH:30.

Эксплуатационная последовательность
-----------------------------------

1. Сначала price DB должна быть в порядке.
2. Потом можно удалить старую prepared.sqlite3.
3. Затем запустить этот скрипт в режиме sync_range.
4. После сборки проверить prepared DB через inspect_reader по offset 0 и 1800.
"""

import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name, initialize_prepared_database
from ts.prepared_builder import (
    build_prepared_rows_for_analysis_window,
    replace_prepared_analysis_window,
)
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.prepared_sync import sync_prepared_analysis_windows_for_range
from ts.ts_time import resolve_allowed_hour_slots

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

# Режимы:
# - "inspect_reader"
# - "sync_range"
# - "rebuild_one_window"
PREPARED_TOOL_MODE = "sync_range"

INSTRUMENT_CODE = "MNQ"

# ------------------------------------------------------------
# Режим inspect_reader
# ------------------------------------------------------------

# CT-hour slot старта analysis window.
# Например:
# - для окна 08:00..09:00 HOUR_SLOT_CT = 8, offset = 0;
# - для окна 08:30..09:30 HOUR_SLOT_CT = 8, offset = 1800.
HOUR_SLOT_CT = 8

# Какие соседние CT-hour slots разрешены стратегией для поиска исторических окон.
# Обычно оставляем через resolve_allowed_hour_slots(HOUR_SLOT_CT).
USE_ALLOWED_HOUR_SLOT_GROUP = True

# Фильтр по offset старта analysis window:
# - 0    -> только HH:00..HH+1:00;
# - 1800 -> только HH:30..HH+1:30;
# - None -> не фильтровать по offset.
ANALYSIS_WINDOW_START_OFFSET_SECONDS = 0

# Ограничение истории: читать только окна строго раньше этого момента на CT-оси.
# Формат: "YYYY-MM-DD HH:MM:SS" в CT-axis проекта.
# Допускается только точная 30-минутная граница: HH:00:00 или HH:30:00.
# Если None - ограничение не применяется.
BEFORE_ANALYSIS_WINDOW_START_CT_TEXT = None

# ------------------------------------------------------------
# Режим sync_range
# ------------------------------------------------------------

# Диапазон start analysis windows в UTC.
# Оба ограничения необязательные.
# Формат: "YYYY-MM-DD HH:MM:SS" UTC.
# Допускается только точная 30-минутная граница: HH:00:00 или HH:30:00.
#
# Если None - ограничение не применяется.
#
# При полной пересборке prepared DB после удаления файла можно оставить None/None.
SYNC_START_ANALYSIS_WINDOW_TEXT_UTC = None
SYNC_END_ANALYSIS_WINDOW_TEXT_UTC = None

# ------------------------------------------------------------
# Режим rebuild_one_window
# ------------------------------------------------------------

# Старт 60-минутного analysis window в UTC.
# Может быть HH:00:00 или HH:30:00.
REBUILD_ANALYSIS_WINDOW_START_TEXT_UTC = "2026-03-19 14:00:00"

CHICAGO_TZ = ZoneInfo("America/Chicago")

HALF_HOUR_SECONDS = 1800
ANALYSIS_WINDOW_SECONDS = 3600
EXPECTED_PREPARED_ROWS = 720


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================


def get_instrument_row(instrument_code):
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    return Instrument[instrument_code]


def build_instrument_table_name(instrument_code):
    instrument_row = get_instrument_row(instrument_code)

    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )


def ensure_half_hour_boundary(dt, source_text):
    if dt.minute not in (0, 30) or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            "Ожидалась точная 30-минутная граница HH:00:00 или HH:30:00, "
            f"получено: {source_text}"
        )


def parse_optional_ct_analysis_window_start_text(text_ct):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в timestamp локальной CT-оси проекта.
    # Это именно CT-axis timestamp, поэтому timezone.utc здесь используется как
    # техническая числовая ось проекта, а не как реальный UTC-момент.
    if text_ct is None:
        return None

    dt = datetime.strptime(text_ct, "%Y-%m-%d %H:%M:%S")
    ensure_half_hour_boundary(dt, text_ct)

    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def parse_optional_utc_analysis_window_start_text(text_utc):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в UTC timestamp старта analysis window.
    if text_utc is None:
        return None

    dt = datetime.strptime(text_utc, "%Y-%m-%d %H:%M:%S")
    ensure_half_hour_boundary(dt, text_utc)

    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def parse_required_utc_analysis_window_start_text(text_utc):
    analysis_window_start_ts = parse_optional_utc_analysis_window_start_text(text_utc)

    if analysis_window_start_ts is None:
        raise ValueError("Требуется указать start analysis window, но получено None")

    return analysis_window_start_ts


def format_utc_ts(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_ct_ts_from_utc(ts):
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CHICAGO_TZ)
    return dt_ct.strftime("%Y-%m-%d %H:%M:%S")


def offset_text(offset_seconds):
    if offset_seconds is None:
        return "ALL"
    if offset_seconds == 0:
        return "HH:00"
    if offset_seconds == HALF_HOUR_SECONDS:
        return "HH:30"
    return str(offset_seconds)


def print_range_line(label, ts):
    if ts is None:
        print(f"{label}: None")
        return

    print(
        f"{label}: "
        f"UTC={format_utc_ts(ts)} | "
        f"CT={format_ct_ts_from_utc(ts)}"
    )


def print_prepared_window_summary(prefix, payload):
    print(f"{prefix}:")
    print(f"  analysis_start_ts:     {payload['hour_start_ts']}")
    print(f"  analysis_start_ts_ct:  {payload['hour_start_ts_ct']}")
    print(f"  analysis_start_ct:     {payload['hour_start_ct']} CT")
    print(f"  analysis_offset_sec:   {payload['hour_start_ts_ct'] % 3600}")
    print(f"  hour_slot_ct:          {payload['hour_slot_ct']}")
    print(f"  contract:              {payload['contract']}")
    print(f"  len(y):                {len(payload['y'])}")
    print(f"  len(sum_y):            {len(payload['sum_y'])}")
    print(f"  len(sum_y2):           {len(payload['sum_y2'])}")

    print("  y[0:3]:")
    for value in payload["y"][0:3]:
        print(f"    {value}")

    print("  y[-3:]:")
    for value in payload["y"][-3:]:
        print(f"    {value}")

    print("  sum_y[0:3]:")
    for value in payload["sum_y"][0:3]:
        print(f"    {value}")

    print("  sum_y[-3:]:")
    for value in payload["sum_y"][-3:]:
        print(f"    {value}")

    print("  sum_y2[0:3]:")
    for value in payload["sum_y2"][0:3]:
        print(f"    {value}")

    print("  sum_y2[-3:]:")
    for value in payload["sum_y2"][-3:]:
        print(f"    {value}")

    print("")


def print_prepared_db_offset_stats(table_name):
    conn = sqlite3.connect(settings.prepared_db_path)

    try:
        rows = conn.execute(
            f"""
            SELECT
                hour_start_ts_ct % 3600 AS offset_seconds,
                COUNT(DISTINCT hour_start_ts) AS windows_count,
                COUNT(*) AS rows_count
            FROM {table_name}
            GROUP BY hour_start_ts_ct % 3600
            ORDER BY offset_seconds
            ;
            """
        ).fetchall()

        print("Статистика prepared DB по offset старта окна анализа:")
        if not rows:
            print("  prepared DB пуста")
            return

        for offset_seconds, windows_count, rows_count in rows:
            print(
                f"  offset={offset_seconds:4d} sec ({offset_text(offset_seconds)}): "
                f"windows={windows_count}, rows={rows_count}"
            )
    finally:
        conn.close()


# ============================================================
# ПОДГОТОВКА PREPARED DB
# ============================================================


def ensure_prepared_database_initialized():
    """Гарантируем, что prepared DB и её таблицы созданы штатной проектной инициализацией."""
    initialize_prepared_database(settings)


# ============================================================
# РЕЖИМ inspect_reader
# ============================================================


def run_inspect_reader_mode():
    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    ensure_prepared_database_initialized()

    before_analysis_window_start_ts_ct = parse_optional_ct_analysis_window_start_text(
        BEFORE_ANALYSIS_WINDOW_START_CT_TEXT
    )

    if USE_ALLOWED_HOUR_SLOT_GROUP:
        hour_slots_ct = resolve_allowed_hour_slots(HOUR_SLOT_CT)
    else:
        hour_slots_ct = [HOUR_SLOT_CT]

    print("Режим: inspect_reader")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print(f"Текущий HOUR_SLOT_CT: {HOUR_SLOT_CT}")
    print(f"USE_ALLOWED_HOUR_SLOT_GROUP: {USE_ALLOWED_HOUR_SLOT_GROUP}")
    print(f"Читаемые hour_slot_ct: {hour_slots_ct}")
    print(
        "ANALYSIS_WINDOW_START_OFFSET_SECONDS: "
        f"{ANALYSIS_WINDOW_START_OFFSET_SECONDS} "
        f"({offset_text(ANALYSIS_WINDOW_START_OFFSET_SECONDS)})"
    )
    print(f"BEFORE_ANALYSIS_WINDOW_START_CT_TEXT: {BEFORE_ANALYSIS_WINDOW_START_CT_TEXT}")
    print("")

    conn = sqlite3.connect(settings.prepared_db_path)

    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000;")

        windows = load_prepared_hours_by_slots(
            prepared_conn=conn,
            table_name=table_name,
            hour_slots_ct=hour_slots_ct,
            before_hour_start_ts_ct=before_analysis_window_start_ts_ct,
            analysis_window_start_offset_seconds=ANALYSIS_WINDOW_START_OFFSET_SECONDS,
        )

        print(f"Найдено prepared analysis windows: {len(windows)}")
        print("")

        if not windows:
            print("По заданным фильтрам ничего не найдено.")
            return

        print_prepared_window_summary("Первое prepared analysis window", windows[0])
        print_prepared_window_summary("Последнее prepared analysis window", windows[-1])

    finally:
        conn.close()

    print_prepared_db_offset_stats(table_name)


# ============================================================
# РЕЖИМ sync_range
# ============================================================


def run_sync_range_mode():
    start_ts = parse_optional_utc_analysis_window_start_text(
        SYNC_START_ANALYSIS_WINDOW_TEXT_UTC
    )
    end_ts = parse_optional_utc_analysis_window_start_text(
        SYNC_END_ANALYSIS_WINDOW_TEXT_UTC
    )

    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    ensure_prepared_database_initialized()

    print("Режим: sync_range")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print_range_line("SYNC_START_ANALYSIS_WINDOW_TEXT_UTC", start_ts)
    print_range_line("SYNC_END_ANALYSIS_WINDOW_TEXT_UTC", end_ts)
    print("")

    stats = sync_prepared_analysis_windows_for_range(
        settings=settings,
        instrument_code=INSTRUMENT_CODE,
        start_analysis_window_ts=start_ts,
        end_analysis_window_ts=end_ts,
        verbose=True,
    )

    print("")
    print("ИТОГ:")
    print(f"  candidate windows:          {stats.candidate_windows}")
    print(f"  вставлено новых windows:    {stats.inserted_windows}")
    print(f"  уже существовало windows:   {stats.skipped_existing_windows}")
    print(f"  невалидных windows:         {stats.skipped_invalid_windows}")
    print("")

    print_prepared_db_offset_stats(table_name)


# ============================================================
# РЕЖИМ rebuild_one_window
# ============================================================


def run_rebuild_one_window_mode():
    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    ensure_prepared_database_initialized()

    analysis_window_start_ts = parse_required_utc_analysis_window_start_text(
        REBUILD_ANALYSIS_WINDOW_START_TEXT_UTC
    )

    print("Режим: rebuild_one_window")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print_range_line("REBUILD_ANALYSIS_WINDOW_START_TEXT_UTC", analysis_window_start_ts)
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print("")

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row

        price_conn.execute("PRAGMA busy_timeout=5000;")
        prepared_conn.execute("PRAGMA busy_timeout=5000;")

        prepared_rows = build_prepared_rows_for_analysis_window(
            price_conn=price_conn,
            table_name=table_name,
            analysis_window_start_ts=analysis_window_start_ts,
        )

        replace_prepared_analysis_window(
            prepared_conn=prepared_conn,
            table_name=table_name,
            analysis_window_start_ts=analysis_window_start_ts,
            prepared_rows=prepared_rows,
        )

        print(f"Записано строк в prepared DB: {len(prepared_rows)}")

        if len(prepared_rows) != EXPECTED_PREPARED_ROWS:
            raise ValueError(
                f"Ожидалось {EXPECTED_PREPARED_ROWS} prepared rows, "
                f"получено {len(prepared_rows)}"
            )

        first_prepared_row = prepared_rows[0]
        last_prepared_row = prepared_rows[-1]

        # Формат prepared-строки:
        # 0  hour_start_ts
        # 1  hour_start_ts_ct
        # 2  hour_start_ct
        # 3  hour_slot_ct
        # 4  contract
        # 5  bar_index
        # 6  y
        # 7  sum_y
        # 8  sum_y2

        print("")
        print("Проверка результата:")
        print(f"  analysis_start_ts:    {first_prepared_row[0]}")
        print(f"  analysis_start_ts_ct: {first_prepared_row[1]}")
        print(f"  analysis_start_ct:    {first_prepared_row[2]}")
        print(f"  analysis_offset_sec:  {first_prepared_row[1] % 3600}")
        print(f"  hour_slot_ct:         {first_prepared_row[3]}")
        print(f"  contract:             {first_prepared_row[4]}")
        print("")
        print(f"  Первый bar_index:     {first_prepared_row[5]}")
        print(f"  Первый y:             {first_prepared_row[6]}")
        print(f"  Первый sum_y:         {first_prepared_row[7]}")
        print(f"  Первый sum_y2:        {first_prepared_row[8]}")
        print("")
        print(f"  Последний bar_index:  {last_prepared_row[5]}")
        print(f"  Последний y:          {last_prepared_row[6]}")
        print(f"  Последний sum_y:      {last_prepared_row[7]}")
        print(f"  Последний sum_y2:     {last_prepared_row[8]}")
        print("")
        print("Готово.")

    finally:
        price_conn.close()
        prepared_conn.close()

    print_prepared_db_offset_stats(table_name)


# ============================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================


def main():
    if PREPARED_TOOL_MODE == "inspect_reader":
        run_inspect_reader_mode()
        return

    if PREPARED_TOOL_MODE == "sync_range":
        run_sync_range_mode()
        return

    if PREPARED_TOOL_MODE == "rebuild_one_window":
        run_rebuild_one_window_mode()
        return

    raise ValueError(
        "Неподдерживаемый PREPARED_TOOL_MODE. "
        "Ожидалось: inspect_reader, sync_range, rebuild_one_window"
    )


if __name__ == "__main__":
    main()

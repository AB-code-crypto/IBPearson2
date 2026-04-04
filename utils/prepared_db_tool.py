"""
Единый служебный скрипт для работы с prepared DB.

Зачем нужен
-----------
Этот файл объединяет три разовых утилиты, которые раньше были разнесены по
отдельным скриптам:

1. проверка чтения prepared DB через prepared_reader;
2. массовая синхронизация prepared DB по диапазону исторических часов;
3. точечная пересборка одного конкретного prepared-часа.

Идея объединения простая:
все три сценария относятся к одному и тому же слою проекта — prepared DB,
поэтому удобнее держать их в одном инструменте с явным режимом запуска,
чем поддерживать несколько почти одинаковых файлов с повторяющимися импортами,
настройками и форматированием вывода.

Какие режимы есть
-----------------
Скрипт поддерживает три режима, задаваемых через PREPARED_TOOL_MODE:

1. inspect_reader
   Диагностический режим.
   Загружает prepared-часы по группе CT-slot через load_prepared_hours_by_slots,
   при необходимости ограничивает историю часами строго раньше указанного
   момента на CT-оси и печатает краткую сводку по найденным данным.

   Этот режим нужен, когда надо быстро понять:
   - читается ли prepared DB вообще;
   - какие CT-slot реально попадают в поиск;
   - есть ли исторические prepared-часы до заданного момента;
   - как выглядят первый и последний найденный prepared-час.

2. sync_range
   Режим массового заполнения prepared DB.
   Работает через общий sync-механизм sync_prepared_hours_for_range(...),
   который:
   - находит candidate-часы в price DB;
   - проверяет, есть ли они уже в prepared DB;
   - вставляет только отсутствующие prepared-часы;
   - пропускает уже существующие;
   - отдельно считает невалидные часы, которые нельзя подготовить.

   Этот режим нужен после загрузки истории, после ремонта price DB,
   после миграций или просто когда надо дозаполнить prepared DB по диапазону.

3. rebuild_one_hour
   Точечная пересборка одного конкретного исторического часа.
   Скрипт:
   - берёт выбранный UTC-час из price DB;
   - строит prepared-строки через build_prepared_rows_for_one_hour(...);
   - полностью заменяет этот час в prepared DB через replace_prepared_hour(...);
   - печатает краткую проверку результата.

   Этот режим нужен, когда проблема локализована в одном конкретном часу
   и нет смысла пересинхронизировать целый диапазон.

Почему здесь одновременно есть UTC и CT
---------------------------------------
Технический якорь prepared-часа в проекте — это hour_start_ts в UTC.
Поэтому диапазоны sync_range и rebuild_one_hour задаются именно через UTC.

Но сама стратегия живёт на CT-логике, и prepared DB хранит дополнительные CT-поля:
- hour_start_ts_ct
- hour_start_ct
- hour_slot_ct

Поэтому в выводе скрипт показывает и UTC, и CT там, где это полезно,
а в inspect_reader есть отдельный параметр BEFORE_HOUR_START_CT_TEXT,
который задаётся именно в CT — потому что он относится к фильтрации истории
на CT-оси проекта.

Что скрипт НЕ делает
--------------------
Скрипт не работает с price DB как с ремонтом котировок, не чинит дырки и не
докачивает историю из брокера. Его зона ответственности — только prepared DB.

То есть последовательность эксплуатации обычно такая:
1. сначала приводим в порядок price DB;
2. потом этим скриптом проверяем или пересобираем prepared DB.

Итог
----
Этот файл — единая точка обслуживания prepared DB.
Он покрывает три базовых рабочих сценария:
- посмотреть, что reader реально читает;
- массово синхронизировать prepared DB по диапазону;
- точечно пересобрать один выбранный prepared-час.
"""

import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import (
    build_prepared_rows_for_one_hour,
    replace_prepared_hour,
)
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.prepared_sync import sync_prepared_hours_for_range
from ts.ts_time import resolve_allowed_hour_slots


# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

# Режимы:
# - "inspect_reader"
# - "sync_range"
# - "rebuild_one_hour"
PREPARED_TOOL_MODE = "inspect_reader"

INSTRUMENT_CODE = "MNQ"

# ------------------------------------------------------------
# Режим inspect_reader
# ------------------------------------------------------------
HOUR_SLOT_CT = 8

# Если нужно, можно ограничить только историей строго раньше этого часа на CT-оси.
# Формат: "YYYY-MM-DD HH:MM:SS" в CT.
# Если None - ограничение не применяется.
BEFORE_HOUR_START_CT_TEXT = None

# ------------------------------------------------------------
# Режим sync_range
# ------------------------------------------------------------
# Оба ограничения необязательные.
# Если None - ограничение не применяется.
#
# Здесь указываем именно начало часа в UTC.
SYNC_START_HOUR_TEXT_UTC = None
SYNC_END_HOUR_TEXT_UTC = None

# ------------------------------------------------------------
# Режим rebuild_one_hour
# ------------------------------------------------------------
# Начало исторического часа в UTC.
REBUILD_HOUR_START_TEXT_UTC = "2026-03-19 14:00:00"

CHICAGO_TZ = ZoneInfo("America/Chicago")


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



def parse_optional_ct_hour_start_text(hour_start_text_ct):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в timestamp локальной CT-оси проекта.
    # Если передан None, возвращаем None.
    if hour_start_text_ct is None:
        return None

    dt = datetime.strptime(hour_start_text_ct, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"Ожидалось точное начало часа, получено: {hour_start_text_ct}"
        )

    return int(dt.timestamp())



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



def parse_required_utc_hour_start_text(hour_start_text):
    hour_start_ts = parse_optional_utc_hour_start_text(hour_start_text)

    if hour_start_ts is None:
        raise ValueError("Требуется указать час, но получено None")

    return hour_start_ts



def format_utc_hour_start_ts(hour_start_ts):
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")



def format_ct_hour_start_ts_from_utc(hour_start_ts):
    dt_utc = datetime.fromtimestamp(hour_start_ts, tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CHICAGO_TZ)
    return dt_ct.strftime("%Y-%m-%d %H:%M:%S")



def print_range_line(label, hour_start_ts):
    if hour_start_ts is None:
        print(f"{label}: None")
        return

    print(
        f"{label}: "
        f"UTC={format_utc_hour_start_ts(hour_start_ts)} | "
        f"CT={format_ct_hour_start_ts_from_utc(hour_start_ts)}"
    )



def print_prepared_hour_summary(prefix, hour_payload):
    print(f"{prefix}:")
    print(f"  hour_start_ts:     {hour_payload['hour_start_ts']}")
    print(f"  hour_start_ts_ct:  {hour_payload['hour_start_ts_ct']}")
    print(f"  hour_start_ct:     {hour_payload['hour_start_ct']} CT")
    print(f"  hour_slot_ct:      {hour_payload['hour_slot_ct']}")
    print(f"  contract:          {hour_payload['contract']}")
    print(f"  len(y):            {len(hour_payload['y'])}")
    print(f"  len(sum_y):        {len(hour_payload['sum_y'])}")
    print(f"  len(sum_y2):       {len(hour_payload['sum_y2'])}")

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


# ============================================================
# РЕЖИМ inspect_reader
# ============================================================


def run_inspect_reader_mode():
    instrument_row = get_instrument_row(INSTRUMENT_CODE)
    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    before_hour_start_ts_ct = parse_optional_ct_hour_start_text(BEFORE_HOUR_START_CT_TEXT)
    allowed_hour_slots = resolve_allowed_hour_slots(HOUR_SLOT_CT)

    print(f"Режим: inspect_reader")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print(f"Текущий hour_slot_ct: {HOUR_SLOT_CT}")
    print(f"Разрешённые hour_slot_ct: {allowed_hour_slots}")
    print(f"BEFORE_HOUR_START_CT_TEXT: {BEFORE_HOUR_START_CT_TEXT}")
    print("")

    conn = sqlite3.connect(settings.prepared_db_path)

    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000;")

        hours = load_prepared_hours_by_slots(
            prepared_conn=conn,
            table_name=table_name,
            hour_slots_ct=allowed_hour_slots,
            before_hour_start_ts_ct=before_hour_start_ts_ct,
        )

        print(f"Найдено prepared-часов: {len(hours)}")
        print("")

        if not hours:
            print("По заданной группе hour_slot_ct ничего не найдено.")
            return

        first_hour = hours[0]
        last_hour = hours[-1]

        print_prepared_hour_summary("Первый prepared-час", first_hour)
        print_prepared_hour_summary("Последний prepared-час", last_hour)

    finally:
        conn.close()


# ============================================================
# РЕЖИМ sync_range
# ============================================================


def run_sync_range_mode():
    start_hour_ts = parse_optional_utc_hour_start_text(SYNC_START_HOUR_TEXT_UTC)
    end_hour_ts = parse_optional_utc_hour_start_text(SYNC_END_HOUR_TEXT_UTC)

    print(f"Режим: sync_range")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"price DB: {settings.price_db_path}")
    print(f"prepared DB: {settings.prepared_db_path}")
    print_range_line("SYNC_START_HOUR_TEXT_UTC", start_hour_ts)
    print_range_line("SYNC_END_HOUR_TEXT_UTC", end_hour_ts)
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


# ============================================================
# РЕЖИМ rebuild_one_hour
# ============================================================


def run_rebuild_one_hour_mode():
    instrument_row = get_instrument_row(INSTRUMENT_CODE)
    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    hour_start_ts = parse_required_utc_hour_start_text(REBUILD_HOUR_START_TEXT_UTC)
    hour_start_ct = format_ct_hour_start_ts_from_utc(hour_start_ts)

    print(f"Режим: rebuild_one_hour")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Таблица: {table_name}")
    print(f"Час UTC: {REBUILD_HOUR_START_TEXT_UTC}")
    print(f"Час CT:  {hour_start_ct}")
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
        print(f"  hour_start_ts:    {first_prepared_row[0]}")
        print(f"  hour_start_ts_ct: {first_prepared_row[1]}")
        print(f"  hour_start_ct:    {first_prepared_row[2]}")
        print(f"  hour_slot_ct:     {first_prepared_row[3]}")
        print(f"  contract:         {first_prepared_row[4]}")
        print("")
        print(f"  Первый bar_index:    {first_prepared_row[5]}")
        print(f"  Первый y:            {first_prepared_row[6]}")
        print(f"  Первый sum_y:        {first_prepared_row[7]}")
        print(f"  Первый sum_y2:       {first_prepared_row[8]}")
        print("")
        print(f"  Последний bar_index: {last_prepared_row[5]}")
        print(f"  Последний y:         {last_prepared_row[6]}")
        print(f"  Последний sum_y:     {last_prepared_row[7]}")
        print(f"  Последний sum_y2:    {last_prepared_row[8]}")
        print("")
        print("Готово.")

    finally:
        price_conn.close()
        prepared_conn.close()


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

    if PREPARED_TOOL_MODE == "rebuild_one_hour":
        run_rebuild_one_hour_mode()
        return

    raise ValueError(
        "Неподдерживаемый PREPARED_TOOL_MODE. "
        "Ожидалось: inspect_reader, sync_range, rebuild_one_hour"
    )


if __name__ == "__main__":
    main()

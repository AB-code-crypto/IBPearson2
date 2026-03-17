import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ============================================================================
# НАСТРОЙКИ СКРИПТА
# ============================================================================
# Скрипт предполагается запускать из папки utils.
# Поэтому путь до корня проекта вычисляем относительно самого файла.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# База данных, в которой ищем дыры.
DB_PATH = PROJECT_ROOT / "data" / "price.sqlite3"

# Таблица, в которой ищем дыры.
TABLE_NAME = "MNQ_5s"

# Интервал поиска дыр в UTC.
#
# Если обе границы равны None, то ищем дыры по всей доступной истории таблицы.
# Если заданы обе границы, то ищем только внутри них.
# При этом отсутствие данных между левой границей и первой свечой,
# а также между последней свечой и правой границей, тоже считается дыркой.
#
# Поддерживаемые форматы:
# - "YYYY-MM-DD HH:MM:SS"
# - "YYYY-MM-DDTHH:MM:SSZ"

SEARCH_FROM_UTC = None
SEARCH_TO_UTC = None
# SEARCH_FROM_UTC = "2024-12-31 21:00:00"
# SEARCH_TO_UTC = "2026-03-16 11:00:00"

# Для 5-секундных данных ожидаем строго такой шаг между соседними свечами.
EXPECTED_STEP_SECONDS = 5


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def format_utc_ts(timestamp_utc):
    # Преобразуем unix timestamp в читаемую строку UTC.
    dt = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_utc_to_ts(value):
    # Преобразуем строку времени в unix timestamp UTC.
    #
    # Если значение не передано, возвращаем None.
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1]

    # Пробуем сначала ISO-формат через fromisoformat().
    # Он понимает и вариант с пробелом, и вариант с буквой T.
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = None

    if dt is None:
        raise ValueError(
            f"Не удалось разобрать дату '{value}'. "
            f"Ожидаю формат YYYY-MM-DD HH:MM:SS или YYYY-MM-DDTHH:MM:SSZ"
        )

    # Все даты в проекте считаем UTC+0.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp())


def validate_search_bounds(search_from_ts, search_to_ts):
    # Проверяем корректность переданных границ поиска.
    if search_from_ts is None and search_to_ts is None:
        return

    if search_from_ts is None or search_to_ts is None:
        raise ValueError(
            "Для интервального поиска нужно задать обе границы: "
            "SEARCH_FROM_UTC и SEARCH_TO_UTC"
        )

    if search_from_ts > search_to_ts:
        raise ValueError("SEARCH_FROM_UTC не может быть больше SEARCH_TO_UTC")


def ensure_table_exists(conn, table_name):
    # Проверяем, что нужная таблица существует в базе.
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
    row = conn.execute(sql, (table_name,)).fetchone()

    if row is None:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе {DB_PATH}")


def fetch_history_bounds(conn, table_name):
    # Получаем минимальный и максимальный timestamp, доступный в таблице.
    sql = f"SELECT MIN(bar_time_ts), MAX(bar_time_ts) FROM {table_name}"
    row = conn.execute(sql).fetchone()

    min_ts = row[0]
    max_ts = row[1]
    return min_ts, max_ts


def build_rows_query(table_name, search_from_ts, search_to_ts):
    # Собираем SQL для чтения отсортированных timestamp свечей.
    #
    # Нам нужен только bar_time_ts. Этого достаточно,
    # чтобы проверить непрерывность 5-секундной истории.
    if search_from_ts is None and search_to_ts is None:
        return f"SELECT bar_time_ts FROM {table_name} ORDER BY bar_time_ts ASC", ()

    sql = (
        f"SELECT bar_time_ts FROM {table_name} "
        f"WHERE bar_time_ts >= ? AND bar_time_ts <= ? "
        f"ORDER BY bar_time_ts ASC"
    )
    return sql, (search_from_ts, search_to_ts)


def print_gap(gap_number, gap_start_ts, gap_end_ts, reason):
    # Печатаем найденную дырку на экран.
    #
    # В случае нормальной дырки внутри истории gap_start_ts и gap_end_ts
    # задают уже именно отсутствующий диапазон.
    #
    # В случае аномальной последовательности reason будет содержать пояснение.
    missing_seconds = gap_end_ts - gap_start_ts + EXPECTED_STEP_SECONDS
    missing_bars = missing_seconds // EXPECTED_STEP_SECONDS

    print(
        f"[{gap_number}] Найдена дырка: "
        f"{format_utc_ts(gap_start_ts)} -> {format_utc_ts(gap_end_ts)} | "
        f"пропущено баров: {missing_bars} | причина: {reason}"
    )


# ============================================================================
# ОСНОВНАЯ ЛОГИКА ПОИСКА ДЫРОК
# ============================================================================
def find_gaps_in_rows(sorted_timestamps, search_from_ts, search_to_ts):
    # Ищем дыры в уже отсортированном списке timestamp-ов.
    #
    # Возвращаем количество найденных дыр.
    gap_count = 0

    if not sorted_timestamps:
        # Если ищем по всей доступной истории и в таблице нет строк,
        # то просто сообщаем, что данных нет.
        if search_from_ts is None and search_to_ts is None:
            print("В таблице нет данных. Проверять нечего.")
            return gap_count

        # Если был задан конкретный интервал, а данных в нём нет,
        # то весь интервал считаем одной большой дыркой.
        gap_count += 1
        print_gap(
            gap_number=gap_count,
            gap_start_ts=search_from_ts,
            gap_end_ts=search_to_ts,
            reason="в выбранном интервале нет ни одной свечи",
        )
        return gap_count

    first_ts = sorted_timestamps[0]
    last_ts = sorted_timestamps[-1]

    # Проверяем левую границу пользовательского интервала.
    if search_from_ts is not None and first_ts > search_from_ts:
        gap_count += 1
        print_gap(
            gap_number=gap_count,
            gap_start_ts=search_from_ts,
            gap_end_ts=first_ts - EXPECTED_STEP_SECONDS,
            reason="нет данных между левой границей поиска и первой свечой",
        )

    # Проверяем внутренние разрывы между соседними свечами.
    previous_ts = first_ts

    for current_ts in sorted_timestamps[1:]:
        delta_seconds = current_ts - previous_ts

        if delta_seconds != EXPECTED_STEP_SECONDS:
            # Для нормальной дырки missing range начинается после previous_ts
            # и заканчивается перед current_ts.
            gap_start_ts = previous_ts + EXPECTED_STEP_SECONDS
            gap_end_ts = current_ts - EXPECTED_STEP_SECONDS

            gap_count += 1

            if delta_seconds > EXPECTED_STEP_SECONDS:
                reason = (
                    f"между соседними свечами разница {delta_seconds} сек, "
                    f"ожидалось {EXPECTED_STEP_SECONDS} сек"
                )
            else:
                reason = (
                    f"аномальная последовательность: между соседними свечами разница "
                    f"{delta_seconds} сек, ожидалось {EXPECTED_STEP_SECONDS} сек"
                )

            # Если delta < 5, нормального отсутствующего диапазона уже нет.
            # Но сам факт аномалии всё равно выводим.
            if gap_start_ts <= gap_end_ts:
                print_gap(
                    gap_number=gap_count,
                    gap_start_ts=gap_start_ts,
                    gap_end_ts=gap_end_ts,
                    reason=reason,
                )
            else:
                print(
                    f"[{gap_count}] Найдена аномалия последовательности: "
                    f"prev={format_utc_ts(previous_ts)}, "
                    f"curr={format_utc_ts(current_ts)}, "
                    f"delta={delta_seconds} сек | причина: {reason}"
                )

        previous_ts = current_ts

    # Проверяем правую границу пользовательского интервала.
    if search_to_ts is not None and last_ts < search_to_ts:
        gap_count += 1
        print_gap(
            gap_number=gap_count,
            gap_start_ts=last_ts + EXPECTED_STEP_SECONDS,
            gap_end_ts=search_to_ts,
            reason="нет данных между последней свечой и правой границей поиска",
        )

    return gap_count


def main():
    search_from_ts = parse_utc_to_ts(SEARCH_FROM_UTC)
    search_to_ts = parse_utc_to_ts(SEARCH_TO_UTC)
    validate_search_bounds(search_from_ts, search_to_ts)

    print(f"База данных: {DB_PATH}")
    print(f"Таблица: {TABLE_NAME}")

    if search_from_ts is None and search_to_ts is None:
        print("Режим поиска: вся доступная история")
    else:
        print(
            "Режим поиска: пользовательский интервал "
            f"{format_utc_ts(search_from_ts)} -> {format_utc_ts(search_to_ts)}"
        )

    conn = sqlite3.connect(DB_PATH)

    try:
        ensure_table_exists(conn, TABLE_NAME)

        history_min_ts, history_max_ts = fetch_history_bounds(conn, TABLE_NAME)

        if history_min_ts is None or history_max_ts is None:
            print("Таблица существует, но данных в ней нет.")
            return

        print(
            "Доступная история в таблице: "
            f"{format_utc_ts(history_min_ts)} -> {format_utc_ts(history_max_ts)}"
        )

        sql, params = build_rows_query(TABLE_NAME, search_from_ts, search_to_ts)
        cursor = conn.execute(sql, params)

        timestamps = [row[0] for row in cursor.fetchall()]

        print(f"Прочитано свечей: {len(timestamps)}")

        gap_count = find_gaps_in_rows(timestamps, search_from_ts, search_to_ts)

        if gap_count == 0:
            print("Дырок не найдено.")
        else:
            print(f"Всего найдено дырок: {gap_count}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

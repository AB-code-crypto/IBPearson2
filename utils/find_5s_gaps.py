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

# Для 5-секундных данных ожидаем строго такой шаг между соседними свечами.
EXPECTED_STEP_SECONDS = 5

# Штатные паузы MNQ/CME, которые не считаем дырками.
#
# Все правила собраны в единый список.
# Благодаря этому:
# 1. Не нужно разносить исключения по коду в виде отдельных if-веток.
# 2. Проще добавлять новые биржевые исключения.
# 3. Старые правила для ежедневного клиринга и обычных выходных
#    живут в том же общем механизме, что и праздничные паузы.
#
# Логика правил нарочно сделана простой и максимально контролируемой:
# мы не пытаемся "угадывать" календарь биржи,
# а описываем только те точные шаблоны пропусков,
# которые уже встретились в вашей истории и были проверены вручную.
IGNORED_GAP_RULES = [
    {
        "name": "daily_clearing",
        "description": "Ежедневный клиринг CME",
        "missing_bars": {720},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "regular_weekend",
        "description": "Обычные выходные",
        "missing_bars": {35280},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_spring_forward_weekend",
        "description": "Выходные с весенним переводом часов",
        "missing_bars": {34560},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_fall_back_weekend",
        "description": "Выходные с осенним переводом часов",
        "missing_bars": {36000},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "good_friday_plus_weekend",
        "description": "Good Friday + выходные",
        "missing_bars": {52560},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "us_holiday_midday_close",
        "description": "Праздничное закрытие с 12:00 CT до 17:00 CT",
        "missing_bars": {3600},
        "start_hours_utc": {17, 18},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_eve_early_close",
        "description": "Раннее закрытие накануне Independence Day",
        "missing_bars": {3420},
        "start_hours_utc": {17},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "thanksgiving_friday_plus_weekend",
        "description": "Раннее закрытие после Thanksgiving + выходные",
        "missing_bars": {37980},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "christmas_eve_plus_christmas_day",
        "description": "Christmas Eve early close + Christmas Day",
        "missing_bars": {20700},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "new_year_eve_plus_new_year_day",
        "description": "New Year's Eve close + New Year's Day",
        "missing_bars": {18000},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_plus_weekend",
        "description": "Independence Day + выходные",
        "missing_bars": {38160},
        "start_hours_utc": {17},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "national_day_of_mourning_2025_01_09",
        "description": "Национальный день траура 2025-01-09",
        "missing_bars": {6120},
        "start_hours_utc": {14},
        "start_minutes_utc": {30},
        "start_seconds_utc": {0},
        "start_dates_utc": {"2025-01-09"},
    },
]


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def format_utc_ts(timestamp_utc):
    # Преобразуем unix timestamp в читаемую строку UTC.
    dt = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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


def fetch_all_timestamps(conn, table_name):
    # Читаем все timestamp свечей из таблицы в отсортированном порядке.
    #
    # Нам нужен только bar_time_ts. Этого достаточно,
    # чтобы проверить непрерывность 5-секундной истории.
    sql = f"SELECT bar_time_ts FROM {table_name} ORDER BY bar_time_ts ASC"
    cursor = conn.execute(sql)
    return [row[0] for row in cursor.fetchall()]


def get_missing_bars_count(gap_start_ts, gap_end_ts):
    # Считаем, сколько баров отсутствует внутри найденного диапазона.
    missing_seconds = gap_end_ts - gap_start_ts + EXPECTED_STEP_SECONDS
    return missing_seconds // EXPECTED_STEP_SECONDS


def get_gap_start_datetime_utc(gap_start_ts):
    # Возвращаем datetime начала пропуска в UTC.
    return datetime.fromtimestamp(gap_start_ts, tz=timezone.utc)


def gap_matches_rule(gap_start_ts, gap_end_ts, rule):
    # Проверяем, подходит ли найденный пропуск под одно конкретное правило.
    missing_bars = get_missing_bars_count(gap_start_ts, gap_end_ts)

    if missing_bars not in rule["missing_bars"]:
        return False

    gap_start_dt = get_gap_start_datetime_utc(gap_start_ts)

    if gap_start_dt.hour not in rule["start_hours_utc"]:
        return False

    if gap_start_dt.minute not in rule["start_minutes_utc"]:
        return False

    if gap_start_dt.second not in rule["start_seconds_utc"]:
        return False

    # Не у всех правил есть ограничение по точной дате.
    # Например, ежедневный клиринг и обычные выходные должны работать всегда,
    # а вот национальный день траура 2025-01-09 — это разовое исключение.
    if "start_dates_utc" in rule:
        start_date_text = gap_start_dt.strftime("%Y-%m-%d")

        if start_date_text not in rule["start_dates_utc"]:
            return False

    return True


def get_ignored_gap_rule_name(gap_start_ts, gap_end_ts):
    # Возвращаем имя правила, по которому найденный пропуск нужно игнорировать.
    #
    # Если пропуск не подпадает ни под одно правило,
    # возвращаем None и считаем его обычной дыркой.
    for rule in IGNORED_GAP_RULES:
        if gap_matches_rule(gap_start_ts, gap_end_ts, rule):
            return rule["name"]

    return None


def print_gap(gap_number, gap_start_ts, gap_end_ts, reason):
    # Печатаем найденную дырку на экран.
    #
    # В случае нормальной дырки внутри истории gap_start_ts и gap_end_ts
    # задают уже именно отсутствующий диапазон.
    #
    # В случае аномальной последовательности reason будет содержать пояснение.
    missing_bars = get_missing_bars_count(gap_start_ts, gap_end_ts)
    gap_end_ts_half_open = gap_end_ts + EXPECTED_STEP_SECONDS

    print(
        f"[{gap_number}] Найдена дырка: "
        f"{format_utc_ts(gap_start_ts)} -> {format_utc_ts(gap_end_ts_half_open)} | "
        f"пропущено баров: {missing_bars} | причина: {reason}"
    )


# ============================================================================
# ОСНОВНАЯ ЛОГИКА ПОИСКА ДЫРОК
# ============================================================================
def find_gaps_in_rows(sorted_timestamps):
    # Ищем дыры в уже отсортированном списке timestamp-ов.
    #
    # Возвращаем количество найденных дыр.
    gap_count = 0

    if not sorted_timestamps:
        print("В таблице нет данных. Проверять нечего.")
        return gap_count

    # Проверяем внутренние разрывы между соседними свечами.
    previous_ts = sorted_timestamps[0]

    for current_ts in sorted_timestamps[1:]:
        delta_seconds = current_ts - previous_ts

        if delta_seconds != EXPECTED_STEP_SECONDS:
            # Для нормальной дырки missing range начинается после previous_ts
            # и заканчивается перед current_ts.
            gap_start_ts = previous_ts + EXPECTED_STEP_SECONDS
            gap_end_ts = current_ts - EXPECTED_STEP_SECONDS

            if delta_seconds > EXPECTED_STEP_SECONDS:
                ignored_rule_name = get_ignored_gap_rule_name(
                    gap_start_ts=gap_start_ts,
                    gap_end_ts=gap_end_ts,
                )

                # Если пропуск подпадает под известное биржевое правило,
                # то это не дыра в данных, а штатная пауза торговли.
                if ignored_rule_name is not None:
                    previous_ts = current_ts
                    continue

                reason = (
                    f"между соседними свечами разница {delta_seconds} сек, "
                    f"ожидалось {EXPECTED_STEP_SECONDS} сек"
                )
            else:
                reason = (
                    f"аномальная последовательность: между соседними свечами разница "
                    f"{delta_seconds} сек, ожидалось {EXPECTED_STEP_SECONDS} сек"
                )

            gap_count += 1

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

    return gap_count


def main():
    print(f"База данных: {DB_PATH}")
    print(f"Таблица: {TABLE_NAME}")
    print("Режим поиска: вся доступная история таблицы")

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

        timestamps = fetch_all_timestamps(conn, TABLE_NAME)

        print(f"Прочитано свечей: {len(timestamps)}")

        gap_count = find_gaps_in_rows(timestamps)

        if gap_count == 0:
            print("Дырок не найдено.")
        else:
            print(f"Всего найдено дырок: {gap_count}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

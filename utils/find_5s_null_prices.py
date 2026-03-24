'''
скрипт для поиска свечей в price DB, у которых отсутствует хотя бы одна из 8 цен (ask_* и bid_*). Проходит по всей таблице, выводит timestamp каждой
проблемной свечи и список конкретных полей, где найден NULL.
'''
import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from config import settings_live as settings

# ============================================================================
# НАСТРОЙКИ СКРИПТА
# ============================================================================
# Скрипт предполагается запускать из папки utils.
# Поэтому путь до корня проекта вычисляем относительно самого файла.

# База данных, в которой ищем проблемы.
DB_PATH = settings.price_db_path

# Таблица, в которой ищем свечи с отсутствующими ценами.
TABLE_NAME = "MNQ_5s"

# Все 8 цен, которые должны быть заполнены у свечки.
PRICE_COLUMNS = [
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
]

CHICAGO_TZ = ZoneInfo("America/Chicago")


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def format_utc_ts(timestamp_utc):
    # Преобразуем unix timestamp в читаемую строку UTC.
    dt = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_ct_ts(timestamp_utc):
    # Преобразуем тот же самый абсолютный timestamp в читаемую строку CT.
    dt_utc = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CHICAGO_TZ)
    return dt_ct.strftime("%Y-%m-%d %H:%M:%S")


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


def fetch_rows_with_missing_prices(conn, table_name):
    # Читаем все строки, где хотя бы одна из 8 цен отсутствует.
    #
    # Идём по всей таблице в порядке времени.
    # Нам нужно вывести timestamp проблемной свечки и список отсутствующих полей.
    where_conditions = " OR ".join([f"{column} IS NULL" for column in PRICE_COLUMNS])

    sql = f"""
        SELECT
            bar_time_ts,
            ask_open,
            ask_high,
            ask_low,
            ask_close,
            bid_open,
            bid_high,
            bid_low,
            bid_close
        FROM {table_name}
        WHERE {where_conditions}
        ORDER BY bar_time_ts ASC
    """

    cursor = conn.execute(sql)
    return cursor.fetchall()


def get_missing_price_columns(row):
    # По одной строке БД определяем, какие именно ценовые поля отсутствуют.
    #
    # row имеет структуру:
    # 0 - bar_time_ts
    # 1..8 - значения PRICE_COLUMNS в том же порядке.
    missing_columns = []

    for index, column_name in enumerate(PRICE_COLUMNS, start=1):
        if row[index] is None:
            missing_columns.append(column_name)

    return missing_columns


def print_missing_price_row(problem_number, row):
    # Печатаем одну проблемную свечку.
    bar_time_ts = row[0]
    missing_columns = get_missing_price_columns(row)
    missing_columns_text = ", ".join(missing_columns)

    print(
        f"[{problem_number}] Найдена свечка с отсутствующими ценами: "
        f"UTC={format_utc_ts(bar_time_ts)} | "
        f"CT={format_ct_ts(bar_time_ts)} | "
        f"отсутствуют поля: {missing_columns_text}"
    )


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================
def main():
    print(f"База данных: {DB_PATH}")
    print(f"Таблица: {TABLE_NAME}")
    print("Режим поиска: вся доступная история таблицы")
    print("Проверка: ищем свечки, где хотя бы одна из 8 цен равна NULL")

    conn = sqlite3.connect(DB_PATH)

    try:
        ensure_table_exists(conn, TABLE_NAME)

        history_min_ts, history_max_ts = fetch_history_bounds(conn, TABLE_NAME)

        if history_min_ts is None or history_max_ts is None:
            print("Таблица существует, но данных в ней нет.")
            return

        print(
            "Доступная история в таблице: "
            f"UTC {format_utc_ts(history_min_ts)} -> {format_utc_ts(history_max_ts)} | "
            f"CT {format_ct_ts(history_min_ts)} -> {format_ct_ts(history_max_ts)}"
        )

        sql = f"SELECT COUNT(*) FROM {TABLE_NAME}"
        total_rows = conn.execute(sql).fetchone()[0]
        print(f"Всего свечей в таблице: {total_rows}")

        rows_with_missing_prices = fetch_rows_with_missing_prices(conn, TABLE_NAME)

        print(f"Найдено свечей с отсутствующими ценами: {len(rows_with_missing_prices)}")

        if not rows_with_missing_prices:
            print("Свечек с отсутствующими ценами не найдено.")
            return

        for problem_number, row in enumerate(rows_with_missing_prices, start=1):
            print_missing_price_row(problem_number, row)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
import sqlite3
from typing import Iterable

from contracts import Instrument
from core.db_sql import (
    create_prepared_quotes_indexes_sql,
    create_prepared_quotes_table_sql,
    create_quotes_table_sql,
)
from core.logger import get_logger, log_info

logger = get_logger(__name__)


def build_table_name(instrument_code, bar_size_setting):
    """Приводим barSizeSetting к короткому и предсказуемому суффиксу имени таблицы."""
    suffix = (
        bar_size_setting
        .replace(" ", "")
        .replace("secs", "s")
        .replace("sec", "s")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("mins", "m")
        .replace("min", "m")
    )
    return f"{instrument_code}_{suffix}"


def create_db_objects_if_missing(db_path, sql_list: Iterable[str]):
    """Выполняем набор CREATE-операций в одной транзакции."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA busy_timeout=5000;")
        for sql in sql_list:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


def initialize_price_database(settings):
    """Создаём таблицы ценовой БД только для FUT-инструментов."""
    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] != "FUT":
            continue

        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )
        create_db_objects_if_missing(
            settings.price_db_path,
            [create_quotes_table_sql(table_name)],
        )
        log_info(
            logger,
            f"Проверил таблицу цен {table_name} в БД {settings.price_db_path}: FUT/BID-ASK",
            to_telegram=False,
        )


def initialize_prepared_database(settings):
    """Создаём таблицы prepared БД только для FUT-инструментов."""
    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] != "FUT":
            continue

        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )
        create_db_objects_if_missing(
            settings.prepared_db_path,
            [
                create_prepared_quotes_table_sql(table_name),
                *create_prepared_quotes_indexes_sql(table_name),
            ],
        )
        log_info(
            logger,
            f"Проверил prepared-таблицу {table_name} в БД {settings.prepared_db_path}: FUT/Pearson",
            to_telegram=False,
        )


def initialize_databases_sync(settings):
    """Синхронная точка входа инициализации всех проектных БД."""
    log_info(logger, "Запускаю инициализацию проектных БД", to_telegram=False)
    initialize_price_database(settings)
    initialize_prepared_database(settings)
    log_info(logger, "Инициализация проектных БД завершена", to_telegram=False)


async def initialize_databases(settings):
    """Совместимый async-wrapper для существующих мест вызова."""
    initialize_databases_sync(settings)

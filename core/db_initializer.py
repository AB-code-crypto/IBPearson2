import sqlite3

from contracts import Instrument
from core.logger import get_logger, log_info
from core.db_sql import (
    create_quotes_table_sql,
    create_prepared_quotes_table_sql,
    create_prepared_quotes_indexes_sql, create_trades_table_sql, create_trade_events_table_sql, create_trade_runtime_state_table_sql, create_trade_indexes_sql,
)

logger = get_logger(__name__)


def build_table_name(instrument_code, bar_size_setting):
    # Приводим barSizeSetting к короткому и предсказуемому суффиксу имени таблицы.
    #
    # Примеры:
    # - 5 secs -> 5s
    # - 1 hour -> 1h
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


def create_db_objects_if_missing(db_path, sql_list):
    # Выполняем набор CREATE-операторов в одной транзакции.
    #
    # Подходит и для одной таблицы, и для таблицы + индекс(ов).
    # Если файл БД ещё не существует, SQLite создаст его автоматически
    # при первом подключении.
    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA busy_timeout=5000;")

        for sql in sql_list:
            conn.execute(sql)

        conn.commit()
    finally:
        conn.close()


def initialize_price_database(settings):
    # Создаём таблицы ценовой БД только для FUT-инструментов.
    #
    # Сейчас работаем только с BID/ASK-таблицами фьючерсов.
    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] != "FUT":
            continue

        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        sql_list = [
            create_quotes_table_sql(table_name),
        ]

        create_db_objects_if_missing(settings.price_db_path, sql_list)

        log_info(
            logger,
            f"Проверил таблицу цен {table_name} в БД {settings.price_db_path}: FUT/BID-ASK",
            to_telegram=False,
        )


def initialize_prepared_database(settings):
    # Создаём таблицы prepared БД только для FUT-инструментов.
    #
    # Здесь хранятся подготовленные данные для первого шага поиска паттернов:
    # y, sum_y, sum_y2 по завершённым историческим часам.
    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] != "FUT":
            continue

        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        sql_list = [
            create_prepared_quotes_table_sql(table_name),
            *create_prepared_quotes_indexes_sql(table_name),
        ]

        create_db_objects_if_missing(settings.prepared_db_path, sql_list)

        log_info(
            logger,
            f"Проверил prepared-таблицу {table_name} в БД {settings.prepared_db_path}: FUT/Pearson",
            to_telegram=False,
        )


def initialize_trade_database(settings):
    # Создаём торговую БД.
    #
    # В одной БД храним:
    # - trades               : полную историю сделок;
    # - trade_events         : журнал событий по сделкам;
    # - trade_runtime_state  : текущее торговое состояние по инструментам.
    sql_list = [
        create_trades_table_sql(),
        create_trade_events_table_sql(),
        create_trade_runtime_state_table_sql(),
        *create_trade_indexes_sql(),
    ]

    create_db_objects_if_missing(settings.trade_db_path, sql_list)

    log_info(
        logger,
        f"Проверил торговую БД {settings.trade_db_path}: trades/trade_events/trade_runtime_state",
        to_telegram=False,
    )


async def initialize_databases(settings):
    # Точка входа инициализации всех проектных БД.
    #
    # На старте создаём:
    # - ценовую БД;
    # - prepared БД для первого шага поиска паттернов;
    # - trade БД для истории сделок и текущего торгового состояния.
    log_info(logger, "Запускаю инициализацию проектных БД", to_telegram=False)

    initialize_price_database(settings)
    initialize_prepared_database(settings)
    initialize_trade_database(settings)

    log_info(logger, "Инициализация проектных БД завершена", to_telegram=False)
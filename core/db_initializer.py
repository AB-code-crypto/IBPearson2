import sqlite3

from contracts import Instrument
from core.db_sql import get_create_quotes_table_sql, get_create_ohlc_table_sql
from core.logger import get_logger, log_info

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


def create_table_if_missing(db_path, create_sql):
    # Создаём таблицу в SQLite, если её ещё нет.
    #
    # Сам файл БД SQLite создастся автоматически при первом подключении,
    # поэтому отдельный шаг "создать файл БД" нам не нужен.
    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute(create_sql)
        conn.commit()
    finally:
        conn.close()


def initialize_price_database(settings):
    # Создаём все таблицы, которые нужны ценовой БД на текущем этапе проекта.
    #
    # Логика простая:
    # - для FUT создаём таблицу BID/ASK-котировок;
    # - для IND создаём таблицу одиночного OHLC-потока.
    for instrument_code, instrument_row in Instrument.items():
        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        if instrument_row["secType"] == "FUT":
            create_sql = get_create_quotes_table_sql(table_name)
            create_table_if_missing(settings.price_db_path, create_sql)

            log_info(
                logger,
                f"Проверил таблицу цен {table_name} в БД {settings.price_db_path}: FUT/BID-ASK",
                to_telegram=False,
            )
            continue

        if instrument_row["secType"] == "IND":
            create_sql = get_create_ohlc_table_sql(table_name)
            create_table_if_missing(settings.price_db_path, create_sql)

            log_info(
                logger,
                f"Проверил таблицу цен {table_name} в БД {settings.price_db_path}: IND/OHLC",
                to_telegram=False,
            )
            continue

        raise ValueError(
            f"Неподдерживаемый secType в Instrument[{instrument_code}]: {instrument_row['secType']}"
        )


async def initialize_databases(settings):
    # Точка входа инициализации всех проектных БД.
    #
    # Пока на старте создаём только ценовую БД и нужные таблицы в ней.
    # Позже сюда можно будет добавить:
    # - БД кеша;
    # - торговую БД;
    # - служебные таблицы и другие структуры.
    log_info(logger, "Запускаю инициализацию проектных БД", to_telegram=False)

    initialize_price_database(settings)

    log_info(logger, "Инициализация проектных БД завершена", to_telegram=False)

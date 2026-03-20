def create_quotes_table_sql(table_name):
    # Таблица для BID/ASK-баров.
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bar_time_ts INTEGER PRIMARY KEY,
        bar_time    TEXT NOT NULL,
        contract    TEXT NOT NULL,

        ask_open    REAL,
        bid_open    REAL,

        ask_high    REAL,
        bid_high    REAL,

        ask_low     REAL,
        bid_low     REAL,

        ask_close   REAL,
        bid_close   REAL,

        volume      REAL,
        average     REAL,
        bar_count   INTEGER
    );
    """


def upsert_quotes_sql(table_name):
    # UPSERT для BID/ASK-таблицы.
    #
    # Этот вариант подходит, когда вся строка бара уже полностью собрана,
    # например при исторической загрузке BID + ASK за один и тот же интервал.
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        contract,

        ask_open,
        bid_open,

        ask_high,
        bid_high,

        ask_low,
        bid_low,

        ask_close,
        bid_close,

        volume,
        average,
        bar_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        contract = excluded.contract,

        ask_open = excluded.ask_open,
        bid_open = excluded.bid_open,

        ask_high = excluded.ask_high,
        bid_high = excluded.bid_high,

        ask_low = excluded.ask_low,
        bid_low = excluded.bid_low,

        ask_close = excluded.ask_close,
        bid_close = excluded.bid_close,

        volume = excluded.volume,
        average = excluded.average,
        bar_count = excluded.bar_count
    ;
    """


def upsert_quotes_ask_sql(table_name):
    # UPSERT только для ASK-стороны realtime-бара.
    #
    # Обновляем только ask_* поля и не трогаем bid_*.
    # Это важно, потому что BID и ASK в realtime приходят отдельными потоками,
    # и более поздний UPSERT одной стороны не должен затирать другую сторону в NULL.
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        contract,
        ask_open,
        ask_high,
        ask_low,
        ask_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        contract = excluded.contract,
        ask_open = excluded.ask_open,
        ask_high = excluded.ask_high,
        ask_low = excluded.ask_low,
        ask_close = excluded.ask_close
    ;
    """


def upsert_quotes_bid_sql(table_name):
    # UPSERT только для BID-стороны realtime-бара.
    #
    # Обновляем только bid_* поля и не трогаем ask_*.
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        contract,
        bid_open,
        bid_high,
        bid_low,
        bid_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        contract = excluded.contract,
        bid_open = excluded.bid_open,
        bid_high = excluded.bid_high,
        bid_low = excluded.bid_low,
        bid_close = excluded.bid_close
    ;
    """


def create_ohlc_table_sql(table_name):
    # Таблица для одиночного потока OHLC, например VIX/TRADES.
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bar_time_ts INTEGER PRIMARY KEY,
        bar_time    TEXT NOT NULL,
        contract    TEXT NOT NULL,

        open        REAL,
        high        REAL,
        low         REAL,
        close       REAL,

        volume      REAL,
        average     REAL,
        bar_count   INTEGER
    );
    """


def upsert_ohlc_sql(table_name):
    # UPSERT для одиночного OHLC-потока.
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        contract,
        open,
        high,
        low,
        close,
        volume,
        average,
        bar_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        contract = excluded.contract,
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume,
        average = excluded.average,
        bar_count = excluded.bar_count
    ;
    """


def create_prepared_quotes_table_sql(table_name):
    # Таблица подготовленных данных для первого шага поиска паттернов.
    #
    # Храним только то, что нужно для инкрементального Пирсона
    # по историческим завершённым часам:
    # - y
    # - sum_y
    # - sum_y2
    #
    # Один исторический час = 720 строк.
    # Таблица находится в отдельной prepared DB, поэтому имя таблицы
    # можно оставить таким же, как в price DB, например: MNQ_5s

    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        hour_start_ts INTEGER NOT NULL,
        hour_start    TEXT NOT NULL,
        hour_slot     INTEGER NOT NULL,
        contract      TEXT NOT NULL,

        bar_index     INTEGER NOT NULL,

        y             REAL NOT NULL,
        sum_y         REAL NOT NULL,
        sum_y2        REAL NOT NULL,

        PRIMARY KEY (hour_start_ts, bar_index),

        CHECK (hour_slot >= 0 AND hour_slot < 24),
        CHECK (bar_index >= 0 AND bar_index < 720)
    ) WITHOUT ROWID;
    """


def create_prepared_quotes_indexes_sql(table_name):
    # Дополнительные индексы для prepared-таблицы.
    #
    # Основной сценарий выборки:
    # - найти все исторические часы конкретного часа суток (hour_slot)
    # - затем читать их по времени
    #
    # По PRIMARY KEY(hour_start_ts, bar_index) отдельный индекс на hour_start_ts
    # не нужен: он уже покрывается началом первичного ключа.
    return [
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_hour_slot_hour_start_ts
        ON {table_name}(hour_slot, hour_start_ts);
        """
    ]


def delete_prepared_hour_sql(table_name):
    # Удаляем из prepared-таблицы все строки одного исторического часа.
    #
    # Это удобно для простого и надёжного сценария:
    # сначала полностью удалили час, потом заново вставили все 720 строк.
    return f"""
    DELETE FROM {table_name}
    WHERE hour_start_ts = ?
    ;
    """


def insert_prepared_quote_sql(table_name):
    # Простая INSERT-команда для prepared-таблицы.
    #
    # Здесь не используем UPSERT, потому что в разовом скрипте
    # проще и надёжнее сначала удалить целый час, а потом вставить его заново.
    return f"""
    INSERT INTO {table_name} (
        hour_start_ts,
        hour_start,
        hour_slot,
        contract,
        bar_index,
        y,
        sum_y,
        sum_y2
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ;
    """

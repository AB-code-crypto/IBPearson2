def get_create_quotes_table_sql(table_name):
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


def get_upsert_quotes_sql(table_name):
    # UPSERT для BID/ASK-таблицы.
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


def get_create_ohlc_table_sql(table_name):
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


def get_upsert_ohlc_sql(table_name):
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
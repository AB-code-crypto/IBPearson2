def create_quotes_table_sql(table_name):
    # Таблица для BID/ASK-баров.
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bar_time_ts    INTEGER PRIMARY KEY,
        bar_time       TEXT NOT NULL,

        bar_time_ts_ct INTEGER NOT NULL,
        bar_time_ct    TEXT NOT NULL,

        contract       TEXT NOT NULL,

        ask_open       REAL,
        bid_open       REAL,

        ask_high       REAL,
        bid_high       REAL,

        ask_low        REAL,
        bid_low        REAL,

        ask_close      REAL,
        bid_close      REAL,

        volume         REAL,
        average        REAL,
        bar_count      INTEGER
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

        bar_time_ts_ct,
        bar_time_ct,

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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,

        bar_time_ts_ct = excluded.bar_time_ts_ct,
        bar_time_ct = excluded.bar_time_ct,

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

        bar_time_ts_ct,
        bar_time_ct,

        contract,

        ask_open,
        ask_high,
        ask_low,
        ask_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,

        bar_time_ts_ct = excluded.bar_time_ts_ct,
        bar_time_ct = excluded.bar_time_ct,

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

        bar_time_ts_ct,
        bar_time_ct,

        contract,

        bid_open,
        bid_high,
        bid_low,
        bid_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,

        bar_time_ts_ct = excluded.bar_time_ts_ct,
        bar_time_ct = excluded.bar_time_ct,

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
        bar_time_ts    INTEGER PRIMARY KEY,
        bar_time       TEXT NOT NULL,

        bar_time_ts_ct INTEGER NOT NULL,
        bar_time_ct    TEXT NOT NULL,

        contract       TEXT NOT NULL,

        open           REAL,
        high           REAL,
        low            REAL,
        close          REAL,

        volume         REAL,
        average        REAL,
        bar_count      INTEGER
    );
    """


def upsert_ohlc_sql(table_name):
    # UPSERT для одиночного OHLC-потока.
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,

        bar_time_ts_ct,
        bar_time_ct,

        contract,

        open,
        high,
        low,
        close,

        volume,
        average,
        bar_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,

        bar_time_ts_ct = excluded.bar_time_ts_ct,
        bar_time_ct = excluded.bar_time_ct,

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
    # Храним:
    # - hour_start_ts     : технический UTC-якорь часа
    # - hour_start_ts_ct  : локальная числовая ось CT
    # - hour_start_ct     : человекочитаемое CT-время
    # - hour_slot_ct      : номер часа суток в CT
    # - y / sum_y / sum_y2
    #
    # Один исторический час = 720 строк.
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        hour_start_ts    INTEGER NOT NULL,
        hour_start_ts_ct INTEGER NOT NULL,
        hour_start_ct    TEXT NOT NULL,
        hour_slot_ct     INTEGER NOT NULL,
        contract         TEXT NOT NULL,

        bar_index        INTEGER NOT NULL,

        y                REAL NOT NULL,
        sum_y            REAL NOT NULL,
        sum_y2           REAL NOT NULL,

        PRIMARY KEY (hour_start_ts, bar_index),

        CHECK (hour_slot_ct >= 0 AND hour_slot_ct < 24),
        CHECK (bar_index >= 0 AND bar_index < 720)
    ) WITHOUT ROWID;
    """


def create_prepared_quotes_indexes_sql(table_name):
    # Дополнительные индексы для prepared-таблицы.
    #
    # Основной сценарий выборки:
    # - найти все исторические часы конкретного часа суток в CT (hour_slot_ct)
    # - затем читать их по CT-времени
    #
    # По PRIMARY KEY(hour_start_ts, bar_index) отдельный индекс на hour_start_ts
    # не нужен: он уже покрывается началом первичного ключа.
    return [
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_hour_slot_ct_hour_start_ts_ct
        ON {table_name}(hour_slot_ct, hour_start_ts_ct);
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
        hour_start_ts_ct,
        hour_start_ct,
        hour_slot_ct,
        contract,
        bar_index,
        y,
        sum_y,
        sum_y2
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ;
    """


def count_prepared_hour_rows_sql(table_name):
    return f"""
    SELECT COUNT(*)
    FROM {table_name}
    WHERE hour_start_ts = ?
    ;
    """


def select_price_time_bounds_sql(table_name):
    return f"""
    SELECT
        MIN(bar_time_ts) AS min_ts,
        MAX(bar_time_ts) AS max_ts
    FROM {table_name}
    ;
    """


def select_prepared_hour_rows_sql(table_name):
    # Все строки одного prepared-часа.
    return f"""
    SELECT
        hour_start_ts,
        hour_start_ts_ct,
        hour_start_ct,
        hour_slot_ct,
        contract,
        bar_index,
        y,
        sum_y,
        sum_y2
    FROM {table_name}
    WHERE hour_start_ts = ?
    ORDER BY bar_index
    ;
    """


def select_prepared_rows_by_slots_sql(table_name, slot_count):
    # Все строки всех prepared-часов по списку hour_slot_ct.
    #
    # Это основная быстрая выборка для runtime:
    # одним запросом читаем все исторические часы нужных CT-slot,
    # затем группируем в Python по hour_start_ts.
    #
    # before_hour_start_ts_ct:
    # - если NULL, ограничение не применяется
    # - если задан, возвращаем только часы строго раньше него по CT-оси
    placeholders = ", ".join(["?"] * slot_count)

    return f"""
    SELECT
        hour_start_ts,
        hour_start_ts_ct,
        hour_start_ct,
        hour_slot_ct,
        contract,
        bar_index,
        y,
        sum_y,
        sum_y2
    FROM {table_name}
    WHERE hour_slot_ct IN ({placeholders})
      AND (? IS NULL OR hour_start_ts_ct < ?)
    ORDER BY hour_start_ts_ct, bar_index
    ;
    """


def create_trades_table_sql():
    # Главная таблица истории сделок.
    #
    # Одна строка = одна сделка стратегии от появления сигнала
    # до полного закрытия позиции.
    return """
           CREATE TABLE IF NOT EXISTS trades
           (
               trade_id                   INTEGER PRIMARY KEY AUTOINCREMENT,

               instrument_code            TEXT    NOT NULL,
               contract_local_symbol      TEXT,

               side                       TEXT    NOT NULL,
               quantity                   INTEGER NOT NULL,

               status                     TEXT    NOT NULL,

               signal_hour_start_ts       INTEGER,
               signal_hour_start_ts_ct    INTEGER,
               signal_hour_start_ct       TEXT,

               signal_bar_index           INTEGER,
               signal_bar_time_ts         INTEGER,
               signal_bar_time_ts_ct      INTEGER,
               signal_bar_time_ct         TEXT,

               decision                   TEXT,
               decision_reason            TEXT,

               best_similarity_score      REAL,

               forecast_direction         TEXT,
               forecast_candidate_count   INTEGER,
               forecast_positive_ratio    REAL,
               forecast_negative_ratio    REAL,
               forecast_mean_final_move   REAL,
               forecast_median_final_move REAL,

               decision_payload_json      TEXT,
               forecast_summary_json      TEXT,

               entry_submitted_ts         INTEGER,
               entry_submitted_time       TEXT,
               entry_order_id             INTEGER,
               entry_perm_id              INTEGER,

               entry_filled_ts            INTEGER,
               entry_filled_time          TEXT,
               entry_avg_fill_price       REAL,

               exit_submitted_ts          INTEGER,
               exit_submitted_time        TEXT,
               exit_order_id              INTEGER,
               exit_perm_id               INTEGER,

               exit_filled_ts             INTEGER,
               exit_filled_time           TEXT,
               exit_avg_fill_price        REAL,

               commissions_total          REAL    NOT NULL DEFAULT 0,
               realized_pnl               REAL,

               error_text                 TEXT,

               created_at                 TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
               updated_at                 TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
           ); \
           """


def create_trade_events_table_sql():
    # Журнал событий по сделкам.
    #
    # Одна строка = одно важное событие:
    # - сигнал появился
    # - ордер отправлен
    # - ордер исполнен
    # - сделка подхвачена после рестарта
    # - ошибка / рассинхрон / принудительное закрытие
    return """
           CREATE TABLE IF NOT EXISTS trade_events
           (
               event_id        INTEGER PRIMARY KEY AUTOINCREMENT,

               trade_id        INTEGER,
               instrument_code TEXT    NOT NULL,

               event_type      TEXT    NOT NULL,
               event_time_ts   INTEGER NOT NULL,
               event_time      TEXT    NOT NULL,

               message         TEXT,
               payload_json    TEXT,

               created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,

               FOREIGN KEY (trade_id) REFERENCES trades (trade_id)
           ); \
           """


def create_trade_runtime_state_table_sql():
    # Таблица текущего торгового состояния.
    #
    # Здесь не история, а только "что робот считает текущим состоянием
    # по каждому инструменту" на данный момент.
    #
    # Одна строка = один инструмент.
    return """
           CREATE TABLE IF NOT EXISTS trade_runtime_state
           (
               instrument_code        TEXT PRIMARY KEY,

               current_trade_id       INTEGER,

               position_side          TEXT,
               position_qty           INTEGER NOT NULL DEFAULT 0,

               entry_hour_start_ts    INTEGER,
               entry_hour_start_ts_ct INTEGER,
               entry_hour_start_ct    TEXT,

               broker_position_qty    INTEGER NOT NULL DEFAULT 0,
               broker_avg_cost        REAL,

               last_decision          TEXT,
               last_decision_reason   TEXT,

               last_snapshot_time_ts  INTEGER,
               last_snapshot_time     TEXT,

               updated_at             TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,

               FOREIGN KEY (current_trade_id) REFERENCES trades (trade_id)
           ) WITHOUT ROWID; \
           """


def create_trade_indexes_sql():
    # Индексы торговой БД.
    return [
        """
        CREATE INDEX IF NOT EXISTS idx_trades_status
            ON trades (status);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_trades_instrument_status
            ON trades (instrument_code, status);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_trades_signal_hour_start_ts
            ON trades (signal_hour_start_ts);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id
            ON trade_events (trade_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_trade_events_instrument_time
            ON trade_events (instrument_code, event_time_ts);
        """,
    ]

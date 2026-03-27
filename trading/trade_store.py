import json
import sqlite3
from contextlib import contextmanager


@contextmanager
def _trade_db_connection(db_path):
    """Контекст подключения к торговой БД с едиными настройками."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _trade_db_write(db_path):
    """Контекст записи в торговую БД с единым commit/rollback поведением."""
    with _trade_db_connection(db_path) as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _json_dumps_or_none(value):
    if value is None:
        return None
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
    )


def _row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def _fetch_one_dict(db_path, sql, params=()):
    with _trade_db_connection(db_path) as conn:
        cursor = conn.execute(sql, params)
        return _row_to_dict(cursor.fetchone())


def _fetch_all_dicts(db_path, sql, params=()):
    with _trade_db_connection(db_path) as conn:
        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def _execute_write(db_path, sql, params=()):
    with _trade_db_write(db_path) as conn:
        conn.execute(sql, params)


def _execute_insert(db_path, sql, params=()):
    with _trade_db_write(db_path) as conn:
        cursor = conn.execute(sql, params)
        return cursor.lastrowid


def create_trade(
    db_path,
    *,
    instrument_code,
    contract_local_symbol,
    side,
    quantity,
    status,
    signal_hour_start_ts=None,
    signal_hour_start_ts_ct=None,
    signal_hour_start_ct=None,
    signal_bar_index=None,
    signal_bar_time_ts=None,
    signal_bar_time_ts_ct=None,
    signal_bar_time_ct=None,
    decision=None,
    decision_reason=None,
    best_similarity_score=None,
    forecast_direction=None,
    forecast_candidate_count=None,
    forecast_positive_ratio=None,
    forecast_negative_ratio=None,
    forecast_mean_final_move=None,
    forecast_median_final_move=None,
    decision_payload=None,
    forecast_summary=None,
):
    """Создаём новую сделку в истории и возвращаем её trade_id."""
    return _execute_insert(
        db_path,
        """
        INSERT INTO trades (
            instrument_code,
            contract_local_symbol,
            side,
            quantity,
            status,
            signal_hour_start_ts,
            signal_hour_start_ts_ct,
            signal_hour_start_ct,
            signal_bar_index,
            signal_bar_time_ts,
            signal_bar_time_ts_ct,
            signal_bar_time_ct,
            decision,
            decision_reason,
            best_similarity_score,
            forecast_direction,
            forecast_candidate_count,
            forecast_positive_ratio,
            forecast_negative_ratio,
            forecast_mean_final_move,
            forecast_median_final_move,
            decision_payload_json,
            forecast_summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            instrument_code,
            contract_local_symbol,
            side,
            quantity,
            status,
            signal_hour_start_ts,
            signal_hour_start_ts_ct,
            signal_hour_start_ct,
            signal_bar_index,
            signal_bar_time_ts,
            signal_bar_time_ts_ct,
            signal_bar_time_ct,
            decision,
            decision_reason,
            best_similarity_score,
            forecast_direction,
            forecast_candidate_count,
            forecast_positive_ratio,
            forecast_negative_ratio,
            forecast_mean_final_move,
            forecast_median_final_move,
            _json_dumps_or_none(decision_payload),
            _json_dumps_or_none(forecast_summary),
        ),
    )


def append_trade_event(
    db_path,
    *,
    instrument_code,
    event_type,
    event_time_ts,
    event_time,
    trade_id=None,
    message=None,
    payload=None,
):
    """Добавляем событие в журнал trade_events и возвращаем event_id."""
    return _execute_insert(
        db_path,
        """
        INSERT INTO trade_events (
            trade_id,
            instrument_code,
            event_type,
            event_time_ts,
            event_time,
            message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            trade_id,
            instrument_code,
            event_type,
            event_time_ts,
            event_time,
            message,
            _json_dumps_or_none(payload),
        ),
    )


def get_trade_by_id(db_path, trade_id):
    """Загружаем одну сделку по trade_id."""
    return _fetch_one_dict(
        db_path,
        """
        SELECT *
        FROM trades
        WHERE trade_id = ?;
        """,
        (trade_id,),
    )


def get_open_trade_for_instrument(db_path, instrument_code):
    """Получаем текущую незакрытую сделку по инструменту."""
    return _fetch_one_dict(
        db_path,
        """
        SELECT *
        FROM trades
        WHERE instrument_code = ?
          AND status NOT IN ('CLOSED', 'CANCELLED')
        ORDER BY trade_id DESC
        LIMIT 1;
        """,
        (instrument_code,),
    )


def list_open_trades(db_path):
    """Получаем все незакрытые сделки."""
    return _fetch_all_dicts(
        db_path,
        """
        SELECT *
        FROM trades
        WHERE status NOT IN ('CLOSED', 'CANCELLED')
        ORDER BY trade_id;
        """,
    )


def update_trade_status(db_path, *, trade_id, status):
    """Обновляем только статус сделки."""
    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (status, trade_id),
    )


def mark_entry_submitted(
    db_path,
    *,
    trade_id,
    status,
    entry_submitted_ts,
    entry_submitted_time,
    entry_order_id=None,
    entry_perm_id=None,
):
    """Фиксируем отправку ордера на вход."""
    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = ?,
            entry_submitted_ts = ?,
            entry_submitted_time = ?,
            entry_order_id = ?,
            entry_perm_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (
            status,
            entry_submitted_ts,
            entry_submitted_time,
            entry_order_id,
            entry_perm_id,
            trade_id,
        ),
    )


def mark_entry_filled(
    db_path,
    *,
    trade_id,
    status,
    entry_filled_ts,
    entry_filled_time,
    entry_avg_fill_price,
    commissions_total=None,
):
    """Фиксируем фактическое исполнение входа."""
    if commissions_total is None:
        _execute_write(
            db_path,
            """
            UPDATE trades
            SET status = ?,
                entry_filled_ts = ?,
                entry_filled_time = ?,
                entry_avg_fill_price = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE trade_id = ?;
            """,
            (
                status,
                entry_filled_ts,
                entry_filled_time,
                entry_avg_fill_price,
                trade_id,
            ),
        )
        return

    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = ?,
            entry_filled_ts = ?,
            entry_filled_time = ?,
            entry_avg_fill_price = ?,
            commissions_total = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (
            status,
            entry_filled_ts,
            entry_filled_time,
            entry_avg_fill_price,
            commissions_total,
            trade_id,
        ),
    )


def mark_exit_submitted(
    db_path,
    *,
    trade_id,
    status,
    exit_submitted_ts,
    exit_submitted_time,
    exit_order_id=None,
    exit_perm_id=None,
):
    """Фиксируем отправку ордера на выход."""
    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = ?,
            exit_submitted_ts = ?,
            exit_submitted_time = ?,
            exit_order_id = ?,
            exit_perm_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (
            status,
            exit_submitted_ts,
            exit_submitted_time,
            exit_order_id,
            exit_perm_id,
            trade_id,
        ),
    )


def mark_exit_filled_and_close(
    db_path,
    *,
    trade_id,
    exit_filled_ts,
    exit_filled_time,
    exit_avg_fill_price,
    commissions_total=None,
    realized_pnl=None,
):
    """Фиксируем исполнение выхода и закрываем сделку."""
    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = 'CLOSED',
            exit_filled_ts = ?,
            exit_filled_time = ?,
            exit_avg_fill_price = ?,
            commissions_total = COALESCE(?, commissions_total),
            realized_pnl = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (
            exit_filled_ts,
            exit_filled_time,
            exit_avg_fill_price,
            commissions_total,
            realized_pnl,
            trade_id,
        ),
    )


def mark_trade_error(
    db_path,
    *,
    trade_id,
    status="ERROR",
    error_text=None,
):
    """Переводим сделку в ошибочное состояние."""
    _execute_write(
        db_path,
        """
        UPDATE trades
        SET status = ?,
            error_text = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trade_id = ?;
        """,
        (
            status,
            error_text,
            trade_id,
        ),
    )


def load_trade_runtime_state(db_path, instrument_code):
    """Получаем текущее торговое состояние по инструменту."""
    return _fetch_one_dict(
        db_path,
        """
        SELECT *
        FROM trade_runtime_state
        WHERE instrument_code = ?;
        """,
        (instrument_code,),
    )


def upsert_trade_runtime_state(
    db_path,
    *,
    instrument_code,
    current_trade_id=None,
    position_side=None,
    position_qty=0,
    entry_hour_start_ts=None,
    entry_hour_start_ts_ct=None,
    entry_hour_start_ct=None,
    broker_position_qty=0,
    broker_avg_cost=None,
    last_decision=None,
    last_decision_reason=None,
    last_snapshot_time_ts=None,
    last_snapshot_time=None,
):
    """Создаём или обновляем текущее runtime-состояние по инструменту."""
    _execute_write(
        db_path,
        """
        INSERT INTO trade_runtime_state (
            instrument_code,
            current_trade_id,
            position_side,
            position_qty,
            entry_hour_start_ts,
            entry_hour_start_ts_ct,
            entry_hour_start_ct,
            broker_position_qty,
            broker_avg_cost,
            last_decision,
            last_decision_reason,
            last_snapshot_time_ts,
            last_snapshot_time,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(instrument_code) DO UPDATE SET
            current_trade_id = excluded.current_trade_id,
            position_side = excluded.position_side,
            position_qty = excluded.position_qty,
            entry_hour_start_ts = excluded.entry_hour_start_ts,
            entry_hour_start_ts_ct = excluded.entry_hour_start_ts_ct,
            entry_hour_start_ct = excluded.entry_hour_start_ct,
            broker_position_qty = excluded.broker_position_qty,
            broker_avg_cost = excluded.broker_avg_cost,
            last_decision = excluded.last_decision,
            last_decision_reason = excluded.last_decision_reason,
            last_snapshot_time_ts = excluded.last_snapshot_time_ts,
            last_snapshot_time = excluded.last_snapshot_time,
            updated_at = CURRENT_TIMESTAMP;
        """,
        (
            instrument_code,
            current_trade_id,
            position_side,
            position_qty,
            entry_hour_start_ts,
            entry_hour_start_ts_ct,
            entry_hour_start_ct,
            broker_position_qty,
            broker_avg_cost,
            last_decision,
            last_decision_reason,
            last_snapshot_time_ts,
            last_snapshot_time,
        ),
    )


def clear_trade_runtime_state(db_path, instrument_code):
    """Очищаем runtime-состояние инструмента, не удаляя саму строку."""
    _execute_write(
        db_path,
        """
        INSERT INTO trade_runtime_state (
            instrument_code,
            current_trade_id,
            position_side,
            position_qty,
            entry_hour_start_ts,
            entry_hour_start_ts_ct,
            entry_hour_start_ct,
            broker_position_qty,
            broker_avg_cost,
            last_decision,
            last_decision_reason,
            last_snapshot_time_ts,
            last_snapshot_time,
            updated_at
        )
        VALUES (?, NULL, NULL, 0, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)
        ON CONFLICT(instrument_code) DO UPDATE SET
            current_trade_id = NULL,
            position_side = NULL,
            position_qty = 0,
            entry_hour_start_ts = NULL,
            entry_hour_start_ts_ct = NULL,
            entry_hour_start_ct = NULL,
            broker_position_qty = 0,
            broker_avg_cost = NULL,
            last_decision = NULL,
            last_decision_reason = NULL,
            last_snapshot_time_ts = NULL,
            last_snapshot_time = NULL,
            updated_at = CURRENT_TIMESTAMP;
        """,
        (instrument_code,),
    )


def list_trade_events(db_path, *, trade_id=None, instrument_code=None, limit=100):
    """Читаем события из trade_events."""
    if trade_id is not None:
        return _fetch_all_dicts(
            db_path,
            """
            SELECT *
            FROM trade_events
            WHERE trade_id = ?
            ORDER BY event_id DESC
            LIMIT ?;
            """,
            (trade_id, limit),
        )

    if instrument_code is not None:
        return _fetch_all_dicts(
            db_path,
            """
            SELECT *
            FROM trade_events
            WHERE instrument_code = ?
            ORDER BY event_id DESC
            LIMIT ?;
            """,
            (instrument_code, limit),
        )

    return _fetch_all_dicts(
        db_path,
        """
        SELECT *
        FROM trade_events
        ORDER BY event_id DESC
        LIMIT ?;
        """,
        (limit,),
    )

import csv
import json
import sqlite3
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from config import settings_live, BASE_DIR
from contracts import Instrument
from core.db_initializer import build_table_name
from core.logger import get_logger, log_info, setup_logging
from ts.pearson_live import PearsonLiveRuntime, floor_to_hour_ts
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS, StrategyParams

# ============================================================
# НАСТРОЙКИ ЗАПУСКА
# Границы теста задаются в UTC.
# Внутри теста вся логика стратегии продолжает работать в CT,
# как и в боевом роботе.
# ============================================================

START_UTC = "2026-03-30 18:00:00"
END_UTC = "2026-03-31 19:00:00"


def _sanitize_name_part(value: str) -> str:
    return value.strip().replace(" ", "_").replace(":", "-")


RUN_NAME = f"single_run_{_sanitize_name_part(START_UTC)}__to__{_sanitize_name_part(END_UTC)}"

# Всегда кладём результаты в <project>/tester/runs/<RUN_NAME>,
# независимо от того, откуда именно запущен файл.
OUTPUT_DIR = str(BASE_DIR / "tester" / "runs" / RUN_NAME)

PRICE_DB_PATH = settings_live.price_db_path
PREPARED_DB_PATH = settings_live.prepared_db_path

INSTRUMENT_CODE = "MNQ"
QUANTITY = 1
COMMISSION_PER_SIDE_USD = 0.62

# Выход на следующем баре после того, как current_bar_index >= EXIT_BAR_INDEX.
EXIT_BAR_INDEX = settings_live.trading_exit_bar_index

SAVE_RESULT_FILES = True
PRINT_SUMMARY_TO_CONSOLE = False

# Как часто печатать прогресс по числу обработанных баров.
PROGRESS_LOG_EVERY_BARS = 1000

# Печатать ли отдельный лог на старте каждого нового CT-часа.
LOG_EACH_NEW_CT_HOUR = True

# ============================================================
# OVERRIDE ПАРАМЕТРОВ СТРАТЕГИИ
# Меняем только нужные поля, не трогая весь объект целиком.
#
# Примеры:
# STRATEGY_PARAM_OVERRIDES = {
#     "decision_min_last_similarity_score": 0.0,
# }
#
# STRATEGY_PARAM_OVERRIDES = {
#     "pearson_shortlist_min_correlation": 0.78,
#     "pearson_shortlist_top_n": 20,
#     "forecast_top_n_after_similarity": 7,
# }
# ============================================================

STRATEGY_PARAM_OVERRIDES = {
    "decision_min_last_similarity_score": 0.0,
}


def build_run_strategy_params() -> StrategyParams:
    if not STRATEGY_PARAM_OVERRIDES:
        return DEFAULT_STRATEGY_PARAMS

    valid_fields = set(DEFAULT_STRATEGY_PARAMS.__dataclass_fields__.keys())
    unknown_fields = sorted(set(STRATEGY_PARAM_OVERRIDES.keys()) - valid_fields)

    if unknown_fields:
        raise ValueError(
            "Неизвестные поля в STRATEGY_PARAM_OVERRIDES: "
            + ", ".join(unknown_fields)
        )

    return replace(
        DEFAULT_STRATEGY_PARAMS,
        **STRATEGY_PARAM_OVERRIDES,
    )


RUN_STRATEGY_PARAMS = build_run_strategy_params()

UTC_TZ = ZoneInfo("UTC")
CT_TZ = ZoneInfo("America/Chicago")

logger = get_logger(__name__)


@dataclass(slots=True)
class BacktestExecutionParams:
    instrument_code: str = "MNQ"
    quantity: int = 1
    commission_per_side_usd: float = 0.62
    exit_bar_index: int = 717


@dataclass(slots=True)
class BacktestTrade:
    instrument_code: str
    side: str
    quantity: int
    entry_bar_time_ts: int
    entry_bar_time_ts_ct: int
    entry_bar_time_ct: str
    entry_hour_start_ts_ct: int
    entry_price: float
    entry_fill_basis: str
    entry_signal_bar_time_ts: int
    entry_signal_bar_time_ct: str
    entry_signal_bar_index: int
    entry_decision_reason: str
    exit_bar_time_ts: int
    exit_bar_time_ts_ct: int
    exit_bar_time_ct: str
    exit_price: float
    exit_fill_basis: str
    exit_reason: str
    gross_pnl_points: float
    gross_pnl_usd: float
    commission_usd: float
    net_pnl_usd: float
    bars_held: int


@dataclass(slots=True)
class PendingEntry:
    decision: str
    signal_bar_time_ts: int
    signal_bar_time_ct: str
    signal_bar_index: int
    signal_hour_start_ts_ct: int
    decision_reason: str


@dataclass(slots=True)
class PendingExit:
    reason: str
    signal_bar_time_ts: int
    signal_bar_time_ct: str
    signal_bar_index: int


@dataclass(slots=True)
class OpenPosition:
    instrument_code: str
    side: str
    quantity: int
    entry_bar_time_ts: int
    entry_bar_time_ts_ct: int
    entry_bar_time_ct: str
    entry_hour_start_ts_ct: int
    entry_price: float
    entry_fill_basis: str
    entry_signal_bar_time_ts: int
    entry_signal_bar_time_ct: str
    entry_signal_bar_index: int
    entry_decision_reason: str


@dataclass(slots=True)
class BacktestResult:
    summary: dict
    trades: list[BacktestTrade]
    decision_events: list[dict]
    invalid_hours: list[dict]


def parse_utc_input_start(value: str) -> datetime:
    value = value.strip()
    if len(value) == 10:
        return datetime.fromisoformat(value).replace(tzinfo=UTC_TZ)

    return datetime.fromisoformat(value.replace("T", " ")).replace(tzinfo=UTC_TZ)


def parse_utc_input_end_exclusive(value: str) -> datetime:
    value = value.strip()
    if len(value) == 10:
        return (datetime.fromisoformat(value) + timedelta(days=1)).replace(tzinfo=UTC_TZ)

    return datetime.fromisoformat(value.replace("T", " ")).replace(tzinfo=UTC_TZ)


def dt_to_sql_text(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def utc_to_ct_sql_text(dt_utc: datetime) -> str:
    return dt_to_sql_text(dt_utc.astimezone(CT_TZ).replace(tzinfo=None))


def open_sqlite_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def count_price_bars_utc(
        price_db_path: str,
        table_name: str,
        start_utc: str,
        end_utc_exclusive: str,
) -> int:
    conn = open_sqlite_ro(price_db_path)
    try:
        sql = f"""
            SELECT COUNT(*) AS cnt
            FROM {table_name}
            WHERE bar_time >= ?
              AND bar_time < ?
              AND ask_open IS NOT NULL
              AND bid_open IS NOT NULL
              AND ask_close IS NOT NULL
              AND bid_close IS NOT NULL
        """
        row = conn.execute(sql, (start_utc, end_utc_exclusive)).fetchone()
        return int(row["cnt"])
    finally:
        conn.close()


def iter_price_bars_utc(
        price_db_path: str,
        table_name: str,
        start_utc: str,
        end_utc_exclusive: str,
) -> Iterable[sqlite3.Row]:
    conn = open_sqlite_ro(price_db_path)
    try:
        sql = f"""
            SELECT
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
                bid_close
            FROM {table_name}
            WHERE bar_time >= ?
              AND bar_time < ?
              AND ask_open IS NOT NULL
              AND bid_open IS NOT NULL
              AND ask_close IS NOT NULL
              AND bid_close IS NOT NULL
            ORDER BY bar_time_ts
        """
        cursor = conn.execute(sql, (start_utc, end_utc_exclusive))
        yield from cursor
    finally:
        conn.close()


def entry_fill_price(bar: sqlite3.Row, decision: str) -> tuple[float, str]:
    if decision == "LONG":
        return float(bar["ask_open"]), "ask_open"
    if decision == "SHORT":
        return float(bar["bid_open"]), "bid_open"
    raise ValueError(f"Unsupported entry decision={decision}")


def exit_fill_price(bar: sqlite3.Row, side: str) -> tuple[float, str]:
    if side == "LONG":
        return float(bar["bid_open"]), "bid_open"
    if side == "SHORT":
        return float(bar["ask_open"]), "ask_open"
    raise ValueError(f"Unsupported exit side={side}")


def final_close_fill_price(last_bar: sqlite3.Row, side: str) -> tuple[float, str]:
    if side == "LONG":
        return float(last_bar["bid_close"]), "bid_close"
    if side == "SHORT":
        return float(last_bar["ask_close"]), "ask_close"
    raise ValueError(f"Unsupported final close side={side}")


def points_pnl(side: str, entry_price: float, exit_price: float) -> float:
    if side == "LONG":
        return exit_price - entry_price
    if side == "SHORT":
        return entry_price - exit_price
    raise ValueError(f"Unsupported side={side}")


def close_position(
        position: OpenPosition,
        exit_bar: sqlite3.Row,
        exit_price: float,
        exit_fill_basis: str,
        exit_reason: str,
        multiplier: float,
        commission_per_side_usd: float,
        bar_interval_seconds: int,
) -> BacktestTrade:
    gross_pnl_points = points_pnl(position.side, position.entry_price, exit_price)
    gross_pnl_usd = gross_pnl_points * multiplier * position.quantity
    commission_usd = commission_per_side_usd * 2.0 * position.quantity
    net_pnl_usd = gross_pnl_usd - commission_usd
    bars_held = int(
        (int(exit_bar["bar_time_ts"]) - position.entry_bar_time_ts) // bar_interval_seconds
    )

    return BacktestTrade(
        instrument_code=position.instrument_code,
        side=position.side,
        quantity=position.quantity,
        entry_bar_time_ts=position.entry_bar_time_ts,
        entry_bar_time_ts_ct=position.entry_bar_time_ts_ct,
        entry_bar_time_ct=position.entry_bar_time_ct,
        entry_hour_start_ts_ct=position.entry_hour_start_ts_ct,
        entry_price=position.entry_price,
        entry_fill_basis=position.entry_fill_basis,
        entry_signal_bar_time_ts=position.entry_signal_bar_time_ts,
        entry_signal_bar_time_ct=position.entry_signal_bar_time_ct,
        entry_signal_bar_index=position.entry_signal_bar_index,
        entry_decision_reason=position.entry_decision_reason,
        exit_bar_time_ts=int(exit_bar["bar_time_ts"]),
        exit_bar_time_ts_ct=int(exit_bar["bar_time_ts_ct"]),
        exit_bar_time_ct=str(exit_bar["bar_time_ct"]),
        exit_price=exit_price,
        exit_fill_basis=exit_fill_basis,
        exit_reason=exit_reason,
        gross_pnl_points=gross_pnl_points,
        gross_pnl_usd=gross_pnl_usd,
        commission_usd=commission_usd,
        net_pnl_usd=net_pnl_usd,
        bars_held=bars_held,
    )


def run_single_process_backtest(
        start_utc: str,
        end_utc: str,
        strategy_params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
        execution_params: Optional[BacktestExecutionParams] = None,
        settings=None,
) -> BacktestResult:
    if settings is None:
        settings = settings_live
    if execution_params is None:
        execution_params = BacktestExecutionParams()

    start_utc_dt = parse_utc_input_start(start_utc)
    end_utc_exclusive_dt = parse_utc_input_end_exclusive(end_utc)

    if end_utc_exclusive_dt <= start_utc_dt:
        raise ValueError("END_UTC должен быть строго больше START_UTC")

    start_utc_sql = dt_to_sql_text(start_utc_dt.replace(tzinfo=None))
    end_utc_exclusive_sql = dt_to_sql_text(end_utc_exclusive_dt.replace(tzinfo=None))
    start_ct_sql = utc_to_ct_sql_text(start_utc_dt)
    end_ct_exclusive_sql = utc_to_ct_sql_text(end_utc_exclusive_dt)

    instrument_row = Instrument[execution_params.instrument_code]
    multiplier = float(instrument_row["multiplier"])
    table_name = build_table_name(
        instrument_code=execution_params.instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    total_bars_in_range = count_price_bars_utc(
        price_db_path=settings.price_db_path,
        table_name=table_name,
        start_utc=start_utc_sql,
        end_utc_exclusive=end_utc_exclusive_sql,
    )

    log_info(
        logger,
        (
            "BACKTEST START | "
            f"run_name={RUN_NAME} | "
            f"instrument={execution_params.instrument_code} | "
            f"start_utc={start_utc_sql} | "
            f"end_utc_exclusive={end_utc_exclusive_sql} | "
            f"start_ct={start_ct_sql} | "
            f"end_ct_exclusive={end_ct_exclusive_sql} | "
            f"table={table_name} | "
            f"bars_in_range={total_bars_in_range} | "
            f"price_db_path={settings.price_db_path} | "
            f"prepared_db_path={settings.prepared_db_path}"
        ),
        to_telegram=False,
    )

    if total_bars_in_range == 0:
        log_info(
            logger,
            "BACKTEST EMPTY RANGE | В заданном UTC-диапазоне не найдено ни одного полного бара",
            to_telegram=False,
        )

    runtime = PearsonLiveRuntime(
        settings=settings,
        instrument_code=execution_params.instrument_code,
        strategy_params=strategy_params,
    )
    runtime.mark_startup_backfill_completed()

    trades: list[BacktestTrade] = []
    decision_events: list[dict] = []
    invalid_hours: list[dict] = []
    invalid_hours_seen: set[tuple[int, str]] = set()

    position: Optional[OpenPosition] = None
    pending_entry: Optional[PendingEntry] = None
    pending_exit: Optional[PendingExit] = None
    last_entry_hour_start_ts_ct: Optional[int] = None

    bar_count = 0
    decision_count = 0
    trade_signal_count = 0
    last_bar: Optional[sqlite3.Row] = None
    last_logged_hour_start_ts_ct: Optional[int] = None
    t0 = time.perf_counter()

    for bar in iter_price_bars_utc(
            price_db_path=settings.price_db_path,
            table_name=table_name,
            start_utc=start_utc_sql,
            end_utc_exclusive=end_utc_exclusive_sql,
    ):
        bar_count += 1
        last_bar = bar
        current_bar_hour_start_ts_ct = floor_to_hour_ts(int(bar["bar_time_ts_ct"]))

        if LOG_EACH_NEW_CT_HOUR and current_bar_hour_start_ts_ct != last_logged_hour_start_ts_ct:
            last_logged_hour_start_ts_ct = current_bar_hour_start_ts_ct
            elapsed = max(time.perf_counter() - t0, 1e-9)
            bars_per_sec = bar_count / elapsed
            log_info(
                logger,
                (
                    "BACKTEST CT HOUR START | "
                    f"bar_time_utc={bar['bar_time']} | "
                    f"bar_time_ct={bar['bar_time_ct']} | "
                    f"bars_processed={bar_count}/{total_bars_in_range} | "
                    f"decisions={decision_count} | "
                    f"trades={len(trades)} | "
                    f"bars_per_sec={bars_per_sec:.1f}"
                ),
                to_telegram=False,
            )

        if position is not None and position.entry_hour_start_ts_ct != current_bar_hour_start_ts_ct:
            fill_price, fill_basis = exit_fill_price(bar, position.side)
            trade = close_position(
                position=position,
                exit_bar=bar,
                exit_price=fill_price,
                exit_fill_basis=fill_basis,
                exit_reason="FORCED_NEW_HOUR_EXIT",
                multiplier=multiplier,
                commission_per_side_usd=execution_params.commission_per_side_usd,
                bar_interval_seconds=strategy_params.pearson_bar_interval_seconds,
            )
            trades.append(trade)
            log_info(
                logger,
                (
                    "BACKTEST EXIT | "
                    f"reason={trade.exit_reason} | "
                    f"side={trade.side} | "
                    f"entry_ct={trade.entry_bar_time_ct} | "
                    f"exit_ct={trade.exit_bar_time_ct} | "
                    f"net_pnl_usd={trade.net_pnl_usd:.2f}"
                ),
                to_telegram=False,
            )
            position = None
            pending_exit = None

        if pending_exit is not None and position is not None:
            fill_price, fill_basis = exit_fill_price(bar, position.side)
            trade = close_position(
                position=position,
                exit_bar=bar,
                exit_price=fill_price,
                exit_fill_basis=fill_basis,
                exit_reason=pending_exit.reason,
                multiplier=multiplier,
                commission_per_side_usd=execution_params.commission_per_side_usd,
                bar_interval_seconds=strategy_params.pearson_bar_interval_seconds,
            )
            trades.append(trade)
            log_info(
                logger,
                (
                    "BACKTEST EXIT | "
                    f"reason={trade.exit_reason} | "
                    f"side={trade.side} | "
                    f"entry_ct={trade.entry_bar_time_ct} | "
                    f"exit_ct={trade.exit_bar_time_ct} | "
                    f"net_pnl_usd={trade.net_pnl_usd:.2f}"
                ),
                to_telegram=False,
            )
            position = None
            pending_exit = None

        if pending_entry is not None and position is None:
            fill_price, fill_basis = entry_fill_price(bar, pending_entry.decision)
            position = OpenPosition(
                instrument_code=execution_params.instrument_code,
                side=pending_entry.decision,
                quantity=execution_params.quantity,
                entry_bar_time_ts=int(bar["bar_time_ts"]),
                entry_bar_time_ts_ct=int(bar["bar_time_ts_ct"]),
                entry_bar_time_ct=str(bar["bar_time_ct"]),
                entry_hour_start_ts_ct=pending_entry.signal_hour_start_ts_ct,
                entry_price=fill_price,
                entry_fill_basis=fill_basis,
                entry_signal_bar_time_ts=pending_entry.signal_bar_time_ts,
                entry_signal_bar_time_ct=pending_entry.signal_bar_time_ct,
                entry_signal_bar_index=pending_entry.signal_bar_index,
                entry_decision_reason=pending_entry.decision_reason,
            )
            log_info(
                logger,
                (
                    "BACKTEST ENTRY | "
                    f"side={position.side} | "
                    f"entry_ct={position.entry_bar_time_ct} | "
                    f"entry_price={position.entry_price:.4f} | "
                    f"signal_ct={position.entry_signal_bar_time_ct} | "
                    f"signal_bar_index={position.entry_signal_bar_index} | "
                    f"reason={position.entry_decision_reason}"
                ),
                to_telegram=False,
            )
            pending_entry = None

        snapshot = runtime.on_closed_bar(bar)

        if not snapshot.current_hour_valid and snapshot.current_hour_invalid_reason:
            invalid_key = (snapshot.hour_start_ts_ct, snapshot.current_hour_invalid_reason)
            if invalid_key not in invalid_hours_seen:
                invalid_hours_seen.add(invalid_key)
                invalid_hours.append(
                    {
                        "hour_start_ts": snapshot.hour_start_ts,
                        "hour_start_ts_ct": snapshot.hour_start_ts_ct,
                        "hour_start_ct": snapshot.hour_start_ct,
                        "hour_slot_ct": snapshot.hour_slot_ct,
                        "reason": snapshot.current_hour_invalid_reason,
                    }
                )

        if snapshot.decision_result is not None:
            decision_count += 1

            diagnostics = snapshot.decision_result.get("diagnostics") or {}
            decision_events.append(
                {
                    "bar_time_ts": int(bar["bar_time_ts"]),
                    "bar_time": str(bar["bar_time"]),
                    "bar_time_ct": str(bar["bar_time_ct"]),
                    "hour_start_ts": snapshot.hour_start_ts,
                    "hour_start_ts_ct": snapshot.hour_start_ts_ct,
                    "hour_start_ct": snapshot.hour_start_ct,
                    "current_bar_index": snapshot.current_bar_index,
                    "decision": snapshot.decision_result["decision"],
                    "reason": snapshot.decision_result["reason"],
                    "current_hour_valid": snapshot.current_hour_valid,
                    "similarity_candidate_count": len(snapshot.ranked_similarity_candidates),
                    "forecast_candidate_count": (
                        snapshot.forecast_summary["candidate_count"]
                        if snapshot.forecast_summary is not None
                        else 0
                    ),
                    "best_similarity_score": diagnostics.get("best_similarity_score"),
                    "last_similarity_score": diagnostics.get("last_similarity_score"),
                    "mean_final_move": diagnostics.get("mean_final_move"),
                    "median_final_move": diagnostics.get("median_final_move"),
                    "positive_ratio": diagnostics.get("positive_ratio"),
                    "negative_ratio": diagnostics.get("negative_ratio"),
                }
            )

            if (
                    position is None
                    and pending_entry is None
                    and snapshot.current_hour_valid
                    and snapshot.decision_result["decision"] in {"LONG", "SHORT"}
                    and snapshot.hour_start_ts_ct != last_entry_hour_start_ts_ct
            ):
                pending_entry = PendingEntry(
                    decision=snapshot.decision_result["decision"],
                    signal_bar_time_ts=int(bar["bar_time_ts"]),
                    signal_bar_time_ct=str(bar["bar_time_ct"]),
                    signal_bar_index=(
                        int(snapshot.current_bar_index)
                        if snapshot.current_bar_index is not None
                        else -1
                    ),
                    signal_hour_start_ts_ct=int(snapshot.hour_start_ts_ct),
                    decision_reason=str(snapshot.decision_result["reason"]),
                )
                trade_signal_count += 1
                last_entry_hour_start_ts_ct = snapshot.hour_start_ts_ct
                log_info(
                    logger,
                    (
                        "BACKTEST SIGNAL | "
                        f"decision={pending_entry.decision} | "
                        f"signal_ct={pending_entry.signal_bar_time_ct} | "
                        f"signal_bar_index={pending_entry.signal_bar_index} | "
                        f"hour_start_ct={snapshot.hour_start_ct} | "
                        f"reason={pending_entry.decision_reason}"
                    ),
                    to_telegram=False,
                )

        if (
                position is not None
                and pending_exit is None
                and snapshot.hour_start_ts_ct == position.entry_hour_start_ts_ct
                and snapshot.current_bar_index is not None
                and snapshot.current_bar_index >= execution_params.exit_bar_index
        ):
            pending_exit = PendingExit(
                reason="EXIT_BAR_INDEX",
                signal_bar_time_ts=int(bar["bar_time_ts"]),
                signal_bar_time_ct=str(bar["bar_time_ct"]),
                signal_bar_index=int(snapshot.current_bar_index),
            )
            log_info(
                logger,
                (
                    "BACKTEST SCHEDULE EXIT | "
                    f"side={position.side} | "
                    f"signal_ct={pending_exit.signal_bar_time_ct} | "
                    f"signal_bar_index={pending_exit.signal_bar_index}"
                ),
                to_telegram=False,
            )

        if PROGRESS_LOG_EVERY_BARS > 0 and (bar_count % PROGRESS_LOG_EVERY_BARS == 0):
            elapsed = max(time.perf_counter() - t0, 1e-9)
            bars_per_sec = bar_count / elapsed
            progress_pct = (bar_count / total_bars_in_range * 100.0) if total_bars_in_range else 0.0
            log_info(
                logger,
                (
                    "BACKTEST PROGRESS | "
                    f"bars_processed={bar_count}/{total_bars_in_range} | "
                    f"progress_pct={progress_pct:.2f} | "
                    f"decisions={decision_count} | "
                    f"trade_signals={trade_signal_count} | "
                    f"closed_trades={len(trades)} | "
                    f"invalid_hours={len(invalid_hours)} | "
                    f"bars_per_sec={bars_per_sec:.1f}"
                ),
                to_telegram=False,
            )

    if position is not None and last_bar is not None:
        fill_price, fill_basis = final_close_fill_price(last_bar, position.side)
        trade = close_position(
            position=position,
            exit_bar=last_bar,
            exit_price=fill_price,
            exit_fill_basis=fill_basis,
            exit_reason="END_OF_RANGE_LAST_CLOSE",
            multiplier=multiplier,
            commission_per_side_usd=execution_params.commission_per_side_usd,
            bar_interval_seconds=strategy_params.pearson_bar_interval_seconds,
        )
        trades.append(trade)
        log_info(
            logger,
            (
                "BACKTEST FINAL CLOSE | "
                f"side={trade.side} | "
                f"exit_ct={trade.exit_bar_time_ct} | "
                f"net_pnl_usd={trade.net_pnl_usd:.2f}"
            ),
            to_telegram=False,
        )

    gross_pnl_usd = sum(item.gross_pnl_usd for item in trades)
    commission_usd = sum(item.commission_usd for item in trades)
    net_pnl_usd = sum(item.net_pnl_usd for item in trades)
    winning_trades = sum(1 for item in trades if item.net_pnl_usd > 0.0)
    losing_trades = sum(1 for item in trades if item.net_pnl_usd < 0.0)
    long_trades = sum(1 for item in trades if item.side == "LONG")
    short_trades = sum(1 for item in trades if item.side == "SHORT")
    elapsed_total = time.perf_counter() - t0

    summary = {
        "run_name": RUN_NAME,
        "output_dir": str(Path(OUTPUT_DIR).resolve()),
        "instrument_code": execution_params.instrument_code,
        "start_utc": start_utc_sql,
        "end_utc_exclusive": end_utc_exclusive_sql,
        "start_ct": start_ct_sql,
        "end_ct_exclusive": end_ct_exclusive_sql,
        "bar_count": bar_count,
        "decision_count": decision_count,
        "trade_signal_count": trade_signal_count,
        "trade_count": len(trades),
        "long_trade_count": long_trades,
        "short_trade_count": short_trades,
        "winning_trade_count": winning_trades,
        "losing_trade_count": losing_trades,
        "flat_trade_count": len(trades) - winning_trades - losing_trades,
        "win_rate": (winning_trades / len(trades)) if trades else 0.0,
        "gross_pnl_usd": gross_pnl_usd,
        "commission_usd": commission_usd,
        "net_pnl_usd": net_pnl_usd,
        "avg_net_pnl_per_trade_usd": (net_pnl_usd / len(trades)) if trades else 0.0,
        "invalid_hour_count": len(invalid_hours),
        "elapsed_seconds": elapsed_total,
        "bars_per_second": (bar_count / elapsed_total) if elapsed_total > 0 else 0.0,
        "execution_params": asdict(execution_params),
        "strategy_params": asdict(strategy_params),
        "strategy_param_overrides": dict(STRATEGY_PARAM_OVERRIDES),
        "price_db_path": settings.price_db_path,
        "prepared_db_path": settings.prepared_db_path,
        "table_name": table_name,
    }

    log_info(
        logger,
        (
            "BACKTEST FINISH | "
            f"bars={bar_count} | "
            f"decisions={decision_count} | "
            f"trade_signals={trade_signal_count} | "
            f"trade_count={len(trades)} | "
            f"net_pnl_usd={net_pnl_usd:.2f} | "
            f"invalid_hours={len(invalid_hours)} | "
            f"elapsed_seconds={elapsed_total:.2f} | "
            f"bars_per_second={(bar_count / elapsed_total) if elapsed_total > 0 else 0.0:.1f}"
        ),
        to_telegram=False,
    )

    return BacktestResult(
        summary=summary,
        trades=trades,
        decision_events=decision_events,
        invalid_hours=invalid_hours,
    )


def save_backtest_result(result: BacktestResult, output_dir: str | Path) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary_path = output_path / "summary.json"
    trades_path = output_path / "trades.csv"
    decisions_path = output_path / "decision_events.jsonl"
    invalid_hours_path = output_path / "invalid_hours.json"

    summary_path.write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    trade_fieldnames = [
        "instrument_code",
        "side",
        "quantity",
        "entry_bar_time_ts",
        "entry_bar_time_ts_ct",
        "entry_bar_time_ct",
        "entry_hour_start_ts_ct",
        "entry_price",
        "entry_fill_basis",
        "entry_signal_bar_time_ts",
        "entry_signal_bar_time_ct",
        "entry_signal_bar_index",
        "entry_decision_reason",
        "exit_bar_time_ts",
        "exit_bar_time_ts_ct",
        "exit_bar_time_ct",
        "exit_price",
        "exit_fill_basis",
        "exit_reason",
        "gross_pnl_points",
        "gross_pnl_usd",
        "commission_usd",
        "net_pnl_usd",
        "bars_held",
    ]

    with trades_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trade_fieldnames)
        writer.writeheader()
        for trade in result.trades:
            writer.writerow(asdict(trade))

    with decisions_path.open("w", encoding="utf-8") as f:
        for item in result.decision_events:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    invalid_hours_path.write_text(
        json.dumps(result.invalid_hours, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_runtime_settings():
    return replace(
        settings_live,
        price_db_path=PRICE_DB_PATH,
        prepared_db_path=PREPARED_DB_PATH,
    )


def build_execution_params() -> BacktestExecutionParams:
    return BacktestExecutionParams(
        instrument_code=INSTRUMENT_CODE,
        quantity=QUANTITY,
        commission_per_side_usd=COMMISSION_PER_SIDE_USD,
        exit_bar_index=EXIT_BAR_INDEX,
    )


def main() -> None:
    setup_logging()

    settings = build_runtime_settings()
    execution_params = build_execution_params()

    result = run_single_process_backtest(
        start_utc=START_UTC,
        end_utc=END_UTC,
        strategy_params=RUN_STRATEGY_PARAMS,
        execution_params=execution_params,
        settings=settings,
    )

    if SAVE_RESULT_FILES:
        save_backtest_result(result, OUTPUT_DIR)

    if PRINT_SUMMARY_TO_CONSOLE:
        print(json.dumps(result.summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

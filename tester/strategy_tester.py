import csv
import itertools
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_decision import evaluate_decision_layer
from ts.candidate_forecast import build_group_forecast_from_prepared_candidates
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.pearson_runtime import PearsonAnalysisWindow
from ts.prepared_builder import (
    load_price_rows_for_analysis_window,
    validate_analysis_window_price_rows,
)
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS
from ts.ts_config import TradingHalfHourMode

COMMISSION_PER_SIDE_USD = 0.62
HALF_HOUR_SECONDS = 1800
ANALYSIS_WINDOW_SECONDS = 3600

SUMMARY_EXCLUDED_STRATEGY_FIELDS = {
    "pearson_bar_interval_seconds",
    "pearson_hour_seconds",
    "pearson_eval_start_minute",
    "pearson_eval_end_minute",
    "search_slot_groups",
}


def floor_to_half_hour_ts(ts: int) -> int:
    return (int(ts) // HALF_HOUR_SECONDS) * HALF_HOUR_SECONDS


def ceil_to_half_hour_ts(ts: int) -> int:
    floored = floor_to_half_hour_ts(ts)
    if int(ts) == floored:
        return int(ts)
    return floored + HALF_HOUR_SECONDS


def floor_to_hour_ts(ts: int) -> int:
    return (int(ts) // 3600) * 3600


def utc_datetime_to_ts(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def utc_ts_to_text(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def local_axis_ts_to_text(local_axis_ts: int) -> str:
    # local_axis_ts уже является CT-axis timestamp проекта.
    # timezone.utc здесь используется как нейтральный formatter.
    return datetime.fromtimestamp(int(local_axis_ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_elapsed(seconds: float) -> str:
    total_seconds = int(seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def sanitize_dt_for_filename(dt_str: str) -> str:
    return dt_str.replace("-", "").replace(":", "").replace(" ", "_")


def open_readonly_sqlite(db_path: str | Path):
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def get_strategy_param_field_names() -> list[str]:
    return [field.name for field in fields(DEFAULT_STRATEGY_PARAMS)]


def normalize_param_spec_values(values) -> list:
    values = list(values)
    if not values:
        raise ValueError("Parameter values list must not be empty")
    return values


def build_param_grid(param_specs: dict[str, list]) -> list[dict]:
    valid_fields = set(get_strategy_param_field_names())

    unknown_fields = [name for name in param_specs.keys() if name not in valid_fields]
    if unknown_fields:
        raise ValueError(f"Unknown StrategyParams fields: {unknown_fields}")

    if not param_specs:
        return [{}]

    param_names = list(param_specs.keys())
    param_value_lists = [normalize_param_spec_values(param_specs[name]) for name in param_names]

    combinations = []
    for combo_values in itertools.product(*param_value_lists):
        combinations.append(dict(zip(param_names, combo_values)))

    return combinations


def build_run_output_csv_path(output_dir: Path, instrument_code: str, combo_index: int) -> Path:
    return output_dir / f"strategy_tester_{instrument_code}_{combo_index}.csv"


def build_summary_output_csv_path(output_dir: Path, start_utc: str, end_utc: str) -> Path:
    start_part = sanitize_dt_for_filename(start_utc)
    end_part = sanitize_dt_for_filename(end_utc)
    return output_dir / f"parameter_sweep_summary_{start_part}_{end_part}.csv"


def parse_summary_csv_value(value: str):
    if value == "":
        return value

    if value == "True":
        return True

    if value == "False":
        return False

    try:
        if "." not in value and "e" not in value.lower():
            return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return value


def load_existing_summary_rows(summary_csv_path: str | Path) -> list[dict]:
    summary_path = Path(summary_csv_path)
    if not summary_path.exists():
        return []

    with summary_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [
            {key: parse_summary_csv_value(value) for key, value in row.items()}
            for row in reader
        ]


def get_resume_start_run_index(summary_rows: list[dict]) -> int:
    if not summary_rows:
        return 1

    last_completed_run_index = max(int(row["run_index"]) for row in summary_rows)
    return last_completed_run_index + 1


def strategy_params_to_csv_dict(strategy_params) -> dict:
    result = {}
    for key, value in asdict(strategy_params).items():
        if isinstance(value, TradingHalfHourMode):
            result[key] = value.value
        else:
            result[key] = value
    return result


def build_prepared_window_map(prepared_windows: list[dict]) -> dict:
    result = {}
    for item in prepared_windows:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])
        result[key] = item
    return result


def build_shortlist_prepared_windows(
        ranked_candidates: list[dict],
        prepared_window_map: dict,
) -> list[dict]:
    result = []

    for item in ranked_candidates:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])
        prepared_payload = prepared_window_map.get(key)
        if prepared_payload is None:
            raise ValueError(
                "Prepared analysis window for ranked candidate not found: "
                f"hour_start_ts={item['hour_start_ts']}, "
                f"hour_start_ts_ct={item['hour_start_ts_ct']}"
            )

        shortlist_item = dict(prepared_payload)
        shortlist_item["correlation"] = item["correlation"]
        result.append(shortlist_item)

    return result


def pick_prepared_windows_by_ranked_candidates(
        ranked_candidates: list[dict],
        prepared_window_map: dict,
        limit: int | None = None,
) -> list[dict]:
    result = []
    source_items = ranked_candidates if limit is None else ranked_candidates[:limit]

    for item in source_items:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])
        prepared_payload = prepared_window_map.get(key)
        if prepared_payload is None:
            raise ValueError(
                "Prepared analysis window for ranked candidate not found: "
                f"hour_start_ts={item['hour_start_ts']}, "
                f"hour_start_ts_ct={item['hour_start_ts_ct']}"
            )
        result.append(prepared_payload)

    return result


def build_current_reference_price(current_analysis_window, current_bar_index: int):
    if current_analysis_window.mid_open_0 is None:
        return None
    if current_bar_index is None:
        return None
    if not (0 <= current_bar_index < len(current_analysis_window.x)):
        return None

    return current_analysis_window.mid_open_0 * (1.0 + current_analysis_window.x[current_bar_index])


def normalize_trading_half_hour_mode(mode) -> TradingHalfHourMode:
    if isinstance(mode, TradingHalfHourMode):
        return mode
    return TradingHalfHourMode(mode)


def is_trade_slot_allowed_by_mode(trade_slot_start_ts: int, strategy_params) -> bool:
    mode = normalize_trading_half_hour_mode(strategy_params.trading_half_hour_mode)

    is_first_half = (int(trade_slot_start_ts) % 3600) == 0

    if mode == TradingHalfHourMode.ANY_HALF:
        return True
    if mode == TradingHalfHourMode.FIRST_HALF_ONLY:
        return is_first_half
    if mode == TradingHalfHourMode.SECOND_HALF_ONLY:
        return not is_first_half

    return False


def is_friday_weekly_cutoff_trade_slot(trade_slot_start_ts_ct: int) -> bool:
    # CT-axis timestamp форматируем/интерпретируем как локальное Chicago-время.
    dt_ct = datetime.fromtimestamp(int(trade_slot_start_ts_ct), tz=timezone.utc)
    return dt_ct.weekday() == 4 and dt_ct.hour >= 15


def iter_trade_slot_start_ts_range(start_ts: int, end_ts: int):
    current_trade_slot_start_ts = floor_to_half_hour_ts(start_ts)
    end_trade_slot_exclusive_ts = ceil_to_half_hour_ts(end_ts)

    while current_trade_slot_start_ts < end_trade_slot_exclusive_ts:
        yield current_trade_slot_start_ts
        current_trade_slot_start_ts += HALF_HOUR_SECONDS


def build_analysis_window_start_ts_list(
        start_ts: int,
        end_ts: int,
        strategy_params,
) -> list[int]:
    result = []

    for trade_slot_start_ts in iter_trade_slot_start_ts_range(start_ts, end_ts):
        if not is_trade_slot_allowed_by_mode(
                trade_slot_start_ts=trade_slot_start_ts,
                strategy_params=strategy_params,
        ):
            continue

        result.append(trade_slot_start_ts - HALF_HOUR_SECONDS)

    return result


def chunk_list(items: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def load_analysis_window_price_rows(
        price_conn,
        table_name: str,
        analysis_window_start_ts: int,
):
    rows = load_price_rows_for_analysis_window(
        price_conn=price_conn,
        table_name=table_name,
        analysis_window_start_ts=analysis_window_start_ts,
    )

    validate_analysis_window_price_rows(
        rows=rows,
        analysis_window_start_ts=analysis_window_start_ts,
    )

    return rows


def load_prepared_candidate_windows(
        prepared_conn,
        table_name: str,
        current_analysis_window_start_ts_ct: int,
        strategy_params,
) -> list[dict]:
    current_hour_slot_ct = (int(current_analysis_window_start_ts_ct) // 3600) % 24
    allowed_hour_slots_ct = strategy_params.resolve_allowed_hour_slots(current_hour_slot_ct)

    if not allowed_hour_slots_ct:
        return []

    analysis_window_start_offset_seconds = int(current_analysis_window_start_ts_ct) % 3600

    return load_prepared_hours_by_slots(
        prepared_conn=prepared_conn,
        table_name=table_name,
        hour_slots_ct=allowed_hour_slots_ct,
        before_hour_start_ts_ct=current_analysis_window_start_ts_ct,
        analysis_window_start_offset_seconds=analysis_window_start_offset_seconds,
    )


def build_base_summary_row(
        current_analysis_window,
        row,
        current_bar_count: int | None,
        current_bar_index: int | None,
        pearson_ranked_candidates: list[dict],
        similarity_ranked_candidates: list[dict],
        forecast_summary: dict | None,
        decision_result: dict | None,
        pearson_min_shortlist_passed: bool,
        current_reference_price: float | None,
        reason_override: str | None = None,
) -> dict:
    diagnostics = decision_result["diagnostics"] if decision_result is not None else {}

    decision = decision_result["decision"] if decision_result is not None else "NO_TRADE"
    reason = decision_result["reason"] if decision_result is not None else "PEARSON_MIN_SHORTLIST_NOT_REACHED"

    if reason_override is not None:
        decision = "NO_TRADE"
        reason = reason_override

    analysis_window_start_ts = current_analysis_window.hour_start_ts
    analysis_window_start_ts_ct = current_analysis_window.hour_start_ts_ct
    trade_slot_start_ts = analysis_window_start_ts + HALF_HOUR_SECONDS
    trade_slot_start_ts_ct = analysis_window_start_ts_ct + HALF_HOUR_SECONDS

    return {
        # Старые названия оставлены для совместимости CSV.
        # По смыслу hour_* = start 60-минутного analysis window.
        "hour_start_ts": analysis_window_start_ts,
        "hour_start_ts_ct": analysis_window_start_ts_ct,
        "hour_start": current_analysis_window.hour_start,
        "hour_start_ct": current_analysis_window.hour_start_ct,
        "hour_slot_ct": current_analysis_window.hour_slot_ct,

        # Новые явные поля.
        "analysis_window_start_ts": analysis_window_start_ts,
        "analysis_window_start_ts_ct": analysis_window_start_ts_ct,
        "analysis_window_start": current_analysis_window.hour_start,
        "analysis_window_start_ct": current_analysis_window.hour_start_ct,
        "trade_slot_start_ts": trade_slot_start_ts,
        "trade_slot_start_ts_ct": trade_slot_start_ts_ct,
        "trade_slot_start": utc_ts_to_text(trade_slot_start_ts),
        "trade_slot_start_ct": local_axis_ts_to_text(trade_slot_start_ts_ct),

        "last_bar_time_ts": row["bar_time_ts"] if row is not None else None,
        "last_bar_time": row["bar_time"] if row is not None else None,
        "last_bar_time_ts_ct": row["bar_time_ts_ct"] if row is not None else None,
        "last_bar_time_ct": row["bar_time_ct"] if row is not None else None,
        "current_bar_index": current_bar_index,
        "current_bar_count": current_bar_count,
        "pearson_ranked_count": len(pearson_ranked_candidates),
        "pearson_min_shortlist_passed": pearson_min_shortlist_passed,
        "similarity_ranked_count": len(similarity_ranked_candidates),
        "forecast_candidate_count": (
            forecast_summary["candidate_count"] if forecast_summary is not None else None
        ),
        "decision": decision,
        "reason": reason,
        "current_reference_price": current_reference_price,
        "best_similarity_score": diagnostics.get("best_similarity_score"),
        "last_similarity_score": diagnostics.get("last_similarity_score"),
        "mean_final_move": diagnostics.get("mean_final_move"),
        "median_final_move": diagnostics.get("median_final_move"),
        "mean_final_move_points": diagnostics.get("mean_final_move_points"),
        "median_final_move_points": diagnostics.get("median_final_move_points"),
        "min_final_move_points": diagnostics.get("min_final_move_points"),
        "positive_ratio": diagnostics.get("positive_ratio"),
        "negative_ratio": diagnostics.get("negative_ratio"),
        "mean_max_upside": diagnostics.get("mean_max_upside"),
        "mean_max_drawdown": diagnostics.get("mean_max_drawdown"),
        "trade_opened": False,
        "trade_side": None,
        "entry_time": None,
        "entry_time_ct": None,
        "exit_time": None,
        "exit_time_ct": None,
        "entry_price": None,
        "exit_price": None,
        "net_pnl": None,
    }


def apply_trade_to_row(
        summary_row: dict,
        analysis_window_rows,
        side: str,
        signal_bar_index: int,
        multiplier: float,
        exit_bar_index: int,
) -> dict:
    entry_exec_index = signal_bar_index + 1
    exit_exec_index = exit_bar_index + 1

    if entry_exec_index >= len(analysis_window_rows):
        summary_row["decision"] = "NO_TRADE"
        summary_row["reason"] = "ENTRY_NEXT_BAR_NOT_AVAILABLE"
        return summary_row

    if exit_exec_index >= len(analysis_window_rows):
        summary_row["decision"] = "NO_TRADE"
        summary_row["reason"] = "EXIT_BAR_NOT_AVAILABLE"
        return summary_row

    if exit_exec_index <= entry_exec_index:
        summary_row["decision"] = "NO_TRADE"
        summary_row["reason"] = "EXIT_BEFORE_ENTRY"
        return summary_row

    entry_row = analysis_window_rows[entry_exec_index]
    exit_row = analysis_window_rows[exit_exec_index]

    if side == "LONG":
        entry_price = entry_row["ask_open"]
        exit_price = exit_row["bid_open"]
        net_pnl = (exit_price - entry_price) * multiplier - (COMMISSION_PER_SIDE_USD * 2.0)
    elif side == "SHORT":
        entry_price = entry_row["bid_open"]
        exit_price = exit_row["ask_open"]
        net_pnl = (entry_price - exit_price) * multiplier - (COMMISSION_PER_SIDE_USD * 2.0)
    else:
        raise ValueError(f"Unsupported side: {side}")

    summary_row["trade_opened"] = True
    summary_row["trade_side"] = side
    summary_row["entry_time"] = entry_row["bar_time"]
    summary_row["entry_time_ct"] = entry_row["bar_time_ct"]
    summary_row["exit_time"] = exit_row["bar_time"]
    summary_row["exit_time_ct"] = exit_row["bar_time_ct"]
    summary_row["entry_price"] = entry_price
    summary_row["exit_price"] = exit_price
    summary_row["net_pnl"] = net_pnl
    return summary_row


def save_analysis_window_summary_to_csv(rows: list[dict], output_csv_path: str | Path):
    trade_rows = [row for row in rows if row.get("trade_opened")]
    if not trade_rows:
        return

    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "hour_start_ts",
        "hour_start_ts_ct",
        "hour_start",
        "hour_start_ct",
        "hour_slot_ct",
        "analysis_window_start_ts",
        "analysis_window_start_ts_ct",
        "analysis_window_start",
        "analysis_window_start_ct",
        "trade_slot_start_ts",
        "trade_slot_start_ts_ct",
        "trade_slot_start",
        "trade_slot_start_ct",
        "last_bar_time_ts",
        "last_bar_time",
        "last_bar_time_ts_ct",
        "last_bar_time_ct",
        "current_bar_index",
        "current_bar_count",
        "pearson_ranked_count",
        "pearson_min_shortlist_passed",
        "similarity_ranked_count",
        "forecast_candidate_count",
        "decision",
        "reason",
        "current_reference_price",
        "best_similarity_score",
        "last_similarity_score",
        "mean_final_move",
        "median_final_move",
        "mean_final_move_points",
        "median_final_move_points",
        "min_final_move_points",
        "positive_ratio",
        "negative_ratio",
        "mean_max_upside",
        "mean_max_drawdown",
        "trade_opened",
        "trade_side",
        "entry_time",
        "entry_time_ct",
        "exit_time",
        "exit_time_ct",
        "entry_price",
        "exit_price",
        "net_pnl",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in trade_rows:
            writer.writerow(row)


def save_runs_summary_to_csv(rows: list[dict], output_csv_path: str | Path) -> None:
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        raise ValueError("rows is empty")

    preferred_prefix = [
        "run_index",
        "analysis_windows_total_in_range",
        "analysis_windows_skipped",
        "hours_total_in_range",
        "hours_skipped",
        "total_snapshot_count",
        "trades_count",
        "long_count",
        "short_count",
        "win_count",
        "loss_count",
        "flat_count",
        "net_pnl_total",
        "avg_trade_net_pnl",
    ]

    all_keys = []
    seen = set()

    for key in preferred_prefix:
        if key not in seen and any(key in row for row in rows):
            seen.add(key)
            all_keys.append(key)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                all_keys.append(key)

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_analysis_window_pipeline(
        analysis_window_rows,
        prepared_candidate_windows,
        analysis_window_start_ts: int,
        strategy_params,
        multiplier: float,
        exit_bar_index: int,
) -> tuple[dict, int]:
    if not analysis_window_rows:
        raise ValueError("analysis_window_rows is empty")

    first_row = analysis_window_rows[0]
    analysis_window_start_ts_ct = first_row["bar_time_ts_ct"]

    current_analysis_window = PearsonAnalysisWindow(
        analysis_window_start_ts=analysis_window_start_ts,
        analysis_window_start_ts_ct=analysis_window_start_ts_ct,
    )
    current_analysis_window.set_candidates(prepared_candidate_windows)

    prepared_window_map = build_prepared_window_map(prepared_candidate_windows)

    trade_slot_start_ts_ct = analysis_window_start_ts_ct + HALF_HOUR_SECONDS
    if is_friday_weekly_cutoff_trade_slot(trade_slot_start_ts_ct):
        return build_base_summary_row(
            current_analysis_window=current_analysis_window,
            row=None,
            current_bar_count=None,
            current_bar_index=None,
            pearson_ranked_candidates=[],
            similarity_ranked_candidates=[],
            forecast_summary=None,
            decision_result=None,
            pearson_min_shortlist_passed=False,
            current_reference_price=None,
            reason_override="FRIDAY_WEEKLY_CUTOFF_SLOT",
        ), 0

    start_bar_count = strategy_params.pearson_eval_start_bar_count()
    end_bar_count_exclusive = strategy_params.pearson_eval_end_bar_count_exclusive()
    start_eval_index = max(start_bar_count - 1, 0)

    for row in analysis_window_rows[:start_eval_index]:
        current_analysis_window.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

    last_summary_row = None
    evaluated_rows_count = 0

    for row in analysis_window_rows[start_eval_index:]:
        current_analysis_window.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

        current_bar_count = current_analysis_window.current_n()
        current_bar_index = current_analysis_window.current_bar_index()

        if current_bar_count < start_bar_count:
            continue

        if current_bar_count >= end_bar_count_exclusive:
            break

        if not current_analysis_window.candidates_initialized:
            current_analysis_window.initialize_candidates()
        else:
            current_analysis_window.update_candidates_for_last_bar()

        pearson_ranked_candidates = current_analysis_window.get_ranked_candidates(
            min_correlation=strategy_params.pearson_shortlist_min_correlation,
            top_n=strategy_params.pearson_shortlist_top_n,
        )

        pearson_min_shortlist_passed = strategy_params.pearson_has_enough_shortlist_candidates(
            len(pearson_ranked_candidates)
        )

        current_reference_price = build_current_reference_price(
            current_analysis_window=current_analysis_window,
            current_bar_index=current_bar_index,
        )

        similarity_ranked_candidates = []
        forecast_summary = None
        decision_result = None

        if pearson_min_shortlist_passed:
            shortlist_prepared_windows = build_shortlist_prepared_windows(
                ranked_candidates=pearson_ranked_candidates,
                prepared_window_map=prepared_window_map,
            )

            similarity_ranked_candidates = rank_prepared_candidates_by_similarity(
                current_values=list(current_analysis_window.x),
                prepared_hours=shortlist_prepared_windows,
                params=strategy_params,
            )

            forecast_prepared_windows = pick_prepared_windows_by_ranked_candidates(
                ranked_candidates=similarity_ranked_candidates,
                prepared_window_map=prepared_window_map,
                limit=strategy_params.forecast_top_n_after_similarity,
            )

            if forecast_prepared_windows:
                forecast_summary = build_group_forecast_from_prepared_candidates(
                    prepared_hours=forecast_prepared_windows,
                    current_bar_index=current_bar_index,
                )

            decision_result = evaluate_decision_layer(
                ranked_similarity_candidates=similarity_ranked_candidates,
                forecast_summary=forecast_summary,
                current_reference_price=current_reference_price,
                params=strategy_params,
            )

        summary_row = build_base_summary_row(
            current_analysis_window=current_analysis_window,
            row=row,
            current_bar_count=current_bar_count,
            current_bar_index=current_bar_index,
            pearson_ranked_candidates=pearson_ranked_candidates,
            similarity_ranked_candidates=similarity_ranked_candidates,
            forecast_summary=forecast_summary,
            decision_result=decision_result,
            pearson_min_shortlist_passed=pearson_min_shortlist_passed,
            current_reference_price=current_reference_price,
        )

        evaluated_rows_count += 1
        last_summary_row = summary_row

        if summary_row["decision"] in {"LONG", "SHORT"}:
            summary_row = apply_trade_to_row(
                summary_row=summary_row,
                analysis_window_rows=analysis_window_rows,
                side=summary_row["decision"],
                signal_bar_index=current_bar_index,
                multiplier=multiplier,
                exit_bar_index=exit_bar_index,
            )
            return summary_row, evaluated_rows_count

    if last_summary_row is not None:
        return last_summary_row, evaluated_rows_count

    return build_base_summary_row(
        current_analysis_window=current_analysis_window,
        row=None,
        current_bar_count=current_analysis_window.current_n(),
        current_bar_index=current_analysis_window.current_bar_index(),
        pearson_ranked_candidates=[],
        similarity_ranked_candidates=[],
        forecast_summary=None,
        decision_result=None,
        pearson_min_shortlist_passed=False,
        current_reference_price=None,
        reason_override="NO_EVAL_WINDOW",
    ), evaluated_rows_count


def process_analysis_window_chunk(
        instrument_code: str,
        analysis_window_start_ts_chunk: list[int],
        strategy_params,
        price_db_path: str | Path,
        prepared_db_path: str | Path,
        multiplier: float,
        exit_bar_index: int,
) -> dict:
    instrument_row = Instrument[instrument_code]
    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    analysis_window_summary_rows = []
    skipped_analysis_windows = []
    total_snapshot_count = 0

    price_conn = open_readonly_sqlite(price_db_path)
    prepared_conn = open_readonly_sqlite(prepared_db_path)

    try:
        for analysis_window_start_ts in analysis_window_start_ts_chunk:
            try:
                analysis_window_rows = load_analysis_window_price_rows(
                    price_conn=price_conn,
                    table_name=table_name,
                    analysis_window_start_ts=analysis_window_start_ts,
                )

                first_row = analysis_window_rows[0]
                analysis_window_start_ts_ct = first_row["bar_time_ts_ct"]

                prepared_candidate_windows = load_prepared_candidate_windows(
                    prepared_conn=prepared_conn,
                    table_name=table_name,
                    current_analysis_window_start_ts_ct=analysis_window_start_ts_ct,
                    strategy_params=strategy_params,
                )

                result_row, evaluated_rows_count = run_analysis_window_pipeline(
                    analysis_window_rows=analysis_window_rows,
                    prepared_candidate_windows=prepared_candidate_windows,
                    analysis_window_start_ts=analysis_window_start_ts,
                    strategy_params=strategy_params,
                    multiplier=multiplier,
                    exit_bar_index=exit_bar_index,
                )

                total_snapshot_count += evaluated_rows_count
                analysis_window_summary_rows.append(result_row)

            except Exception as exc:
                skipped_analysis_windows.append(
                    {
                        "analysis_window_start_ts": analysis_window_start_ts,
                        "analysis_window_start": utc_ts_to_text(analysis_window_start_ts),
                        # old compatible keys
                        "hour_start_ts": analysis_window_start_ts,
                        "hour_start": utc_ts_to_text(analysis_window_start_ts),
                        "error": str(exc),
                    }
                )

        return {
            "analysis_window_summary_rows": analysis_window_summary_rows,
            "skipped_analysis_windows": skipped_analysis_windows,
            "total_snapshot_count": total_snapshot_count,
            "chunk_analysis_windows_count": len(analysis_window_start_ts_chunk),
        }

    finally:
        price_conn.close()
        prepared_conn.close()


def run_single_tester_multiprocess(
        instrument_code: str,
        start_utc: str,
        end_utc: str,
        strategy_params,
        price_db_path: str | Path,
        prepared_db_path: str | Path,
        multiplier: float,
        max_workers: int,
        chunk_size: int,
        exit_bar_index: int,
):
    start_ts = utc_datetime_to_ts(start_utc)
    end_ts = utc_datetime_to_ts(end_utc)

    if end_ts < start_ts:
        raise ValueError(f"end_utc < start_utc: {end_utc} < {start_utc}")

    all_analysis_window_starts = build_analysis_window_start_ts_list(
        start_ts=start_ts,
        end_ts=end_ts,
        strategy_params=strategy_params,
    )
    analysis_windows_total_in_range = len(all_analysis_window_starts)

    if not all_analysis_window_starts:
        raise ValueError("No analysis windows in requested range")

    analysis_window_chunks = chunk_list(all_analysis_window_starts, chunk_size)
    run_started_perf = time.perf_counter()

    analysis_window_summary_rows = []
    skipped_analysis_windows = []
    total_snapshot_count = 0
    completed_analysis_windows = 0
    completed_chunks = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_analysis_window_chunk,
                instrument_code,
                analysis_window_chunk,
                strategy_params,
                price_db_path,
                prepared_db_path,
                multiplier,
                exit_bar_index,
            )
            for analysis_window_chunk in analysis_window_chunks
        ]

        for future in as_completed(futures):
            chunk_result = future.result()

            analysis_window_summary_rows.extend(chunk_result["analysis_window_summary_rows"])
            skipped_analysis_windows.extend(chunk_result["skipped_analysis_windows"])
            total_snapshot_count += chunk_result["total_snapshot_count"]
            completed_analysis_windows += chunk_result["chunk_analysis_windows_count"]
            completed_chunks += 1

            elapsed = time.perf_counter() - run_started_perf
            percent = (
                (completed_analysis_windows / analysis_windows_total_in_range) * 100
                if analysis_windows_total_in_range else 100.0
            )
            print(
                f"[progress] {completed_analysis_windows}/{analysis_windows_total_in_range} "
                f"analysis_windows ({percent:.1f}%) | "
                f"chunks={completed_chunks}/{len(analysis_window_chunks)} "
                f"| elapsed={format_elapsed(elapsed)}"
            )

    analysis_window_summary_rows.sort(
        key=lambda row: (
            row["trade_slot_start_ts"],
            row["hour_start_ts"],
        )
    )
    skipped_analysis_windows.sort(key=lambda row: row["analysis_window_start_ts"])

    decision_counter = Counter(row["decision"] for row in analysis_window_summary_rows)
    reason_counter = Counter(row["reason"] for row in analysis_window_summary_rows)

    trade_rows = [row for row in analysis_window_summary_rows if row["trade_opened"]]
    long_count = sum(1 for row in trade_rows if row["trade_side"] == "LONG")
    short_count = sum(1 for row in trade_rows if row["trade_side"] == "SHORT")
    win_count = sum(1 for row in trade_rows if row["net_pnl"] is not None and row["net_pnl"] > 0)
    loss_count = sum(1 for row in trade_rows if row["net_pnl"] is not None and row["net_pnl"] < 0)
    flat_count = sum(1 for row in trade_rows if row["net_pnl"] is not None and row["net_pnl"] == 0)
    net_pnl_total = sum(row["net_pnl"] for row in trade_rows if row["net_pnl"] is not None)
    avg_trade_net_pnl = (net_pnl_total / len(trade_rows)) if trade_rows else 0.0

    elapsed_seconds = time.perf_counter() - run_started_perf

    result = {
        "input": {
            "instrument_code": instrument_code,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "strategy_params": strategy_params_to_csv_dict(strategy_params),
            "multiplier": multiplier,
            "commission_per_side_usd": COMMISSION_PER_SIDE_USD,
            "max_workers": max_workers,
            "chunk_size": chunk_size,
            "exit_bar_index": exit_bar_index,
        },
        "summary": {
            "analysis_windows_total_in_range": analysis_windows_total_in_range,
            "analysis_windows_processed": len(analysis_window_summary_rows),
            "analysis_windows_skipped": len(skipped_analysis_windows),
            # Старые ключи оставлены, чтобы summary CSV/старые утилиты не падали.
            "hours_total_in_range": analysis_windows_total_in_range,
            "hours_processed": len(analysis_window_summary_rows),
            "hours_skipped": len(skipped_analysis_windows),
            "total_snapshot_count": total_snapshot_count,
            "decision_counts": dict(decision_counter),
            "reason_counts": dict(reason_counter),
            "hour_decision_counts": dict(decision_counter),
            "hour_reason_counts": dict(reason_counter),
            "trades_count": len(trade_rows),
            "long_count": long_count,
            "short_count": short_count,
            "win_count": win_count,
            "loss_count": loss_count,
            "flat_count": flat_count,
            "net_pnl_total": net_pnl_total,
            "avg_trade_net_pnl": avg_trade_net_pnl,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_hms": format_elapsed(elapsed_seconds),
        },
        "skipped_analysis_windows": skipped_analysis_windows,
        "skipped_hours": skipped_analysis_windows,
    }

    return result, analysis_window_summary_rows


def run_parameter_sweep(
        instrument_code: str,
        start_utc: str,
        end_utc: str,
        param_specs: dict[str, list],
        price_db_path: str | Path,
        prepared_db_path: str | Path,
        multiplier: float,
        max_workers: int,
        chunk_size: int,
        output_dir: str | Path,
        resume_from_existing: bool = False,
        exit_bar_index: int = 717,
):
    sweep_started_at = datetime.now().astimezone()
    sweep_started_perf = time.perf_counter()

    output_dir = Path(output_dir)
    output_summary_csv_path = build_summary_output_csv_path(
        output_dir=output_dir,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    param_grid = build_param_grid(param_specs)
    total_runs = len(param_grid)

    if resume_from_existing:
        summary_rows = load_existing_summary_rows(output_summary_csv_path)
        start_run_index = get_resume_start_run_index(summary_rows)
    else:
        summary_rows = []
        start_run_index = 1

    print(f"sweep_start_local = {sweep_started_at.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"instrument_code = {instrument_code}")
    print(f"start_utc = {start_utc}")
    print(f"end_utc = {end_utc}")
    print(f"max_workers = {max_workers}")
    print(f"chunk_size = {chunk_size}")
    print(f"exit_bar_index = {exit_bar_index}")
    print(f"summary_csv = {output_summary_csv_path}")
    print(f"resume_from_existing = {resume_from_existing}")
    print("param_specs =")
    if param_specs:
        for param_name, values in param_specs.items():
            print(f"  {param_name}: {len(values)} values -> {values}")
    else:
        print("  {} -> будет один прогон на DEFAULT_STRATEGY_PARAMS")
    print(f"total_runs = {total_runs}")

    if resume_from_existing:
        if summary_rows:
            print(f"resume_last_completed_run_index = {start_run_index - 1}")
            print(f"resume_next_run_index = {start_run_index}")
        else:
            print("resume_last_completed_run_index = 0")
            print("resume_next_run_index = 1")

    if start_run_index > total_runs:
        print("sweep_status = already_finished")
        print("Все прогоны уже есть в summary-файле.")
        return summary_rows

    print("sweep_status = started")

    for combo_index, combo_params in enumerate(param_grid, start=1):
        if combo_index < start_run_index:
            continue

        run_started_perf = time.perf_counter()
        strategy_params_for_run = replace(DEFAULT_STRATEGY_PARAMS, **combo_params)

        output_csv_path = build_run_output_csv_path(
            output_dir=output_dir,
            instrument_code=instrument_code,
            combo_index=combo_index,
        )

        print()
        print(f"[run {combo_index}/{total_runs}] started")
        print(f"params = {combo_params if combo_params else 'DEFAULT_STRATEGY_PARAMS'}")
        print(f"output_csv_path = {output_csv_path}")

        result, analysis_window_summary_rows = run_single_tester_multiprocess(
            instrument_code=instrument_code,
            start_utc=start_utc,
            end_utc=end_utc,
            strategy_params=strategy_params_for_run,
            price_db_path=price_db_path,
            prepared_db_path=prepared_db_path,
            multiplier=multiplier,
            max_workers=max_workers,
            chunk_size=chunk_size,
            exit_bar_index=exit_bar_index,
        )

        save_analysis_window_summary_to_csv(
            rows=analysis_window_summary_rows,
            output_csv_path=output_csv_path,
        )

        run_elapsed_seconds = time.perf_counter() - run_started_perf
        summary = result["summary"]

        strategy_params_dict = {
            key: value
            for key, value in strategy_params_to_csv_dict(strategy_params_for_run).items()
            if key not in SUMMARY_EXCLUDED_STRATEGY_FIELDS
        }

        summary_row = {
            "run_index": combo_index,
            "analysis_windows_total_in_range": summary["analysis_windows_total_in_range"],
            "analysis_windows_skipped": summary["analysis_windows_skipped"],
            # Старые имена для совместимости.
            "hours_total_in_range": summary["hours_total_in_range"],
            "hours_skipped": summary["hours_skipped"],
            "total_snapshot_count": summary["total_snapshot_count"],
            "trades_count": summary["trades_count"],
            "long_count": summary["long_count"],
            "short_count": summary["short_count"],
            "win_count": summary["win_count"],
            "loss_count": summary["loss_count"],
            "flat_count": summary["flat_count"],
            "net_pnl_total": summary["net_pnl_total"],
            "avg_trade_net_pnl": summary["avg_trade_net_pnl"],
            "elapsed_seconds": summary["elapsed_seconds"],
            "elapsed_hms": summary["elapsed_hms"],
            "run_elapsed_seconds": run_elapsed_seconds,
            "run_elapsed_hms": format_elapsed(run_elapsed_seconds),
            "output_csv_path": str(output_csv_path),
            **strategy_params_dict,
        }

        summary_rows.append(summary_row)
        save_runs_summary_to_csv(summary_rows, output_summary_csv_path)

        print(f"[run {combo_index}/{total_runs}] finished")
        print(f"  trades_count = {summary['trades_count']}")
        print(f"  long_count = {summary['long_count']}")
        print(f"  short_count = {summary['short_count']}")
        print(f"  net_pnl_total = {summary['net_pnl_total']:.2f}")
        print(f"  avg_trade_net_pnl = {summary['avg_trade_net_pnl']:.2f}")
        print(f"  analysis_windows_skipped = {summary['analysis_windows_skipped']}")
        print(f"  elapsed = {format_elapsed(run_elapsed_seconds)}")
        print(f"  summary_saved = {output_summary_csv_path}")

    sweep_elapsed_seconds = time.perf_counter() - sweep_started_perf
    print()
    print("sweep_status = finished")
    print(f"sweep_elapsed = {format_elapsed(sweep_elapsed_seconds)}")
    print(f"summary_csv = {output_summary_csv_path}")

    return summary_rows


if __name__ == "__main__":
    instrument_code = "MNQ"

    start_utc = "2026-01-01 00:00:00"
    end_utc = "2026-04-11 00:00:00"

    # Для проверки эквивалентности старой логике оставь SECOND_HALF_ONLY.
    # Для новой торговли каждые полчаса поставь TradingHalfHourMode.ANY_HALF.
    param_specs = {
        "trading_half_hour_mode": [TradingHalfHourMode.ANY_HALF],

        "pearson_shortlist_min_correlation": [0.70],
        "pearson_shortlist_top_n": [30],
        "pearson_min_shortlist": [15],
        "forecast_top_n_after_similarity": [5],
        "decision_min_last_similarity_score": [0.30],
        "decision_min_final_move_points": [10.0],

        "similarity_weight_pearson": [4.0],
        "similarity_weight_range": [1.0],
        "similarity_weight_net_move": [2.0],
        "similarity_weight_mean_abs_diff": [2.0],
        "similarity_weight_efficiency": [2.0],
        "similarity_weight_range_position": [2.0],
        "similarity_weight_diff_pearson": [0.0],
        "similarity_weight_diff_sign_match": [2.0],
    }

    multiplier = float(Instrument[instrument_code]["multiplier"])

    output_dir = Path(__file__).resolve().parent / "output"

    run_parameter_sweep(
        instrument_code=instrument_code,
        start_utc=start_utc,
        end_utc=end_utc,
        param_specs=param_specs,
        price_db_path=settings_live.price_db_path,
        prepared_db_path=settings_live.prepared_db_path,
        multiplier=multiplier,
        max_workers=max(1, min((os.cpu_count() or 1), 18)),
        chunk_size=4,
        output_dir=output_dir,
        resume_from_existing=False,
        exit_bar_index=settings_live.trading_exit_bar_index,
    )

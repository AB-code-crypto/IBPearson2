import csv
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_forecast import build_group_forecast_from_prepared_candidates
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.pearson_runtime import PearsonCurrentHour
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS

from tester.current_hour_price_loader import (
    open_price_connection,
    load_current_hour_price_rows,
)
from tester.prepared_candidates_loader import (
    open_prepared_connection,
    load_prepared_candidate_hours,
)


def floor_to_hour_ts(ts: int) -> int:
    return (ts // 3600) * 3600


def utc_datetime_to_ts(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_prepared_hour_map(prepared_candidate_hours: list[dict]) -> dict:
    result = {}
    for item in prepared_candidate_hours:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])
        result[key] = item
    return result


def pick_prepared_hours_by_ranked_candidates(
        ranked_candidates: list[dict],
        prepared_hour_map: dict,
        limit: int | None = None,
) -> list[dict]:
    result = []
    source_items = ranked_candidates if limit is None else ranked_candidates[:limit]

    for item in source_items:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])
        prepared_hour_payload = prepared_hour_map.get(key)
        if prepared_hour_payload is None:
            raise ValueError(
                "Prepared hour for ranked candidate not found: "
                f"hour_start_ts={item['hour_start_ts']}, "
                f"hour_start_ts_ct={item['hour_start_ts_ct']}"
            )
        result.append(prepared_hour_payload)

    return result


def build_compact_similarity_candidates(ranked_similarity_candidates: list[dict]) -> list[dict]:
    result = []
    for item in ranked_similarity_candidates:
        result.append(
            {
                "hour_start_ts": item["hour_start_ts"],
                "hour_start_ts_ct": item["hour_start_ts_ct"],
                "hour_start_ct": item["hour_start_ct"],
                "hour_slot_ct": item["hour_slot_ct"],
                "contract": item["contract"],
                "pearson": item["pearson"],
                "final_score": item["final_score"],
            }
        )
    return result


def build_compact_forecast_summary(forecast_summary: dict) -> dict:
    return {
        "candidate_count": forecast_summary["candidate_count"],
        "positive_count": forecast_summary["positive_count"],
        "negative_count": forecast_summary["negative_count"],
        "flat_count": forecast_summary["flat_count"],
        "positive_ratio": forecast_summary["positive_ratio"],
        "negative_ratio": forecast_summary["negative_ratio"],
        "flat_ratio": forecast_summary["flat_ratio"],
        "mean_final_move": forecast_summary["mean_final_move"],
        "median_final_move": forecast_summary["median_final_move"],
        "mean_max_upside": forecast_summary["mean_max_upside"],
        "median_max_upside": forecast_summary["median_max_upside"],
        "mean_max_drawdown": forecast_summary["mean_max_drawdown"],
        "median_max_drawdown": forecast_summary["median_max_drawdown"],
        "mean_future_path": forecast_summary["mean_future_path"],
        "median_future_path": forecast_summary["median_future_path"],
    }


def build_compact_forecast_future_items(forecast_future_items: list[dict]) -> list[dict]:
    result = []
    for item in forecast_future_items:
        result.append(
            {
                "hour_start_ts": item["hour_start_ts"],
                "hour_start_ts_ct": item["hour_start_ts_ct"],
                "hour_start_ct": item["hour_start_ct"],
                "hour_slot_ct": item["hour_slot_ct"],
                "contract": item["contract"],
                "future_path": item["future_path"],
                "final_move": item["final_move"],
                "max_upside": item["max_upside"],
                "max_drawdown": item["max_drawdown"],
            }
        )
    return result


def value_direction(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def build_forecast_summary_rows(forecast_snapshots: list[dict], strategy_params) -> list[dict]:
    result = []

    for snapshot in forecast_snapshots:
        forecast_summary = snapshot["forecast_summary"]
        mean_final_move = forecast_summary["mean_final_move"]
        median_final_move = forecast_summary["median_final_move"]

        dominant_directional_ratio = max(
            forecast_summary["positive_ratio"],
            forecast_summary["negative_ratio"],
        )

        mean_direction = value_direction(mean_final_move)
        median_direction = value_direction(median_final_move)
        same_direction = mean_direction == median_direction and mean_direction != 0

        passes_min_forecast_candidates = (
                forecast_summary["candidate_count"] >= strategy_params.decision_min_forecast_candidates
        )
        passes_directional_ratio = (
                dominant_directional_ratio >= strategy_params.decision_min_directional_ratio
        )
        passes_mean_final_move_abs = (
                abs(mean_final_move) >= strategy_params.decision_min_mean_final_move_abs
        )
        passes_median_final_move_abs = (
                abs(median_final_move) >= strategy_params.decision_min_median_final_move_abs
        )

        result.append(
            {
                "current_bar_index": snapshot["current_bar_index"],
                "current_bar_count": snapshot["current_bar_count"],
                "last_bar_time_ts": snapshot["last_bar_time_ts"],
                "last_bar_time": snapshot["last_bar_time"],
                "last_bar_time_ts_ct": snapshot["last_bar_time_ts_ct"],
                "last_bar_time_ct": snapshot["last_bar_time_ct"],
                "pearson_ranked_count": len(snapshot["pearson_ranked_candidates"]),
                "similarity_ranked_count": len(snapshot["similarity_ranked_candidates"]),
                "forecast_candidate_count": forecast_summary["candidate_count"],
                "positive_ratio": forecast_summary["positive_ratio"],
                "negative_ratio": forecast_summary["negative_ratio"],
                "mean_final_move": mean_final_move,
                "median_final_move": median_final_move,
                "mean_max_upside": forecast_summary["mean_max_upside"],
                "mean_max_drawdown": forecast_summary["mean_max_drawdown"],
                "dominant_directional_ratio": dominant_directional_ratio,
                "passes_min_forecast_candidates": passes_min_forecast_candidates,
                "passes_directional_ratio": passes_directional_ratio,
                "passes_mean_final_move_abs": passes_mean_final_move_abs,
                "passes_median_final_move_abs": passes_median_final_move_abs,
                "mean_median_same_direction": same_direction,
            }
        )

    return result


def save_forecast_summary_to_csv(forecast_summary_rows: list[dict], output_csv_path: str | Path):
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "current_bar_index",
        "current_bar_count",
        "last_bar_time_ts",
        "last_bar_time",
        "last_bar_time_ts_ct",
        "last_bar_time_ct",
        "pearson_ranked_count",
        "similarity_ranked_count",
        "forecast_candidate_count",
        "positive_ratio",
        "negative_ratio",
        "mean_final_move",
        "median_final_move",
        "mean_max_upside",
        "mean_max_drawdown",
        "dominant_directional_ratio",
        "passes_min_forecast_candidates",
        "passes_directional_ratio",
        "passes_mean_final_move_abs",
        "passes_median_final_move_abs",
        "mean_median_same_direction",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in forecast_summary_rows:
            writer.writerow(row)


def save_result_to_json(result: dict, output_json_path: str | Path):
    output_path = Path(output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def run_hour_forecast_loop(
        current_hour_rows,
        prepared_candidate_hours,
        current_hour_start_ts: int,
        pearson_min_correlation=None,
        pearson_top_n=None,
        strategy_params=DEFAULT_STRATEGY_PARAMS,
):
    if not current_hour_rows:
        raise ValueError("current_hour_rows is empty")

    if pearson_min_correlation is None:
        pearson_min_correlation = strategy_params.pearson_shortlist_min_correlation
    if pearson_top_n is None:
        pearson_top_n = strategy_params.pearson_shortlist_top_n

    first_row = current_hour_rows[0]
    current_hour_start_ts_ct = floor_to_hour_ts(first_row["bar_time_ts_ct"])

    current_hour = PearsonCurrentHour(
        hour_start_ts=current_hour_start_ts,
        hour_start_ts_ct=current_hour_start_ts_ct,
    )
    current_hour.set_candidates(prepared_candidate_hours)

    prepared_hour_map = build_prepared_hour_map(prepared_candidate_hours)

    start_bar_count = strategy_params.pearson_eval_start_bar_count()
    end_bar_count_exclusive = strategy_params.pearson_eval_end_bar_count_exclusive()

    snapshots = []

    for row in current_hour_rows:
        current_hour.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

        current_bar_count = current_hour.current_n()
        current_bar_index = current_hour.current_bar_index()

        if current_bar_count < start_bar_count:
            continue
        if current_bar_count >= end_bar_count_exclusive:
            break

        if not current_hour.candidates_initialized:
            current_hour.initialize_candidates()
        else:
            current_hour.update_candidates_for_last_bar()

        pearson_ranked_candidates = current_hour.get_ranked_candidates(
            min_correlation=pearson_min_correlation,
            top_n=pearson_top_n,
        )

        pearson_shortlist_prepared_hours = pick_prepared_hours_by_ranked_candidates(
            ranked_candidates=pearson_ranked_candidates,
            prepared_hour_map=prepared_hour_map,
            limit=None,
        )

        current_values = list(current_hour.x)

        similarity_ranked_candidates = rank_prepared_candidates_by_similarity(
            current_values=current_values,
            prepared_hours=pearson_shortlist_prepared_hours,
            min_required_pearson=None,
            params=strategy_params,
        )

        forecast_prepared_hours = pick_prepared_hours_by_ranked_candidates(
            ranked_candidates=similarity_ranked_candidates,
            prepared_hour_map=prepared_hour_map,
            limit=strategy_params.forecast_top_n_after_similarity,
        )

        raw_forecast_summary = build_group_forecast_from_prepared_candidates(
            prepared_hours=forecast_prepared_hours,
            current_bar_index=current_bar_index,
        )
        forecast_future_items = raw_forecast_summary["future_items"]

        snapshots.append(
            {
                "last_bar_time_ts": row["bar_time_ts"],
                "last_bar_time": row["bar_time"],
                "last_bar_time_ts_ct": row["bar_time_ts_ct"],
                "last_bar_time_ct": row["bar_time_ct"],
                "current_bar_count": current_bar_count,
                "current_bar_index": current_bar_index,
                "pearson_ranked_candidates": [
                    {
                        "hour_start_ts": item["hour_start_ts"],
                        "hour_start_ts_ct": item["hour_start_ts_ct"],
                        "hour_start_ct": item["hour_start_ct"],
                        "hour_slot_ct": item["hour_slot_ct"],
                        "correlation": item["correlation"],
                    }
                    for item in pearson_ranked_candidates
                ],
                "similarity_ranked_candidates": build_compact_similarity_candidates(
                    similarity_ranked_candidates
                ),
                "forecast_summary": build_compact_forecast_summary(raw_forecast_summary),
                "forecast_future_items": build_compact_forecast_future_items(
                    forecast_future_items
                ),
            }
        )

    return {
        "current_hour": {
            "hour_start_ts": current_hour.hour_start_ts,
            "hour_start_ts_ct": current_hour.hour_start_ts_ct,
            "hour_start": current_hour.hour_start,
            "hour_start_ct": current_hour.hour_start_ct,
            "hour_slot_ct": current_hour.hour_slot_ct,
        },
        "pearson_shortlist_params": {
            "min_correlation": pearson_min_correlation,
            "top_n": pearson_top_n,
        },
        "forecast_params": {
            "forecast_top_n_after_similarity": strategy_params.forecast_top_n_after_similarity,
        },
        "search_window": {
            "start_bar_count": start_bar_count,
            "end_bar_count_exclusive": end_bar_count_exclusive,
        },
        "history_candidate_count": len(current_hour.candidates),
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
    }


if __name__ == "__main__":
    instrument_code = "MNQ"

    price_db_path = settings_live.price_db_path
    prepared_db_path = settings_live.prepared_db_path

    current_hour_start_utc = "2026-04-08 07:00:00"
    current_hour_start_ts = utc_datetime_to_ts(current_hour_start_utc)

    pearson_min_correlation = 0.70
    pearson_top_n = 50

    strategy_params_for_run = replace(DEFAULT_STRATEGY_PARAMS)

    output_base_name = (
        f"hour_forecast_result_"
        f"corr_{str(pearson_min_correlation).replace('.', '_')}_"
        f"top_{pearson_top_n}"
    )

    output_json_path = f"output/json/{output_base_name}.json"
    output_csv_path = f"output/csv/{output_base_name}.csv"

    instrument_row = Instrument[instrument_code]
    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    price_conn = open_price_connection(price_db_path)
    prepared_conn = open_prepared_connection(prepared_db_path)

    try:
        current_hour_rows = load_current_hour_price_rows(
            price_conn=price_conn,
            table_name=table_name,
            current_hour_start_ts=current_hour_start_ts,
        )

        if not current_hour_rows:
            raise ValueError(
                f"No current hour rows found: table_name={table_name}, "
                f"current_hour_start_ts={current_hour_start_ts}"
            )

        first_row = current_hour_rows[0]
        current_hour_start_ts_ct = floor_to_hour_ts(first_row["bar_time_ts_ct"])

        prepared_candidate_hours = load_prepared_candidate_hours(
            prepared_conn=prepared_conn,
            table_name=table_name,
            current_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        result = run_hour_forecast_loop(
            current_hour_rows=current_hour_rows,
            prepared_candidate_hours=prepared_candidate_hours,
            current_hour_start_ts=current_hour_start_ts,
            pearson_min_correlation=pearson_min_correlation,
            pearson_top_n=pearson_top_n,
            strategy_params=strategy_params_for_run,
        )

        result["input"] = {
            "instrument_code": instrument_code,
            "current_hour_start_utc": current_hour_start_utc,
            "current_hour_start_ts": current_hour_start_ts,
            "current_hour_start_ts_ct": current_hour_start_ts_ct,
            "pearson_min_correlation": pearson_min_correlation,
            "pearson_top_n": pearson_top_n,
            "forecast_top_n_after_similarity": strategy_params_for_run.forecast_top_n_after_similarity,
            "decision_min_forecast_candidates": strategy_params_for_run.decision_min_forecast_candidates,
            "decision_min_directional_ratio": strategy_params_for_run.decision_min_directional_ratio,
            "decision_min_mean_final_move_abs": strategy_params_for_run.decision_min_mean_final_move_abs,
            "decision_min_median_final_move_abs": strategy_params_for_run.decision_min_median_final_move_abs,
            "decision_require_mean_and_median_same_direction": (
                strategy_params_for_run.decision_require_mean_and_median_same_direction
            ),
        }

        forecast_summary_rows = build_forecast_summary_rows(
            result["snapshots"],
            strategy_params=strategy_params_for_run,
        )

        save_result_to_json(result=result, output_json_path=output_json_path)
        save_forecast_summary_to_csv(
            forecast_summary_rows=forecast_summary_rows,
            output_csv_path=output_csv_path,
        )

        search_window = result["search_window"]
        search_bar_count = (
                search_window["end_bar_count_exclusive"] - search_window["start_bar_count"]
        )

        print(f"saved json: {output_json_path}")
        print(f"saved csv: {output_csv_path}")
        print(f"instrument_code = {instrument_code}")
        print(f"current_hour_start_utc = {current_hour_start_utc}")
        print(f"current_hour_start_ts = {current_hour_start_ts}")
        print(f"current_hour_start_ts_ct = {current_hour_start_ts_ct}")
        print(f"pearson_min_correlation = {pearson_min_correlation}")
        print(f"pearson_top_n = {pearson_top_n}")
        print(
            "forecast_top_n_after_similarity = "
            f"{result['input']['forecast_top_n_after_similarity']}"
        )
        print(
            "decision_min_forecast_candidates = "
            f"{result['input']['decision_min_forecast_candidates']}"
        )
        print(
            "decision_min_directional_ratio = "
            f"{result['input']['decision_min_directional_ratio']}"
        )
        print(
            "decision_min_mean_final_move_abs = "
            f"{result['input']['decision_min_mean_final_move_abs']}"
        )
        print(
            "decision_min_median_final_move_abs = "
            f"{result['input']['decision_min_median_final_move_abs']}"
        )
        print(
            "decision_require_mean_and_median_same_direction = "
            f"{result['input']['decision_require_mean_and_median_same_direction']}"
        )
        print(f"history_candidate_count = {result['history_candidate_count']}")
        print(f"snapshot_count = {result['snapshot_count']}")
        print(
            f"search window: "
            f"{search_window['start_bar_count']}.."
            f"{search_window['end_bar_count_exclusive'] - 1} "
            f"({search_bar_count} bars)"
        )

        print()
        print("forecast summary:")
        print(
            "bar_index | bar_count | time_ct             | "
            "sim_count | fc_count | pos_ratio | neg_ratio | "
            "mean_move | median_move | pass_count | pass_ratio | pass_mean | pass_median | same_dir"
        )
        print("-" * 170)

        for row in forecast_summary_rows:
            print(
                f"{row['current_bar_index']:>9} | "
                f"{row['current_bar_count']:>9} | "
                f"{row['last_bar_time_ct']} | "
                f"{row['similarity_ranked_count']:>9} | "
                f"{row['forecast_candidate_count']:>8} | "
                f"{row['positive_ratio']:.3f} | "
                f"{row['negative_ratio']:.3f} | "
                f"{row['mean_final_move']:.6f} | "
                f"{row['median_final_move']:.6f} | "
                f"{str(row['passes_min_forecast_candidates']):>10} | "
                f"{str(row['passes_directional_ratio']):>10} | "
                f"{str(row['passes_mean_final_move_abs']):>9} | "
                f"{str(row['passes_median_final_move_abs']):>11} | "
                f"{str(row['mean_median_same_direction'])}"
            )

    finally:
        price_conn.close()
        prepared_conn.close()

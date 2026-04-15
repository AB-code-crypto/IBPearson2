import csv
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
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


def pick_prepared_hours_for_pearson_shortlist(
    pearson_ranked_candidates: list[dict],
    prepared_hour_map: dict,
) -> list[dict]:
    result = []

    for item in pearson_ranked_candidates:
        key = (item["hour_start_ts"], item["hour_start_ts_ct"])

        prepared_hour_payload = prepared_hour_map.get(key)
        if prepared_hour_payload is None:
            raise ValueError(
                "Prepared hour for Pearson-shortlist candidate not found: "
                f"hour_start_ts={item['hour_start_ts']}, "
                f"hour_start_ts_ct={item['hour_start_ts_ct']}"
            )

        result.append(prepared_hour_payload)

    return result


def build_compact_similarity_candidates(
    ranked_similarity_candidates: list[dict],
) -> list[dict]:
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
                "diff_pearson": item["diff_pearson"],
                "diff_sign_match_ratio": item["diff_sign_match_ratio"],
                "range_distance": item["range_distance"],
                "net_move_distance": item["net_move_distance"],
                "range_position_distance": item["range_position_distance"],
                "mean_abs_diff_distance": item["mean_abs_diff_distance"],
                "efficiency_distance": item["efficiency_distance"],
                "pearson_score": item["pearson_score"],
                "diff_pearson_score": item["diff_pearson_score"],
                "diff_sign_match_score": item["diff_sign_match_score"],
                "range_score": item["range_score"],
                "net_move_score": item["net_move_score"],
                "range_position_score": item["range_position_score"],
                "mean_abs_diff_score": item["mean_abs_diff_score"],
                "efficiency_score": item["efficiency_score"],
                "final_score": item["final_score"],
            }
        )

    return result


def build_similarity_summary(
    similarity_snapshots: list[dict],
    strategy_params,
) -> list[dict]:
    result = []

    forecast_top_n = strategy_params.forecast_top_n_after_similarity
    decision_min_last_similarity_score = (
        strategy_params.decision_min_last_similarity_score
    )

    for snapshot in similarity_snapshots:
        pearson_ranked_candidates = snapshot["pearson_ranked_candidates"]
        similarity_ranked_candidates = snapshot["similarity_ranked_candidates"]

        best_similarity_item = (
            similarity_ranked_candidates[0] if similarity_ranked_candidates else None
        )

        last_forecast_candidate_item = None
        if len(similarity_ranked_candidates) >= forecast_top_n:
            last_forecast_candidate_item = similarity_ranked_candidates[
                forecast_top_n - 1
            ]

        last_forecast_candidate_final_score = (
            last_forecast_candidate_item["final_score"]
            if last_forecast_candidate_item
            else None
        )

        result.append(
            {
                "current_bar_index": snapshot["current_bar_index"],
                "current_bar_count": snapshot["current_bar_count"],
                "last_bar_time_ts": snapshot["last_bar_time_ts"],
                "last_bar_time": snapshot["last_bar_time"],
                "last_bar_time_ts_ct": snapshot["last_bar_time_ts_ct"],
                "last_bar_time_ct": snapshot["last_bar_time_ct"],
                "pearson_ranked_count": len(pearson_ranked_candidates),
                "similarity_ranked_count": len(similarity_ranked_candidates),
                "forecast_top_n_after_similarity": forecast_top_n,
                "decision_min_last_similarity_score": decision_min_last_similarity_score,
                "best_similarity_hour_start_ct": (
                    best_similarity_item["hour_start_ct"]
                    if best_similarity_item
                    else None
                ),
                "best_similarity_pearson": (
                    best_similarity_item["pearson"]
                    if best_similarity_item
                    else None
                ),
                "best_similarity_final_score": (
                    best_similarity_item["final_score"]
                    if best_similarity_item
                    else None
                ),
                "last_forecast_candidate_final_score": (
                    last_forecast_candidate_final_score
                ),
                "passes_last_similarity_score_filter": (
                    last_forecast_candidate_final_score is not None
                    and last_forecast_candidate_final_score
                    >= decision_min_last_similarity_score
                ),
            }
        )

    return result


def save_similarity_summary_to_csv(
    similarity_summary: list[dict],
    output_csv_path: str | Path,
):
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
        "forecast_top_n_after_similarity",
        "decision_min_last_similarity_score",
        "best_similarity_hour_start_ct",
        "best_similarity_pearson",
        "best_similarity_final_score",
        "last_forecast_candidate_final_score",
        "passes_last_similarity_score_filter",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in similarity_summary:
            writer.writerow(row)


def save_result_to_json(result: dict, output_json_path: str | Path):
    output_path = Path(output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def run_hour_similarity_loop(
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

        pearson_shortlist_prepared_hours = pick_prepared_hours_for_pearson_shortlist(
            pearson_ranked_candidates=pearson_ranked_candidates,
            prepared_hour_map=prepared_hour_map,
        )

        current_values = list(current_hour.x)

        ranked_similarity_candidates = rank_prepared_candidates_by_similarity(
            current_values=current_values,
            prepared_hours=pearson_shortlist_prepared_hours,
            min_required_pearson=None,
            params=strategy_params,
        )

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
                    ranked_similarity_candidates
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

    current_hour_start_utc = "2026-04-14 15:00:00"
    current_hour_start_ts = utc_datetime_to_ts(current_hour_start_utc)

    pearson_min_correlation = 0.70
    pearson_top_n = 30

    strategy_params_for_run = replace(
        DEFAULT_STRATEGY_PARAMS,
        similarity_weight_range_position=0.0,
        similarity_weight_diff_pearson=0.0,
        similarity_weight_diff_sign_match=0.0,
    )

    output_base_name = (
        f"hour_similarity_result_"
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

        result = run_hour_similarity_loop(
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
            "forecast_top_n_after_similarity": (
                strategy_params_for_run.forecast_top_n_after_similarity
            ),
            "decision_min_last_similarity_score": (
                strategy_params_for_run.decision_min_last_similarity_score
            ),
            "similarity_weights": {
                "pearson": strategy_params_for_run.similarity_weight_pearson,
                "range": strategy_params_for_run.similarity_weight_range,
                "net_move": strategy_params_for_run.similarity_weight_net_move,
                "range_position": strategy_params_for_run.similarity_weight_range_position,
                "mean_abs_diff": strategy_params_for_run.similarity_weight_mean_abs_diff,
                "efficiency": strategy_params_for_run.similarity_weight_efficiency,
                "diff_pearson": strategy_params_for_run.similarity_weight_diff_pearson,
                "diff_sign_match": strategy_params_for_run.similarity_weight_diff_sign_match,
            },
        }

        similarity_summary = build_similarity_summary(
            result["snapshots"],
            strategy_params=strategy_params_for_run,
        )

        save_result_to_json(
            result=result,
            output_json_path=output_json_path,
        )

        save_similarity_summary_to_csv(
            similarity_summary=similarity_summary,
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
            "decision_min_last_similarity_score = "
            f"{result['input']['decision_min_last_similarity_score']}"
        )
        print("similarity_weights =")
        for key, value in result["input"]["similarity_weights"].items():
            print(f"  {key}: {value}")
        print(f"history_candidate_count = {result['history_candidate_count']}")
        print(f"snapshot_count = {result['snapshot_count']}")
        print(
            f"search window: "
            f"{search_window['start_bar_count']}.."
            f"{search_window['end_bar_count_exclusive'] - 1} "
            f"({search_bar_count} bars)"
        )

        print()
        print("similarity summary:")
        print(
            "bar_index | bar_count | time_ct             | "
            "pearson_count | similarity_count | best_final_score | "
            "last_forecast_score | min_last_score | pass_last_score"
        )
        print("-" * 145)

        for summary_row in similarity_summary:
            best_final_score = summary_row["best_similarity_final_score"]
            last_forecast_score = summary_row["last_forecast_candidate_final_score"]
            min_last_score = summary_row["decision_min_last_similarity_score"]

            best_final_score_str = (
                f"{best_final_score:.6f}" if best_final_score is not None else "None"
            )
            last_forecast_score_str = (
                f"{last_forecast_score:.6f}" if last_forecast_score is not None else "None"
            )
            min_last_score_str = (
                f"{min_last_score:.6f}" if min_last_score is not None else "None"
            )
            pass_last_score_str = str(summary_row["passes_last_similarity_score_filter"])

            print(
                f"{summary_row['current_bar_index']:>9} | "
                f"{summary_row['current_bar_count']:>9} | "
                f"{summary_row['last_bar_time_ct']} | "
                f"{summary_row['pearson_ranked_count']:>13} | "
                f"{summary_row['similarity_ranked_count']:>16} | "
                f"{best_final_score_str:>16} | "
                f"{last_forecast_score_str:>19} | "
                f"{min_last_score_str:>14} | "
                f"{pass_last_score_str}"
            )

    finally:
        price_conn.close()
        prepared_conn.close()

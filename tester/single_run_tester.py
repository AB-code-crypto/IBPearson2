import csv
import json
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_decision import evaluate_decision_layer
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


def ceil_to_hour_ts(ts: int) -> int:
    floored = floor_to_hour_ts(ts)
    if ts == floored:
        return ts
    return floored + 3600


def utc_datetime_to_ts(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def utc_ts_to_text(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def iter_hour_start_ts_range(start_ts: int, end_ts: int):
    current_hour_start_ts = floor_to_hour_ts(start_ts)
    end_hour_exclusive_ts = ceil_to_hour_ts(end_ts)

    while current_hour_start_ts < end_hour_exclusive_ts:
        yield current_hour_start_ts
        current_hour_start_ts += 3600


def build_snapshot_row(
        current_hour,
        row,
        current_bar_count: int,
        current_bar_index: int,
        pearson_ranked_candidates: list[dict],
        similarity_ranked_candidates: list[dict],
        forecast_summary: dict,
        decision_result: dict,
) -> dict:
    diagnostics = decision_result["diagnostics"]

    return {
        "hour_start_ts": current_hour.hour_start_ts,
        "hour_start_ts_ct": current_hour.hour_start_ts_ct,
        "hour_start": current_hour.hour_start,
        "hour_start_ct": current_hour.hour_start_ct,
        "hour_slot_ct": current_hour.hour_slot_ct,
        "last_bar_time_ts": row["bar_time_ts"],
        "last_bar_time": row["bar_time"],
        "last_bar_time_ts_ct": row["bar_time_ts_ct"],
        "last_bar_time_ct": row["bar_time_ct"],
        "current_bar_index": current_bar_index,
        "current_bar_count": current_bar_count,
        "pearson_ranked_count": len(pearson_ranked_candidates),
        "similarity_ranked_count": len(similarity_ranked_candidates),
        "forecast_candidate_count": forecast_summary["candidate_count"],
        "decision": decision_result["decision"],
        "reason": decision_result["reason"],
        "best_similarity_score": diagnostics["best_similarity_score"],
        "last_similarity_score": diagnostics["last_similarity_score"],
        "mean_final_move": diagnostics["mean_final_move"],
        "median_final_move": diagnostics["median_final_move"],
        "positive_ratio": diagnostics["positive_ratio"],
        "negative_ratio": diagnostics["negative_ratio"],
        "mean_max_upside": diagnostics["mean_max_upside"],
        "mean_max_drawdown": diagnostics["mean_max_drawdown"],
    }


def build_hour_result(
        current_hour,
        snapshots: list[dict],
) -> dict:
    first_trade_snapshot = None
    for snapshot in snapshots:
        if snapshot["decision"] in {"LONG", "SHORT"}:
            first_trade_snapshot = snapshot
            break

    last_snapshot = snapshots[-1] if snapshots else None

    if first_trade_snapshot is not None:
        summary_source = first_trade_snapshot
        hour_decision = first_trade_snapshot["decision"]
        hour_reason = first_trade_snapshot["reason"]
    elif last_snapshot is not None:
        summary_source = last_snapshot
        hour_decision = "NO_TRADE"
        hour_reason = last_snapshot["reason"]
    else:
        summary_source = None
        hour_decision = "NO_TRADE"
        hour_reason = "NO_EVAL_WINDOW"

    result = {
        "hour_start_ts": current_hour.hour_start_ts,
        "hour_start_ts_ct": current_hour.hour_start_ts_ct,
        "hour_start": current_hour.hour_start,
        "hour_start_ct": current_hour.hour_start_ct,
        "hour_slot_ct": current_hour.hour_slot_ct,
        "snapshot_count": len(snapshots),
        "hour_decision": hour_decision,
        "hour_reason": hour_reason,
        "first_trade_snapshot": first_trade_snapshot,
        "snapshots": snapshots,
    }

    if summary_source is not None:
        result["summary"] = {
            "last_bar_time_ts": summary_source["last_bar_time_ts"],
            "last_bar_time": summary_source["last_bar_time"],
            "last_bar_time_ts_ct": summary_source["last_bar_time_ts_ct"],
            "last_bar_time_ct": summary_source["last_bar_time_ct"],
            "current_bar_index": summary_source["current_bar_index"],
            "current_bar_count": summary_source["current_bar_count"],
            "pearson_ranked_count": summary_source["pearson_ranked_count"],
            "similarity_ranked_count": summary_source["similarity_ranked_count"],
            "forecast_candidate_count": summary_source["forecast_candidate_count"],
            "best_similarity_score": summary_source["best_similarity_score"],
            "last_similarity_score": summary_source["last_similarity_score"],
            "mean_final_move": summary_source["mean_final_move"],
            "median_final_move": summary_source["median_final_move"],
            "positive_ratio": summary_source["positive_ratio"],
            "negative_ratio": summary_source["negative_ratio"],
            "mean_max_upside": summary_source["mean_max_upside"],
            "mean_max_drawdown": summary_source["mean_max_drawdown"],
        }
    else:
        result["summary"] = None

    return result


def build_hour_summary_rows(hours: list[dict]) -> list[dict]:
    rows = []

    for hour in hours:
        summary = hour["summary"]

        row = {
            "hour_start_ts": hour["hour_start_ts"],
            "hour_start_ts_ct": hour["hour_start_ts_ct"],
            "hour_start": hour["hour_start"],
            "hour_start_ct": hour["hour_start_ct"],
            "hour_slot_ct": hour["hour_slot_ct"],
            "snapshot_count": hour["snapshot_count"],
            "hour_decision": hour["hour_decision"],
            "hour_reason": hour["hour_reason"],
        }

        if summary is not None:
            row.update(summary)
        else:
            row.update(
                {
                    "last_bar_time_ts": None,
                    "last_bar_time": None,
                    "last_bar_time_ts_ct": None,
                    "last_bar_time_ct": None,
                    "current_bar_index": None,
                    "current_bar_count": None,
                    "pearson_ranked_count": None,
                    "similarity_ranked_count": None,
                    "forecast_candidate_count": None,
                    "best_similarity_score": None,
                    "last_similarity_score": None,
                    "mean_final_move": None,
                    "median_final_move": None,
                    "positive_ratio": None,
                    "negative_ratio": None,
                    "mean_max_upside": None,
                    "mean_max_drawdown": None,
                }
            )

        rows.append(row)

    return rows


def save_hour_summary_to_csv(
        hour_summary_rows: list[dict],
        output_csv_path: str | Path,
):
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "hour_start_ts",
        "hour_start_ts_ct",
        "hour_start",
        "hour_start_ct",
        "hour_slot_ct",
        "snapshot_count",
        "hour_decision",
        "hour_reason",
        "last_bar_time_ts",
        "last_bar_time",
        "last_bar_time_ts_ct",
        "last_bar_time_ct",
        "current_bar_index",
        "current_bar_count",
        "pearson_ranked_count",
        "similarity_ranked_count",
        "forecast_candidate_count",
        "best_similarity_score",
        "last_similarity_score",
        "mean_final_move",
        "median_final_move",
        "positive_ratio",
        "negative_ratio",
        "mean_max_upside",
        "mean_max_drawdown",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in hour_summary_rows:
            writer.writerow(row)


def save_result_to_json(result: dict, output_json_path: str | Path):
    output_path = Path(output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def run_hour_pipeline(
        current_hour_rows,
        prepared_candidate_hours,
        current_hour_start_ts: int,
        strategy_params,
):
    if not current_hour_rows:
        raise ValueError("current_hour_rows is empty")

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
            min_correlation=strategy_params.pearson_shortlist_min_correlation,
            top_n=strategy_params.pearson_shortlist_top_n,
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

        forecast_summary = build_group_forecast_from_prepared_candidates(
            prepared_hours=forecast_prepared_hours,
            current_bar_index=current_bar_index,
        )

        decision_result = evaluate_decision_layer(
            ranked_similarity_candidates=similarity_ranked_candidates,
            forecast_summary=forecast_summary,
            params=strategy_params,
        )

        snapshots.append(
            build_snapshot_row(
                current_hour=current_hour,
                row=row,
                current_bar_count=current_bar_count,
                current_bar_index=current_bar_index,
                pearson_ranked_candidates=pearson_ranked_candidates,
                similarity_ranked_candidates=similarity_ranked_candidates,
                forecast_summary=forecast_summary,
                decision_result=decision_result,
            )
        )

    return build_hour_result(
        current_hour=current_hour,
        snapshots=snapshots,
    )


def run_single_tester(
        instrument_code: str,
        start_utc: str,
        end_utc: str,
        strategy_params,
        price_db_path: str | Path,
        prepared_db_path: str | Path,
):
    start_ts = utc_datetime_to_ts(start_utc)
    end_ts = utc_datetime_to_ts(end_utc)

    if end_ts < start_ts:
        raise ValueError(f"end_utc < start_utc: {end_utc} < {start_utc}")

    instrument_row = Instrument[instrument_code]
    table_name = build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    price_conn = open_price_connection(price_db_path)
    prepared_conn = open_prepared_connection(prepared_db_path)

    try:
        hours = []
        skipped_hours = []

        for hour_start_ts in iter_hour_start_ts_range(start_ts, end_ts):
            try:
                current_hour_rows = load_current_hour_price_rows(
                    price_conn=price_conn,
                    table_name=table_name,
                    current_hour_start_ts=hour_start_ts,
                )

                first_row = current_hour_rows[0]
                current_hour_start_ts_ct = floor_to_hour_ts(first_row["bar_time_ts_ct"])

                prepared_candidate_hours = load_prepared_candidate_hours(
                    prepared_conn=prepared_conn,
                    table_name=table_name,
                    current_hour_start_ts_ct=current_hour_start_ts_ct,
                    strategy_params=strategy_params,
                )

                hour_result = run_hour_pipeline(
                    current_hour_rows=current_hour_rows,
                    prepared_candidate_hours=prepared_candidate_hours,
                    current_hour_start_ts=hour_start_ts,
                    strategy_params=strategy_params,
                )

                hours.append(hour_result)

            except Exception as exc:
                skipped_hours.append(
                    {
                        "hour_start_ts": hour_start_ts,
                        "hour_start": utc_ts_to_text(hour_start_ts),
                        "error": str(exc),
                    }
                )

        hour_summary_rows = build_hour_summary_rows(hours)

        decision_counter = Counter(row["hour_decision"] for row in hour_summary_rows)
        reason_counter = Counter(row["hour_reason"] for row in hour_summary_rows)

        total_snapshot_count = sum(hour["snapshot_count"] for hour in hours)

        result = {
            "input": {
                "instrument_code": instrument_code,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "strategy_params": asdict(strategy_params),
            },
            "summary": {
                "hours_total_in_range": len(list(iter_hour_start_ts_range(start_ts, end_ts))),
                "hours_processed": len(hours),
                "hours_skipped": len(skipped_hours),
                "total_snapshot_count": total_snapshot_count,
                "hour_decision_counts": dict(decision_counter),
                "hour_reason_counts": dict(reason_counter),
            },
            "hours": hours,
            "skipped_hours": skipped_hours,
        }

        return result, hour_summary_rows

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    instrument_code = "MNQ"

    # UTC input range
    start_utc = "2026-04-07 00:00:00"
    end_utc = "2026-04-08 09:00:00"

    # Один объект параметров на весь прогон
    strategy_params_for_run = replace(
        DEFAULT_STRATEGY_PARAMS,
        pearson_shortlist_min_correlation=0.80,
        pearson_shortlist_top_n=30,
        forecast_top_n_after_similarity=5,
        decision_min_last_similarity_score=0.3,
        similarity_weight_range_position=0.0,
        similarity_weight_diff_pearson=0.0,
        similarity_weight_diff_sign_match=0.0,
    )

    # Удобное имя прогона, чтобы сравнивать варианты
    run_name = "baseline"

    price_db_path = settings_live.price_db_path
    prepared_db_path = settings_live.prepared_db_path

    output_base_name = (
        f"single_run_tester_"
        f"{instrument_code}_"
        f"{start_utc.replace('-', '').replace(':', '').replace(' ', '_')}_"
        f"{end_utc.replace('-', '').replace(':', '').replace(' ', '_')}_"
        f"{run_name}"
    )

    output_json_path = f"output/json/{output_base_name}.json"
    output_csv_path = f"output/csv/{output_base_name}.csv"

    result, hour_summary_rows = run_single_tester(
        instrument_code=instrument_code,
        start_utc=start_utc,
        end_utc=end_utc,
        strategy_params=strategy_params_for_run,
        price_db_path=price_db_path,
        prepared_db_path=prepared_db_path,
    )

    save_result_to_json(
        result=result,
        output_json_path=output_json_path,
    )

    save_hour_summary_to_csv(
        hour_summary_rows=hour_summary_rows,
        output_csv_path=output_csv_path,
    )

    summary = result["summary"]

    print(f"saved json: {output_json_path}")
    print(f"saved csv: {output_csv_path}")
    print(f"instrument_code = {instrument_code}")
    print(f"start_utc = {start_utc}")
    print(f"end_utc = {end_utc}")
    print(f"run_name = {run_name}")
    print(f"hours_total_in_range = {summary['hours_total_in_range']}")
    print(f"hours_processed = {summary['hours_processed']}")
    print(f"hours_skipped = {summary['hours_skipped']}")
    print(f"total_snapshot_count = {summary['total_snapshot_count']}")
    print("hour_decision_counts =")
    for key, value in summary["hour_decision_counts"].items():
        print(f"  {key}: {value}")

    print("top_hour_reasons =")
    for reason, count in Counter(summary["hour_reason_counts"]).most_common(10):
        print(f"  {reason}: {count}")

    if result["skipped_hours"]:
        print("first_skipped_hour =")
        first_skipped = result["skipped_hours"][0]
        print(f"  hour_start = {first_skipped['hour_start']}")
        print(f"  error = {first_skipped['error']}")

import csv
import itertools
import time
from dataclasses import asdict, fields, replace
from datetime import datetime
from pathlib import Path

from config import settings_live
from contracts import Instrument
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS

from single_run_tester_multiprocess import (
    run_single_tester_multiprocess,
    save_hour_summary_to_csv,
)


def format_elapsed(seconds: float) -> str:
    total_seconds = int(seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


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

    param_names = list(param_specs.keys())
    param_value_lists = [normalize_param_spec_values(param_specs[name]) for name in param_names]

    combinations = []
    for combo_values in itertools.product(*param_value_lists):
        combinations.append(dict(zip(param_names, combo_values)))

    return combinations


def make_run_name(combo_index: int) -> str:
    return f"run_{combo_index}"


def build_run_output_csv_path(output_dir: Path, instrument_code: str, combo_index: int) -> Path:
    return output_dir / f"single_run_tester_mp_{instrument_code}_{combo_index}.csv"


def save_runs_summary_to_csv(rows: list[dict], output_csv_path: str | Path) -> None:
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        raise ValueError("rows is empty")

    all_keys = []
    seen = set()

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


if __name__ == "__main__":
    sweep_started_at = datetime.now().astimezone()
    sweep_started_perf = time.perf_counter()

    instrument_code = "MNQ"

    # UTC input range
    start_utc = "2026-04-07 00:00:00"
    end_utc = "2026-04-08 09:00:00"

    instrument_row = Instrument[instrument_code]
    multiplier = float(instrument_row["multiplier"])

    PARAM_SPECS = {
        "pearson_shortlist_min_correlation": [0.70, 0.75, 0.80],
        "pearson_shortlist_top_n": [30, 50],
        "forecast_top_n_after_similarity": [5, 7, 10],
        "decision_min_last_similarity_score": [0.20, 0.30],
        "similarity_weight_range_position": [0.0, 1.0],
        "similarity_weight_diff_pearson": [0.0, 1.0],
        "similarity_weight_diff_sign_match": [0.0, 1.0],
    }

    max_workers = 28
    chunk_size = 1

    price_db_path = settings_live.price_db_path
    prepared_db_path = settings_live.prepared_db_path

    output_dir = Path("output/test")
    output_summary_csv_path = output_dir / "parameter_sweep_summary.csv"

    param_grid = build_param_grid(PARAM_SPECS)
    total_runs = len(param_grid)

    print(f"sweep_start_local = {sweep_started_at.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"instrument_code = {instrument_code}")
    print(f"start_utc = {start_utc}")
    print(f"end_utc = {end_utc}")
    print(f"max_workers = {max_workers}")
    print(f"chunk_size = {chunk_size}")
    print(f"summary_csv = {output_summary_csv_path}")
    print("param_specs =")
    for param_name, values in PARAM_SPECS.items():
        print(f"  {param_name}: {len(values)} values -> {values}")
    print(f"total_runs = {total_runs}")
    print("sweep_status = started")

    summary_rows = []

    for combo_index, combo_params in enumerate(param_grid, start=1):
        run_started_perf = time.perf_counter()
        run_name = make_run_name(combo_index)

        strategy_params_for_run = replace(
            DEFAULT_STRATEGY_PARAMS,
            **combo_params,
        )

        output_csv_path = build_run_output_csv_path(
            output_dir=output_dir,
            instrument_code=instrument_code,
            combo_index=combo_index,
        )

        print()
        print(f"[run {combo_index}/{total_runs}] started")
        print(f"run_name = {run_name}")
        print(f"params = {combo_params}")
        print(f"output_csv_path = {output_csv_path}")

        result, hour_summary_rows = run_single_tester_multiprocess(
            instrument_code=instrument_code,
            start_utc=start_utc,
            end_utc=end_utc,
            strategy_params=strategy_params_for_run,
            price_db_path=price_db_path,
            prepared_db_path=prepared_db_path,
            multiplier=multiplier,
            max_workers=max_workers,
            chunk_size=chunk_size,
        )

        save_hour_summary_to_csv(
            hour_summary_rows=hour_summary_rows,
            output_csv_path=output_csv_path,
        )

        run_elapsed_seconds = time.perf_counter() - run_started_perf
        summary = result["summary"]

        summary_row = {
            "run_index": combo_index,
            "run_name": run_name,
            "instrument_code": instrument_code,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "output_csv_path": str(output_csv_path),
            "hours_total_in_range": summary["hours_total_in_range"],
            "hours_processed": summary["hours_processed"],
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
            "run_elapsed_seconds": run_elapsed_seconds,
            "run_elapsed_hms": format_elapsed(run_elapsed_seconds),
        }

        strategy_params_dict = asdict(strategy_params_for_run)
        summary_row.update(strategy_params_dict)

        summary_rows.append(summary_row)

        summary_rows_sorted = sorted(
            summary_rows,
            key=lambda row: row["net_pnl_total"],
            reverse=True,
        )
        save_runs_summary_to_csv(
            rows=summary_rows_sorted,
            output_csv_path=output_summary_csv_path,
        )

        print(f"[run {combo_index}/{total_runs}] finished")
        print(f"net_pnl_total = {summary['net_pnl_total']:.2f}")
        print(f"trades_count = {summary['trades_count']}")
        print(f"run_elapsed_hms = {format_elapsed(run_elapsed_seconds)}")

    sweep_elapsed_seconds = time.perf_counter() - sweep_started_perf
    sweep_finished_at = datetime.now().astimezone()

    best_row = None
    if summary_rows:
        best_row = max(summary_rows, key=lambda row: row["net_pnl_total"])

    print()
    print(f"sweep_finish_local = {sweep_finished_at.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"sweep_elapsed_seconds = {sweep_elapsed_seconds:.3f}")
    print(f"sweep_elapsed_hms = {format_elapsed(sweep_elapsed_seconds)}")
    print(f"summary_csv_saved = {output_summary_csv_path}")

    if best_row is not None:
        print("best_run =")
        print(f"  run_name = {best_row['run_name']}")
        print(f"  net_pnl_total = {best_row['net_pnl_total']:.2f}")
        print(f"  trades_count = {best_row['trades_count']}")

    print("sweep_status = finished")

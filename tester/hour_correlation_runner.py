import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
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
    """
    Преобразует UTC datetime-строку вида '2026-04-08 08:00:00'
    в Unix timestamp.
    """
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_compact_ranked_candidates(ranked_candidates: list[dict]) -> list[dict]:
    """
    Оставляем только то, что нужно в JSON:
    - время часа кандидата
    - корреляция

    Список уже приходит отсортированным по correlation по убыванию.
    """
    result = []

    for item in ranked_candidates:
        result.append(
            {
                "hour_start_ts": item["hour_start_ts"],
                "hour_start_ts_ct": item["hour_start_ts_ct"],
                "hour_start_ct": item["hour_start_ct"],
                "hour_slot_ct": item["hour_slot_ct"],
                "correlation": item["correlation"],
            }
        )

    return result


def build_correlation_summary(correlation_snapshots: list[dict]) -> list[dict]:
    """
    Строит короткую выжимку по каждому шагу окна поиска.
    """
    result = []

    for snapshot in correlation_snapshots:
        ranked_candidates = snapshot["ranked_candidates"]

        result.append(
            {
                "current_bar_index": snapshot["current_bar_index"],
                "current_bar_count": snapshot["current_bar_count"],
                "last_bar_time_ts": snapshot["last_bar_time_ts"],
                "last_bar_time": snapshot["last_bar_time"],
                "last_bar_time_ts_ct": snapshot["last_bar_time_ts_ct"],
                "last_bar_time_ct": snapshot["last_bar_time_ct"],
                "ranked_count": len(ranked_candidates),
                "max_correlation": (
                    ranked_candidates[0]["correlation"]
                    if ranked_candidates
                    else None
                ),
            }
        )

    return result


def save_correlation_summary_to_csv(
        correlation_summary: list[dict],
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
        "ranked_count",
        "max_correlation",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in correlation_summary:
            writer.writerow(row)


def run_hour_correlation_loop(
        current_hour_rows,
        prepared_candidate_hours,
        current_hour_start_ts: int,
        min_correlation=None,
        top_n=None,
        strategy_params=DEFAULT_STRATEGY_PARAMS,
):
    """
    Прогоняет один текущий час против уже загруженных historical candidates.

    На входе только UTC-start часа.
    CT-start часа вычисляем из первого бара current_hour_rows,
    чтобы не было рассогласования.

    Возвращает JSON-готовую структуру:
    - метаданные текущего часа
    - массив snapshots по шагам окна поиска
    - в каждом snapshot уже отсортированный список кандидатов
      с временем часа и корреляцией
    """

    if not current_hour_rows:
        raise ValueError("current_hour_rows is empty")

    if min_correlation is None:
        min_correlation = strategy_params.pearson_shortlist_min_correlation

    if top_n is None:
        top_n = strategy_params.pearson_shortlist_top_n

    first_row = current_hour_rows[0]
    current_hour_start_ts_ct = floor_to_hour_ts(first_row["bar_time_ts_ct"])

    current_hour = PearsonCurrentHour(
        hour_start_ts=current_hour_start_ts,
        hour_start_ts_ct=current_hour_start_ts_ct,
    )
    current_hour.set_candidates(prepared_candidate_hours)

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

        ranked_candidates = current_hour.get_ranked_candidates(
            min_correlation=min_correlation,
            top_n=top_n,
        )

        snapshots.append(
            {
                "last_bar_time_ts": row["bar_time_ts"],
                "last_bar_time": row["bar_time"],
                "last_bar_time_ts_ct": row["bar_time_ts_ct"],
                "last_bar_time_ct": row["bar_time_ct"],
                "current_bar_count": current_bar_count,
                "current_bar_index": current_bar_index,
                "ranked_candidates": build_compact_ranked_candidates(ranked_candidates),
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
            "min_correlation": min_correlation,
            "top_n": top_n,
        },
        "search_window": {
            "start_bar_count": start_bar_count,
            "end_bar_count_exclusive": end_bar_count_exclusive,
        },
        "history_candidate_count": len(current_hour.candidates),
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
    }


def save_result_to_json(result: dict, output_json_path: str | Path):
    output_path = Path(output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    instrument_code = "MNQ"

    price_db_path = settings_live.price_db_path
    prepared_db_path = settings_live.prepared_db_path

    # Вводим время в удобном UTC-виде
    current_hour_start_utc = "2026-04-08 07:00:00"

    # Внутри проекта дальше используем timestamp
    current_hour_start_ts = utc_datetime_to_ts(current_hour_start_utc)

    # Параметры Pearson-shortlist для тестера
    min_correlation = 0.8
    top_n = 30

    output_base_name = (
        f"hour_correlation_result_"
        f"corr_{str(min_correlation).replace('.', '_')}_"
        f"top_{top_n}"
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

        result = run_hour_correlation_loop(
            current_hour_rows=current_hour_rows,
            prepared_candidate_hours=prepared_candidate_hours,
            current_hour_start_ts=current_hour_start_ts,
            min_correlation=min_correlation,
            top_n=top_n,
        )

        result["input"] = {
            "instrument_code": instrument_code,
            "current_hour_start_utc": current_hour_start_utc,
            "current_hour_start_ts": current_hour_start_ts,
            "current_hour_start_ts_ct": current_hour_start_ts_ct,
            "min_correlation": min_correlation,
            "top_n": top_n,
        }

        correlation_summary = build_correlation_summary(result["snapshots"])

        save_result_to_json(
            result=result,
            output_json_path=output_json_path,
        )

        save_correlation_summary_to_csv(
            correlation_summary=correlation_summary,
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
        print(f"min_correlation = {min_correlation}")
        print(f"top_n = {top_n}")
        print(f"history_candidate_count = {result['history_candidate_count']}")
        print(f"snapshot_count = {result['snapshot_count']}")
        print(
            f"search window: "
            f"{search_window['start_bar_count']}.."
            f"{search_window['end_bar_count_exclusive'] - 1} "
            f"({search_bar_count} bars)"
        )

        print()
        print("correlation summary:")
        print(
            "bar_index | bar_count | time_ct             | "
            "ranked_count | max_correlation"
        )
        print("-" * 76)

        for summary_row in correlation_summary:
            max_correlation = summary_row["max_correlation"]
            max_correlation_str = (
                f"{max_correlation:.6f}" if max_correlation is not None else "None"
            )

            print(
                f"{summary_row['current_bar_index']:>9} | "
                f"{summary_row['current_bar_count']:>9} | "
                f"{summary_row['last_bar_time_ct']} | "
                f"{summary_row['ranked_count']:>12} | "
                f"{max_correlation_str}"
            )

    finally:
        price_conn.close()
        prepared_conn.close()

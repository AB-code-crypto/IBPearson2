import json
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

    # Вводим время только один раз: UTC-start текущего часа
    current_hour_start_ts = 1775635200

    # Куда сохранить JSON
    output_json_path = "output/hour_correlation_result.json"

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
        )

        save_result_to_json(
            result=result,
            output_json_path=output_json_path,
        )

        print(f"saved json: {output_json_path}")
        print(f"snapshot_count = {result['snapshot_count']}")
        print(f"history_candidate_count = {result['history_candidate_count']}")

        if result["snapshots"]:
            first_snapshot = result["snapshots"][0]
            print(f"first_snapshot_last_bar_time_ct = {first_snapshot['last_bar_time_ct']}")
            print(f"first_snapshot_ranked_count = {len(first_snapshot['ranked_candidates'])}")

            if first_snapshot["ranked_candidates"]:
                best_item = first_snapshot["ranked_candidates"][0]
                print(f"best_hour_start_ct = {best_item['hour_start_ct']}")
                print(f"best_correlation = {best_item['correlation']}")
    finally:
        price_conn.close()
        prepared_conn.close()

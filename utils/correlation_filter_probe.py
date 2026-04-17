from dataclasses import fields, replace
from datetime import datetime, timezone

from config import settings_live
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.pearson_runtime import PearsonCurrentHour
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS
from ts.ts_time import (
    SEARCH_SLOT_GROUPS_ALL_HOURS,
    SEARCH_SLOT_GROUPS_BLOCK_2H,
    SEARCH_SLOT_GROUPS_BLOCK_3H,
    SEARCH_SLOT_GROUPS_BLOCK_4H,
    SEARCH_SLOT_GROUPS_SAME_HOUR_ONLY,
)

from tester.current_hour_price_loader import (
    load_current_hour_price_rows,
    open_price_connection,
)
from tester.prepared_candidates_loader import (
    load_prepared_candidate_hours,
    open_prepared_connection,
)


# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

SETTINGS = settings_live
INSTRUMENT_CODE = "MNQ"

# Точка проверки задаётся в UTC.
# Формат: "YYYY-MM-DD HH:MM:SS"
CHECK_MOMENT_TEXT_UTC = "2026-04-14 15:30:00"

# Локальные переопределения StrategyParams.
# Меняются только здесь, боевые ts_config / DEFAULT_STRATEGY_PARAMS не трогаем.
#
# Примеры:
# STRATEGY_PARAMS_OVERRIDES = {
#     "pearson_eval_start_minute": 25,
#     "pearson_eval_end_minute": 55,
#     "pearson_shortlist_min_correlation": 0.85,
#     "pearson_shortlist_top_n": 50,
#     "search_slot_groups": SEARCH_SLOT_GROUPS_BLOCK_2H,
# }
STRATEGY_PARAMS_OVERRIDES = {
    # "pearson_shortlist_min_correlation": 0.80,
    # "pearson_shortlist_top_n": 30,
}


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================


def utc_datetime_to_ts(dt_text: str) -> int:
    dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())



def ts_to_utc_text(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")



def floor_to_hour_ts(ts: int) -> int:
    return (ts // 3600) * 3600



def build_strategy_params(overrides: dict):
    valid_fields = {field.name for field in fields(DEFAULT_STRATEGY_PARAMS)}
    unknown_fields = sorted(set(overrides) - valid_fields)

    if unknown_fields:
        raise ValueError(f"Unknown StrategyParams fields: {unknown_fields}")

    return replace(DEFAULT_STRATEGY_PARAMS, **overrides)



def build_instrument_table_name(instrument_code: str) -> str:
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    instrument_row = Instrument[instrument_code]
    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )



def validate_check_moment(check_ts: int, strategy_params):
    hour_start_ts = floor_to_hour_ts(check_ts)
    seconds_from_hour_start = check_ts - hour_start_ts
    bar_interval_seconds = strategy_params.pearson_bar_interval_seconds

    if seconds_from_hour_start % bar_interval_seconds != 0:
        raise ValueError(
            "Момент проверки не попадает на границу 5-секундного бара: "
            f"seconds_from_hour_start={seconds_from_hour_start}, "
            f"bar_interval_seconds={bar_interval_seconds}"
        )

    completed_bar_count = seconds_from_hour_start // bar_interval_seconds

    start_bar_count = strategy_params.pearson_eval_start_bar_count()
    end_bar_count_exclusive = strategy_params.pearson_eval_end_bar_count_exclusive()

    if completed_bar_count < start_bar_count:
        raise ValueError(
            "Момент проверки раньше рабочего интервала Pearson: "
            f"completed_bar_count={completed_bar_count}, "
            f"start_bar_count={start_bar_count}"
        )

    if completed_bar_count >= end_bar_count_exclusive:
        raise ValueError(
            "Момент проверки вне рабочего интервала Pearson справа: "
            f"completed_bar_count={completed_bar_count}, "
            f"end_bar_count_exclusive={end_bar_count_exclusive}"
        )

    return {
        "hour_start_ts": hour_start_ts,
        "seconds_from_hour_start": seconds_from_hour_start,
        "completed_bar_count": completed_bar_count,
        "current_bar_index": completed_bar_count - 1,
        "start_bar_count": start_bar_count,
        "end_bar_count_exclusive": end_bar_count_exclusive,
    }



def build_ranked_candidates(correlation_items, min_correlation, top_n):
    filtered = []

    for item in correlation_items:
        correlation = item["correlation"]

        if correlation is None:
            continue

        if min_correlation is not None and correlation < min_correlation:
            continue

        filtered.append(item)

    filtered.sort(key=lambda item: item["correlation"], reverse=True)

    if top_n is not None:
        filtered = filtered[:top_n]

    return filtered


# ============================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================


def main():
    strategy_params = build_strategy_params(STRATEGY_PARAMS_OVERRIDES)
    table_name = build_instrument_table_name(INSTRUMENT_CODE)

    check_ts = utc_datetime_to_ts(CHECK_MOMENT_TEXT_UTC)
    check_meta = validate_check_moment(check_ts, strategy_params)

    hour_start_ts = check_meta["hour_start_ts"]
    completed_bar_count = check_meta["completed_bar_count"]

    price_conn = open_price_connection(SETTINGS.price_db_path)
    prepared_conn = open_prepared_connection(SETTINGS.prepared_db_path)

    try:
        current_hour_rows = load_current_hour_price_rows(
            price_conn=price_conn,
            table_name=table_name,
            current_hour_start_ts=hour_start_ts,
        )

        prefix_rows = current_hour_rows[:completed_bar_count]

        if len(prefix_rows) != completed_bar_count:
            raise ValueError(
                "Не удалось получить нужный префикс текущего часа: "
                f"ожидалось {completed_bar_count} баров, "
                f"получено {len(prefix_rows)}"
            )

        first_row = current_hour_rows[0]
        current_hour_start_ts_ct = floor_to_hour_ts(first_row["bar_time_ts_ct"])
        current_hour_slot_ct = (current_hour_start_ts_ct // 3600) % 24
        allowed_hour_slots_ct = strategy_params.resolve_allowed_hour_slots(current_hour_slot_ct)

        prepared_candidate_hours = load_prepared_candidate_hours(
            prepared_conn=prepared_conn,
            table_name=table_name,
            current_hour_start_ts_ct=current_hour_start_ts_ct,
            strategy_params=strategy_params,
        )

        current_hour = PearsonCurrentHour(
            hour_start_ts=hour_start_ts,
            hour_start_ts_ct=current_hour_start_ts_ct,
        )
        current_hour.set_candidates(prepared_candidate_hours)

        for row in prefix_rows:
            current_hour.add_bar(
                ask_open=row["ask_open"],
                bid_open=row["bid_open"],
                ask_close=row["ask_close"],
                bid_close=row["bid_close"],
            )

        current_hour.initialize_candidates()
        correlation_items = current_hour.calculate_all_correlations()

        ranked_candidates = build_ranked_candidates(
            correlation_items=correlation_items,
            min_correlation=strategy_params.pearson_shortlist_min_correlation,
            top_n=strategy_params.pearson_shortlist_top_n,
        )

        valid_correlation_items = [
            item for item in correlation_items if item["correlation"] is not None
        ]

        raw_max_correlation = None
        if valid_correlation_items:
            raw_max_correlation = max(item["correlation"] for item in valid_correlation_items)

        shortlist_max_correlation = None
        if ranked_candidates:
            shortlist_max_correlation = ranked_candidates[0]["correlation"]

        print(f"Инструмент: {INSTRUMENT_CODE}")
        print(f"Таблица: {table_name}")
        print(f"price DB: {SETTINGS.price_db_path}")
        print(f"prepared DB: {SETTINGS.prepared_db_path}")
        print("")
        print(f"CHECK_MOMENT_TEXT_UTC: {CHECK_MOMENT_TEXT_UTC}")
        print(f"CHECK_MOMENT_TS_UTC:   {check_ts}")
        print(f"Час UTC:               {ts_to_utc_text(hour_start_ts)}")
        print(f"Час CT:                {current_hour.hour_start_ct}")
        print(f"hour_slot_ct:          {current_hour_slot_ct}")
        print(f"allowed_hour_slots_ct: {allowed_hour_slots_ct}")
        print("")
        print(
            "Рабочее окно Pearson: "
            f"[{strategy_params.pearson_eval_start_minute:02d}:00, "
            f"{strategy_params.pearson_eval_end_minute:02d}:00)"
        )
        print(f"seconds_from_hour:     {check_meta['seconds_from_hour_start']}")
        print(f"completed_bar_count:   {completed_bar_count}")
        print(f"current_bar_index:     {check_meta['current_bar_index']}")
        print("")
        print("Стратегические настройки:")
        print(
            f"  pearson_shortlist_min_correlation = "
            f"{strategy_params.pearson_shortlist_min_correlation}"
        )
        print(
            f"  pearson_shortlist_top_n = "
            f"{strategy_params.pearson_shortlist_top_n}"
        )
        print("")
        print("Статистика по корреляции:")
        print(f"  обработано historical-часов: {len(prepared_candidate_hours)}")
        print(f"  часов с валидной correlation: {len(valid_correlation_items)}")
        print(f"  часов после shortlist-фильтра: {len(ranked_candidates)}")
        print(
            "  максимальная correlation (raw): "
            f"{raw_max_correlation:.6f}" if raw_max_correlation is not None else
            "  максимальная correlation (raw): None"
        )
        print(
            "  максимальная correlation (shortlist): "
            f"{shortlist_max_correlation:.6f}" if shortlist_max_correlation is not None else
            "  максимальная correlation (shortlist): None"
        )

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()

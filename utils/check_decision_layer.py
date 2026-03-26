import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_decision import evaluate_decision_layer
from ts.candidate_features import (
    build_first_diff,
    calc_mean_abs_diff,
    calc_net_move,
    calc_path_efficiency,
    calc_range,
)
from ts.candidate_forecast import build_group_forecast_from_prepared_candidates
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_config import FORECAST_TOP_N_AFTER_SIMILARITY
from ts.ts_time import resolve_allowed_hour_slots


INSTRUMENT_CODE = "MNQ"
CURRENT_HOUR_START_TEXT_UTC = "2026-03-19 18:00:00"
TARGET_BAR_INDEX = 359
MIN_REQUIRED_PEARSON = None
OUTPUT_DIR = Path("png")


def parse_utc_hour_start_text(hour_start_text):
    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"CURRENT_HOUR_START_TEXT_UTC должен указывать ровно на начало часа, "
            f"получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def build_current_hour_y(rows):
    if not rows:
        raise ValueError("Нельзя построить y-ряд: rows пустой")

    first_row = rows[0]
    mid_open_0 = (first_row["ask_open"] + first_row["bid_open"]) / 2.0

    if mid_open_0 == 0.0:
        raise ValueError("mid_open_0 == 0, деление невозможно")

    y = []

    for row in rows:
        mid_close = (row["ask_close"] + row["bid_close"]) / 2.0
        y_value = (mid_close / mid_open_0) - 1.0
        y.append(y_value)

    return y


def build_plot_file_path(current_hour_start_text_utc, target_bar_index):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    safe_hour_text = current_hour_start_text_utc.replace(":", "-").replace(" ", "_")
    file_name = f"decision_layer_{safe_hour_text}_bar_{target_bar_index}.png"

    return OUTPUT_DIR / file_name


def save_decision_plot(
    current_hour_start_text_utc,
    current_hour_start_text_ct,
    current_full_y,
    target_bar_index,
    selected_candidates,
    prepared_hours_map,
    forecast_summary,
    decision_result,
):
    plt.figure(figsize=(14, 8))

    full_x = list(range(len(current_full_y)))

    plt.plot(
        full_x,
        current_full_y,
        linewidth=2.5,
        label=f"Текущий час | UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct}",
    )

    mean_future_path = forecast_summary["mean_future_path"]
    median_future_path = forecast_summary["median_future_path"]
    future_start_x = target_bar_index + 1

    if mean_future_path:
        mean_x = list(range(future_start_x, future_start_x + len(mean_future_path)))
        plt.plot(
            mean_x,
            [current_full_y[target_bar_index] + value for value in mean_future_path],
            linewidth=2.0,
            linestyle="--",
            label=f"Средний future-path | decision={decision_result['decision']}",
        )

    if median_future_path:
        median_x = list(range(future_start_x, future_start_x + len(median_future_path)))
        plt.plot(
            median_x,
            [current_full_y[target_bar_index] + value for value in median_future_path],
            linewidth=2.0,
            linestyle=":",
            label="Медианный future-path",
        )

    for rank, item in enumerate(selected_candidates, start=1):
        candidate_y = prepared_hours_map[item["hour_start_ts"]]["y"]

        plt.plot(
            full_x,
            candidate_y,
            linewidth=1.0,
            alpha=0.7,
            label=(
                f"{rank}. {item['hour_start_ct']} CT | "
                f"score={item['final_score']:.4f} | "
                f"Пирсон={item['pearson']:.4f}"
            ),
        )

    plt.axvline(
        x=target_bar_index,
        linestyle="--",
        linewidth=1.5,
        label=f"Точка входа: bar_index={target_bar_index}",
    )

    plt.title(
        f"Decision layer | UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct} | "
        f"decision={decision_result['decision']}"
    )
    plt.xlabel("bar_index")
    plt.ylabel("y")
    plt.legend(loc="best", fontsize=8)
    plt.grid(True)

    output_path = build_plot_file_path(
        current_hour_start_text_utc=current_hour_start_text_utc,
        target_bar_index=target_bar_index,
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return output_path


def print_current_hour_block(
    current_hour_start_text_utc,
    current_hour_start_text_ct,
    current_hour_slot_ct,
    current_bar_start_text_utc,
    current_bar_start_text_ct,
    current_prefix_y,
):
    current_range = calc_range(current_prefix_y)
    current_net_move = calc_net_move(current_prefix_y)
    current_mean_abs_diff = calc_mean_abs_diff(build_first_diff(current_prefix_y))
    current_efficiency = calc_path_efficiency(current_prefix_y)

    print("ХАРАКТЕРИСТИКИ ТЕКУЩЕГО ЧАСА:")
    print(f"  Начало часа UTC: {current_hour_start_text_utc}")
    print(f"  Начало часа CT:  {current_hour_start_text_ct} CT")
    print(f"  CT-слот часа:    {current_hour_slot_ct}")
    print(f"  Индекс текущего бара: {TARGET_BAR_INDEX}")
    print(f"  Начало бара UTC:      {current_bar_start_text_utc} UTC")
    print(f"  Начало бара CT:       {current_bar_start_text_ct} CT")
    print(f"  Длина префикса:       {len(current_prefix_y)}")
    print(f"  Диапазон участка:     {calc_range(current_prefix_y) * 100:+.4f}%")
    print(f"  Итоговое смещение:    {calc_net_move(current_prefix_y) * 100:+.4f}%")
    print(f"  Средний шаг:          {calc_mean_abs_diff(build_first_diff(current_prefix_y)) * 100:+.4f}%")
    print(f"  Эффективность пути:   {calc_path_efficiency(current_prefix_y):.4f}")
    print("")


def print_decision_result(decision_result):
    print("РЕШЕНИЕ DECISION LAYER:")
    print("")
    print(f"  Решение:   {decision_result['decision']}")
    print(f"  Причина:   {decision_result['reason']}")
    print("")
    diagnostics = decision_result["diagnostics"]
    print("  Диагностика:")
    print(f"    similarity_candidate_count:  {diagnostics['similarity_candidate_count']}")
    print(f"    forecast_candidate_count:    {diagnostics['forecast_candidate_count']}")
    print(f"    best_similarity_score:       {diagnostics['best_similarity_score']}")
    print(f"    mean_final_move:             {diagnostics['mean_final_move'] * 100:+.4f}%")
    print(f"    median_final_move:           {diagnostics['median_final_move'] * 100:+.4f}%")
    print(f"    mean_direction:              {diagnostics['mean_direction']}")
    print(f"    median_direction:            {diagnostics['median_direction']}")
    print(f"    positive_ratio:              {diagnostics['positive_ratio']:.4f}")
    print(f"    negative_ratio:              {diagnostics['negative_ratio']:.4f}")
    print(f"    mean_max_upside:             {diagnostics['mean_max_upside'] * 100:+.4f}%")
    print(f"    mean_max_drawdown:           {diagnostics['mean_max_drawdown'] * 100:+.4f}%")
    print("")


def main():
    instrument_row = Instrument[INSTRUMENT_CODE]
    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    current_hour_start_ts = parse_utc_hour_start_text(CURRENT_HOUR_START_TEXT_UTC)

    if TARGET_BAR_INDEX < 1 or TARGET_BAR_INDEX >= 720:
        raise ValueError("TARGET_BAR_INDEX должен быть в диапазоне 1..719")

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row
        prepared_conn.row_factory = sqlite3.Row

        current_hour_rows = load_price_rows_for_one_hour(
            price_conn=price_conn,
            table_name=table_name,
            hour_start_ts=current_hour_start_ts,
        )

        validate_price_rows(
            rows=current_hour_rows,
            hour_start_ts=current_hour_start_ts,
        )

        current_hour_start_ts_ct = current_hour_rows[0]["bar_time_ts_ct"]
        current_hour_start_text_ct = current_hour_rows[0]["bar_time_ct"]
        current_hour_slot_ct = (current_hour_start_ts_ct // 3600) % 24

        allowed_hour_slots_ct = resolve_allowed_hour_slots(current_hour_slot_ct)

        prepared_hours = load_prepared_hours_by_slots(
            prepared_conn=prepared_conn,
            table_name=table_name,
            hour_slots_ct=allowed_hour_slots_ct,
            before_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        if not prepared_hours:
            raise ValueError(
                f"Не найдено historical prepared-часов для CT-slot={allowed_hour_slots_ct} "
                f"строго раньше {current_hour_start_text_ct} CT"
            )

        current_full_y = build_current_hour_y(current_hour_rows)
        current_prefix_y = current_full_y[: TARGET_BAR_INDEX + 1]

        prepared_hours_map = {
            item["hour_start_ts"]: item for item in prepared_hours
        }

        ranked_similarity_candidates = rank_prepared_candidates_by_similarity(
            current_values=current_prefix_y,
            prepared_hours=prepared_hours,
            min_required_pearson=MIN_REQUIRED_PEARSON,
        )

        selected_candidates = ranked_similarity_candidates[:FORECAST_TOP_N_AFTER_SIMILARITY]
        selected_prepared_hours = [
            prepared_hours_map[item["hour_start_ts"]] for item in selected_candidates
        ]

        forecast_summary = build_group_forecast_from_prepared_candidates(
            prepared_hours=selected_prepared_hours,
            current_bar_index=TARGET_BAR_INDEX,
        )

        decision_result = evaluate_decision_layer(
            ranked_similarity_candidates=ranked_similarity_candidates,
            forecast_summary=forecast_summary,
        )

        print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
        print(f"Таблица: {table_name}")
        print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
        print(f"Начало текущего часа CT:           {current_hour_start_text_ct} CT")
        print(f"Текущий CT-слот:                   {current_hour_slot_ct}")
        print(f"Разрешённые CT-слоты для поиска:   {allowed_hour_slots_ct}")
        print(f"Исторических prepared-кандидатов:  {len(prepared_hours)}")
        print(f"Точка сравнения (bar_index):       {TARGET_BAR_INDEX}")
        print(f"Кандидатов после similarity:       {len(ranked_similarity_candidates)}")
        print(f"Кандидатов в прогнозном слое:      {len(selected_candidates)}")
        print("")

        print_current_hour_block(
            current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
            current_hour_start_text_ct=current_hour_start_text_ct,
            current_hour_slot_ct=current_hour_slot_ct,
            current_bar_start_text_utc=current_hour_rows[TARGET_BAR_INDEX]["bar_time"],
            current_bar_start_text_ct=current_hour_rows[TARGET_BAR_INDEX]["bar_time_ct"],
            current_prefix_y=current_prefix_y,
        )

        print_decision_result(decision_result)

        output_path = save_decision_plot(
            current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
            current_hour_start_text_ct=current_hour_start_text_ct,
            current_full_y=current_full_y,
            target_bar_index=TARGET_BAR_INDEX,
            selected_candidates=selected_candidates,
            prepared_hours_map=prepared_hours_map,
            forecast_summary=forecast_summary,
            decision_result=decision_result,
        )

        print(f"PNG сохранён: {output_path}")

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()

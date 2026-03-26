import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_features import (
    build_first_diff,
    calc_mean_abs_diff,
    calc_net_move,
    calc_path_efficiency,
    calc_range,
)
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_config import (
    SIMILARITY_EFFICIENCY_DISTANCE_ZERO_AT,
    SIMILARITY_MEAN_ABS_DIFF_DISTANCE_ZERO_AT,
    SIMILARITY_NET_MOVE_DISTANCE_ZERO_AT,
    SIMILARITY_PEARSON_SCORE_ONE_AT,
    SIMILARITY_PEARSON_SCORE_ZERO_AT,
    SIMILARITY_RANGE_DISTANCE_ZERO_AT,
    SIMILARITY_WEIGHT_EFFICIENCY,
    SIMILARITY_WEIGHT_MEAN_ABS_DIFF,
    SIMILARITY_WEIGHT_NET_MOVE,
    SIMILARITY_WEIGHT_PEARSON,
    SIMILARITY_WEIGHT_RANGE,
)
from ts.ts_time import resolve_allowed_hour_slots


# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Какой уже закрытый исторический час берём как "текущий".
CURRENT_HOUR_START_TEXT_UTC = "2026-03-19 18:00:00"

# На каком баре внутри часа сравниваем текущий префикс с historical-кандидатами.
# 359 = первые 30 минут (360 баров по 5 секунд)
TARGET_BAR_INDEX = 359

# Сколько лучших кандидатов печатать и рисовать.
TOP_N_PRINT = 10
TOP_N_PLOT = 5

# Если нужно, можно отсечь совсем слабых кандидатов по базовому Пирсону.
# Если None - порог не применяется.
MIN_REQUIRED_PEARSON = None

# Куда сохранять PNG.
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
    file_name = f"weighted_filters_shared_{safe_hour_text}_bar_{target_bar_index}.png"

    return OUTPUT_DIR / file_name


def save_weighted_score_plot(
    current_hour_start_text_utc,
    current_hour_start_text_ct,
    current_full_y,
    target_bar_index,
    top_candidates,
    prepared_hours_map,
):
    plt.figure(figsize=(14, 8))

    x_values = list(range(len(current_full_y)))

    plt.plot(
        x_values,
        current_full_y,
        linewidth=2.5,
        label=f"Текущий час | UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct}",
    )

    for rank, item in enumerate(top_candidates, start=1):
        candidate_hour = prepared_hours_map[item["hour_start_ts"]]
        candidate_y = candidate_hour["y"]

        plt.plot(
            x_values,
            candidate_y,
            linewidth=1.2,
            alpha=0.8,
            label=(
                f"{rank}. {item['hour_start_ct']} CT | "
                f"итог={item['final_score']:.4f} | "
                f"Пирсон={item['pearson']:.4f}"
            ),
        )

    plt.axvline(
        x=target_bar_index,
        linestyle="--",
        linewidth=1.5,
        label=f"Точка сравнения: bar_index={target_bar_index}",
    )

    plt.title(
        f"Итоговый score похожести кандидатов | "
        f"UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct}"
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
    print(f"  Диапазон участка:     {current_range * 100:+.4f}%")
    print(f"  Итоговое смещение:    {current_net_move * 100:+.4f}%")
    print(f"  Средний шаг:          {current_mean_abs_diff * 100:+.4f}%")
    print(f"  Эффективность пути:   {current_efficiency:.4f}")
    print("")


def print_candidate_block(index, item):
    print(f"[{index}]")
    print(f"  CT-час кандидата:               {item['hour_start_ct']} CT")
    print(f"  CT-слот:                        {item['hour_slot_ct']}")
    print(f"  Контракт:                       {item['contract']}")
    print("")
    print(f"  Основной Пирсон:                {item['pearson']:.4f}")
    print(f"  Оценка Пирсона:                 {item['pearson_score']:.4f}")
    print("")
    print(f"  Диапазон участка кандидата:     {item['candidate_range'] * 100:+.4f}%")
    print(f"  Отличие по диапазону:           {item['range_distance']:.4f}")
    print(f"  Оценка по диапазону:            {item['range_score']:.4f}")
    print("")
    print(f"  Итоговое смещение кандидата:    {item['candidate_net_move'] * 100:+.4f}%")
    print(f"  Отличие по смещению:            {item['net_move_distance']:.4f}")
    print(f"  Оценка по смещению:             {item['net_move_score']:.4f}")
    print("")
    print(f"  Средний шаг кандидата:          {item['candidate_mean_abs_diff'] * 100:+.4f}%")
    print(f"  Отличие по среднему шагу:       {item['mean_abs_diff_distance']:.4f}")
    print(f"  Оценка по среднему шагу:        {item['mean_abs_diff_score']:.4f}")
    print("")
    print(f"  Эффективность пути кандидата:   {item['candidate_path_efficiency']:.4f}")
    print(f"  Отличие по эффективности:       {item['efficiency_distance']:.4f}")
    print(f"  Оценка по эффективности:        {item['efficiency_score']:.4f}")
    print("")
    print(f"  ИТОГОВЫЙ ВЗВЕШЕННЫЙ SCORE:      {item['final_score']:.4f}")
    print("")


def print_summary(best_by_pearson, best_by_final_score, total_candidates):
    print("ИТОГОВЫЙ РЕЗУЛЬТАТ:")
    print("")
    print(f"  Всего кандидатов с вычислимым Пирсоном: {total_candidates}")
    print("")
    print("  Лучший по исходному Пирсону:")
    print(f"    CT-час:             {best_by_pearson['hour_start_ct']} CT")
    print(f"    Контракт:           {best_by_pearson['contract']}")
    print(f"    CT-слот:            {best_by_pearson['hour_slot_ct']}")
    print(f"    Пирсон:             {best_by_pearson['pearson']:.4f}")
    print(f"    Итоговый score:     {best_by_pearson['final_score']:.4f}")
    print("")
    print("  Лучший по итоговому взвешенному score:")
    print(f"    CT-час:             {best_by_final_score['hour_start_ct']} CT")
    print(f"    Контракт:           {best_by_final_score['contract']}")
    print(f"    CT-слот:            {best_by_final_score['hour_slot_ct']}")
    print(f"    Пирсон:             {best_by_final_score['pearson']:.4f}")
    print(f"    Итоговый score:     {best_by_final_score['final_score']:.4f}")
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

        prepared_hours_map = {}

        for prepared_hour in prepared_hours:
            prepared_hours_map[prepared_hour["hour_start_ts"]] = prepared_hour

        ranked_by_final_score = rank_prepared_candidates_by_similarity(
            current_values=current_prefix_y,
            prepared_hours=prepared_hours,
            min_required_pearson=MIN_REQUIRED_PEARSON,
        )

        if not ranked_by_final_score:
            raise ValueError("Не найдено ни одного кандидата после общего similarity scoring")

        best_by_pearson = max(
            ranked_by_final_score,
            key=lambda item: item["pearson"],
        )

        best_by_final_score = ranked_by_final_score[0]

        print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
        print(f"Таблица: {table_name}")
        print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
        print(f"Начало текущего часа CT:           {current_hour_start_text_ct} CT")
        print(f"Текущий CT-слот:                   {current_hour_slot_ct}")
        print(f"Разрешённые CT-слоты для поиска:   {allowed_hour_slots_ct}")
        print(f"Исторических prepared-кандидатов:  {len(prepared_hours)}")
        print(f"Точка сравнения (bar_index):       {TARGET_BAR_INDEX}")
        print(f"Длина текущего префикса:           {len(current_prefix_y)}")
        print(f"Кандидатов после общего score:     {len(ranked_by_final_score)}")
        print("")
        print("ШКАЛЫ ОЦЕНКИ ФИЛЬТРОВ:")
        print(f"  Пирсон:              {SIMILARITY_PEARSON_SCORE_ZERO_AT:.2f} -> 0, {SIMILARITY_PEARSON_SCORE_ONE_AT:.2f} -> 1")
        print(f"  Диапазон:            отличие >= {SIMILARITY_RANGE_DISTANCE_ZERO_AT:.2f} -> 0")
        print(f"  Смещение:            отличие >= {SIMILARITY_NET_MOVE_DISTANCE_ZERO_AT:.2f} -> 0")
        print(f"  Средний шаг:         отличие >= {SIMILARITY_MEAN_ABS_DIFF_DISTANCE_ZERO_AT:.2f} -> 0")
        print(f"  Эффективность:       отличие >= {SIMILARITY_EFFICIENCY_DISTANCE_ZERO_AT:.2f} -> 0")
        print("")
        print("ВЕСА ФИЛЬТРОВ:")
        print(f"  Пирсон:              {SIMILARITY_WEIGHT_PEARSON}")
        print(f"  Диапазон:            {SIMILARITY_WEIGHT_RANGE}")
        print(f"  Смещение:            {SIMILARITY_WEIGHT_NET_MOVE}")
        print(f"  Средний шаг:         {SIMILARITY_WEIGHT_MEAN_ABS_DIFF}")
        print(f"  Эффективность:       {SIMILARITY_WEIGHT_EFFICIENCY}")
        print("")

        print_current_hour_block(
            current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
            current_hour_start_text_ct=current_hour_start_text_ct,
            current_hour_slot_ct=current_hour_slot_ct,
            current_bar_start_text_utc=current_hour_rows[TARGET_BAR_INDEX]["bar_time"],
            current_bar_start_text_ct=current_hour_rows[TARGET_BAR_INDEX]["bar_time_ct"],
            current_prefix_y=current_prefix_y,
        )

        print("ЛУЧШИЕ КАНДИДАТЫ ПО ИТОГОВОМУ ВЗВЕШЕННОМУ SCORE:")
        print("")

        for index, item in enumerate(ranked_by_final_score[:TOP_N_PRINT], start=1):
            print_candidate_block(index=index, item=item)

        print_summary(
            best_by_pearson=best_by_pearson,
            best_by_final_score=best_by_final_score,
            total_candidates=len(ranked_by_final_score),
        )

        output_path = save_weighted_score_plot(
            current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
            current_hour_start_text_ct=current_hour_start_text_ct,
            current_full_y=current_full_y,
            target_bar_index=TARGET_BAR_INDEX,
            top_candidates=ranked_by_final_score[:TOP_N_PLOT],
            prepared_hours_map=prepared_hours_map,
        )

        print(f"PNG сохранён: {output_path}")

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()

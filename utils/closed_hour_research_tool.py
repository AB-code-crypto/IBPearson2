"""
Универсальный исследовательский скрипт для анализа одного уже закрытого исторического часа.

Зачем нужен
-----------
Этот файл объединяет несколько разовых исследовательских утилит, которые раньше были
разнесены по отдельным скриптам, но по сути работали с одним и тем же конвейером:

1. загружали один уже закрытый исторический час из price DB;
2. валидировали его как полный 5-секундный час;
3. определяли CT-слот часа;
4. загружали historical prepared-кандидаты по разрешённым CT-slot;
5. анализировали этот час на одном из уровней стратегии.

Теперь всё это собрано в одном файле, чтобы:
- не дублировать одинаковую загрузку данных;
- не держать 6 почти одинаковых утилит;
- переключаться между уровнями анализа одной переменной MODE;
- хранить общие настройки часа и путей в одном месте.

Какие режимы есть
-----------------
1. MODE = "candidate_features"
   Разбирает признаки кандидатов относительно текущего часа:
   range, net_move, mean_abs_diff, path_efficiency,
   diff_pearson, diff_sign_match_ratio.
   Можно задать TARGET_BAR_INDEX вручную или оставить None,
   тогда скрипт сам ищет первый сигнал в окне MIN_BARS_TO_START .. MAX_BARS_EXCLUSIVE.

2. MODE = "weighted_similarity"
   Строит weighted similarity ranking по reusable-модулю candidate_scoring,
   печатает score по фильтрам и сохраняет PNG с лучшими кандидатами.

3. MODE = "similarity_forecast"
   Берёт similarity ranking, отбирает top-N кандидатов в forecast layer,
   считает общий прогноз и сохраняет PNG.

4. MODE = "decision_layer"
   Полностью повторяет closed-hour pipeline:
   similarity -> forecast -> decision,
   печатает diagnostics decision layer и сохраняет PNG.

5. MODE = "pearson_runtime"
   Прогоняет уже закрытый час как будто в realtime,
   используя PearsonCurrentHour, и останавливается на первом сигнале
   или показывает лучший snapshot за час, если сигнала не было.
   Сохраняет PNG с текущим часом и кандидатами.

6. MODE = "pearson_runtime_future_plot"
   То же самое, что pearson_runtime,
   но дополнительно строит projected future-path кандидатов
   и медианный projected future-path поверх текущего часа.

Общий принцип работы
--------------------
Режимы candidate_features / weighted_similarity / similarity_forecast / decision_layer
используют фиксированный TARGET_BAR_INDEX внутри уже закрытого часа.

Режимы pearson_runtime / pearson_runtime_future_plot
идут по часу бар за баром как будто в realtime и ищут момент сигнала
по чистому Pearson runtime.

Что скрипт НЕ делает
--------------------
- не пишет ничего в price DB;
- не пишет ничего в prepared DB;
- не отправляет торговые ордера;
- не использует IB;
- не заменяет основной тестер.

Это именно исследовательский инструмент для анализа логики стратегии
на одном конкретном уже закрытом историческом часе.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from math import sqrt
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.candidate_decision import evaluate_decision_layer
from ts.candidate_features import (
    build_first_diff,
    calc_diff_pearson,
    calc_diff_sign_match_ratio,
    calc_mean_abs_diff,
    calc_net_move,
    calc_path_efficiency,
    calc_pearson_corr,
    calc_range,
)
from ts.candidate_forecast import build_group_forecast_from_prepared_candidates
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.pearson_runtime import PearsonCurrentHour
from ts.ts_config import (
    FORECAST_TOP_N_AFTER_SIMILARITY,
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

# Режимы:
# - "candidate_features"
# - "weighted_similarity"
# - "similarity_forecast"
# - "decision_layer"
# - "pearson_runtime"
# - "pearson_runtime_future_plot"
MODE = "decision_layer"

INSTRUMENT_CODE = "MNQ"
CURRENT_HOUR_START_TEXT_UTC = "2026-03-19 18:00:00"
OUTPUT_DIR = Path("png")

# ============================================================
# НАСТРОЙКИ ДЛЯ FIXED-BAR РЕЖИМОВ
# candidate_features / weighted_similarity / similarity_forecast / decision_layer
# ============================================================

# Если TARGET_BAR_INDEX = None, это поддерживается только для режима
# candidate_features: тогда скрипт сам ищет первый сигнал в окне.
TARGET_BAR_INDEX = 359
MIN_REQUIRED_PEARSON = None

TOP_N_PRINT = 10
TOP_N_PLOT = 5

# Для режима candidate_features, если TARGET_BAR_INDEX = None.
MIN_BARS_TO_START = 360
MAX_BARS_EXCLUSIVE = 600
TOP_N_FEATURES = 10
REQUIRED_CORRELATION_FEATURES = 0.90
REQUIRED_MATCH_COUNT_FEATURES = 6
MIN_HISTORY_CANDIDATES_FEATURES = 100

# Для similarity_forecast.
FORECAST_TOP_N = FORECAST_TOP_N_AFTER_SIMILARITY

# ============================================================
# НАСТРОЙКИ ДЛЯ RUNTIME РЕЖИМОВ
# pearson_runtime / pearson_runtime_future_plot
# ============================================================

MIN_BARS_TO_START_RUNTIME = 360
TOP_N_RUNTIME = 10
PLOT_CANDIDATES_COUNT_RUNTIME = 10
STOP_AFTER_BAR_INDEX_RUNTIME = None
REQUIRED_CORRELATION_RUNTIME = 0.80
REQUIRED_MATCH_COUNT_RUNTIME = 6
MIN_HISTORY_CANDIDATES_RUNTIME = 100
STOP_ON_FIRST_SIGNAL_RUNTIME = True


# ============================================================
# БАЗОВЫЕ HELPER-ФУНКЦИИ
# ============================================================

def parse_utc_hour_start_text(hour_start_text):
    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"CURRENT_HOUR_START_TEXT_UTC должен указывать ровно на начало часа, "
            f"получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def floor_to_hour_ts(ts):
    return (ts // 3600) * 3600


def hour_start_text_from_ts(hour_start_ts):
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_ct_axis_ts(ts_ct):
    return datetime.fromtimestamp(ts_ct, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_bar_close_time_text(bar_start_time_text):
    dt = datetime.strptime(bar_start_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return (dt + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")


def safe_ratio_text(value):
    if value is None:
        return "нет"
    return f"{value:.4f}"


def pct_text(value):
    return f"{value * 100:+.4f}%"


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
        y.append((mid_close / mid_open_0) - 1.0)

    return y


def build_current_hour_y_prefix(rows, end_bar_index):
    if end_bar_index < 0:
        raise ValueError("end_bar_index должен быть >= 0")

    if end_bar_index >= len(rows):
        raise ValueError(
            f"end_bar_index={end_bar_index} вне диапазона rows длины {len(rows)}"
        )

    return build_current_hour_y(rows[: end_bar_index + 1])


def calc_feature_pack(values):
    diff_values = build_first_diff(values)

    return {
        "range": calc_range(values),
        "net_move": calc_net_move(values),
        "mean_abs_diff": calc_mean_abs_diff(diff_values),
        "path_efficiency": calc_path_efficiency(values),
    }


def calc_feature_distance(current_metric, candidate_metric):
    abs_diff = abs(current_metric - candidate_metric)

    if current_metric == 0.0 and candidate_metric == 0.0:
        return 0.0

    base = max(abs(current_metric), abs(candidate_metric))

    if base == 0.0:
        return abs_diff

    return abs_diff / base


def print_current_hour_block(
    current_hour_start_text_utc,
    current_hour_start_text_ct,
    current_hour_slot_ct,
    current_bar_start_text_utc,
    current_bar_start_text_ct,
    current_prefix_y,
    target_bar_index,
):
    current_range = calc_range(current_prefix_y)
    current_net_move = calc_net_move(current_prefix_y)
    current_mean_abs_diff = calc_mean_abs_diff(build_first_diff(current_prefix_y))
    current_efficiency = calc_path_efficiency(current_prefix_y)

    print("ХАРАКТЕРИСТИКИ ТЕКУЩЕГО ЧАСА:")
    print(f"  Начало часа UTC: {current_hour_start_text_utc}")
    print(f"  Начало часа CT:  {current_hour_start_text_ct} CT")
    print(f"  CT-слот часа:    {current_hour_slot_ct}")
    print(f"  Индекс текущего бара: {target_bar_index}")
    print(f"  Начало бара UTC:      {current_bar_start_text_utc} UTC")
    print(f"  Закрытие бара UTC:    {get_bar_close_time_text(current_bar_start_text_utc)} UTC")
    print(f"  Начало бара CT:       {current_bar_start_text_ct} CT")
    print(f"  Закрытие бара CT:     {get_bar_close_time_text(current_bar_start_text_ct)} CT")
    print(f"  Длина префикса:       {len(current_prefix_y)}")
    print(f"  Диапазон участка:     {pct_text(current_range)}")
    print(f"  Итоговое смещение:    {pct_text(current_net_move)}")
    print(f"  Средний шаг:          {pct_text(current_mean_abs_diff)}")
    print(f"  Эффективность пути:   {safe_ratio_text(current_efficiency)}")
    print("")


def build_plot_file_path(mode_name, current_hour_start_text_utc, bar_index):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_hour_text = current_hour_start_text_utc.replace(":", "-").replace(" ", "_")
    file_name = f"{mode_name}_{safe_hour_text}_bar_{bar_index:03d}.png"
    return OUTPUT_DIR / file_name


def load_closed_hour_context():
    instrument_row = Instrument[INSTRUMENT_CODE]
    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    current_hour_start_ts = parse_utc_hour_start_text(CURRENT_HOUR_START_TEXT_UTC)

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row
        prepared_conn.row_factory = sqlite3.Row
        price_conn.execute("PRAGMA busy_timeout=5000;")
        prepared_conn.execute("PRAGMA busy_timeout=5000;")

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

        prepared_hours_map = {
            item["hour_start_ts"]: item for item in prepared_hours
        }

        current_full_y = build_current_hour_y(current_hour_rows)

        return {
            "instrument_row": instrument_row,
            "table_name": table_name,
            "current_hour_start_ts": current_hour_start_ts,
            "current_hour_rows": current_hour_rows,
            "current_hour_start_ts_ct": current_hour_start_ts_ct,
            "current_hour_start_text_ct": current_hour_start_text_ct,
            "current_hour_slot_ct": current_hour_slot_ct,
            "allowed_hour_slots_ct": allowed_hour_slots_ct,
            "prepared_hours": prepared_hours,
            "prepared_hours_map": prepared_hours_map,
            "current_full_y": current_full_y,
        }
    finally:
        price_conn.close()
        prepared_conn.close()


# ============================================================
# MODE: candidate_features
# ============================================================

class SimplePearsonCandidate:
    def __init__(self, prepared_hour_payload):
        self.hour_start_ts = prepared_hour_payload["hour_start_ts"]
        self.hour_start_ts_ct = prepared_hour_payload["hour_start_ts_ct"]
        self.hour_start_ct = prepared_hour_payload["hour_start_ct"]
        self.hour_slot_ct = prepared_hour_payload["hour_slot_ct"]
        self.contract = prepared_hour_payload["contract"]
        self.y = prepared_hour_payload["y"]
        self.sum_y = prepared_hour_payload["sum_y"]
        self.sum_y2 = prepared_hour_payload["sum_y2"]
        self.sum_xy = 0.0
        self.last_correlation = None

    def initialize_sum_xy(self, current_x):
        self.sum_xy = 0.0
        for bar_index, x_value in enumerate(current_x):
            self.sum_xy += x_value * self.y[bar_index]

    def update_sum_xy_for_last_bar(self, x_value, bar_index):
        self.sum_xy += x_value * self.y[bar_index]

    def calculate_correlation(self, current_sum_x, current_sum_x2, current_n, current_bar_index):
        sum_y = self.sum_y[current_bar_index]
        sum_y2 = self.sum_y2[current_bar_index]

        numerator = (current_n * self.sum_xy) - (current_sum_x * sum_y)
        left = (current_n * current_sum_x2) - (current_sum_x * current_sum_x)
        right = (current_n * sum_y2) - (sum_y * sum_y)

        if left <= 0.0 or right <= 0.0:
            self.last_correlation = None
            return None

        denominator = sqrt(left * right)
        if denominator == 0.0:
            self.last_correlation = None
            return None

        correlation = numerator / denominator
        self.last_correlation = correlation
        return correlation


class SimplePearsonCurrentHour:
    def __init__(self, hour_start_ts, hour_start_ts_ct, hour_start_ct):
        self.hour_start_ts = hour_start_ts
        self.hour_start_utc = hour_start_text_from_ts(hour_start_ts)
        self.hour_start_ts_ct = hour_start_ts_ct
        self.hour_start_ct = hour_start_ct
        self.hour_slot_ct = (hour_start_ts_ct // 3600) % 24
        self.mid_open_0 = None
        self.x = []
        self.sum_x = 0.0
        self.sum_x2 = 0.0
        self.candidates = []
        self.candidates_initialized = False

    def current_bar_index(self):
        if not self.x:
            return None
        return len(self.x) - 1

    def current_n(self):
        return len(self.x)

    def add_bar(self, ask_open, bid_open, ask_close, bid_close):
        if self.mid_open_0 is None:
            self.mid_open_0 = (ask_open + bid_open) / 2.0
            if self.mid_open_0 == 0.0:
                raise ValueError("mid_open_0 == 0, деление невозможно")

        mid_close = (ask_close + bid_close) / 2.0
        x_value = (mid_close / self.mid_open_0) - 1.0
        self.x.append(x_value)
        self.sum_x += x_value
        self.sum_x2 += x_value * x_value
        return x_value

    def set_candidates(self, prepared_hours):
        self.candidates = [SimplePearsonCandidate(item) for item in prepared_hours]
        self.candidates_initialized = False

    def initialize_candidates(self):
        if not self.x:
            raise ValueError("Нельзя инициализировать кандидатов: текущий x пустой")
        for candidate in self.candidates:
            candidate.initialize_sum_xy(self.x)
        self.candidates_initialized = True

    def update_candidates_for_last_bar(self):
        if not self.candidates_initialized:
            raise ValueError("Кандидаты ещё не инициализированы")
        last_bar_index = len(self.x) - 1
        last_x_value = self.x[last_bar_index]
        for candidate in self.candidates:
            candidate.update_sum_xy_for_last_bar(
                x_value=last_x_value,
                bar_index=last_bar_index,
            )

    def calculate_all_correlations(self):
        if not self.x:
            return []

        current_bar_index = len(self.x) - 1
        current_n = len(self.x)
        result = []

        for candidate in self.candidates:
            correlation = candidate.calculate_correlation(
                current_sum_x=self.sum_x,
                current_sum_x2=self.sum_x2,
                current_n=current_n,
                current_bar_index=current_bar_index,
            )
            result.append(
                {
                    "hour_start_ts": candidate.hour_start_ts,
                    "hour_start_ts_ct": candidate.hour_start_ts_ct,
                    "hour_start_ct": candidate.hour_start_ct,
                    "hour_slot_ct": candidate.hour_slot_ct,
                    "contract": candidate.contract,
                    "correlation": correlation,
                    "candidate_values": candidate.y[:current_n],
                }
            )

        return result

    def get_ranked_candidates(self, min_correlation=None, top_n=None):
        correlations = self.calculate_all_correlations()
        filtered = []

        for item in correlations:
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


def calculate_candidate_correlations(current_values, prepared_hours):
    result = []
    current_n = len(current_values)

    for item in prepared_hours:
        candidate_values = item["y"][:current_n]
        correlation = calc_pearson_corr(current_values, candidate_values)
        if correlation is None:
            continue

        result.append(
            {
                "hour_start_ts": item["hour_start_ts"],
                "hour_start_ts_ct": item["hour_start_ts_ct"],
                "hour_start_ct": item["hour_start_ct"],
                "hour_slot_ct": item["hour_slot_ct"],
                "contract": item["contract"],
                "correlation": correlation,
                "candidate_values": candidate_values,
            }
        )

    result.sort(key=lambda x: x["correlation"], reverse=True)
    return result


def build_candidate_features_snapshot(rows, prepared_hours):
    if TARGET_BAR_INDEX is not None:
        current_values = build_current_hour_y_prefix(rows, TARGET_BAR_INDEX)
        ranked = calculate_candidate_correlations(current_values, prepared_hours)
        matched = [item for item in ranked[:TOP_N_FEATURES] if item["correlation"] >= REQUIRED_CORRELATION_FEATURES]

        return {
            "bar_index": TARGET_BAR_INDEX,
            "current_values": current_values,
            "ranked_candidates": ranked[:TOP_N_FEATURES],
            "matched_candidates": matched,
            "signal_found": len(matched) >= REQUIRED_MATCH_COUNT_FEATURES,
        }

    best_snapshot = None

    for bar_index in range(MIN_BARS_TO_START - 1, min(MAX_BARS_EXCLUSIVE, len(rows))):
        current_values = build_current_hour_y_prefix(rows, bar_index)
        ranked = calculate_candidate_correlations(current_values, prepared_hours)
        top_ranked = ranked[:TOP_N_FEATURES]
        matched = [item for item in top_ranked if item["correlation"] >= REQUIRED_CORRELATION_FEATURES]
        signal_found = len(matched) >= REQUIRED_MATCH_COUNT_FEATURES

        snapshot = {
            "bar_index": bar_index,
            "current_values": current_values,
            "ranked_candidates": top_ranked,
            "matched_candidates": matched,
            "signal_found": signal_found,
        }

        if signal_found:
            return snapshot

        if best_snapshot is None:
            best_snapshot = snapshot
        else:
            current_best = top_ranked[0]["correlation"] if top_ranked else None
            saved_best = best_snapshot["ranked_candidates"][0]["correlation"] if best_snapshot["ranked_candidates"] else None

            if saved_best is None:
                best_snapshot = snapshot
            elif current_best is not None and current_best > saved_best:
                best_snapshot = snapshot

    return best_snapshot


def build_candidate_feature_rows(snapshot, current_feature_pack):
    rows = []
    current_values = snapshot["current_values"]

    for rank, item in enumerate(snapshot["ranked_candidates"], start=1):
        candidate_values = item["candidate_values"]
        candidate_feature_pack = calc_feature_pack(candidate_values)

        diff_pearson = calc_diff_pearson(current_values, candidate_values)
        diff_sign_match_ratio = calc_diff_sign_match_ratio(current_values, candidate_values)

        range_distance = calc_feature_distance(
            current_feature_pack["range"],
            candidate_feature_pack["range"],
        )
        net_move_distance = calc_feature_distance(
            current_feature_pack["net_move"],
            candidate_feature_pack["net_move"],
        )
        mean_abs_diff_distance = calc_feature_distance(
            current_feature_pack["mean_abs_diff"],
            candidate_feature_pack["mean_abs_diff"],
        )
        path_efficiency_distance = calc_feature_distance(
            current_feature_pack["path_efficiency"],
            candidate_feature_pack["path_efficiency"],
        )

        total_feature_distance = (
            range_distance +
            net_move_distance +
            mean_abs_diff_distance +
            path_efficiency_distance
        )

        rows.append(
            {
                "rank": rank,
                "is_matched": item in snapshot["matched_candidates"],
                "hour_start_ct": item["hour_start_ct"],
                "hour_slot_ct": item["hour_slot_ct"],
                "contract": item["contract"],
                "pearson": item["correlation"],
                "diff_pearson": diff_pearson,
                "diff_sign_match_ratio": diff_sign_match_ratio,
                "range": candidate_feature_pack["range"],
                "range_distance": range_distance,
                "net_move": candidate_feature_pack["net_move"],
                "net_move_distance": net_move_distance,
                "mean_abs_diff": candidate_feature_pack["mean_abs_diff"],
                "mean_abs_diff_distance": mean_abs_diff_distance,
                "path_efficiency": candidate_feature_pack["path_efficiency"],
                "path_efficiency_distance": path_efficiency_distance,
                "total_feature_distance": total_feature_distance,
            }
        )

    return rows


def print_candidates_summary(snapshot, current_feature_pack):
    candidate_feature_rows = build_candidate_feature_rows(
        snapshot=snapshot,
        current_feature_pack=current_feature_pack,
    )

    if not candidate_feature_rows:
        print("Подходящих кандидатов не найдено.")
        print("")
        return candidate_feature_rows

    print("КАНДИДАТЫ И ИХ ХАРАКТЕРИСТИКИ:")
    print("")

    for row in candidate_feature_rows:
        matched_mark = "ПРОШЁЛ ПОРОГ" if row["is_matched"] else "-"

        print(f"[{row['rank']}] {matched_mark}")
        print(f"  CT-час кандидата:                  {row['hour_start_ct']} CT")
        print(f"  CT-слот:                           {row['hour_slot_ct']}")
        print(f"  Контракт:                          {row['contract']}")
        print(f"  Пирсон:                            {safe_ratio_text(row['pearson'])}")
        print(f"  Пирсон по первой разности:         {safe_ratio_text(row['diff_pearson'])}")
        print(f"  Совпадение знаков первой разности: {safe_ratio_text(row['diff_sign_match_ratio'])}")
        print(f"  Диапазон участка:                  {pct_text(row['range'])}")
        print(f"  Отличие по диапазону:              {safe_ratio_text(row['range_distance'])}")
        print(f"  Итоговое смещение:                 {pct_text(row['net_move'])}")
        print(f"  Отличие по смещению:               {safe_ratio_text(row['net_move_distance'])}")
        print(f"  Средний абсолютный шаг:            {pct_text(row['mean_abs_diff'])}")
        print(f"  Отличие по среднему шагу:          {safe_ratio_text(row['mean_abs_diff_distance'])}")
        print(f"  Эффективность пути:                {safe_ratio_text(row['path_efficiency'])}")
        print(f"  Отличие по эффективности:          {safe_ratio_text(row['path_efficiency_distance'])}")
        print(f"  Суммарное отличие по признакам:    {safe_ratio_text(row['total_feature_distance'])}")
        print("")

    return candidate_feature_rows


def print_candidate_features_final_summary(snapshot, candidate_feature_rows):
    print("ИТОГОВЫЙ РЕЗУЛЬТАТ:")
    print("")

    if not candidate_feature_rows:
        print("  Кандидаты отсутствуют, итоговый вывод сделать нельзя.")
        print("")
        return

    best_by_pearson = candidate_feature_rows[0]
    best_by_features = min(
        candidate_feature_rows,
        key=lambda row: row["total_feature_distance"],
    )

    matched_rows = [row for row in candidate_feature_rows if row["is_matched"]]

    print(f"  Сигнал по исходному Пирсону найден: {'ДА' if snapshot['signal_found'] else 'НЕТ'}")
    print(f"  Кандидатов в TOP-N: {len(candidate_feature_rows)}")
    print(f"  Кандидатов, прошедших порог корреляции: {len(matched_rows)}")
    print("")

    print("  Лучший по исходному Пирсону:")
    print(f"    CT-час:                {best_by_pearson['hour_start_ct']} CT")
    print(f"    Контракт:              {best_by_pearson['contract']}")
    print(f"    Пирсон:                {safe_ratio_text(best_by_pearson['pearson'])}")
    print(f"    Пирсон по разностям:   {safe_ratio_text(best_by_pearson['diff_pearson'])}")
    print(f"    Совпадение знаков:     {safe_ratio_text(best_by_pearson['diff_sign_match_ratio'])}")
    print(f"    Суммарное отличие:     {safe_ratio_text(best_by_pearson['total_feature_distance'])}")
    print("")

    print("  Лучший по второму этапу фильтрации:")
    print(f"    CT-час:                {best_by_features['hour_start_ct']} CT")
    print(f"    Контракт:              {best_by_features['contract']}")
    print(f"    Пирсон:                {safe_ratio_text(best_by_features['pearson'])}")
    print(f"    Пирсон по разностям:   {safe_ratio_text(best_by_features['diff_pearson'])}")
    print(f"    Совпадение знаков:     {safe_ratio_text(best_by_features['diff_sign_match_ratio'])}")
    print(f"    Суммарное отличие:     {safe_ratio_text(best_by_features['total_feature_distance'])}")
    print("")

    if matched_rows:
        best_matched_by_features = min(
            matched_rows,
            key=lambda row: row["total_feature_distance"],
        )
        print("  Лучший среди кандидатов, прошедших порог корреляции:")
        print(f"    CT-час:                {best_matched_by_features['hour_start_ct']} CT")
        print(f"    Контракт:              {best_matched_by_features['contract']}")
        print(f"    Пирсон:                {safe_ratio_text(best_matched_by_features['pearson'])}")
        print(f"    Пирсон по разностям:   {safe_ratio_text(best_matched_by_features['diff_pearson'])}")
        print(f"    Совпадение знаков:     {safe_ratio_text(best_matched_by_features['diff_sign_match_ratio'])}")
        print(f"    Суммарное отличие:     {safe_ratio_text(best_matched_by_features['total_feature_distance'])}")
        print("")


# ============================================================
# MODE: weighted_similarity / similarity_forecast / decision_layer
# ============================================================

def print_weighted_candidate_block(index, item):
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


def print_weighted_summary(best_by_pearson, best_by_final_score, total_candidates):
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
        mode_name="weighted_similarity",
        current_hour_start_text_utc=current_hour_start_text_utc,
        bar_index=target_bar_index,
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return output_path


def print_similarity_candidates(selected_candidates):
    print("КАНДИДАТЫ ПОСЛЕ SIMILARITY-СЛОЯ:")
    print("")

    for index, item in enumerate(selected_candidates, start=1):
        print(f"[{index}]")
        print(f"  CT-час кандидата:         {item['hour_start_ct']} CT")
        print(f"  CT-слот:                  {item['hour_slot_ct']}")
        print(f"  Контракт:                 {item['contract']}")
        print(f"  Пирсон:                   {item['pearson']:.4f}")
        print(f"  Итоговый similarity:      {item['final_score']:.4f}")
        print("")


def print_forecast_block(forecast):
    print("СВОДКА ПРОГНОЗНОГО СЛОЯ:")
    print("")
    print(f"  Кандидатов в прогнозе:              {forecast['candidate_count']}")
    print(f"  Доля кандидатов вверх:              {forecast['positive_ratio']:.4f}")
    print(f"  Доля кандидатов вниз:               {forecast['negative_ratio']:.4f}")
    print(f"  Доля нейтральных кандидатов:        {forecast['flat_ratio']:.4f}")
    print("")
    print(f"  Среднее итоговое движение:          {forecast['mean_final_move'] * 100:+.4f}%")
    print(f"  Медианное итоговое движение:        {forecast['median_final_move'] * 100:+.4f}%")
    print("")
    print(f"  Средний максимальный апсайд:        {forecast['mean_max_upside'] * 100:+.4f}%")
    print(f"  Медианный максимальный апсайд:      {forecast['median_max_upside'] * 100:+.4f}%")
    print("")
    print(f"  Средний максимальный даундроу:      {forecast['mean_max_drawdown'] * 100:+.4f}%")
    print(f"  Медианный максимальный даундроу:    {forecast['median_max_drawdown'] * 100:+.4f}%")
    print("")

    mean_final_move = forecast["mean_final_move"]
    median_final_move = forecast["median_final_move"]

    if mean_final_move > 0.0 and median_final_move > 0.0:
        direction = "ВВЕРХ"
    elif mean_final_move < 0.0 and median_final_move < 0.0:
        direction = "ВНИЗ"
    else:
        direction = "НЕОДНОЗНАЧНО"

    print(f"  Предварительное направление:        {direction}")
    print("")


def save_forecast_plot(
    current_hour_start_text_utc,
    current_hour_start_text_ct,
    current_full_y,
    target_bar_index,
    selected_candidates,
    prepared_hours_map,
    forecast,
):
    plt.figure(figsize=(14, 8))
    full_x = list(range(len(current_full_y)))
    plt.plot(
        full_x,
        current_full_y,
        linewidth=2.5,
        label=f"Текущий час | UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct}",
    )

    future_start_x = target_bar_index + 1
    mean_future_path = forecast["mean_future_path"]
    median_future_path = forecast["median_future_path"]

    if mean_future_path:
        mean_x = list(range(future_start_x, future_start_x + len(mean_future_path)))
        plt.plot(
            mean_x,
            [current_full_y[target_bar_index] + value for value in mean_future_path],
            linewidth=2.0,
            linestyle="--",
            label=f"Средний future-path top-{len(selected_candidates)}",
        )

    if median_future_path:
        median_x = list(range(future_start_x, future_start_x + len(median_future_path)))
        plt.plot(
            median_x,
            [current_full_y[target_bar_index] + value for value in median_future_path],
            linewidth=2.0,
            linestyle=":",
            label=f"Медианный future-path top-{len(selected_candidates)}",
        )

    for rank, item in enumerate(selected_candidates, start=1):
        candidate_hour = prepared_hours_map[item["hour_start_ts"]]
        candidate_y = candidate_hour["y"]
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
        f"Similarity + Forecast | UTC {current_hour_start_text_utc} | CT {current_hour_start_text_ct}"
    )
    plt.xlabel("bar_index")
    plt.ylabel("y")
    plt.legend(loc="best", fontsize=8)
    plt.grid(True)

    output_path = build_plot_file_path(
        mode_name="similarity_forecast",
        current_hour_start_text_utc=current_hour_start_text_utc,
        bar_index=target_bar_index,
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return output_path


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
    print(f"    last_similarity_score:       {diagnostics.get('last_similarity_score')}")
    print(f"    mean_final_move:             {diagnostics['mean_final_move'] * 100:+.4f}%")
    print(f"    median_final_move:           {diagnostics['median_final_move'] * 100:+.4f}%")
    print(f"    mean_direction:              {diagnostics['mean_direction']}")
    print(f"    median_direction:            {diagnostics['median_direction']}")
    print(f"    positive_ratio:              {diagnostics['positive_ratio']:.4f}")
    print(f"    negative_ratio:              {diagnostics['negative_ratio']:.4f}")
    print(f"    mean_max_upside:             {diagnostics['mean_max_upside'] * 100:+.4f}%")
    print(f"    mean_max_drawdown:           {diagnostics['mean_max_drawdown'] * 100:+.4f}%")
    print("")


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
        mode_name="decision_layer",
        current_hour_start_text_utc=current_hour_start_text_utc,
        bar_index=target_bar_index,
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return output_path


# ============================================================
# MODE: pearson_runtime / pearson_runtime_future_plot
# ============================================================

def get_candidates_above_correlation(ranked_candidates, required_correlation):
    matched = []
    for item in ranked_candidates:
        if item["correlation"] >= required_correlation:
            matched.append(item)
    return matched


def build_forward_stats(prepared_hour_payload, current_bar_index):
    y = prepared_hour_payload["y"]

    if current_bar_index < 0 or current_bar_index >= len(y):
        raise ValueError(
            f"Некорректный current_bar_index={current_bar_index} "
            f"для prepared hour длины {len(y)}"
        )

    current_rel = 1.0 + y[current_bar_index]
    bars_left = (len(y) - 1) - current_bar_index

    if bars_left <= 0:
        return {
            "bars_left": 0,
            "forward_to_end": 0.0,
            "max_up_after_signal": 0.0,
            "max_down_after_signal": 0.0,
        }

    future_y = y[current_bar_index + 1:]
    future_rel = [1.0 + value for value in future_y]

    end_rel = future_rel[-1]
    max_future_rel = max(future_rel)
    min_future_rel = min(future_rel)

    forward_to_end = (end_rel / current_rel) - 1.0
    max_up_after_signal = (max_future_rel / current_rel) - 1.0
    max_down_after_signal = (min_future_rel / current_rel) - 1.0

    return {
        "bars_left": bars_left,
        "forward_to_end": forward_to_end,
        "max_up_after_signal": max_up_after_signal,
        "max_down_after_signal": max_down_after_signal,
    }


def build_aggregate_forward_stats(matched_candidates, prepared_hours_map, current_bar_index):
    if not matched_candidates:
        return None

    forward_to_end_values = []
    max_up_values = []
    max_down_values = []

    positive_count = 0
    negative_count = 0
    flat_count = 0

    for item in matched_candidates:
        prepared_hour_payload = prepared_hours_map[item["hour_start_ts"]]
        forward_stats = build_forward_stats(
            prepared_hour_payload=prepared_hour_payload,
            current_bar_index=current_bar_index,
        )

        forward_to_end = forward_stats["forward_to_end"]
        max_up_after_signal = forward_stats["max_up_after_signal"]
        max_down_after_signal = forward_stats["max_down_after_signal"]

        forward_to_end_values.append(forward_to_end)
        max_up_values.append(max_up_after_signal)
        max_down_values.append(max_down_after_signal)

        if forward_to_end > 0.0:
            positive_count += 1
        elif forward_to_end < 0.0:
            negative_count += 1
        else:
            flat_count += 1

    return {
        "count": len(matched_candidates),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "flat_count": flat_count,
        "avg_to_end": sum(forward_to_end_values) / len(forward_to_end_values),
        "median_to_end": median(forward_to_end_values),
        "best_to_end": max(forward_to_end_values),
        "worst_to_end": min(forward_to_end_values),
        "avg_max_up": sum(max_up_values) / len(max_up_values),
        "median_max_up": median(max_up_values),
        "best_max_up": max(max_up_values),
        "avg_max_down": sum(max_down_values) / len(max_down_values),
        "median_max_down": median(max_down_values),
        "worst_max_down": min(max_down_values),
    }


def build_group_direction_decision(aggregate):
    if aggregate is None:
        return None

    count = aggregate["count"]
    positive_count = aggregate["positive_count"]
    negative_count = aggregate["negative_count"]
    avg_to_end = aggregate["avg_to_end"]
    median_to_end = aggregate["median_to_end"]

    if positive_count > negative_count and avg_to_end > 0.0 and median_to_end > 0.0:
        direction = "ВВЕРХ"
        majority_count = positive_count
    elif negative_count > positive_count and avg_to_end < 0.0 and median_to_end < 0.0:
        direction = "ВНИЗ"
        majority_count = negative_count
    else:
        direction = "НЕ ОПРЕДЕЛЕНО"
        majority_count = max(positive_count, negative_count)

    majority_share = majority_count / count if count > 0 else 0.0

    return {
        "direction": direction,
        "majority_count": majority_count,
        "majority_share": majority_share,
        "avg_to_end": avg_to_end,
        "median_to_end": median_to_end,
    }


def build_signal_quality_score(aggregate, direction_decision):
    if aggregate is None or direction_decision is None:
        return None

    direction = direction_decision["direction"]

    if direction == "НЕ ОПРЕДЕЛЕНО":
        return {
            "score": 0.0,
            "grade": "СЛАБЫЙ",
            "consensus_score": 0.0,
            "move_score": 0.0,
            "risk_reward_score": 0.0,
        }

    majority_share = direction_decision["majority_share"]
    avg_to_end = aggregate["avg_to_end"]
    median_to_end = aggregate["median_to_end"]
    avg_max_up = aggregate["avg_max_up"]
    avg_max_down = aggregate["avg_max_down"]

    consensus_score = majority_share

    if direction == "ВВЕРХ":
        move_strength = max(avg_to_end, 0.0, median_to_end)
    else:
        move_strength = max(-avg_to_end, 0.0, -median_to_end)

    move_score = min(move_strength / 0.0020, 1.0)

    if direction == "ВВЕРХ":
        favorable = max(avg_max_up, 0.0)
        adverse = max(-avg_max_down, 0.0)
    else:
        favorable = max(-avg_max_down, 0.0)
        adverse = max(avg_max_up, 0.0)

    edge = favorable - adverse
    risk_reward_score = min(max(edge, 0.0) / 0.0020, 1.0)

    score = (
        consensus_score * 40.0 +
        move_score * 35.0 +
        risk_reward_score * 25.0
    )

    if score >= 80.0:
        grade = "ОЧЕНЬ СИЛЬНЫЙ"
    elif score >= 60.0:
        grade = "СИЛЬНЫЙ"
    elif score >= 40.0:
        grade = "СРЕДНИЙ"
    else:
        grade = "СЛАБЫЙ"

    return {
        "score": score,
        "grade": grade,
        "consensus_score": consensus_score,
        "move_score": move_score,
        "risk_reward_score": risk_reward_score,
    }


def print_ranked_candidates(ranked_candidates, required_correlation):
    for rank, item in enumerate(ranked_candidates, start=1):
        passed = item["correlation"] >= required_correlation
        mark = "*" if passed else " "
        print(
            f" {mark}{rank:>2}. "
            f"{item['hour_start_ct']} CT | "
            f"{item['contract']} | "
            f"корреляция={item['correlation']:.6f}"
        )


def print_forward_block(matched_candidates, prepared_hours_map, current_bar_index):
    if not matched_candidates:
        return

    print("")
    print("ЧТО БЫЛО ДАЛЬШЕ У ПОДОШЕДШИХ КАНДИДАТОВ:")

    for rank, item in enumerate(matched_candidates, start=1):
        prepared_hour_payload = prepared_hours_map[item["hour_start_ts"]]
        forward_stats = build_forward_stats(
            prepared_hour_payload=prepared_hour_payload,
            current_bar_index=current_bar_index,
        )

        print(
            f"  {rank:>2}. "
            f"{item['hour_start_ct']} CT | "
            f"{item['contract']} | "
            f"корреляция={item['correlation']:.6f} | "
            f"баров_до_конца={forward_stats['bars_left']} | "
            f"до_конца_часа={forward_stats['forward_to_end'] * 100:+.4f}% | "
            f"макс_вверх={forward_stats['max_up_after_signal'] * 100:+.4f}% | "
            f"макс_вниз={forward_stats['max_down_after_signal'] * 100:+.4f}%"
        )


def print_aggregate_forward_block(matched_candidates, prepared_hours_map, current_bar_index):
    aggregate = build_aggregate_forward_stats(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )

    if aggregate is None:
        return

    print("")
    print("АГРЕГАТ ПО ПОДОШЕДШИМ КАНДИДАТАМ:")
    print(
        f"  всего={aggregate['count']} | "
        f"вверх={aggregate['positive_count']} | "
        f"вниз={aggregate['negative_count']} | "
        f"в_ноль={aggregate['flat_count']}"
    )
    print(
        f"  движение_до_конца_часа: "
        f"среднее={aggregate['avg_to_end'] * 100:+.4f}% | "
        f"медиана={aggregate['median_to_end'] * 100:+.4f}% | "
        f"лучшее={aggregate['best_to_end'] * 100:+.4f}% | "
        f"худшее={aggregate['worst_to_end'] * 100:+.4f}%"
    )
    print(
        f"  максимальный_ход_вверх: "
        f"среднее={aggregate['avg_max_up'] * 100:+.4f}% | "
        f"медиана={aggregate['median_max_up'] * 100:+.4f}% | "
        f"лучший={aggregate['best_max_up'] * 100:+.4f}%"
    )
    print(
        f"  максимальный_ход_вниз: "
        f"среднее={aggregate['avg_max_down'] * 100:+.4f}% | "
        f"медиана={aggregate['median_max_down'] * 100:+.4f}% | "
        f"худший={aggregate['worst_max_down'] * 100:+.4f}%"
    )


def print_group_direction_block(matched_candidates, prepared_hours_map, current_bar_index):
    aggregate = build_aggregate_forward_stats(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )

    decision = build_group_direction_decision(aggregate)

    if decision is None:
        return

    print("")
    print("ПРЕДВАРИТЕЛЬНЫЙ ВЫВОД ПО НАПРАВЛЕНИЮ:")
    print(
        f"  направление={decision['direction']} | "
        f"доля_большинства={decision['majority_share'] * 100:.2f}%"
    )
    print(
        f"  среднее_движение_до_конца_часа={decision['avg_to_end'] * 100:+.4f}% | "
        f"медиана_движения_до_конца_часа={decision['median_to_end'] * 100:+.4f}%"
    )


def print_signal_quality_block(matched_candidates, prepared_hours_map, current_bar_index):
    aggregate = build_aggregate_forward_stats(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )

    direction_decision = build_group_direction_decision(aggregate)
    quality = build_signal_quality_score(aggregate, direction_decision)

    if quality is None:
        return

    print("")
    print("ОЦЕНКА КАЧЕСТВА СИГНАЛА:")
    print(
        f"  score={quality['score']:.2f}/100 | "
        f"класс={quality['grade']}"
    )
    print(
        f"  согласованность_группы={quality['consensus_score'] * 100:.2f}% | "
        f"сила_движения={quality['move_score'] * 100:.2f}% | "
        f"потенциал_против_риска={quality['risk_reward_score'] * 100:.2f}%"
    )


def build_rebased_future_path(candidate_y, current_full_y, current_bar_index):
    current_signal_rel = 1.0 + current_full_y[current_bar_index]
    candidate_signal_rel = 1.0 + candidate_y[current_bar_index]

    rebased_path = []

    for idx in range(current_bar_index, len(candidate_y)):
        candidate_rel_from_signal = ((1.0 + candidate_y[idx]) / candidate_signal_rel) - 1.0
        rebased_rel = (current_signal_rel * (1.0 + candidate_rel_from_signal)) - 1.0
        rebased_path.append(rebased_rel)

    return rebased_path


def build_median_projected_future_path(
    candidates_to_plot,
    prepared_hours_map,
    current_full_y,
    current_bar_index,
):
    if not candidates_to_plot:
        return None

    projected_paths = []

    for item in candidates_to_plot:
        candidate_payload = prepared_hours_map[item["hour_start_ts"]]
        projected_path = build_rebased_future_path(
            candidate_y=candidate_payload["y"],
            current_full_y=current_full_y,
            current_bar_index=current_bar_index,
        )
        projected_paths.append(projected_path)

    if not projected_paths:
        return None

    median_path = []
    path_len = len(projected_paths[0])

    for idx in range(path_len):
        values = [path[idx] for path in projected_paths]
        median_path.append(median(values))

    return median_path


def save_runtime_plot(
    current_hour_start_text,
    current_hour_start_text_ct,
    current_full_y,
    ranked_candidates,
    prepared_hours_map,
    current_bar_index,
    required_correlation,
    plot_candidates_count,
    include_projected_future,
):
    candidates_to_plot = ranked_candidates[:plot_candidates_count]

    if not candidates_to_plot:
        print("PNG-график не сохранён: нет кандидатов для рисования.")
        return None

    output_path = build_plot_file_path(
        mode_name=(
            "pearson_runtime_future_plot"
            if include_projected_future
            else "pearson_runtime"
        ),
        current_hour_start_text_utc=current_hour_start_text,
        bar_index=current_bar_index,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    x_values = list(range(len(current_full_y)))
    current_full_y_pct = [value * 100.0 for value in current_full_y]

    plt.figure(figsize=(16, 9))
    plt.plot(
        x_values,
        current_full_y_pct,
        linewidth=2.5,
        alpha=0.75,
        label=f"CURRENT full | {current_hour_start_text}",
    )

    if include_projected_future:
        future_x_values = list(range(current_bar_index, len(current_full_y)))
        current_future_y_pct = [value * 100.0 for value in current_full_y[current_bar_index:]]
        plt.plot(
            future_x_values,
            current_future_y_pct,
            linewidth=3.0,
            label="CURRENT future after signal",
        )

    for rank, item in enumerate(candidates_to_plot, start=1):
        candidate_payload = prepared_hours_map[item["hour_start_ts"]]
        candidate_y_pct = [value * 100.0 for value in candidate_payload["y"]]
        passed = item["correlation"] >= required_correlation
        passed_mark = "*" if passed else ""

        plt.plot(
            x_values,
            candidate_y_pct,
            linewidth=1.0,
            alpha=0.35,
            label=(
                f"{rank}. {passed_mark}{item['hour_start_ct']} CT | "
                f"corr={item['correlation']:.4f}"
            ),
        )

        if include_projected_future:
            rebased_future_path = build_rebased_future_path(
                candidate_y=candidate_payload["y"],
                current_full_y=current_full_y,
                current_bar_index=current_bar_index,
            )
            rebased_future_y_pct = [value * 100.0 for value in rebased_future_path]
            plt.plot(
                future_x_values,
                rebased_future_y_pct,
                linewidth=1.5,
                alpha=0.8,
                linestyle="--",
            )

    if include_projected_future:
        median_projected_future_path = build_median_projected_future_path(
            candidates_to_plot=candidates_to_plot,
            prepared_hours_map=prepared_hours_map,
            current_full_y=current_full_y,
            current_bar_index=current_bar_index,
        )

        if median_projected_future_path is not None:
            median_projected_future_y_pct = [value * 100.0 for value in median_projected_future_path]
            plt.plot(
                future_x_values,
                median_projected_future_y_pct,
                linewidth=3.0,
                linestyle=":",
                label="MEDIAN projected future",
            )

    plt.axvline(
        x=current_bar_index,
        linestyle="--",
        linewidth=1.5,
        label=f"signal bar {current_bar_index}",
    )

    plt.title(
        f"Текущий час и найденные кандидаты | "
        f"{current_hour_start_text} UTC | {current_hour_start_text_ct} CT | "
        f"bar_index={current_bar_index}"
    )
    plt.xlabel("bar_index")
    plt.ylabel("накопленное движение, %")
    plt.grid(True, alpha=0.3)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"PNG-график сохранён: {output_path}")
    return output_path


def print_signal_found(
    current_hour,
    ranked_candidates,
    matched_candidates,
    current_bar_index,
    current_bar_start_time_text,
    current_bar_start_time_text_ct,
    required_correlation,
    required_match_count,
    prepared_hours_map,
):
    current_bar_close_time_text = get_bar_close_time_text(current_bar_start_time_text)
    current_bar_close_time_text_ct = get_bar_close_time_text(current_bar_start_time_text_ct)

    print("")
    print("СИГНАЛ НАЙДЕН")
    print(
        f"  bar_index={current_bar_index} | "
        f"время_закрытия_бара={current_bar_close_time_text} UTC | "
        f"{current_bar_close_time_text_ct} CT | "
        f"точек_в_префиксе={current_hour.current_n()} | "
        f"размер_top={len(ranked_candidates)}"
    )
    print(
        f"  требуемая_корреляция={required_correlation} | "
        f"требуемое_число_совпадений={required_match_count} | "
        f"совпало={len(matched_candidates)}"
    )

    print_ranked_candidates(
        ranked_candidates=ranked_candidates,
        required_correlation=required_correlation,
    )
    print_forward_block(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )
    print_aggregate_forward_block(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )
    print_group_direction_block(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )
    print_signal_quality_block(
        matched_candidates=matched_candidates,
        prepared_hours_map=prepared_hours_map,
        current_bar_index=current_bar_index,
    )
    print("")


def print_best_result_without_signal(
    best_snapshot,
    required_correlation,
    required_match_count,
    prepared_hours_map,
):
    if best_snapshot is None:
        print("Сигнал не найден. За час не удалось получить ни одного снимка ranking.")
        return

    print("")
    print("СИГНАЛ НЕ НАЙДЕН")
    print("ЛУЧШИЙ ДОСТИГНУТЫЙ РЕЗУЛЬТАТ ЗА ЧАС:")
    print(
        f"  bar_index={best_snapshot['bar_index']} | "
        f"время_закрытия_бара={best_snapshot['bar_close_time_text']} UTC | "
        f"{best_snapshot['bar_close_time_text_ct']} CT | "
        f"размер_top={len(best_snapshot['ranked_candidates'])}"
    )
    print(
        f"  требуемая_корреляция={required_correlation} | "
        f"требуемое_число_совпадений={required_match_count} | "
        f"совпало={len(best_snapshot['matched_candidates'])}"
    )

    print_ranked_candidates(
        ranked_candidates=best_snapshot["ranked_candidates"],
        required_correlation=required_correlation,
    )
    print_forward_block(
        matched_candidates=best_snapshot["matched_candidates"],
        prepared_hours_map=prepared_hours_map,
        current_bar_index=best_snapshot["bar_index"],
    )
    print_aggregate_forward_block(
        matched_candidates=best_snapshot["matched_candidates"],
        prepared_hours_map=prepared_hours_map,
        current_bar_index=best_snapshot["bar_index"],
    )
    print_group_direction_block(
        matched_candidates=best_snapshot["matched_candidates"],
        prepared_hours_map=prepared_hours_map,
        current_bar_index=best_snapshot["bar_index"],
    )
    print_signal_quality_block(
        matched_candidates=best_snapshot["matched_candidates"],
        prepared_hours_map=prepared_hours_map,
        current_bar_index=best_snapshot["bar_index"],
    )
    print("")


def build_best_snapshot_score(snapshot):
    ranked_candidates = snapshot["ranked_candidates"]
    matched_candidates = snapshot["matched_candidates"]
    matched_count = len(matched_candidates)
    best_correlation = ranked_candidates[0]["correlation"] if ranked_candidates else float("-inf")
    return matched_count, best_correlation


# ============================================================
# РЕАЛИЗАЦИЯ MODE-ОВ
# ============================================================

def run_candidate_features_mode(context):
    if len(context["prepared_hours"]) < MIN_HISTORY_CANDIDATES_FEATURES:
        raise ValueError(
            f"Слишком мало prepared-кандидатов для анализа: "
            f"{len(context['prepared_hours'])} < {MIN_HISTORY_CANDIDATES_FEATURES}"
        )

    snapshot = build_candidate_features_snapshot(
        rows=context["current_hour_rows"],
        prepared_hours=context["prepared_hours"],
    )

    if snapshot is None:
        raise ValueError("Не удалось построить snapshot для анализа")

    print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
    print(f"Таблица: {context['table_name']}")
    print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
    print(f"Начало текущего часа CT:           {format_ct_axis_ts(context['current_hour_start_ts_ct'])} CT")
    print(f"Текущий CT-слот:                   {context['current_hour_slot_ct']}")
    print(f"Разрешённые CT-слоты для поиска:   {context['allowed_hour_slots_ct']}")
    print(f"Исторических prepared-кандидатов:  {len(context['prepared_hours'])}")
    print(f"TOP_N:                             {TOP_N_FEATURES}")
    print(f"Минимальная корреляция:            {REQUIRED_CORRELATION_FEATURES}")
    print(f"Минимум кандидатов по порогу:      {REQUIRED_MATCH_COUNT_FEATURES}")
    print(f"Сигнал по исходному Пирсону:       {'ДА' if snapshot['signal_found'] else 'НЕТ'}")
    print("")

    current_feature_pack = calc_feature_pack(snapshot["current_values"])
    current_row = context["current_hour_rows"][snapshot["bar_index"]]

    print_current_hour_block(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=format_ct_axis_ts(context['current_hour_start_ts_ct']),
        current_hour_slot_ct=context["current_hour_slot_ct"],
        current_bar_start_text_utc=current_row["bar_time"],
        current_bar_start_text_ct=current_row["bar_time_ct"],
        current_prefix_y=snapshot["current_values"],
        target_bar_index=snapshot["bar_index"],
    )

    candidate_feature_rows = print_candidates_summary(
        snapshot=snapshot,
        current_feature_pack=current_feature_pack,
    )
    print_candidate_features_final_summary(snapshot, candidate_feature_rows)


def run_weighted_similarity_mode(context):
    if TARGET_BAR_INDEX is None:
        raise ValueError("Для MODE='weighted_similarity' TARGET_BAR_INDEX должен быть задан")

    if TARGET_BAR_INDEX < 1 or TARGET_BAR_INDEX >= 720:
        raise ValueError("TARGET_BAR_INDEX должен быть в диапазоне 1..719")

    current_prefix_y = context["current_full_y"][: TARGET_BAR_INDEX + 1]

    ranked = rank_prepared_candidates_by_similarity(
        current_values=current_prefix_y,
        prepared_hours=context["prepared_hours"],
        min_required_pearson=MIN_REQUIRED_PEARSON,
    )

    if not ranked:
        raise ValueError("Не найдено ни одного кандидата после общего similarity scoring")

    best_by_pearson = max(ranked, key=lambda item: item["pearson"])
    best_by_final_score = ranked[0]

    print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
    print(f"Таблица: {context['table_name']}")
    print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
    print(f"Начало текущего часа CT:           {context['current_hour_start_text_ct']} CT")
    print(f"Текущий CT-слот:                   {context['current_hour_slot_ct']}")
    print(f"Разрешённые CT-слоты для поиска:   {context['allowed_hour_slots_ct']}")
    print(f"Исторических prepared-кандидатов:  {len(context['prepared_hours'])}")
    print(f"Точка сравнения (bar_index):       {TARGET_BAR_INDEX}")
    print(f"Длина текущего префикса:           {len(current_prefix_y)}")
    print(f"Кандидатов после общего score:     {len(ranked)}")
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

    current_row = context["current_hour_rows"][TARGET_BAR_INDEX]
    print_current_hour_block(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context['current_hour_start_text_ct'],
        current_hour_slot_ct=context["current_hour_slot_ct"],
        current_bar_start_text_utc=current_row["bar_time"],
        current_bar_start_text_ct=current_row["bar_time_ct"],
        current_prefix_y=current_prefix_y,
        target_bar_index=TARGET_BAR_INDEX,
    )

    print("ЛУЧШИЕ КАНДИДАТЫ ПО ИТОГОВОМУ ВЗВЕШЕННОМУ SCORE:")
    print("")
    for index, item in enumerate(ranked[:TOP_N_PRINT], start=1):
        print_weighted_candidate_block(index=index, item=item)

    print_weighted_summary(
        best_by_pearson=best_by_pearson,
        best_by_final_score=best_by_final_score,
        total_candidates=len(ranked),
    )

    output_path = save_weighted_score_plot(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context["current_hour_start_text_ct"],
        current_full_y=context["current_full_y"],
        target_bar_index=TARGET_BAR_INDEX,
        top_candidates=ranked[:TOP_N_PLOT],
        prepared_hours_map=context["prepared_hours_map"],
    )
    print(f"PNG сохранён: {output_path}")


def run_similarity_forecast_mode(context):
    if TARGET_BAR_INDEX is None:
        raise ValueError("Для MODE='similarity_forecast' TARGET_BAR_INDEX должен быть задан")

    current_prefix_y = context["current_full_y"][: TARGET_BAR_INDEX + 1]

    ranked = rank_prepared_candidates_by_similarity(
        current_values=current_prefix_y,
        prepared_hours=context["prepared_hours"],
        min_required_pearson=MIN_REQUIRED_PEARSON,
    )

    if not ranked:
        raise ValueError("После similarity-слоя не осталось ни одного кандидата")

    selected_candidates = ranked[:FORECAST_TOP_N]
    selected_prepared_hours = [
        context["prepared_hours_map"][item["hour_start_ts"]] for item in selected_candidates
    ]

    forecast = build_group_forecast_from_prepared_candidates(
        prepared_hours=selected_prepared_hours,
        current_bar_index=TARGET_BAR_INDEX,
    )

    print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
    print(f"Таблица: {context['table_name']}")
    print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
    print(f"Начало текущего часа CT:           {context['current_hour_start_text_ct']} CT")
    print(f"Текущий CT-слот:                   {context['current_hour_slot_ct']}")
    print(f"Разрешённые CT-слоты для поиска:   {context['allowed_hour_slots_ct']}")
    print(f"Исторических prepared-кандидатов:  {len(context['prepared_hours'])}")
    print(f"Точка сравнения (bar_index):       {TARGET_BAR_INDEX}")
    print(f"Длина текущего префикса:           {len(current_prefix_y)}")
    print(f"Кандидатов после similarity:       {len(ranked)}")
    print(f"Кандидатов в прогнозном слое:      {len(selected_candidates)}")
    print("")

    current_row = context["current_hour_rows"][TARGET_BAR_INDEX]
    print_current_hour_block(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context['current_hour_start_text_ct'],
        current_hour_slot_ct=context["current_hour_slot_ct"],
        current_bar_start_text_utc=current_row["bar_time"],
        current_bar_start_text_ct=current_row["bar_time_ct"],
        current_prefix_y=current_prefix_y,
        target_bar_index=TARGET_BAR_INDEX,
    )

    print_similarity_candidates(selected_candidates)
    print_forecast_block(forecast)

    output_path = save_forecast_plot(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context["current_hour_start_text_ct"],
        current_full_y=context["current_full_y"],
        target_bar_index=TARGET_BAR_INDEX,
        selected_candidates=selected_candidates,
        prepared_hours_map=context["prepared_hours_map"],
        forecast=forecast,
    )
    print(f"PNG сохранён: {output_path}")


def run_decision_layer_mode(context):
    if TARGET_BAR_INDEX is None:
        raise ValueError("Для MODE='decision_layer' TARGET_BAR_INDEX должен быть задан")

    current_prefix_y = context["current_full_y"][: TARGET_BAR_INDEX + 1]

    ranked = rank_prepared_candidates_by_similarity(
        current_values=current_prefix_y,
        prepared_hours=context["prepared_hours"],
        min_required_pearson=MIN_REQUIRED_PEARSON,
    )

    selected_candidates = ranked[:FORECAST_TOP_N_AFTER_SIMILARITY]
    selected_prepared_hours = [
        context["prepared_hours_map"][item["hour_start_ts"]] for item in selected_candidates
    ]

    forecast_summary = build_group_forecast_from_prepared_candidates(
        prepared_hours=selected_prepared_hours,
        current_bar_index=TARGET_BAR_INDEX,
    )

    decision_result = evaluate_decision_layer(
        ranked_similarity_candidates=ranked,
        forecast_summary=forecast_summary,
    )

    print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
    print(f"Таблица: {context['table_name']}")
    print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
    print(f"Начало текущего часа CT:           {context['current_hour_start_text_ct']} CT")
    print(f"Текущий CT-слот:                   {context['current_hour_slot_ct']}")
    print(f"Разрешённые CT-слоты для поиска:   {context['allowed_hour_slots_ct']}")
    print(f"Исторических prepared-кандидатов:  {len(context['prepared_hours'])}")
    print(f"Точка сравнения (bar_index):       {TARGET_BAR_INDEX}")
    print(f"Кандидатов после similarity:       {len(ranked)}")
    print(f"Кандидатов в прогнозном слое:      {len(selected_candidates)}")
    print("")

    current_row = context["current_hour_rows"][TARGET_BAR_INDEX]
    print_current_hour_block(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context['current_hour_start_text_ct'],
        current_hour_slot_ct=context["current_hour_slot_ct"],
        current_bar_start_text_utc=current_row["bar_time"],
        current_bar_start_text_ct=current_row["bar_time_ct"],
        current_prefix_y=current_prefix_y,
        target_bar_index=TARGET_BAR_INDEX,
    )

    print_decision_result(decision_result)

    output_path = save_decision_plot(
        current_hour_start_text_utc=CURRENT_HOUR_START_TEXT_UTC,
        current_hour_start_text_ct=context["current_hour_start_text_ct"],
        current_full_y=context["current_full_y"],
        target_bar_index=TARGET_BAR_INDEX,
        selected_candidates=selected_candidates,
        prepared_hours_map=context["prepared_hours_map"],
        forecast_summary=forecast_summary,
        decision_result=decision_result,
    )
    print(f"PNG сохранён: {output_path}")


def run_pearson_runtime_mode(context, include_projected_future):
    if REQUIRED_MATCH_COUNT_RUNTIME < 1:
        raise ValueError("REQUIRED_MATCH_COUNT_RUNTIME должен быть >= 1")

    if PLOT_CANDIDATES_COUNT_RUNTIME < 1:
        raise ValueError("PLOT_CANDIDATES_COUNT_RUNTIME должен быть >= 1")

    if len(context["prepared_hours"]) < MIN_HISTORY_CANDIDATES_RUNTIME:
        raise ValueError(
            f"Исторических prepared-кандидатов слишком мало: {len(context['prepared_hours'])}. "
            f"Минимально требуется: {MIN_HISTORY_CANDIDATES_RUNTIME}"
        )

    current_hour = PearsonCurrentHour(
        hour_start_ts=context["current_hour_start_ts"],
        hour_start_ts_ct=context["current_hour_start_ts_ct"],
    )
    current_hour.set_candidates(context["prepared_hours"])

    signal_found = False
    best_snapshot = None
    best_snapshot_score = None

    for row in context["current_hour_rows"]:
        current_bar_index = len(current_hour.x)

        if (
            STOP_AFTER_BAR_INDEX_RUNTIME is not None
            and current_bar_index > STOP_AFTER_BAR_INDEX_RUNTIME
        ):
            break

        current_hour.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

        if not current_hour.can_start_comparison(min_bars=MIN_BARS_TO_START_RUNTIME):
            continue

        if not current_hour.candidates_initialized:
            current_hour.initialize_candidates()
        else:
            current_hour.update_candidates_for_last_bar()

        ranked_candidates = current_hour.get_ranked_candidates(
            min_correlation=None,
            top_n=TOP_N_RUNTIME,
        )

        if not ranked_candidates:
            continue

        matched_candidates = get_candidates_above_correlation(
            ranked_candidates=ranked_candidates,
            required_correlation=REQUIRED_CORRELATION_RUNTIME,
        )

        current_snapshot = {
            "bar_index": current_bar_index,
            "bar_start_time_text": row["bar_time"],
            "bar_start_time_text_ct": row["bar_time_ct"],
            "bar_close_time_text": get_bar_close_time_text(row["bar_time"]),
            "bar_close_time_text_ct": get_bar_close_time_text(row["bar_time_ct"]),
            "ranked_candidates": ranked_candidates,
            "matched_candidates": matched_candidates,
        }

        current_snapshot_score = build_best_snapshot_score(current_snapshot)

        if best_snapshot is None or current_snapshot_score > best_snapshot_score:
            best_snapshot = current_snapshot
            best_snapshot_score = current_snapshot_score

        if len(matched_candidates) >= REQUIRED_MATCH_COUNT_RUNTIME:
            print_signal_found(
                current_hour=current_hour,
                ranked_candidates=ranked_candidates,
                matched_candidates=matched_candidates,
                current_bar_index=current_bar_index,
                current_bar_start_time_text=row["bar_time"],
                current_bar_start_time_text_ct=row["bar_time_ct"],
                required_correlation=REQUIRED_CORRELATION_RUNTIME,
                required_match_count=REQUIRED_MATCH_COUNT_RUNTIME,
                prepared_hours_map=context["prepared_hours_map"],
            )

            save_runtime_plot(
                current_hour_start_text=CURRENT_HOUR_START_TEXT_UTC,
                current_hour_start_text_ct=context["current_hour_start_text_ct"],
                current_full_y=context["current_full_y"],
                ranked_candidates=ranked_candidates,
                prepared_hours_map=context["prepared_hours_map"],
                current_bar_index=current_bar_index,
                required_correlation=REQUIRED_CORRELATION_RUNTIME,
                plot_candidates_count=PLOT_CANDIDATES_COUNT_RUNTIME,
                include_projected_future=include_projected_future,
            )

            signal_found = True

            if STOP_ON_FIRST_SIGNAL_RUNTIME:
                break

    if not signal_found:
        print_best_result_without_signal(
            best_snapshot=best_snapshot,
            required_correlation=REQUIRED_CORRELATION_RUNTIME,
            required_match_count=REQUIRED_MATCH_COUNT_RUNTIME,
            prepared_hours_map=context["prepared_hours_map"],
        )

        if best_snapshot is not None and best_snapshot["ranked_candidates"]:
            save_runtime_plot(
                current_hour_start_text=CURRENT_HOUR_START_TEXT_UTC,
                current_hour_start_text_ct=context["current_hour_start_text_ct"],
                current_full_y=context["current_full_y"],
                ranked_candidates=best_snapshot["ranked_candidates"],
                prepared_hours_map=context["prepared_hours_map"],
                current_bar_index=best_snapshot["bar_index"],
                required_correlation=REQUIRED_CORRELATION_RUNTIME,
                plot_candidates_count=PLOT_CANDIDATES_COUNT_RUNTIME,
                include_projected_future=include_projected_future,
            )

    print("")
    print("Готово.")


# ============================================================
# MAIN
# ============================================================

def main():
    context = load_closed_hour_context()

    print(f"РЕЖИМ: {MODE}")
    print("")

    if MODE == "candidate_features":
        run_candidate_features_mode(context)
        return

    if MODE == "weighted_similarity":
        run_weighted_similarity_mode(context)
        return

    if MODE == "similarity_forecast":
        run_similarity_forecast_mode(context)
        return

    if MODE == "decision_layer":
        run_decision_layer_mode(context)
        return

    if MODE == "pearson_runtime":
        run_pearson_runtime_mode(context, include_projected_future=False)
        return

    if MODE == "pearson_runtime_future_plot":
        run_pearson_runtime_mode(context, include_projected_future=True)
        return

    raise ValueError(f"Неподдерживаемый MODE: {MODE}")


if __name__ == "__main__":
    main()


import sqlite3
from datetime import datetime, timezone, timedelta

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_time import resolve_allowed_hour_slots
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

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Уже закрытый исторический час, который анализируем.
CURRENT_HOUR_START_TEXT_UTC = "2026-03-19 18:00:00"

# Если TARGET_BAR_INDEX задан, анализируем именно этот бар.
# Если None, ищем первый сигнал в окне 30..50 минут.
TARGET_BAR_INDEX = None

# Если TARGET_BAR_INDEX = None, ищем сигнал в этом окне.
MIN_BARS_TO_START = 360
MAX_BARS_EXCLUSIVE = 600

TOP_N = 10
REQUIRED_CORRELATION = 0.90
REQUIRED_MATCH_COUNT = 6
MIN_HISTORY_CANDIDATES = 100


def parse_utc_hour_start_text(hour_start_text):
    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"Ожидалось точное начало часа, получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def floor_to_hour_ts(ts):
    return (ts // 3600) * 3600


def format_ct_axis_ts(ts_ct):
    # Локальная числовая CT-ось проекта -> читаемый текст.
    return datetime.fromtimestamp(ts_ct, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_bar_close_time_text(bar_start_time_text):
    dt = datetime.strptime(bar_start_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return (dt + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")


def build_current_hour_y_prefix(rows, end_bar_index):
    # Строим y-подобный ряд текущего часа до end_bar_index включительно.
    if end_bar_index < 0:
        raise ValueError("end_bar_index должен быть >= 0")

    if end_bar_index >= len(rows):
        raise ValueError(
            f"end_bar_index={end_bar_index} вне диапазона rows длины {len(rows)}"
        )

    first_row = rows[0]
    mid_open_0 = (first_row["ask_open"] + first_row["bid_open"]) / 2.0

    if mid_open_0 == 0.0:
        raise ValueError("mid_open_0 == 0, деление невозможно")

    values = []

    for row in rows[: end_bar_index + 1]:
        mid_close = (row["ask_close"] + row["bid_close"]) / 2.0
        values.append((mid_close / mid_open_0) - 1.0)

    return values


def calculate_candidate_correlations(current_values, prepared_hours):
    result = []

    current_n = len(current_values)

    for item in prepared_hours:
        candidate_values = item["y"][:current_n]
        correlation = calc_pearson_corr(current_values, candidate_values)

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

    filtered = []

    for item in result:
        if item["correlation"] is None:
            continue

        filtered.append(item)

    filtered.sort(key=lambda x: x["correlation"], reverse=True)
    return filtered


def build_signal_snapshot(rows, prepared_hours):
    # Возвращаем snapshot для диагностики.
    #
    # Если TARGET_BAR_INDEX задан, берём именно его.
    # Иначе ищем первый сигнал в окне.
    if TARGET_BAR_INDEX is not None:
        current_values = build_current_hour_y_prefix(rows, TARGET_BAR_INDEX)
        ranked = calculate_candidate_correlations(current_values, prepared_hours)
        matched = [item for item in ranked[:TOP_N] if item["correlation"] >= REQUIRED_CORRELATION]

        return {
            "bar_index": TARGET_BAR_INDEX,
            "current_values": current_values,
            "ranked_candidates": ranked[:TOP_N],
            "matched_candidates": matched,
            "signal_found": len(matched) >= REQUIRED_MATCH_COUNT,
        }

    best_snapshot = None

    for bar_index in range(MIN_BARS_TO_START - 1, min(MAX_BARS_EXCLUSIVE, len(rows))):
        current_values = build_current_hour_y_prefix(rows, bar_index)
        ranked = calculate_candidate_correlations(current_values, prepared_hours)
        top_ranked = ranked[:TOP_N]
        matched = [item for item in top_ranked if item["correlation"] >= REQUIRED_CORRELATION]
        signal_found = len(matched) >= REQUIRED_MATCH_COUNT

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


def calc_feature_pack(values):
    diff_values = build_first_diff(values)

    return {
        "range": calc_range(values),
        "net_move": calc_net_move(values),
        "mean_abs_diff": calc_mean_abs_diff(diff_values),
        "path_efficiency": calc_path_efficiency(values),
    }


def calc_feature_distance(current_metric, candidate_metric):
    # Простой относительный разрыв. Для нуля — только абсолютная разница.
    abs_diff = abs(current_metric - candidate_metric)

    if current_metric == 0.0 and candidate_metric == 0.0:
        return 0.0

    base = max(abs(current_metric), abs(candidate_metric))

    if base == 0.0:
        return abs_diff

    return abs_diff / base


def pct_text(value):
    return f"{value * 100:+.4f}%"


def ratio_text(value):
    return f"{value:.4f}"


def print_current_hour_summary(rows, snapshot, current_hour_start_ts_ct):
    first_row = rows[0]
    current_feature_pack = calc_feature_pack(snapshot["current_values"])

    print("ХАРАКТЕРИСТИКИ ТЕКУЩЕГО ЧАСА:")
    print(f"  Начало часа UTC: {CURRENT_HOUR_START_TEXT_UTC}")
    print(f"  Начало часа CT:  {format_ct_axis_ts(current_hour_start_ts_ct)} CT")
    print(f"  CT-слот часа:    {(current_hour_start_ts_ct // 3600) % 24}")
    print(f"  Индекс текущего бара: {snapshot['bar_index']}")
    print(f"  Начало бара UTC:      {rows[snapshot['bar_index']]['bar_time']} UTC")
    print(f"  Закрытие бара UTC:    {get_bar_close_time_text(rows[snapshot['bar_index']]['bar_time'])} UTC")
    print(f"  Начало бара CT:       {rows[snapshot['bar_index']]['bar_time_ct']} CT")
    print(f"  Закрытие бара CT:     {get_bar_close_time_text(rows[snapshot['bar_index']]['bar_time_ct'])} CT")
    print(f"  Длина префикса:       {len(snapshot['current_values'])}")
    print(f"  Диапазон участка:     {pct_text(current_feature_pack['range'])}")
    print(f"  Итоговое смещение:    {pct_text(current_feature_pack['net_move'])}")
    print(f"  Средний шаг:         {pct_text(current_feature_pack['mean_abs_diff'])}")
    print(f"  Эффективность пути:   {safe_ratio_text(current_feature_pack['path_efficiency'])}")
    print("")
    return current_feature_pack



def safe_ratio_text(value):
    if value is None:
        return "нет"
    return f"{value:.4f}"


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

        # Простая итоговая метрика второго этапа.
        # Чем меньше суммарное отклонение, тем кандидат ближе к текущему часу.
        total_feature_distance = (
            range_distance
            + net_move_distance
            + mean_abs_diff_distance
            + path_efficiency_distance
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


def print_final_summary(snapshot, candidate_feature_rows):
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

def main():
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

        current_hour_rows = load_price_rows_for_one_hour(
            price_conn=price_conn,
            table_name=table_name,
            hour_start_ts=current_hour_start_ts,
        )

        validate_price_rows(
            rows=current_hour_rows,
            hour_start_ts=current_hour_start_ts,
        )

        current_hour_start_ts_ct = floor_to_hour_ts(current_hour_rows[0]["bar_time_ts_ct"])
        current_hour_slot_ct = (current_hour_start_ts_ct // 3600) % 24
        allowed_hour_slots_ct = resolve_allowed_hour_slots(current_hour_slot_ct)

        prepared_hours = load_prepared_hours_by_slots(
            prepared_conn=prepared_conn,
            table_name=table_name,
            hour_slots_ct=allowed_hour_slots_ct,
            before_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        if len(prepared_hours) < MIN_HISTORY_CANDIDATES:
            raise ValueError(
                f"Слишком мало prepared-кандидатов для анализа: "
                f"{len(prepared_hours)} < {MIN_HISTORY_CANDIDATES}"
            )

        snapshot = build_signal_snapshot(
            rows=current_hour_rows,
            prepared_hours=prepared_hours,
        )

        if snapshot is None:
            raise ValueError("Не удалось построить snapshot для анализа")

        print(f"ИНСТРУМЕНТ: {INSTRUMENT_CODE}")
        print(f"Таблица: {table_name}")
        print(f"Начало текущего часа UTC:          {CURRENT_HOUR_START_TEXT_UTC}")
        print(f"Начало текущего часа CT:           {format_ct_axis_ts(current_hour_start_ts_ct)} CT")
        print(f"Текущий CT-слот:                   {current_hour_slot_ct}")
        print(f"Разрешённые CT-слоты для поиска:   {allowed_hour_slots_ct}")
        print(f"Исторических prepared-кандидатов:  {len(prepared_hours)}")
        print(f"TOP_N:                             {TOP_N}")
        print(f"Минимальная корреляция:            {REQUIRED_CORRELATION}")
        print(f"Минимум кандидатов по порогу:      {REQUIRED_MATCH_COUNT}")
        print(f"Сигнал по исходному Пирсону:       {'ДА' if snapshot['signal_found'] else 'НЕТ'}")
        print("")

        current_feature_pack = print_current_hour_summary(
            rows=current_hour_rows,
            snapshot=snapshot,
            current_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        candidate_feature_rows = print_candidates_summary(
            snapshot=snapshot,
            current_feature_pack=current_feature_pack,
        )

        print_final_summary(
            snapshot=snapshot,
            candidate_feature_rows=candidate_feature_rows,
        )

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()
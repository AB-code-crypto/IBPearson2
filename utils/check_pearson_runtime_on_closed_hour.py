'''
 исследовательский скрипт для прогона уже закрытого исторического часа как будто в realtime. Накапливает текущий час бар за баром, запускает Пирсона после
 заданного порога, строит ranking кандидатов, отбирает совпавшие по корреляции и печатает диагностические блоки: что было дальше у кандидатов,
 агрегаты по группе, предварительное направление и score качества сигнала.

 Дополнительно сохраняет PNG-график:
 - на одном графике показывается рассматриваемый текущий час;
 - поверх него накладываются лучшие найденные кандидаты;
 - вертикальная линия отмечает бар, на котором был получен сигнал
   или лучший достигнутый результат, если сигнала не было.
'''
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import (
    load_price_rows_for_one_hour,
    validate_price_rows,
)
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.pearson_runtime import PearsonCurrentHour
from ts.ts_time import resolve_allowed_hour_slots

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Исторический уже закрытый час, который хотим прогнать как будто в realtime.
CURRENT_HOUR_START_TEXT = "2026-03-19 18:00:00"

# После 360 баров (30 минут) начинаем сравнение.
MIN_BARS_TO_START = 360

# Сколько лучших кандидатов печатать в момент сигнала
# или в лучшем достигнутом результате, если сигнала не было.
TOP_N = 5

# Сколько кандидатов рисовать на PNG-графике.
# Берём первых кандидатов из уже построенного ranking.
PLOT_CANDIDATES_COUNT = 5

# Куда сохранять PNG-файл.
OUTPUT_DIR = Path("./png/")

# Если None - идём до конца часа.
STOP_AFTER_BAR_INDEX = None

# Как только не меньше REQUIRED_MATCH_COUNT кандидатов из TOP_N
# достигли REQUIRED_CORRELATION, считаем, что найден рабочий сигнал.
REQUIRED_CORRELATION = 0.8
REQUIRED_MATCH_COUNT = 1

# Минимально допустимое число исторических prepared-кандидатов.
MIN_HISTORY_CANDIDATES = 100

# Если True - останавливаемся на первом найденном сигнале.
# Если False - продолжаем идти дальше по часу и печатаем каждый найденный сигнал.
STOP_ON_FIRST_SIGNAL = True


def parse_utc_hour_start_text(hour_start_text):
    # Преобразуем текст "YYYY-MM-DD HH:MM:SS" в UTC timestamp начала часа.
    dt = datetime.strptime(hour_start_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)

    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError(
            f"CURRENT_HOUR_START_TEXT должен указывать ровно на начало часа, "
            f"получено: {hour_start_text}"
        )

    return int(dt.timestamp())


def format_ct_axis_ts(ts_ct):
    # Преобразуем timestamp локальной CT-оси проекта в читаемую строку.
    return datetime.fromtimestamp(ts_ct, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_bar_close_time_text_ct(bar_start_time_text_ct):
    # В таблице price DB bar_time_ct - это время начала 5-секундного бара на CT-оси.
    dt = datetime.strptime(bar_start_time_text_ct, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    close_dt = dt + timedelta(seconds=5)
    return close_dt.strftime("%Y-%m-%d %H:%M:%S")


def get_bar_close_time_text(bar_start_time_text):
    # В таблице price DB bar_time - это время начала 5-секундного бара.
    # Для сигнала удобнее печатать фактическое время закрытия этого бара.
    dt = datetime.strptime(bar_start_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    close_dt = dt + timedelta(seconds=5)
    return close_dt.strftime("%Y-%m-%d %H:%M:%S")


def get_candidates_above_correlation(ranked_candidates, required_correlation):
    # Возвращаем только тех кандидатов, которые прошли заданный порог корреляции.
    matched = []

    for item in ranked_candidates:
        if item["correlation"] >= required_correlation:
            matched.append(item)

    return matched


def build_prepared_hours_map(prepared_hours):
    # Быстрый доступ к prepared-часу по hour_start_ts.
    result = {}

    for item in prepared_hours:
        result[item["hour_start_ts"]] = item

    return result


def build_forward_stats(prepared_hour_payload, current_bar_index):
    # Считаем, что было дальше у исторического кандидата
    # после текущего сигнального бара и до конца часа.
    #
    # Возвращаем:
    # - bars_left
    # - forward_to_end
    # - max_up_after_signal
    # - max_down_after_signal
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
    # Агрегированная сводка по тому, что было дальше у matched-кандидатов.
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

    result = {
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

    return result


def build_group_direction_decision(aggregate):
    # Первичная интерпретация направления по matched-группе.
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
    # Строим простой и прозрачный score качества сигнала в диапазоне 0..100.
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
    # Печатаем весь TOP_N список.
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
    # Печатаем, что происходило дальше у кандидатов, которые прошли порог.
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
    # Печатаем агрегированную сводку по matched-кандидатам.
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
    # Печатаем первичную интерпретацию направления группы.
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
    # Печатаем числовую оценку качества сигнала.
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


def build_full_current_hour_y(current_hour_rows, current_hour_start_ts, current_hour_start_ts_ct):
    # Строим полный y-ряд для рассматриваемого текущего часа по всей истории часа.
    runtime = PearsonCurrentHour(
        hour_start_ts=current_hour_start_ts,
        hour_start_ts_ct=current_hour_start_ts_ct,
    )

    for row in current_hour_rows:
        runtime.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

    return list(runtime.x)


def build_plot_file_path(current_hour_start_text, bar_index, plot_kind):
    # Формируем понятное имя PNG-файла.
    safe_hour_text = current_hour_start_text.replace(":", "-").replace(" ", "_")
    file_name = f"pearson_{plot_kind}_{safe_hour_text}_bar_{bar_index:03d}.png"
    return OUTPUT_DIR / file_name


def save_candidates_plot(
        current_hour_start_text,
        current_hour_start_text_ct,
        current_full_y,
        ranked_candidates,
        prepared_hours_map,
        current_bar_index,
        required_correlation,
        plot_candidates_count,
        output_path,
):
    # Сохраняем PNG-график:
    # - весь рассматриваемый текущий час;
    # - несколько лучших найденных кандидатов;
    # - вертикальная линия в точке сигнала / лучшего снимка.
    candidates_to_plot = ranked_candidates[:plot_candidates_count]

    if not candidates_to_plot:
        print(f"PNG-график не сохранён: нет кандидатов для рисования.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    x_values = list(range(len(current_full_y)))
    current_full_y_pct = [value * 100.0 for value in current_full_y]

    plt.figure(figsize=(16, 9))
    plt.plot(
        x_values,
        current_full_y_pct,
        linewidth=2.5,
        label=f"CURRENT UTC {current_hour_start_text} | CT {current_hour_start_text_ct}"
    )

    for rank, item in enumerate(candidates_to_plot, start=1):
        candidate_payload = prepared_hours_map[item["hour_start_ts"]]
        candidate_y_pct = [value * 100.0 for value in candidate_payload["y"]]
        passed = item["correlation"] >= required_correlation
        passed_mark = "*" if passed else ""

        plt.plot(
            x_values,
            candidate_y_pct,
            linewidth=1.2,
            alpha=0.85,
            label=(
                f"{rank}. {passed_mark}{item['hour_start_ct']} CT | "
                f"corr={item['correlation']:.4f}"
            ),
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
    # Печатаем момент, когда найден рабочий сигнал по Пирсону.
    current_bar_close_time_text = get_bar_close_time_text(current_bar_start_time_text)
    current_bar_close_time_text_ct = get_bar_close_time_text_ct(current_bar_start_time_text_ct)

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
    # Если сигнал за час не найден, печатаем лучший достигнутый результат.
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
    # Выбираем "лучший достигнутый результат за час" по простой логике:
    # 1) чем больше matched_count - тем лучше
    # 2) при равенстве - чем выше лучшая корреляция, тем лучше
    ranked_candidates = snapshot["ranked_candidates"]
    matched_candidates = snapshot["matched_candidates"]

    matched_count = len(matched_candidates)

    if ranked_candidates:
        best_correlation = ranked_candidates[0]["correlation"]
    else:
        best_correlation = float("-inf")

    return matched_count, best_correlation


def main():
    if REQUIRED_MATCH_COUNT < 1:
        raise ValueError("REQUIRED_MATCH_COUNT должен быть >= 1")

    if PLOT_CANDIDATES_COUNT < 1:
        raise ValueError("PLOT_CANDIDATES_COUNT должен быть >= 1")

    instrument_row = Instrument[INSTRUMENT_CODE]

    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    current_hour_start_ts = parse_utc_hour_start_text(CURRENT_HOUR_START_TEXT)

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

        allowed_hour_slots = resolve_allowed_hour_slots(current_hour_slot_ct)

        prepared_hours = load_prepared_hours_by_slots(
            prepared_conn=prepared_conn,
            table_name=table_name,
            hour_slots_ct=allowed_hour_slots,
            before_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        if not prepared_hours:
            raise ValueError(
                f"Не найдено ни одного исторического prepared-часа "
                f"для группы hour_slot_ct={allowed_hour_slots}"
            )

        if len(prepared_hours) < MIN_HISTORY_CANDIDATES:
            raise ValueError(
                f"Исторических prepared-кандидатов слишком мало: {len(prepared_hours)}. "
                f"Минимально требуется: {MIN_HISTORY_CANDIDATES}"
            )

        prepared_hours_map = build_prepared_hours_map(prepared_hours)
        current_full_y = build_full_current_hour_y(
            current_hour_rows=current_hour_rows,
            current_hour_start_ts=current_hour_start_ts,
            current_hour_start_ts_ct=current_hour_start_ts_ct,
        )

        current_hour = PearsonCurrentHour(
            hour_start_ts=current_hour_start_ts,
            hour_start_ts_ct=current_hour_start_ts_ct,
        )
        current_hour.set_candidates(prepared_hours)

        signal_found = False
        best_snapshot = None
        best_snapshot_score = None

        for row in current_hour_rows:
            current_bar_index = len(current_hour.x)

            if STOP_AFTER_BAR_INDEX is not None and current_bar_index > STOP_AFTER_BAR_INDEX:
                break

            current_hour.add_bar(
                ask_open=row["ask_open"],
                bid_open=row["bid_open"],
                ask_close=row["ask_close"],
                bid_close=row["bid_close"],
            )

            if not current_hour.can_start_comparison(min_bars=MIN_BARS_TO_START):
                continue

            if not current_hour.candidates_initialized:
                current_hour.initialize_candidates()
            else:
                current_hour.update_candidates_for_last_bar()

            ranked_candidates = current_hour.get_ranked_candidates(
                min_correlation=None,
                top_n=TOP_N,
            )

            if not ranked_candidates:
                continue

            matched_candidates = get_candidates_above_correlation(
                ranked_candidates=ranked_candidates,
                required_correlation=REQUIRED_CORRELATION,
            )

            current_snapshot = {
                "bar_index": current_bar_index,
                "bar_start_time_text": row["bar_time"],
                "bar_start_time_text_ct": row["bar_time_ct"],
                "bar_close_time_text": get_bar_close_time_text(row["bar_time"]),
                "bar_close_time_text_ct": get_bar_close_time_text_ct(row["bar_time_ct"]),
                "ranked_candidates": ranked_candidates,
                "matched_candidates": matched_candidates,
            }

            current_snapshot_score = build_best_snapshot_score(current_snapshot)

            if best_snapshot is None or current_snapshot_score > best_snapshot_score:
                best_snapshot = current_snapshot
                best_snapshot_score = current_snapshot_score

            if len(matched_candidates) >= REQUIRED_MATCH_COUNT:
                print_signal_found(
                    current_hour=current_hour,
                    ranked_candidates=ranked_candidates,
                    matched_candidates=matched_candidates,
                    current_bar_index=current_bar_index,
                    current_bar_start_time_text=row["bar_time"],
                    current_bar_start_time_text_ct=row["bar_time_ct"],
                    required_correlation=REQUIRED_CORRELATION,
                    required_match_count=REQUIRED_MATCH_COUNT,
                    prepared_hours_map=prepared_hours_map,
                )

                plot_path = build_plot_file_path(
                    current_hour_start_text=CURRENT_HOUR_START_TEXT,
                    bar_index=current_bar_index,
                    plot_kind="signal",
                )
                save_candidates_plot(
                    current_hour_start_text=CURRENT_HOUR_START_TEXT,
                    current_hour_start_text_ct=current_hour_start_text_ct,
                    current_full_y=current_full_y,
                    ranked_candidates=ranked_candidates,
                    prepared_hours_map=prepared_hours_map,
                    current_bar_index=current_bar_index,
                    required_correlation=REQUIRED_CORRELATION,
                    plot_candidates_count=PLOT_CANDIDATES_COUNT,
                    output_path=plot_path,
                )

                signal_found = True

                if STOP_ON_FIRST_SIGNAL:
                    break

        if not signal_found:
            print_best_result_without_signal(
                best_snapshot=best_snapshot,
                required_correlation=REQUIRED_CORRELATION,
                required_match_count=REQUIRED_MATCH_COUNT,
                prepared_hours_map=prepared_hours_map,
            )

            if best_snapshot is not None and best_snapshot["ranked_candidates"]:
                plot_path = build_plot_file_path(
                    current_hour_start_text=CURRENT_HOUR_START_TEXT,
                    bar_index=best_snapshot["bar_index"],
                    plot_kind="best_snapshot",
                )
                save_candidates_plot(
                    current_hour_start_text=CURRENT_HOUR_START_TEXT,
                    current_hour_start_text_ct=current_hour_start_text_ct,
                    current_full_y=current_full_y,
                    ranked_candidates=best_snapshot["ranked_candidates"],
                    prepared_hours_map=prepared_hours_map,
                    current_bar_index=best_snapshot["bar_index"],
                    required_correlation=REQUIRED_CORRELATION,
                    plot_candidates_count=PLOT_CANDIDATES_COUNT,
                    output_path=plot_path,
                )

        print("")
        print("Готово.")

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()

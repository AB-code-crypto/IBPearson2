"""
Простой оффлайн-тестер первого шага стратегии с CT-логикой.

Логика:
1. Берём тестовый диапазон часов [TEST_START_HOUR_TEXT_UTC, TEST_END_HOUR_TEXT_UTC).
2. Для каждого тестового часа:
   - определяем его CT-час по bar_time_ts_ct первого бара;
   - через resolve_allowed_hour_slots(...) получаем допустимые CT-slot;
   - берём из prepared DB все historical candidates этих CT-slot
     строго раньше текущего тестового часа по CT-оси.
3. Идём по текущему часу бар за баром как будто в realtime.
4. В окне 30..50 минут ищем TOP_N кандидатов.
5. Если не меньше REQUIRED_MATCH_COUNT кандидатов из TOP_N имеют
   correlation >= REQUIRED_CORRELATION, строим средний future-path
   по matched-кандидатам.
6. Если средний future-path к концу часа > 0, входим LONG.
   Если < 0, входим SHORT.
7. Входим один раз на час, по первому найденному сигналу.
8. Выходим за 10 секунд до конца часа.
9. Результат считаем в пунктах и в деньгах для 1 контракта.

Это пробный и намеренно простой тестер:
- без проскальзывания;
- без повторных входов;
- без риск-менеджмента;
- без усложнений на будущее.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from math import sqrt
from statistics import median

from config import settings_live as settings
from contracts import Instrument
from core.db_initializer import build_table_name
from ts.prepared_builder import load_price_rows_for_one_hour, validate_price_rows
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_config import pearson_eval_start_bar_count, pearson_eval_end_bar_count_exclusive
from ts.ts_time import resolve_allowed_hour_slots

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Тестируем часы в диапазоне [start, end) по UTC-ключу price DB.
TEST_START_HOUR_TEXT_UTC = "2026-01-01 00:00:00"
TEST_END_HOUR_TEXT_UTC = "2026-03-23 00:00:00"

# Для отбора сигнала.
TOP_N = 10
REQUIRED_CORRELATION = 0.80
REQUIRED_MATCH_COUNT = 6
MIN_HISTORY_CANDIDATES = 30

# Вход и выход.
ENTRY_START_BAR_COUNT = pearson_eval_start_bar_count()
ENTRY_END_BAR_COUNT_EXCLUSIVE = pearson_eval_end_bar_count_exclusive()

# Выход за 10 секунд до конца часа.
EXIT_CLOSE_OFFSET_SECONDS = 10

# Печать подробностей по каждой сделке.
PRINT_TRADES = True

# Для MNQ берём multiplier из contracts.py.
MNQ_POINT_VALUE_USD = float(Instrument[INSTRUMENT_CODE]["multiplier"])

# Комиссия на 1 сторону сделки для 1 контракта в USD.
COMMISSION_PER_SIDE_USD = 0.62


@dataclass
class TradeResult:
    hour_start_ts: int
    hour_start_utc: str
    hour_start_ts_ct: int
    hour_start_ct: str
    hour_slot_ct: int

    entry_bar_index: int
    entry_bar_time_utc: str
    entry_bar_close_time_utc: str
    entry_bar_time_ct: str
    entry_bar_close_time_ct: str
    entry_direction: str
    entry_price: float

    exit_bar_index: int
    exit_bar_time_utc: str
    exit_bar_close_time_utc: str
    exit_bar_time_ct: str
    exit_bar_close_time_ct: str
    exit_price: float

    matched_count: int
    best_correlation: float
    avg_future_to_end: float

    points_result: float
    gross_usd_result: float
    commissions_usd: float
    net_usd_result: float


@dataclass
class TesterStats:
    tested_hours: int
    valid_hours: int
    skipped_invalid_hours: int
    skipped_small_history: int
    no_signal_hours: int
    traded_hours: int
    long_trades: int
    short_trades: int
    win_trades: int
    loss_trades: int
    flat_trades: int

    total_points: float
    avg_points: float
    median_points: float
    best_trade_points: float | None
    worst_trade_points: float | None

    total_gross_usd: float
    total_commissions_usd: float
    total_net_usd: float
    avg_net_usd: float
    median_net_usd: float
    best_trade_net_usd: float | None
    worst_trade_net_usd: float | None


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

        if left <= 0.0:
            self.last_correlation = None
            return None

        if right <= 0.0:
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
        self.candidates = []

        for prepared_hour_payload in prepared_hours:
            self.candidates.append(SimplePearsonCandidate(prepared_hour_payload))

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

        if not self.x:
            raise ValueError("Текущий x пустой")

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


def hour_start_text_from_ts(hour_start_ts):
    return datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_ct_axis_ts(ts_ct):
    # ts_ct - это локальная числовая CT-ось проекта.
    return datetime.fromtimestamp(ts_ct, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_bar_close_time_text(bar_start_time_text):
    dt = datetime.strptime(bar_start_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    close_dt = dt + timedelta(seconds=5)
    return close_dt.strftime("%Y-%m-%d %H:%M:%S")


def get_mid_close(row):
    return (row["ask_close"] + row["bid_close"]) / 2.0


def points_to_usd(points_result):
    return points_result * MNQ_POINT_VALUE_USD


def get_round_turn_commissions_usd():
    return COMMISSION_PER_SIDE_USD * 2.0


def build_prepared_hours_map(prepared_hours):
    result = {}

    for item in prepared_hours:
        result[item["hour_start_ts"]] = item

    return result


def get_candidates_above_correlation(ranked_candidates, required_correlation):
    matched = []

    for item in ranked_candidates:
        if item["correlation"] >= required_correlation:
            matched.append(item)

    return matched


def build_average_future_path(matched_candidates, prepared_hours_map, current_bar_index):
    if not matched_candidates:
        return []

    horizon_length = None
    future_paths = []

    for item in matched_candidates:
        prepared_hour = prepared_hours_map[item["hour_start_ts"]]
        y = prepared_hour["y"]

        if current_bar_index < 0 or current_bar_index >= len(y):
            raise ValueError(
                f"Некорректный current_bar_index={current_bar_index} для prepared hour длины {len(y)}"
            )

        current_rel = 1.0 + y[current_bar_index]
        future_y = y[current_bar_index + 1:]

        path = []

        for future_value in future_y:
            future_rel = 1.0 + future_value
            rel_move = (future_rel / current_rel) - 1.0
            path.append(rel_move)

        if horizon_length is None:
            horizon_length = len(path)
        elif len(path) != horizon_length:
            raise ValueError("У matched-кандидатов оказалась разная длина future-path")

        future_paths.append(path)

    if horizon_length is None or horizon_length == 0:
        return []

    avg_path = []

    for index in range(horizon_length):
        value_sum = 0.0

        for path in future_paths:
            value_sum += path[index]

        avg_path.append(value_sum / len(future_paths))

    return avg_path


def build_trade_direction_from_average_future_path(avg_future_path):
    if not avg_future_path:
        return None, 0.0

    avg_future_to_end = avg_future_path[-1]

    if avg_future_to_end > 0.0:
        return "LONG", avg_future_to_end

    if avg_future_to_end < 0.0:
        return "SHORT", avg_future_to_end

    return None, 0.0


def get_exit_bar_index():
    close_time_seconds = 3600 - EXIT_CLOSE_OFFSET_SECONDS
    bar_start_seconds = close_time_seconds - 5

    if bar_start_seconds < 0 or bar_start_seconds % 5 != 0:
        raise ValueError("Некорректный EXIT_CLOSE_OFFSET_SECONDS для 5-секундных баров")

    return bar_start_seconds // 5


def load_test_hour_starts(price_conn, table_name, test_start_ts, test_end_ts):
    sql = f"""
    SELECT DISTINCT
        CAST(bar_time_ts / 3600 AS INTEGER) * 3600 AS hour_start_ts
    FROM {table_name}
    WHERE bar_time_ts >= ?
      AND bar_time_ts < ?
    ORDER BY hour_start_ts
    ;
    """

    cursor = price_conn.execute(sql, (test_start_ts, test_end_ts))
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def load_history_candidates_with_cache(prepared_conn, table_name, cache, current_hour_slot_ct, current_hour_start_ts_ct):
    allowed_hour_slots_ct = resolve_allowed_hour_slots(current_hour_slot_ct)
    cache_key = tuple(allowed_hour_slots_ct)

    if cache_key not in cache:
        all_group_hours = load_prepared_hours_by_slots(
            prepared_conn=prepared_conn,
            table_name=table_name,
            hour_slots_ct=allowed_hour_slots_ct,
            before_hour_start_ts_ct=None,
        )
        cache[cache_key] = all_group_hours

    group_hours = cache[cache_key]

    filtered_hours = []

    for item in group_hours:
        if item["hour_start_ts_ct"] < current_hour_start_ts_ct:
            filtered_hours.append(item)

    return filtered_hours, allowed_hour_slots_ct


def run_one_test_hour(price_conn, prepared_conn, table_name, hour_start_ts, candidate_cache):
    rows = load_price_rows_for_one_hour(
        price_conn=price_conn,
        table_name=table_name,
        hour_start_ts=hour_start_ts,
    )

    validate_price_rows(rows=rows, hour_start_ts=hour_start_ts)

    if not rows:
        raise ValueError("Тестовый час пустой")

    current_hour_start_ts_ct = floor_to_hour_ts(rows[0]["bar_time_ts_ct"])
    current_hour_start_ct = format_ct_axis_ts(current_hour_start_ts_ct)

    current_hour = SimplePearsonCurrentHour(
        hour_start_ts=hour_start_ts,
        hour_start_ts_ct=current_hour_start_ts_ct,
        hour_start_ct=current_hour_start_ct,
    )

    prepared_hours, allowed_hour_slots_ct = load_history_candidates_with_cache(
        prepared_conn=prepared_conn,
        table_name=table_name,
        cache=candidate_cache,
        current_hour_slot_ct=current_hour.hour_slot_ct,
        current_hour_start_ts_ct=current_hour_start_ts_ct,
    )

    if len(prepared_hours) < MIN_HISTORY_CANDIDATES:
        return {
            "status": "small_history",
            "history_candidate_count": len(prepared_hours),
            "allowed_hour_slots_ct": allowed_hour_slots_ct,
            "hour_start_ct": current_hour.hour_start_ct,
        }

    prepared_hours_map = build_prepared_hours_map(prepared_hours)
    current_hour.set_candidates(prepared_hours)

    exit_bar_index = get_exit_bar_index()

    for row in rows:
        current_hour.add_bar(
            ask_open=row["ask_open"],
            bid_open=row["bid_open"],
            ask_close=row["ask_close"],
            bid_close=row["bid_close"],
        )

        current_bar_index = current_hour.current_bar_index()
        current_bar_count = current_hour.current_n()

        if current_bar_count < ENTRY_START_BAR_COUNT:
            continue

        if current_bar_count >= ENTRY_END_BAR_COUNT_EXCLUSIVE:
            break

        if not current_hour.candidates_initialized:
            current_hour.initialize_candidates()
        else:
            current_hour.update_candidates_for_last_bar()

        ranked_candidates = current_hour.get_ranked_candidates(
            min_correlation=None,
            top_n=TOP_N,
        )

        matched_candidates = get_candidates_above_correlation(
            ranked_candidates=ranked_candidates,
            required_correlation=REQUIRED_CORRELATION,
        )

        if len(matched_candidates) < REQUIRED_MATCH_COUNT:
            continue

        avg_future_path = build_average_future_path(
            matched_candidates=matched_candidates,
            prepared_hours_map=prepared_hours_map,
            current_bar_index=current_bar_index,
        )

        direction, avg_future_to_end = build_trade_direction_from_average_future_path(avg_future_path)

        if direction is None:
            continue

        entry_price = get_mid_close(row)
        exit_row = rows[exit_bar_index]
        exit_price = get_mid_close(exit_row)

        if direction == "LONG":
            points_result = exit_price - entry_price
        elif direction == "SHORT":
            points_result = entry_price - exit_price
        else:
            raise ValueError(f"Неизвестное направление: {direction}")

        gross_usd_result = points_to_usd(points_result)
        commissions_usd = get_round_turn_commissions_usd()
        net_usd_result = gross_usd_result - commissions_usd

        best_correlation = matched_candidates[0]["correlation"]

        return {
            "status": "trade",
            "trade": TradeResult(
                hour_start_ts=hour_start_ts,
                hour_start_utc=hour_start_text_from_ts(hour_start_ts),
                hour_start_ts_ct=current_hour.hour_start_ts_ct,
                hour_start_ct=current_hour.hour_start_ct,
                hour_slot_ct=current_hour.hour_slot_ct,
                entry_bar_index=current_bar_index,
                entry_bar_time_utc=row["bar_time"],
                entry_bar_close_time_utc=get_bar_close_time_text(row["bar_time"]),
                entry_bar_time_ct=row["bar_time_ct"],
                entry_bar_close_time_ct=get_bar_close_time_text(row["bar_time_ct"]),
                entry_direction=direction,
                entry_price=entry_price,
                exit_bar_index=exit_bar_index,
                exit_bar_time_utc=exit_row["bar_time"],
                exit_bar_close_time_utc=get_bar_close_time_text(exit_row["bar_time"]),
                exit_bar_time_ct=exit_row["bar_time_ct"],
                exit_bar_close_time_ct=get_bar_close_time_text(exit_row["bar_time_ct"]),
                exit_price=exit_price,
                matched_count=len(matched_candidates),
                best_correlation=best_correlation,
                avg_future_to_end=avg_future_to_end,
                points_result=points_result,
                gross_usd_result=gross_usd_result,
                commissions_usd=commissions_usd,
                net_usd_result=net_usd_result,
            ),
        }

    return {
        "status": "no_signal",
        "hour_start_ct": current_hour.hour_start_ct,
    }


def build_tester_stats(results):
    tested_hours = 0
    valid_hours = 0
    skipped_invalid_hours = 0
    skipped_small_history = 0
    no_signal_hours = 0

    trades = []

    for item in results:
        tested_hours += 1

        if item["status"] == "invalid_hour":
            skipped_invalid_hours += 1
            continue

        valid_hours += 1

        if item["status"] == "small_history":
            skipped_small_history += 1
            continue

        if item["status"] == "no_signal":
            no_signal_hours += 1
            continue

        if item["status"] == "trade":
            trades.append(item["trade"])
            continue

        raise ValueError(f"Неизвестный status: {item['status']}")

    long_trades = sum(1 for trade in trades if trade.entry_direction == "LONG")
    short_trades = sum(1 for trade in trades if trade.entry_direction == "SHORT")

    win_trades = sum(1 for trade in trades if trade.points_result > 0.0)
    loss_trades = sum(1 for trade in trades if trade.points_result < 0.0)
    flat_trades = sum(1 for trade in trades if trade.points_result == 0.0)

    points_values = [trade.points_result for trade in trades]
    total_points = sum(points_values)
    avg_points = total_points / len(points_values) if points_values else 0.0
    median_points = median(points_values) if points_values else 0.0
    best_trade_points = max(points_values) if points_values else None
    worst_trade_points = min(points_values) if points_values else None

    gross_usd_values = [trade.gross_usd_result for trade in trades]
    commissions_usd_values = [trade.commissions_usd for trade in trades]
    net_usd_values = [trade.net_usd_result for trade in trades]

    total_gross_usd = sum(gross_usd_values)
    total_commissions_usd = sum(commissions_usd_values)
    total_net_usd = sum(net_usd_values)
    avg_net_usd = total_net_usd / len(net_usd_values) if net_usd_values else 0.0
    median_net_usd = median(net_usd_values) if net_usd_values else 0.0
    best_trade_net_usd = max(net_usd_values) if net_usd_values else None
    worst_trade_net_usd = min(net_usd_values) if net_usd_values else None

    return TesterStats(
        tested_hours=tested_hours,
        valid_hours=valid_hours,
        skipped_invalid_hours=skipped_invalid_hours,
        skipped_small_history=skipped_small_history,
        no_signal_hours=no_signal_hours,
        traded_hours=len(trades),
        long_trades=long_trades,
        short_trades=short_trades,
        win_trades=win_trades,
        loss_trades=loss_trades,
        flat_trades=flat_trades,
        total_points=total_points,
        avg_points=avg_points,
        median_points=median_points,
        best_trade_points=best_trade_points,
        worst_trade_points=worst_trade_points,
        total_gross_usd=total_gross_usd,
        total_commissions_usd=total_commissions_usd,
        total_net_usd=total_net_usd,
        avg_net_usd=avg_net_usd,
        median_net_usd=median_net_usd,
        best_trade_net_usd=best_trade_net_usd,
        worst_trade_net_usd=worst_trade_net_usd,
    )


def print_trade(trade):
    print(
        f"{trade.hour_start_utc} UTC | {trade.hour_start_ct} CT | "
        f"slot_ct={trade.hour_slot_ct} | "
        f"{trade.entry_direction:<5} | "
        f"вход={trade.entry_bar_close_time_utc} UTC / {trade.entry_bar_close_time_ct} CT "
        f"(bar_index={trade.entry_bar_index}) | "
        f"выход={trade.exit_bar_close_time_utc} UTC / {trade.exit_bar_close_time_ct} CT "
        f"(bar_index={trade.exit_bar_index}) | "
        f"matched={trade.matched_count} | "
        f"best_corr={trade.best_correlation:.6f} | "
        f"avg_future_to_end={trade.avg_future_to_end * 100:+.4f}% | "
        f"points={trade.points_result:+.2f} | "
        f"gross_usd={trade.gross_usd_result:+.2f} | "
        f"comm={trade.commissions_usd:+.2f} | "
        f"net_usd={trade.net_usd_result:+.2f}"
    )


def print_summary(stats):
    print("")
    print("ИТОГИ ТЕСТА:")
    print(f"  всего тестовых часов:              {stats.tested_hours}")
    print(f"  валидных часов:                    {stats.valid_hours}")
    print(f"  пропущено невалидных часов:        {stats.skipped_invalid_hours}")
    print(f"  пропущено из-за малой истории:     {stats.skipped_small_history}")
    print(f"  часов без сигнала:                 {stats.no_signal_hours}")
    print(f"  часов со сделкой:                  {stats.traded_hours}")
    print(f"  long-сделок:                       {stats.long_trades}")
    print(f"  short-сделок:                      {stats.short_trades}")
    print(f"  прибыльных сделок:                 {stats.win_trades}")
    print(f"  убыточных сделок:                  {stats.loss_trades}")
    print(f"  нулевых сделок:                    {stats.flat_trades}")
    print(f"  total points:                      {stats.total_points:+.2f}")
    print(f"  avg points:                        {stats.avg_points:+.2f}")
    print(f"  median points:                     {stats.median_points:+.2f}")
    print(f"  total gross usd:                   {stats.total_gross_usd:+.2f}")
    print(f"  total commissions usd:             {stats.total_commissions_usd:+.2f}")
    print(f"  total net usd:                     {stats.total_net_usd:+.2f}")
    print(f"  avg net usd:                       {stats.avg_net_usd:+.2f}")
    print(f"  median net usd:                    {stats.median_net_usd:+.2f}")

    if stats.best_trade_points is not None:
        print(f"  best trade points:                 {stats.best_trade_points:+.2f}")

    if stats.worst_trade_points is not None:
        print(f"  worst trade points:                {stats.worst_trade_points:+.2f}")

    if stats.best_trade_net_usd is not None:
        print(f"  best trade net usd:                {stats.best_trade_net_usd:+.2f}")

    if stats.worst_trade_net_usd is not None:
        print(f"  worst trade net usd:               {stats.worst_trade_net_usd:+.2f}")


def main():
    instrument_row = Instrument[INSTRUMENT_CODE]
    table_name = build_table_name(
        instrument_code=INSTRUMENT_CODE,
        bar_size_setting=instrument_row["barSizeSetting"],
    )

    test_start_ts = parse_utc_hour_start_text(TEST_START_HOUR_TEXT_UTC)
    test_end_ts = parse_utc_hour_start_text(TEST_END_HOUR_TEXT_UTC)

    if test_end_ts <= test_start_ts:
        raise ValueError("TEST_END_HOUR_TEXT_UTC должен быть строго позже TEST_START_HOUR_TEXT_UTC")

    price_conn = sqlite3.connect(settings.price_db_path)
    prepared_conn = sqlite3.connect(settings.prepared_db_path)

    try:
        price_conn.row_factory = sqlite3.Row
        prepared_conn.row_factory = sqlite3.Row

        price_conn.execute("PRAGMA busy_timeout=5000;")
        prepared_conn.execute("PRAGMA busy_timeout=5000;")

        test_hour_starts = load_test_hour_starts(
            price_conn=price_conn,
            table_name=table_name,
            test_start_ts=test_start_ts,
            test_end_ts=test_end_ts,
        )

        print(f"Тестируем часов: {len(test_hour_starts)}")
        print(f"Тестовый диапазон: {TEST_START_HOUR_TEXT_UTC} -> {TEST_END_HOUR_TEXT_UTC} UTC")
        print("Поиск кандидатов: вся доступная prepared-история строго раньше текущего тестового часа по CT-оси")
        print(f"Входное окно: [{ENTRY_START_BAR_COUNT} .. {ENTRY_END_BAR_COUNT_EXCLUSIVE}) баров")
        print(f"Выход: bar_index={get_exit_bar_index()} (за {EXIT_CLOSE_OFFSET_SECONDS} секунд до конца часа)")
        print(f"MNQ point value: {MNQ_POINT_VALUE_USD:.2f} USD за 1 пункт")
        print(f"Commission per side: {COMMISSION_PER_SIDE_USD:.2f} USD")
        print(f"Round-turn commission: {get_round_turn_commissions_usd():.2f} USD")
        print("")

        candidate_cache = {}
        results = []

        for hour_start_ts in test_hour_starts:
            hour_start_text = hour_start_text_from_ts(hour_start_ts)

            try:
                result = run_one_test_hour(
                    price_conn=price_conn,
                    prepared_conn=prepared_conn,
                    table_name=table_name,
                    hour_start_ts=hour_start_ts,
                    candidate_cache=candidate_cache,
                )
            except ValueError as exc:
                result = {
                    "status": "invalid_hour",
                    "hour_start_ts": hour_start_ts,
                    "hour_start_utc": hour_start_text,
                    "error": str(exc),
                }
                print(f"SKIP INVALID HOUR: {hour_start_text} UTC | {exc}")

            results.append(result)

            if PRINT_TRADES and result["status"] == "trade":
                print_trade(result["trade"])

        stats = build_tester_stats(results)
        print_summary(stats)

    finally:
        price_conn.close()
        prepared_conn.close()


if __name__ == "__main__":
    main()

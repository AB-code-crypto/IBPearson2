"""
скрипт для поиска и автоматического ремонта проблемных участков в price DB.

Скрипт работает в двух режимах:

1. AUTO_REPAIR
   Скрипт сам проходит по price DB, находит проблемные интервалы и затем
   автоматически ремонтирует их.

   В этом режиме он делает следующее:
   - ищет строки, где хотя бы одна из 8 цен равна NULL;
   - ищет реальные дырки между соседними 5-секундными барами;
   - игнорирует штатные рыночные паузы по встроенному расписанию;
   - объединяет найденные проблемные интервалы;
   - для каждого интервала выполняет полную перезакачку BID+ASK;
   - пишет результат обратно в price DB через UPSERT.

2. MANUAL_INTERVAL
   Скрипт ничего не сканирует автоматически, а просто берёт вручную заданный
   UTC-интервал и перезакачивает его по выбранному контракту.

   Это полезно, когда проблемный участок уже известен заранее и не хочется
   запускать полный проход по БД.


При поиске дырок нельзя считать проблемой штатные рыночные паузы:
- ежедневный клиринг;
- обычные выходные;
- DST-сдвиги;
- праздничные ранние закрытия;
- отдельные разовые проверенные исключения.

Для этого используется список IGNORED_GAP_RULES.
Если найденный пропуск совпадает с известным шаблоном нормальной паузы рынка,
скрипт не считает его дыркой и не пытается ремонтировать.

Это защищает от бессмысленных запросов в IB по интервалам,
где рынок официально не торговался.

Что именно записывается обратно в БД
------------------------------------
Скрипт пишет в price DB полные quote-строки:
- время в UTC;
- время в CT;
- контракт;
- ask_open/high/low/close;
- bid_open/high/low/close;
- дополнительные поля объёма/average/bar_count, если они пришли из IB.

Запись идёт через UPSERT:
- существующие строки обновляются;
- отсутствующие строки вставляются.

Ограничения
-----------
Скрипт рассчитан на:
- FUT-инструменты;
- 5-секундные бары;
- исторические запросы BID и ASK через IB.

Скрипт не ремонтирует prepared DB.
Сначала приводится в порядок именно price DB.
После этого prepared DB, если нужно, пересобирается отдельно.

Практический сценарий использования
-----------------------------------
Обычный рабочий порядок такой:
- запускаем AUTO_REPAIR;
- смотрим, какие интервалы найдены;
- скрипт автоматически докачивает проблемные участки;
- после этого при желании можно повторно прогнать чисто диагностическую проверку.

Для безопасной проверки без записи в БД есть флаг DRY_RUN:
- если DRY_RUN=True, скрипт только покажет, что именно собирается чинить;
- если DRY_RUN=False, реально подключится к IB и запишет данные в price DB.
"""
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from ib_async import Contract

from config import settings_for_gap as settings
from contracts import Instrument
from core.db_sql import create_quotes_table_sql, upsert_quotes_sql
from core.ib_connector import connect_ib, disconnect_ib
from core.logger import get_logger, log_info, log_warning, setup_logging

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

# Режимы:
# - "AUTO_REPAIR"    -> сам ищет проблемные интервалы в БД и чинит их
# - "MANUAL_INTERVAL" -> перекачивает вручную заданный UTC-интервал
REPAIR_MODE = "AUTO_REPAIR"

# Предохранитель:
# True  -> только показать, что будет чиниться
# False -> реально качать из IB и писать в БД
DRY_RUN = False

INSTRUMENT_CODE = "MNQ"

# Для MANUAL_INTERVAL указываем точный контракт и интервал.
CONTRACT_LOCAL_SYMBOL = "MNQM6"
MANUAL_START_UTC = "2026-04-13 18:15:00"
MANUAL_END_UTC = "2026-04-13 19:00:00"

# Для AUTO_REPAIR можно ограничить диапазон поиска.
# Если None - ищем по всей таблице.
#
# Формат:
#   "YYYY-MM-DD"
#   "YYYY-MM-DD HH:MM:SS"
#
# END_UTC_TEXT задаётся как правая граница НЕ включительно.
START_UTC_TEXT = "2026-04-17 00:00:00"
# START_UTC_TEXT = None
END_UTC_TEXT = None

# Для 5-секундных данных ожидаем строго такой шаг.
EXPECTED_STEP_SECONDS = 5

# Даже если целевой интервал узкий, у IB исторические BID/ASK запросы
# на 5 секунд обычно работают лучше, если брать минимум 1 минуту.
MIN_HISTORICAL_REQUEST_SECONDS = 60

# Небольшой запас вправо помогает добрать хвостовой бар.
HISTORICAL_RIGHT_PADDING_SECONDS = EXPECTED_STEP_SECONDS

# Максимальный размер одного historical-chunk.
CHUNK_SECONDS = 3600

# Пауза между BID и ASK запросами.
HISTORICAL_REQUEST_DELAY_SECONDS = 11

# Таймаут одного historical-запроса.
HISTORICAL_REQUEST_TIMEOUT_SECONDS = 90

# Сколько раз повторять один chunk при ошибке.
MAX_REQUEST_ATTEMPTS = 3

# Печатать ли список найденных интервалов в консоль.
PRINT_INTERVALS = True

setup_logging()
logger = get_logger(__name__)

DB_PATH = settings.price_db_path
CHICAGO_TZ = ZoneInfo("America/Chicago")

PRICE_COLUMNS = [
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
]

# ============================================================
# ПРАВИЛА ШТАТНЫХ ПАУЗ MNQ/CME
# ============================================================

IGNORED_GAP_RULES = [
    {
        "name": "daily_clearing",
        "description": "Ежедневный клиринг CME",
        "missing_bars": {720},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "regular_weekend",
        "description": "Обычные выходные",
        "missing_bars": {35280},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_spring_forward_weekend",
        "description": "Выходные с весенним переводом часов",
        "missing_bars": {34560},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_fall_back_weekend",
        "description": "Выходные с осенним переводом часов",
        "missing_bars": {36000},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "good_friday_plus_weekend",
        "description": "Good Friday + выходные",
        "missing_bars": {52560},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "us_holiday_midday_close",
        "description": "Праздничное закрытие с 12:00 CT до 17:00 CT",
        "missing_bars": {3600},
        "start_hours_utc": {17, 18},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_eve_early_close",
        "description": "Раннее закрытие накануне Independence Day",
        "missing_bars": {3420},
        "start_hours_utc": {17},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "thanksgiving_friday_plus_weekend",
        "description": "Раннее закрытие после Thanksgiving + выходные",
        "missing_bars": {37980},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "christmas_eve_plus_christmas_day",
        "description": "Christmas Eve early close + Christmas Day",
        "missing_bars": {20700},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "new_year_eve_plus_new_year_day",
        "description": "New Year's Eve close + New Year's Day",
        "missing_bars": {18000},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_plus_weekend",
        "description": "Independence Day + выходные",
        "missing_bars": {38160},
        "start_hours_utc": {17},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "national_day_of_mourning_2025_01_09",
        "description": "Национальный день траура 2025-01-09",
        "missing_bars": {6120},
        "start_hours_utc": {14},
        "start_minutes_utc": {30},
        "start_seconds_utc": {0},
        "start_dates_utc": {"2025-01-09"},
    },
]


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def parse_utc_datetime(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def parse_optional_utc_text(value):
    if value is None:
        return None

    value = value.strip()

    if len(value) == 10:
        dt = datetime.strptime(value, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def format_utc(dt, for_ib=False):
    dt = dt.astimezone(timezone.utc)

    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_utc_ts(timestamp_utc):
    dt = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_ct_ts(timestamp_utc):
    dt = datetime.fromtimestamp(timestamp_utc, tz=timezone.utc)
    return dt.astimezone(CHICAGO_TZ).strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt):
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть строго больше начала")

    return f"{total_seconds} S"


def build_table_name(instrument_code, bar_size_setting):
    suffix = (
        bar_size_setting
        .replace(" ", "")
        .replace("secs", "s")
        .replace("sec", "s")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("mins", "m")
        .replace("min", "m")
    )
    return f"{instrument_code}_{suffix}"


def get_instrument_row(instrument_code):
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] != "FUT":
        raise ValueError(
            f"Скрипт рассчитан только на FUT. Получено: {instrument_row['secType']}"
        )

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Скрипт рассчитан только на 5-секундные данные. "
            f"Получено: {instrument_row['barSizeSetting']}"
        )

    return instrument_row


def get_contract_row(instrument_row, contract_local_symbol):
    if "contracts" not in instrument_row:
        raise ValueError("У инструмента нет списка contracts")

    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == contract_local_symbol:
            return contract_row

    raise ValueError(f"Контракт не найден в contracts.py: {contract_local_symbol}")


def build_futures_contract(instrument_code, instrument_row, contract_row):
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        tradingClass=instrument_row["tradingClass"],
        multiplier=str(instrument_row["multiplier"]),
        conId=contract_row["conId"],
        localSymbol=contract_row["localSymbol"],
        lastTradeDateOrContractMonth=contract_row["lastTradeDateOrContractMonth"],
    )


def build_ct_time_fields_from_utc_dt(dt_utc):
    dt_utc = dt_utc.astimezone(timezone.utc)
    utc_ts = int(dt_utc.timestamp())

    dt_ct = dt_utc.astimezone(CHICAGO_TZ)
    ct_offset = dt_ct.utcoffset()

    if ct_offset is None:
        raise ValueError(
            f"Не удалось определить UTC offset для Chicago time. dt_utc={dt_utc}"
        )

    bar_time_ts_ct = utc_ts + int(ct_offset.total_seconds())
    bar_time_ct = dt_ct.strftime("%Y-%m-%d %H:%M:%S")

    return bar_time_ts_ct, bar_time_ct


def ensure_table_exists(conn, table_name):
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
    row = conn.execute(sql, (table_name,)).fetchone()

    if row is None:
        raise ValueError(f"Таблица '{table_name}' не найдена в базе {DB_PATH}")


def fetch_history_bounds(conn, table_name):
    sql = f"SELECT MIN(bar_time_ts), MAX(bar_time_ts) FROM {table_name}"
    row = conn.execute(sql).fetchone()
    return row[0], row[1]


def build_bar_time_filters(start_ts, end_ts):
    where_parts = []
    params = []

    if start_ts is not None:
        where_parts.append("bar_time_ts >= ?")
        params.append(start_ts)

    if end_ts is not None:
        where_parts.append("bar_time_ts < ?")
        params.append(end_ts)

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    return where_sql, params


def print_selected_range(start_ts, end_ts):
    if start_ts is None and end_ts is None:
        print("Диапазон поиска: вся доступная история")
        return

    start_text = format_utc_ts(start_ts) if start_ts is not None else "-inf"
    end_text = format_utc_ts(end_ts) if end_ts is not None else "+inf"

    print(f"Диапазон поиска UTC: {start_text} -> {end_text} (END не включается)")


def iter_chunks(start_ts, end_ts_exclusive, chunk_seconds):
    chunk_start = start_ts

    while chunk_start < end_ts_exclusive:
        chunk_end = min(chunk_start + chunk_seconds, end_ts_exclusive)
        yield chunk_start, chunk_end
        chunk_start = chunk_end


def validate_price_value(value, field_name, stream_name, contract_name, interval_text, bar_index):
    if value is None:
        return (
            f"{stream_name} {field_name} is None | contract={contract_name}, "
            f"interval={interval_text}, bar_index={bar_index}"
        )

    if value <= 0:
        return (
            f"{stream_name} {field_name} <= 0 | value={value}, "
            f"contract={contract_name}, interval={interval_text}, bar_index={bar_index}"
        )

    return None


# ============================================================
# ПОИСК ПРОБЛЕМНЫХ ИНТЕРВАЛОВ
# ============================================================

def fetch_problem_rows(conn, table_name, start_ts=None, end_ts=None):
    range_where_sql, range_params = build_bar_time_filters(start_ts, end_ts)

    null_condition = " OR ".join([f"{column} IS NULL" for column in PRICE_COLUMNS])

    if range_where_sql:
        sql = f"""
            SELECT
                bar_time_ts,
                contract
            FROM {table_name}
            {range_where_sql}
              AND ({null_condition})
            ORDER BY contract ASC, bar_time_ts ASC
        """
    else:
        sql = f"""
            SELECT
                bar_time_ts,
                contract
            FROM {table_name}
            WHERE {null_condition}
            ORDER BY contract ASC, bar_time_ts ASC
        """

    return conn.execute(sql, tuple(range_params)).fetchall()


def build_null_intervals(problem_rows):
    intervals = []

    current_contract = None
    current_start_ts = None
    current_prev_ts = None

    for row in problem_rows:
        bar_time_ts = row["bar_time_ts"]
        contract_local_symbol = row["contract"]

        if current_start_ts is None:
            current_contract = contract_local_symbol
            current_start_ts = bar_time_ts
            current_prev_ts = bar_time_ts
            continue

        is_same_contract = contract_local_symbol == current_contract
        is_next_bar = bar_time_ts == current_prev_ts + EXPECTED_STEP_SECONDS

        if is_same_contract and is_next_bar:
            current_prev_ts = bar_time_ts
            continue

        intervals.append(
            {
                "contract_local_symbol": current_contract,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "sources": {"NULL_PRICE"},
            }
        )

        current_contract = contract_local_symbol
        current_start_ts = bar_time_ts
        current_prev_ts = bar_time_ts

    if current_start_ts is not None:
        intervals.append(
            {
                "contract_local_symbol": current_contract,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "sources": {"NULL_PRICE"},
            }
        )

    return intervals


def fetch_all_bars_for_gap_scan(conn, table_name, start_ts=None, end_ts=None):
    where_sql, params = build_bar_time_filters(start_ts, end_ts)

    sql = f"""
        SELECT
            bar_time_ts,
            contract
        FROM {table_name}
        {where_sql}
        ORDER BY bar_time_ts ASC
    """

    return conn.execute(sql, tuple(params)).fetchall()


def get_missing_bars_count(gap_start_ts, gap_end_ts):
    missing_seconds = gap_end_ts - gap_start_ts + EXPECTED_STEP_SECONDS
    return missing_seconds // EXPECTED_STEP_SECONDS


def gap_matches_rule(gap_start_ts, gap_end_ts, rule):
    missing_bars = get_missing_bars_count(gap_start_ts, gap_end_ts)

    if missing_bars not in rule["missing_bars"]:
        return False

    gap_start_dt = datetime.fromtimestamp(gap_start_ts, tz=timezone.utc)

    if gap_start_dt.hour not in rule["start_hours_utc"]:
        return False

    if gap_start_dt.minute not in rule["start_minutes_utc"]:
        return False

    if gap_start_dt.second not in rule["start_seconds_utc"]:
        return False

    if "start_dates_utc" in rule:
        start_date_text = gap_start_dt.strftime("%Y-%m-%d")
        if start_date_text not in rule["start_dates_utc"]:
            return False

    return True


def get_ignored_gap_rule_name(gap_start_ts, gap_end_ts):
    for rule in IGNORED_GAP_RULES:
        if gap_matches_rule(gap_start_ts, gap_end_ts, rule):
            return rule["name"]

    return None


def build_gap_intervals(sorted_rows):
    intervals = []

    if not sorted_rows:
        return intervals

    previous_row = sorted_rows[0]

    for current_row in sorted_rows[1:]:
        previous_ts = previous_row["bar_time_ts"]
        current_ts = current_row["bar_time_ts"]
        previous_contract = previous_row["contract"]
        current_contract = current_row["contract"]

        delta_seconds = current_ts - previous_ts

        if delta_seconds > EXPECTED_STEP_SECONDS:
            gap_start_ts = previous_ts + EXPECTED_STEP_SECONDS
            gap_end_ts = current_ts - EXPECTED_STEP_SECONDS

            ignored_rule_name = get_ignored_gap_rule_name(
                gap_start_ts=gap_start_ts,
                gap_end_ts=gap_end_ts,
            )
            if ignored_rule_name is not None:
                previous_row = current_row
                continue

            if previous_contract != current_contract:
                log_warning(
                    logger,
                    "Пропускаю gap на границе разных контрактов: "
                    f"prev_contract={previous_contract}, curr_contract={current_contract}, "
                    f"interval={format_utc_ts(gap_start_ts)} -> "
                    f"{format_utc_ts(gap_end_ts + EXPECTED_STEP_SECONDS)}",
                    to_telegram=False,
                )
                previous_row = current_row
                continue

            intervals.append(
                {
                    "contract_local_symbol": previous_contract,
                    "start_ts": gap_start_ts,
                    "end_ts_exclusive": gap_end_ts + EXPECTED_STEP_SECONDS,
                    "sources": {"GAP"},
                }
            )

        elif delta_seconds < EXPECTED_STEP_SECONDS:
            log_warning(
                logger,
                "Найдена аномальная последовательность bar_time_ts: "
                f"prev_utc={format_utc_ts(previous_ts)}, "
                f"curr_utc={format_utc_ts(current_ts)}, "
                f"delta={delta_seconds} сек",
                to_telegram=False,
            )

        previous_row = current_row

    return intervals


def merge_intervals(intervals):
    if not intervals:
        return []

    sorted_intervals = sorted(
        intervals,
        key=lambda item: (
            item["contract_local_symbol"],
            item["start_ts"],
            item["end_ts_exclusive"],
        ),
    )

    merged = [
        {
            "contract_local_symbol": sorted_intervals[0]["contract_local_symbol"],
            "start_ts": sorted_intervals[0]["start_ts"],
            "end_ts_exclusive": sorted_intervals[0]["end_ts_exclusive"],
            "sources": set(sorted_intervals[0]["sources"]),
        }
    ]

    for interval in sorted_intervals[1:]:
        last = merged[-1]

        same_contract = interval["contract_local_symbol"] == last["contract_local_symbol"]
        overlap_or_touch = interval["start_ts"] <= last["end_ts_exclusive"]

        if same_contract and overlap_or_touch:
            last["end_ts_exclusive"] = max(
                last["end_ts_exclusive"],
                interval["end_ts_exclusive"],
            )
            last["sources"].update(interval["sources"])
        else:
            merged.append(
                {
                    "contract_local_symbol": interval["contract_local_symbol"],
                    "start_ts": interval["start_ts"],
                    "end_ts_exclusive": interval["end_ts_exclusive"],
                    "sources": set(interval["sources"]),
                }
            )

    return merged


def find_repair_intervals(conn, table_name, start_ts=None, end_ts=None):
    problem_rows = fetch_problem_rows(
        conn=conn,
        table_name=table_name,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    null_intervals = build_null_intervals(problem_rows)

    gap_rows = fetch_all_bars_for_gap_scan(
        conn=conn,
        table_name=table_name,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    gap_intervals = build_gap_intervals(gap_rows)

    merged_intervals = merge_intervals(null_intervals + gap_intervals)

    return {
        "problem_rows_count": len(problem_rows),
        "null_intervals": null_intervals,
        "gap_intervals": gap_intervals,
        "merged_intervals": merged_intervals,
    }


def print_intervals(intervals):
    if not intervals:
        print("Интервалов для ремонта не найдено.")
        return

    for index, interval in enumerate(intervals, start=1):
        bars_count = int(
            (interval["end_ts_exclusive"] - interval["start_ts"]) // EXPECTED_STEP_SECONDS
        )
        sources_text = ", ".join(sorted(interval["sources"]))

        print(
            f"[{index}] "
            f"contract={interval['contract_local_symbol']} | "
            f"{format_utc_ts(interval['start_ts'])} -> "
            f"{format_utc_ts(interval['end_ts_exclusive'])} | "
            f"баров={bars_count} | "
            f"sources={sources_text}"
        )


# ============================================================
# ЗАГРУЗКА ИСТОРИИ И ЗАПИСЬ В БД
# ============================================================

async def request_history_once(
        ib,
        contract,
        start_dt,
        end_dt,
        bar_size_setting,
        what_to_show,
        use_rth,
):
    return await asyncio.wait_for(
        ib.reqHistoricalDataAsync(
            contract,
            endDateTime=format_utc(end_dt, for_ib=True),
            durationStr=build_duration_str(start_dt, end_dt),
            barSizeSetting=bar_size_setting,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=2,
            keepUpToDate=False,
        ),
        timeout=HISTORICAL_REQUEST_TIMEOUT_SECONDS,
    )


async def request_history_with_retries(
        ib,
        contract,
        start_dt,
        end_dt,
        bar_size_setting,
        what_to_show,
        use_rth,
):
    last_error = None

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            bars = await request_history_once(
                ib=ib,
                contract=contract,
                start_dt=start_dt,
                end_dt=end_dt,
                bar_size_setting=bar_size_setting,
                what_to_show=what_to_show,
                use_rth=use_rth,
            )
            return bars
        except Exception as exc:
            last_error = exc
            log_warning(
                logger,
                f"Ошибка historical request attempt={attempt}/{MAX_REQUEST_ATTEMPTS} | "
                f"contract={contract.localSymbol} | side={what_to_show} | "
                f"interval={format_utc(start_dt)} -> {format_utc(end_dt)} | "
                f"error={exc}",
                to_telegram=False,
            )

            if attempt < MAX_REQUEST_ATTEMPTS:
                await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    raise last_error


def build_quote_rows(bid_bars, ask_bars, contract_name, target_start_dt, target_end_dt):
    rows_by_ts = {}

    for bars, side_name in ((ask_bars, "ASK"), (bid_bars, "BID")):
        for bar in bars:
            dt = bar.date.astimezone(timezone.utc)

            if not (target_start_dt <= dt < target_end_dt):
                continue

            bar_time_ts = int(dt.timestamp())

            if bar_time_ts not in rows_by_ts:
                bar_time_ts_ct, bar_time_ct = build_ct_time_fields_from_utc_dt(dt)

                rows_by_ts[bar_time_ts] = {
                    "bar_time_ts": bar_time_ts,
                    "bar_time": format_utc(dt),
                    "bar_time_ts_ct": bar_time_ts_ct,
                    "bar_time_ct": bar_time_ct,
                    "contract": contract_name,
                    "ask_open": None,
                    "bid_open": None,
                    "ask_high": None,
                    "bid_high": None,
                    "ask_low": None,
                    "bid_low": None,
                    "ask_close": None,
                    "bid_close": None,
                    "volume": bar.volume,
                    "average": bar.average,
                    "bar_count": bar.barCount,
                }

            for field_name, value in (
                    ("open", bar.open),
                    ("high", bar.high),
                    ("low", bar.low),
                    ("close", bar.close),
            ):
                price_error = validate_price_value(
                    value=value,
                    field_name=field_name,
                    stream_name=side_name,
                    contract_name=contract_name,
                    interval_text=f"{format_utc(target_start_dt)} -> {format_utc(target_end_dt)}",
                    bar_index=-1,
                )
                if price_error is not None:
                    raise ValueError(price_error)

            if side_name == "ASK":
                rows_by_ts[bar_time_ts]["ask_open"] = bar.open
                rows_by_ts[bar_time_ts]["ask_high"] = bar.high
                rows_by_ts[bar_time_ts]["ask_low"] = bar.low
                rows_by_ts[bar_time_ts]["ask_close"] = bar.close
            else:
                rows_by_ts[bar_time_ts]["bid_open"] = bar.open
                rows_by_ts[bar_time_ts]["bid_high"] = bar.high
                rows_by_ts[bar_time_ts]["bid_low"] = bar.low
                rows_by_ts[bar_time_ts]["bid_close"] = bar.close

    rows = []

    for bar_time_ts in sorted(rows_by_ts.keys()):
        row = rows_by_ts[bar_time_ts]
        rows.append(
            (
                row["bar_time_ts"],
                row["bar_time"],
                row["bar_time_ts_ct"],
                row["bar_time_ct"],
                row["contract"],
                row["ask_open"],
                row["bid_open"],
                row["ask_high"],
                row["bid_high"],
                row["ask_low"],
                row["bid_low"],
                row["ask_close"],
                row["bid_close"],
                row["volume"],
                row["average"],
                row["bar_count"],
            )
        )

    return rows


def write_quote_rows_to_sqlite(db_path, table_name, rows):
    if not rows:
        return

    create_sql = create_quotes_table_sql(table_name)
    upsert_sql = upsert_quotes_sql(table_name)

    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute(create_sql)
        conn.executemany(upsert_sql, rows)
        conn.commit()
    finally:
        conn.close()


async def repair_chunk(
        ib,
        contract,
        table_name,
        instrument_row,
        chunk_start_dt,
        chunk_end_dt,
):
    target_duration_seconds = int((chunk_end_dt - chunk_start_dt).total_seconds())

    request_start_dt = chunk_start_dt
    if target_duration_seconds < MIN_HISTORICAL_REQUEST_SECONDS:
        request_start_dt = chunk_end_dt - timedelta(seconds=MIN_HISTORICAL_REQUEST_SECONDS)

    request_end_dt = chunk_end_dt + timedelta(seconds=HISTORICAL_RIGHT_PADDING_SECONDS)

    bid_bars = await request_history_with_retries(
        ib=ib,
        contract=contract,
        start_dt=request_start_dt,
        end_dt=request_end_dt,
        bar_size_setting=instrument_row["barSizeSetting"],
        what_to_show="BID",
        use_rth=instrument_row["useRTH"],
    )

    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    ask_bars = await request_history_with_retries(
        ib=ib,
        contract=contract,
        start_dt=request_start_dt,
        end_dt=request_end_dt,
        bar_size_setting=instrument_row["barSizeSetting"],
        what_to_show="ASK",
        use_rth=instrument_row["useRTH"],
    )

    rows = build_quote_rows(
        bid_bars=bid_bars,
        ask_bars=ask_bars,
        contract_name=contract.localSymbol,
        target_start_dt=chunk_start_dt,
        target_end_dt=chunk_end_dt,
    )

    await asyncio.to_thread(
        write_quote_rows_to_sqlite,
        DB_PATH,
        table_name,
        rows,
    )

    return len(rows)


async def repair_interval(
        ib,
        table_name,
        instrument_row,
        interval,
):
    contract_row = get_contract_row(
        instrument_row=instrument_row,
        contract_local_symbol=interval["contract_local_symbol"],
    )
    contract = build_futures_contract(
        instrument_code=INSTRUMENT_CODE,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )

    total_rows_written = 0

    for chunk_start_ts, chunk_end_ts in iter_chunks(
            interval["start_ts"],
            interval["end_ts_exclusive"],
            CHUNK_SECONDS,
    ):
        chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
        chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

        log_info(
            logger,
            "Начинаю repair-chunk: "
            f"contract={contract.localSymbol}, "
            f"interval={format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)}",
            to_telegram=False,
        )

        rows_written = await repair_chunk(
            ib=ib,
            contract=contract,
            table_name=table_name,
            instrument_row=instrument_row,
            chunk_start_dt=chunk_start_dt,
            chunk_end_dt=chunk_end_dt,
        )

        total_rows_written += rows_written

        log_info(
            logger,
            "Завершён repair-chunk: "
            f"contract={contract.localSymbol}, "
            f"interval={format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)}, "
            f"rows={rows_written}",
            to_telegram=False,
        )

    return total_rows_written


# ============================================================
# ПОСТРОЕНИЕ СПИСКА ИНТЕРВАЛОВ ДЛЯ РЕМОНТА
# ============================================================

def build_manual_intervals():
    start_dt = parse_utc_datetime(MANUAL_START_UTC)
    end_dt = parse_utc_datetime(MANUAL_END_UTC)

    if end_dt <= start_dt:
        raise ValueError("MANUAL_END_UTC должен быть строго больше MANUAL_START_UTC")

    return [
        {
            "contract_local_symbol": CONTRACT_LOCAL_SYMBOL,
            "start_ts": int(start_dt.timestamp()),
            "end_ts_exclusive": int(end_dt.timestamp()),
            "sources": {"MANUAL_INTERVAL"},
        }
    ]


def build_auto_intervals(conn, table_name):
    start_ts = parse_optional_utc_text(START_UTC_TEXT)
    end_ts = parse_optional_utc_text(END_UTC_TEXT)

    print_selected_range(start_ts, end_ts)

    scan_result = find_repair_intervals(
        conn=conn,
        table_name=table_name,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    print(f"Найдено проблемных строк с NULL-ценами: {scan_result['problem_rows_count']}")
    print(f"Найдено NULL-интервалов: {len(scan_result['null_intervals'])}")
    print(f"Найдено GAP-интервалов:  {len(scan_result['gap_intervals'])}")
    print(f"Найдено итоговых интервалов для ремонта: {len(scan_result['merged_intervals'])}")

    return scan_result["merged_intervals"]


# ============================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================

async def main():
    instrument_row = get_instrument_row(INSTRUMENT_CODE)
    table_name = build_table_name(
        INSTRUMENT_CODE,
        instrument_row["barSizeSetting"],
    )

    print(f"База данных: {DB_PATH}")
    print(f"Таблица: {table_name}")
    print(f"Инструмент: {INSTRUMENT_CODE}")
    print(f"Режим ремонта: {REPAIR_MODE}")
    print(f"DRY_RUN: {DRY_RUN}")

    conn = sqlite3.connect(DB_PATH)

    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000;")

        ensure_table_exists(conn, table_name)

        history_min_ts, history_max_ts = fetch_history_bounds(conn, table_name)

        if history_min_ts is None or history_max_ts is None:
            print("Таблица существует, но данных в ней нет.")
            return

        print(
            "Доступная история в таблице: "
            f"UTC {format_utc_ts(history_min_ts)} -> {format_utc_ts(history_max_ts)} | "
            f"CT {format_ct_ts(history_min_ts)} -> {format_ct_ts(history_max_ts)}"
        )

        if REPAIR_MODE == "AUTO_REPAIR":
            intervals = build_auto_intervals(conn, table_name)
        elif REPAIR_MODE == "MANUAL_INTERVAL":
            intervals = build_manual_intervals()
        else:
            raise ValueError(f"Неподдерживаемый REPAIR_MODE: {REPAIR_MODE}")

    finally:
        conn.close()

    if not intervals:
        print("Проблемных интервалов для ремонта не найдено.")
        return

    if PRINT_INTERVALS:
        print_intervals(intervals)

    if DRY_RUN:
        print("DRY_RUN=True -> ремонт не выполнялся.")
        return

    total_rows_written = 0
    ib = None

    try:
        ib, _ = await connect_ib(settings)

        for interval in intervals:
            sources_text = ", ".join(sorted(interval["sources"]))
            bars_count = int(
                (interval["end_ts_exclusive"] - interval["start_ts"]) // EXPECTED_STEP_SECONDS
            )

            log_info(
                logger,
                "Начинаю ремонт интервала: "
                f"contract={interval['contract_local_symbol']}, "
                f"interval={format_utc_ts(interval['start_ts'])} -> "
                f"{format_utc_ts(interval['end_ts_exclusive'])}, "
                f"bars={bars_count}, sources={sources_text}",
                to_telegram=False,
            )

            rows_written = await repair_interval(
                ib=ib,
                table_name=table_name,
                instrument_row=instrument_row,
                interval=interval,
            )

            total_rows_written += rows_written

        print(f"Ремонт завершён. Всего записано строк: {total_rows_written}")

    finally:
        if ib is not None:
            disconnect_ib(ib)
            log_info(logger, "Соединение с IB закрыто", to_telegram=False)


if __name__ == "__main__":
    asyncio.run(main())

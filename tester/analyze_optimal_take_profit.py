from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd


# ============================================================
# НАСТРОЙКИ
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

CSV_PATH = PROJECT_ROOT / "strategy_tester_MNQ_216.csv"
PRICE_DB_PATH = PROJECT_ROOT.parent / "data" / "price.sqlite3"
OUTPUT_DB_PATH = PROJECT_ROOT / "tp_sl_analysis_MNQ_216.sqlite3"

PRICE_TABLE = "MNQ_5s"
TIME_COLUMN = "bar_time_ts"

GRID_TABLE = "tp_sl_grid"
TRADES_TABLE = "tp_sl_trade_diagnostics"
RECREATE_OUTPUT_TABLES = True

# Для LONG закрытие идёт по BID, для SHORT — по ASK.
LONG_TP_COLUMN = "bid_high"
LONG_SL_COLUMN = "bid_low"
SHORT_TP_COLUMN = "ask_low"
SHORT_SL_COLUMN = "ask_high"

POINT_VALUE = 2.0
COMMISSION_PER_TRADE = 1.24

# 0 = сторона выключена
TP_MIN_POINTS = 0.0
TP_MAX_POINTS = 300.0
TP_STEP_POINTS = 0.25

# 0 = сторона выключена
SL_MIN_POINTS = 0.0
SL_MAX_POINTS = 300.0
SL_STEP_POINTS = 0.25

# True  -> включаем бар, стартующий ровно в entry_time
# False -> начинаем строго после entry_time
USE_ENTRY_BAR = True

# Что делать, если и TP и SL были достигнуты в одном и том же 5-сек баре.
# Возможные значения:
#   "sl"       -> считаем, что первым сработал SL (консервативно)
#   "tp"       -> считаем, что первым сработал TP
#   "original" -> считаем такой бар неоднозначным и оставляем исходный выход тестера
SAME_BAR_POLICY = "sl"

CSV_ENTRY_TIME_COLUMN = "entry_time"
CSV_EXIT_TIME_COLUMN = "exit_time"
CSV_SIDE_COLUMN = "trade_side"
CSV_ENTRY_PRICE_COLUMN = "entry_price"
CSV_EXIT_PRICE_COLUMN = "exit_price"
CSV_NET_PNL_COLUMN = "net_pnl"
CSV_TRADE_OPENED_COLUMN = "trade_opened"

CSV_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================


def build_grid(min_value, max_value, step):
    values = []
    current = float(min_value)
    while current <= float(max_value) + 1e-9:
        values.append(round(current, 10))
        current += float(step)
    return np.array(values, dtype=float)



def parse_trade_opened_mask(series):
    if series.dtype == bool:
        return series.fillna(False)

    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])



def to_unix_ts(series):
    dt = pd.to_datetime(series, format=CSV_TIME_FORMAT, errors="coerce", utc=True)
    epoch = pd.Timestamp("1970-01-01 00:00:00", tz="UTC")
    return ((dt - epoch) // pd.Timedelta(seconds=1)).astype("Int64")



def load_trades():
    df = pd.read_csv(CSV_PATH)

    if CSV_TRADE_OPENED_COLUMN in df.columns:
        df = df[parse_trade_opened_mask(df[CSV_TRADE_OPENED_COLUMN])].copy()

    required = [
        CSV_SIDE_COLUMN,
        CSV_ENTRY_TIME_COLUMN,
        CSV_EXIT_TIME_COLUMN,
        CSV_ENTRY_PRICE_COLUMN,
        CSV_EXIT_PRICE_COLUMN,
        CSV_NET_PNL_COLUMN,
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"В CSV отсутствуют колонки: {missing}")

    df[CSV_ENTRY_PRICE_COLUMN] = pd.to_numeric(df[CSV_ENTRY_PRICE_COLUMN], errors="coerce")
    df[CSV_EXIT_PRICE_COLUMN] = pd.to_numeric(df[CSV_EXIT_PRICE_COLUMN], errors="coerce")
    df[CSV_NET_PNL_COLUMN] = pd.to_numeric(df[CSV_NET_PNL_COLUMN], errors="coerce")

    numeric_bad = (
        df[CSV_ENTRY_PRICE_COLUMN].isna()
        | df[CSV_EXIT_PRICE_COLUMN].isna()
        | df[CSV_NET_PNL_COLUMN].isna()
    )
    if numeric_bad.any():
        raise ValueError("Не удалось распарсить entry_price / exit_price / net_pnl у части строк.")

    df["entry_ts"] = to_unix_ts(df[CSV_ENTRY_TIME_COLUMN])
    df["exit_ts"] = to_unix_ts(df[CSV_EXIT_TIME_COLUMN])

    bad_ts = df["entry_ts"].isna() | df["exit_ts"].isna()
    if bad_ts.any():
        raise ValueError("Не удалось распарсить время входа/выхода у части строк.")

    if "last_bar_time_ts" in df.columns:
        last_bar_ts = pd.to_numeric(df["last_bar_time_ts"], errors="coerce")
        ts_diff = (df["entry_ts"] - (last_bar_ts + 5)).abs()
        bad_ts_rows = ts_diff > 10
        if bad_ts_rows.any():
            sample = df.loc[
                bad_ts_rows,
                [CSV_ENTRY_TIME_COLUMN, CSV_EXIT_TIME_COLUMN, "entry_ts", "exit_ts", "last_bar_time_ts"],
            ].head(10)
            raise RuntimeError(
                "Некорректно посчитались entry_ts/exit_ts. "
                f"Примеры:\n{sample.to_string(index=False)}"
            )

    df["entry_ts"] = df["entry_ts"].astype(int)
    df["exit_ts"] = df["exit_ts"].astype(int)

    bad_order = df["exit_ts"] < df["entry_ts"]
    if bad_order.any():
        raise ValueError("Есть сделки, где exit_time раньше entry_time.")

    df["side_norm"] = df[CSV_SIDE_COLUMN].astype(str).str.upper().str.strip()

    bad_side = ~df["side_norm"].isin(["LONG", "SHORT"])
    if bad_side.any():
        bad_values = sorted(df.loc[bad_side, "side_norm"].dropna().unique().tolist())
        raise ValueError(f"Неизвестные направления сделки: {bad_values}")

    return df.reset_index(drop=True)



def ensure_price_table_exists(conn):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (PRICE_TABLE,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"В БД {PRICE_DB_PATH} не найдена таблица {PRICE_TABLE}")



def fetch_trade_bars(conn, entry_ts, exit_ts):
    left_op = ">=" if USE_ENTRY_BAR else ">"

    sql = f"""
    SELECT
        {TIME_COLUMN},
        {LONG_TP_COLUMN},
        {LONG_SL_COLUMN},
        {SHORT_TP_COLUMN},
        {SHORT_SL_COLUMN}
    FROM {PRICE_TABLE}
    WHERE {TIME_COLUMN} {left_op} ?
      AND {TIME_COLUMN} <= ?
    ORDER BY {TIME_COLUMN}
    """
    return conn.execute(sql, (int(entry_ts), int(exit_ts))).fetchall()



def build_trade_path_info(trade_row, bar_rows, tp_grid, sl_grid):
    entry_price = float(trade_row.entry_price)
    side_norm = trade_row.side_norm
    bars_count = len(bar_rows)

    if bars_count == 0:
        return {
            "bars_count": 0,
            "valid_price_count": 0,
            "mfe_points": np.nan,
            "mae_points": np.nan,
            "issue": "no_bars",
            "tp_first_idx": np.full(len(tp_grid), 0, dtype=np.int32),
            "sl_first_idx": np.full(len(sl_grid), 0, dtype=np.int32),
            "path_len": 0,
        }

    favorable = []
    adverse = []

    if side_norm == "LONG":
        for _, bid_high, bid_low, _, _ in bar_rows:
            if bid_high is None or bid_low is None:
                continue
            favorable.append(float(bid_high) - entry_price)
            adverse.append(entry_price - float(bid_low))
    else:
        for _, _, _, ask_low, ask_high in bar_rows:
            if ask_low is None or ask_high is None:
                continue
            favorable.append(entry_price - float(ask_low))
            adverse.append(float(ask_high) - entry_price)

    valid_price_count = len(favorable)
    if valid_price_count == 0:
        return {
            "bars_count": bars_count,
            "valid_price_count": 0,
            "mfe_points": np.nan,
            "mae_points": np.nan,
            "issue": "no_valid_price",
            "tp_first_idx": np.full(len(tp_grid), 0, dtype=np.int32),
            "sl_first_idx": np.full(len(sl_grid), 0, dtype=np.int32),
            "path_len": 0,
        }

    favorable = np.asarray(favorable, dtype=float)
    adverse = np.asarray(adverse, dtype=float)

    cum_favorable = np.maximum.accumulate(favorable)
    cum_adverse = np.maximum.accumulate(adverse)

    tp_first_idx = np.searchsorted(cum_favorable, tp_grid, side="left").astype(np.int32)
    sl_first_idx = np.searchsorted(cum_adverse, sl_grid, side="left").astype(np.int32)

    return {
        "bars_count": bars_count,
        "valid_price_count": valid_price_count,
        "mfe_points": float(cum_favorable[-1]),
        "mae_points": float(cum_adverse[-1]),
        "issue": "",
        "tp_first_idx": tp_first_idx,
        "sl_first_idx": sl_first_idx,
        "path_len": valid_price_count,
    }



def enrich_trades_with_path_info(trades, tp_grid, sl_grid):
    conn = sqlite3.connect(PRICE_DB_PATH)
    ensure_price_table_exists(conn)

    bars_count_list = []
    valid_price_count_list = []
    mfe_points_list = []
    mae_points_list = []
    issue_list = []
    path_info_list = []

    total = len(trades)

    for idx, row in enumerate(trades.itertuples(index=False), start=1):
        bar_rows = fetch_trade_bars(
            conn=conn,
            entry_ts=row.entry_ts,
            exit_ts=row.exit_ts,
        )
        info = build_trade_path_info(row, bar_rows, tp_grid, sl_grid)

        bars_count_list.append(info["bars_count"])
        valid_price_count_list.append(info["valid_price_count"])
        mfe_points_list.append(info["mfe_points"])
        mae_points_list.append(info["mae_points"])
        issue_list.append(info["issue"])
        path_info_list.append(info)

        if idx % 100 == 0 or idx == total:
            print(f"Обработано сделок: {idx}/{total}")

    conn.close()

    out = trades.copy()
    out["bars_count"] = bars_count_list
    out["valid_price_count"] = valid_price_count_list
    out["mfe_points"] = mfe_points_list
    out["mae_points"] = mae_points_list
    out["issue"] = issue_list
    return out, path_info_list



def evaluate_tp_sl_grid(trades, path_info_list, tp_grid, sl_grid):
    tp_pnl_values = tp_grid * POINT_VALUE - COMMISSION_PER_TRADE
    sl_pnl_values = -sl_grid * POINT_VALUE - COMMISSION_PER_TRADE

    total_pnl = np.zeros((len(tp_grid), len(sl_grid)), dtype=float)
    tp_hit_count = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)
    sl_hit_count = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)
    same_bar_count = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)
    winners = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)
    losers = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)
    zeros = np.zeros((len(tp_grid), len(sl_grid)), dtype=np.int32)

    big_index = 10 ** 9

    for trade_row, info in zip(trades.itertuples(index=False), path_info_list):
        tp_idx = info["tp_first_idx"].astype(np.int64)
        sl_idx = info["sl_first_idx"].astype(np.int64)
        path_len = int(info["path_len"])

        tp_idx = np.where(tp_idx >= path_len, big_index, tp_idx)
        sl_idx = np.where(sl_idx >= path_len, big_index, sl_idx)

        # 0 = сторона выключена
        tp_idx = np.where(tp_grid <= 0, big_index, tp_idx)
        sl_idx = np.where(sl_grid <= 0, big_index, sl_idx)

        tp_matrix = tp_idx[:, None]
        sl_matrix = sl_idx[None, :]

        tp_first = tp_matrix < sl_matrix
        sl_first = sl_matrix < tp_matrix
        same_bar = (tp_matrix == sl_matrix) & (tp_matrix < big_index)

        if SAME_BAR_POLICY == "sl":
            sl_first = sl_first | same_bar
        elif SAME_BAR_POLICY == "tp":
            tp_first = tp_first | same_bar
        elif SAME_BAR_POLICY == "original":
            pass
        else:
            raise ValueError(f"Неизвестное SAME_BAR_POLICY: {SAME_BAR_POLICY}")

        pnl_matrix = np.full((len(tp_grid), len(sl_grid)), float(trade_row.net_pnl), dtype=float)
        pnl_matrix = np.where(tp_first, tp_pnl_values[:, None], pnl_matrix)
        pnl_matrix = np.where(sl_first, sl_pnl_values[None, :], pnl_matrix)

        total_pnl += pnl_matrix
        tp_hit_count += tp_first.astype(np.int32)
        sl_hit_count += sl_first.astype(np.int32)
        same_bar_count += same_bar.astype(np.int32)
        winners += (pnl_matrix > 0).astype(np.int32)
        losers += (pnl_matrix < 0).astype(np.int32)
        zeros += (pnl_matrix == 0).astype(np.int32)

    return {
        "total_pnl": total_pnl,
        "tp_hit_count": tp_hit_count,
        "sl_hit_count": sl_hit_count,
        "same_bar_count": same_bar_count,
        "winners": winners,
        "losers": losers,
        "zeros": zeros,
    }



def build_result_dataframe(stats, trades_count, tp_grid, sl_grid):
    rows = []

    for tp_idx, tp_points in enumerate(tp_grid):
        for sl_idx, sl_points in enumerate(sl_grid):
            total_net_pnl = float(stats["total_pnl"][tp_idx, sl_idx])
            tp_hit_count = int(stats["tp_hit_count"][tp_idx, sl_idx])
            sl_hit_count = int(stats["sl_hit_count"][tp_idx, sl_idx])
            same_bar_count = int(stats["same_bar_count"][tp_idx, sl_idx])
            winners = int(stats["winners"][tp_idx, sl_idx])
            losers = int(stats["losers"][tp_idx, sl_idx])
            zeros = int(stats["zeros"][tp_idx, sl_idx])

            rows.append(
                {
                    "tp_points": float(tp_points),
                    "sl_points": float(sl_points),
                    "tp_label": "OFF" if float(tp_points) == 0.0 else float(tp_points),
                    "sl_label": "OFF" if float(sl_points) == 0.0 else float(sl_points),
                    "trades": int(trades_count),
                    "tp_hit_count": tp_hit_count,
                    "tp_hit_ratio": tp_hit_count / trades_count if trades_count else 0.0,
                    "sl_hit_count": sl_hit_count,
                    "sl_hit_ratio": sl_hit_count / trades_count if trades_count else 0.0,
                    "same_bar_count": same_bar_count,
                    "same_bar_ratio": same_bar_count / trades_count if trades_count else 0.0,
                    "total_net_pnl": total_net_pnl,
                    "avg_net_pnl": total_net_pnl / trades_count if trades_count else 0.0,
                    "winners": winners,
                    "losers": losers,
                    "zeros": zeros,
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(["total_net_pnl", "tp_points", "sl_points"], ascending=[False, True, True])
        .reset_index(drop=True)
    )



def save_dataframe_to_sqlite(df, db_path, table_name, create_indexes=None, chunksize=50000):
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        if RECREATE_OUTPUT_TABLES:
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()

        df.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=chunksize)

        if create_indexes:
            for index_sql in create_indexes:
                conn.execute(index_sql)
            conn.commit()



def load_top_rows_from_sqlite(db_path, table_name, limit=20):
    sql = f"""
    SELECT *
    FROM {table_name}
    ORDER BY total_net_pnl DESC, tp_points ASC, sl_points ASC
    LIMIT ?
    """
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=(int(limit),))



def count_rows_in_sqlite(db_path, table_name):
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])



def run():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Не найден CSV: {CSV_PATH}")

    if not PRICE_DB_PATH.exists():
        raise FileNotFoundError(f"Не найдена БД цен: {PRICE_DB_PATH}")

    tp_grid = build_grid(TP_MIN_POINTS, TP_MAX_POINTS, TP_STEP_POINTS)
    sl_grid = build_grid(SL_MIN_POINTS, SL_MAX_POINTS, SL_STEP_POINTS)

    print(f"CSV_PATH = {CSV_PATH}")
    print(f"PRICE_DB_PATH = {PRICE_DB_PATH}")
    print(f"PRICE_TABLE = {PRICE_TABLE}")
    print(f"OUTPUT_DB_PATH = {OUTPUT_DB_PATH}")
    print(f"TP grid size = {len(tp_grid)}")
    print(f"SL grid size = {len(sl_grid)}")
    print(f"Ожидаемое число комбинаций = {len(tp_grid) * len(sl_grid)}")
    print("0 points = OFF")
    print(f"SAME_BAR_POLICY = {SAME_BAR_POLICY}")

    trades = load_trades()
    trades, path_info_list = enrich_trades_with_path_info(trades, tp_grid, sl_grid)

    bad_mask = trades["mfe_points"].isna() | trades["mae_points"].isna()
    if bad_mask.any():
        save_dataframe_to_sqlite(
            df=trades,
            db_path=OUTPUT_DB_PATH,
            table_name=TRADES_TABLE,
            create_indexes=[
                f"CREATE INDEX IF NOT EXISTS idx_{TRADES_TABLE}_entry_ts ON {TRADES_TABLE}(entry_ts)",
                f"CREATE INDEX IF NOT EXISTS idx_{TRADES_TABLE}_issue ON {TRADES_TABLE}(issue)",
            ],
        )
        issue_counts = trades.loc[bad_mask, "issue"].value_counts(dropna=False).to_dict()
        raise RuntimeError(
            "Не удалось посчитать path info по части сделок. "
            f"Разбивка причин: {issue_counts}. "
            f"Диагностика сохранена в {OUTPUT_DB_PATH}, таблица {TRADES_TABLE}"
        )

    save_dataframe_to_sqlite(
        df=trades,
        db_path=OUTPUT_DB_PATH,
        table_name=TRADES_TABLE,
        create_indexes=[
            f"CREATE INDEX IF NOT EXISTS idx_{TRADES_TABLE}_entry_ts ON {TRADES_TABLE}(entry_ts)",
            f"CREATE INDEX IF NOT EXISTS idx_{TRADES_TABLE}_exit_ts ON {TRADES_TABLE}(exit_ts)",
            f"CREATE INDEX IF NOT EXISTS idx_{TRADES_TABLE}_side_norm ON {TRADES_TABLE}(side_norm)",
        ],
    )

    stats = evaluate_tp_sl_grid(trades, path_info_list, tp_grid, sl_grid)
    result_df = build_result_dataframe(stats, len(trades), tp_grid, sl_grid)

    save_dataframe_to_sqlite(
        df=result_df,
        db_path=OUTPUT_DB_PATH,
        table_name=GRID_TABLE,
        create_indexes=[
            f"CREATE INDEX IF NOT EXISTS idx_{GRID_TABLE}_total_net_pnl ON {GRID_TABLE}(total_net_pnl)",
            f"CREATE INDEX IF NOT EXISTS idx_{GRID_TABLE}_tp_sl ON {GRID_TABLE}(tp_points, sl_points)",
            f"CREATE INDEX IF NOT EXISTS idx_{GRID_TABLE}_tp_hit ON {GRID_TABLE}(tp_hit_count)",
            f"CREATE INDEX IF NOT EXISTS idx_{GRID_TABLE}_sl_hit ON {GRID_TABLE}(sl_hit_count)",
        ],
    )

    top20 = load_top_rows_from_sqlite(OUTPUT_DB_PATH, GRID_TABLE, limit=20)
    best = top20.iloc[0]
    grid_rows = count_rows_in_sqlite(OUTPUT_DB_PATH, GRID_TABLE)

    print()
    print("============================================================")
    print("ЛУЧШАЯ СВЯЗКА TAKE PROFIT + STOP LOSS")
    print("============================================================")
    print(f"CSV файл                 : {CSV_PATH}")
    print(f"БД цен                   : {PRICE_DB_PATH}")
    print(f"Таблица цен              : {PRICE_TABLE}")
    print(f"БД результатов           : {OUTPUT_DB_PATH}")
    print(f"Таблица сетки            : {GRID_TABLE}")
    print(f"Таблица диагностики      : {TRADES_TABLE}")
    print(f"Сделок в анализе         : {len(trades)}")
    print(f"Комбинаций в сетке       : {grid_rows}")
    print(f"Лучший TP (пункты)       : {best['tp_points']}")
    print(f"Лучший SL (пункты)       : {best['sl_points']}")
    print(f"Итоговый net_pnl         : {best['total_net_pnl']:.2f}")
    print(f"Средний net_pnl          : {best['avg_net_pnl']:.4f}")
    print(f"Сделок закрыто по TP     : {int(best['tp_hit_count'])} ({best['tp_hit_ratio']:.2%})")
    print(f"Сделок закрыто по SL     : {int(best['sl_hit_count'])} ({best['sl_hit_ratio']:.2%})")
    print(f"Неоднозначных same-bar   : {int(best['same_bar_count'])} ({best['same_bar_ratio']:.2%})")
    print("============================================================")
    print()

    print("Топ-20 связок TP + SL:")
    print(top20.to_string(index=False))

    print()
    print("Справочно по сделкам:")
    print(f"Средний MFE, пункты      : {trades['mfe_points'].mean():.4f}")
    print(f"Медианный MFE, пункты    : {trades['mfe_points'].median():.4f}")
    print(f"Макс. MFE, пункты        : {trades['mfe_points'].max():.4f}")
    print(f"Средний MAE, пункты      : {trades['mae_points'].mean():.4f}")
    print(f"Медианный MAE, пункты    : {trades['mae_points'].median():.4f}")
    print(f"Макс. MAE, пункты        : {trades['mae_points'].max():.4f}")


if __name__ == "__main__":
    run()

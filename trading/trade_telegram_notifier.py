import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt

from contracts import Instrument
from core.db_initializer import build_table_name
from core.telegram_sender import TelegramSender

CHICAGO_TZ = ZoneInfo("America/Chicago")
HALF_HOUR_SECONDS = 1800
ANALYSIS_WINDOW_SECONDS = 3600


class TradeTelegramNotifier:
    # Отправляет торговые уведомления в Telegram:
    # - подробный common-формат;
    # - короткий trading-формат;
    # - promo-формат в тему группы.

    def __init__(self, settings, instrument_code="MNQ"):
        self.settings = settings
        self.instrument_code = instrument_code
        self.sender = TelegramSender(settings)

        self.chat_id_common = settings.telegram_chat_id_common
        self.chat_id_trading = settings.telegram_chat_id_trading
        self.chat_id_promo = settings.telegram_chat_id_promo
        self.thread_id_promo = settings.telegram_thread_id_promo

        self.trade_db_path = settings.trade_db_path
        self.price_db_path = settings.price_db_path
        self.output_dir = Path(settings.trade_db_path).resolve().parent / "telegram_trade_plots"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        instrument_row = Instrument[instrument_code]
        self.table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )
        self.bar_interval_seconds = self._parse_bar_interval_seconds(
            instrument_row["barSizeSetting"]
        )

    async def close(self):
        await self.sender.close()

    async def send_entry_message(
            self,
            *,
            snapshot,
            pearson_live_runtime,
            trade_id,
            local_symbol,
            side,
            quantity,
            placement,
    ):
        common_text = self._build_entry_text(
            snapshot=snapshot,
            trade_id=trade_id,
            local_symbol=local_symbol,
            side=side,
            quantity=quantity,
            placement=placement,
        )
        common_photo_path = None
        try:
            common_photo_path = self._build_entry_plot(
                snapshot=snapshot,
                pearson_live_runtime=pearson_live_runtime,
                trade_id=trade_id,
            )
        except Exception:
            common_photo_path = None

        if self.chat_id_common:
            if common_photo_path is not None and common_photo_path.is_file():
                await self.sender.send_photo_with_text(
                    photo_path=common_photo_path,
                    text=common_text,
                    chat_id=self.chat_id_common,
                )
            else:
                await self.sender.send_text(
                    text=common_text,
                    chat_id=self.chat_id_common,
                )

        if self.chat_id_trading:
            trading_text = self._build_trading_entry_text(
                trade_id=trade_id,
                side=side,
                local_symbol=local_symbol,
                placement=placement,
            )
            await self.sender.send_text(
                text=trading_text,
                chat_id=self.chat_id_trading,
            )

        if self.chat_id_promo:
            promo_text = self._build_promo_entry_text(
                trade_id=trade_id,
                side=side,
                local_symbol=local_symbol,
                quantity=quantity,
                placement=placement,
            )
            promo_photo_path = None
            try:
                promo_photo_path = self._build_promo_entry_plot(
                    snapshot=snapshot,
                    pearson_live_runtime=pearson_live_runtime,
                    trade_id=trade_id,
                )
            except Exception:
                promo_photo_path = None

            if promo_photo_path is not None and promo_photo_path.is_file():
                await self.sender.send_photo_with_text(
                    photo_path=promo_photo_path,
                    text=promo_text,
                    chat_id=self.chat_id_promo,
                    message_thread_id=self.thread_id_promo,
                )
            else:
                await self.sender.send_text(
                    text=promo_text,
                    chat_id=self.chat_id_promo,
                    message_thread_id=self.thread_id_promo,
                )

    async def send_exit_message(
            self,
            *,
            snapshot,
            trade_id,
            entry_side,
            exit_side,
            quantity,
            placement,
    ):
        trade_row = self._load_trade_row(trade_id)

        common_text = self._build_exit_text(
            snapshot=snapshot,
            trade_id=trade_id,
            entry_side=entry_side,
            exit_side=exit_side,
            quantity=quantity,
            placement=placement,
        )
        exit_photo_path = None
        try:
            exit_photo_path = self._build_exit_plot(
                snapshot=snapshot,
                trade_id=trade_id,
                trade_row=trade_row,
            )
        except Exception:
            exit_photo_path = None

        if self.chat_id_common:
            if exit_photo_path is not None and exit_photo_path.is_file():
                await self.sender.send_photo_with_text(
                    photo_path=exit_photo_path,
                    text=common_text,
                    chat_id=self.chat_id_common,
                )
            else:
                await self.sender.send_text(
                    text=common_text,
                    chat_id=self.chat_id_common,
                )

        if self.chat_id_trading:
            trading_text = self._build_trading_exit_text(
                trade_id=trade_id,
                entry_side=entry_side,
                placement=placement,
                trade_row=trade_row,
            )
            await self.sender.send_text(
                text=trading_text,
                chat_id=self.chat_id_trading,
            )

        if self.chat_id_promo:
            promo_text = self._build_promo_exit_text(
                trade_id=trade_id,
                entry_side=entry_side,
                quantity=quantity,
                placement=placement,
                trade_row=trade_row,
            )
            if exit_photo_path is not None and exit_photo_path.is_file():
                await self.sender.send_photo_with_text(
                    photo_path=exit_photo_path,
                    text=promo_text,
                    chat_id=self.chat_id_promo,
                    message_thread_id=self.thread_id_promo,
                )
            else:
                await self.sender.send_text(
                    text=promo_text,
                    chat_id=self.chat_id_promo,
                    message_thread_id=self.thread_id_promo,
                )

    def _build_entry_text(
            self,
            *,
            snapshot,
            trade_id,
            local_symbol,
            side,
            quantity,
            placement,
    ):
        forecast_text = "нет forecast summary"

        if snapshot.forecast_summary is not None:
            direction = self._build_forecast_direction(snapshot.forecast_summary)
            forecast_text = (
                f"{direction} | "
                f"n={snapshot.forecast_summary['candidate_count']} | "
                f"up={snapshot.forecast_summary['positive_ratio']:.2f} | "
                f"down={snapshot.forecast_summary['negative_ratio']:.2f} | "
                f"mean={snapshot.forecast_summary['mean_final_move'] * 100:+.3f}% | "
                f"median={snapshot.forecast_summary['median_final_move'] * 100:+.3f}%"
            )

        entry_time = self._format_utc_ts(
            int(placement.done.checked_at_utc.timestamp())
            if placement.done is not None
            else int(placement.receipt.placed_at_utc.timestamp())
        )

        return (
            f"ОТКРЫТА СДЕЛКА №: {trade_id}\n"
            f"Инструмент: {local_symbol}\n"
            f"Сторона: {side}\n"
            f"Количество: {quantity}\n"
            f"Время UTC: {entry_time}\n"
            f"Окно анализа CT: {snapshot.hour_start_ct}\n"
            f"Торговый слот CT: {self._get_snapshot_trade_slot_start_ct(snapshot)}\n"
            f"bar_index: {snapshot.current_bar_index}\n"
            f"Цена входа: {placement.avg_fill_price}\n"
            f"Комиссия входа: {placement.total_commission}\n"
            f"Прогноз: {forecast_text}"
        )

    def _build_exit_text(
            self,
            *,
            snapshot,
            trade_id,
            entry_side,
            exit_side,
            quantity,
            placement,
    ):
        return (
            f"ЗАКРЫТА СДЕЛКА № {trade_id}\n"
            f"Инструмент: {self.instrument_code}\n"
            f"Сторона: {entry_side}\n"
            f"Количество: {quantity}\n"
            f"Окно анализа CT: {snapshot.hour_start_ct}\n"
            f"Торговый слот CT: {self._get_snapshot_trade_slot_start_ct(snapshot)}\n"
            f"Цена выхода: {placement.avg_fill_price}\n"
            f"Комиссия выхода: {placement.total_commission}\n"
            f"Realized PnL: {placement.realized_pnl}"
        )

    def _build_trading_entry_text(self, *, trade_id, side, local_symbol, placement):
        entry_time_ct = self._format_ct_from_placement(placement)
        return (
            f"ОТКРЫТА СДЕЛКА № {trade_id}\n"
            f"Инструмент: {local_symbol}\n"
            f"Направление: {side}\n"
            f"Время CT: {entry_time_ct}\n"
            f"Цена входа: {placement.avg_fill_price}"
        )

    def _build_trading_exit_text(self, *, trade_id, entry_side, placement, trade_row):
        exit_time_ct = self._format_ct_from_placement(placement)
        entry_price = self._safe_trade_value(trade_row, "entry_avg_fill_price")
        return (
            f"ЗАКРЫТА СДЕЛКА № {trade_id}\n"
            f"Направление: {entry_side}\n"
            f"Время CT: {exit_time_ct}\n"
            f"Цена входа: {entry_price}\n"
            f"Цена выхода: {placement.avg_fill_price}\n"
            f"PnL: {placement.realized_pnl}"
        )

    def _build_promo_entry_text(self, *, trade_id, side, local_symbol, quantity, placement):
        entry_time_ct = self._format_ct_from_placement(placement)
        return (
            f"Открыта сделка №{trade_id}\n"
            f"Инструмент: {local_symbol}\n"
            f"Направление: {side}\n"
            f"Время CT: {entry_time_ct}\n"
            f"Объём: {quantity}\n"
            f"Цена входа: {placement.avg_fill_price}"
        )

    def _build_promo_exit_text(self, *, trade_id, entry_side, quantity, placement, trade_row):
        exit_time_ct = self._format_ct_from_placement(placement)
        entry_price = self._safe_trade_value(trade_row, "entry_avg_fill_price")
        total_commissions = self._safe_trade_value(trade_row, "commissions_total")
        realized_pnl = self._safe_trade_value(
            trade_row,
            "realized_pnl",
            fallback=placement.realized_pnl,
        )
        return (
            f"Закрыта сделка №{trade_id}\n"
            f"Направление: {entry_side}\n"
            f"Время CT: {exit_time_ct}\n"
            f"Объём: {quantity}\n"
            f"Цена входа: {entry_price}\n"
            f"Цена выхода: {placement.avg_fill_price}\n"
            f"PnL: {realized_pnl}\n"
            f"Комиссия: {total_commissions}"
        )

    def _get_plot_candidates(self, snapshot):
        ranked_similarity_candidates = list(snapshot.ranked_similarity_candidates or [])
        if not ranked_similarity_candidates:
            return []

        forecast_summary = snapshot.forecast_summary or {}
        future_items = forecast_summary.get("future_items") or []

        if not future_items:
            return ranked_similarity_candidates

        forecast_analysis_window_start_ts = {
            item["hour_start_ts"]
            for item in future_items
            if item.get("hour_start_ts") is not None
        }
        if not forecast_analysis_window_start_ts:
            return ranked_similarity_candidates

        return [
            item
            for item in ranked_similarity_candidates
            if item["hour_start_ts"] in forecast_analysis_window_start_ts
        ]

    def _build_entry_plot(self, *, snapshot, pearson_live_runtime, trade_id):
        if pearson_live_runtime is None:
            return None
        if pearson_live_runtime.current_hour is None:
            return None

        current_values = list(pearson_live_runtime.current_hour.x)
        if not current_values:
            return None

        entry_x = snapshot.current_bar_index
        if entry_x is None or entry_x >= len(current_values):
            return None

        ranked_similarity_candidates = self._get_plot_candidates(snapshot)
        prepared_hours_map = pearson_live_runtime.current_hour_prepared_hours_map

        output_path = self.output_dir / (
            f"trade_entry_{trade_id}_{snapshot.hour_start_ts}_{snapshot.current_bar_index}.png"
        )

        plt.figure(figsize=(16, 9))
        current_x = list(range(len(current_values)))

        plt.plot(
            current_x,
            current_values,
            linewidth=2.5,
            label=f"Текущее окно анализа | CT {snapshot.hour_start_ct}",
        )

        for rank, item in enumerate(ranked_similarity_candidates, start=1):
            analysis_window_start_ts = item["hour_start_ts"]
            if analysis_window_start_ts not in prepared_hours_map:
                continue

            candidate_y = prepared_hours_map[analysis_window_start_ts]["y"]
            candidate_x = list(range(len(candidate_y)))

            plt.plot(
                candidate_x,
                candidate_y,
                linewidth=1.0,
                alpha=0.8,
                label=(
                    f"{rank}. {item['hour_start_ct']} CT | "
                    f"score={item['final_score']:.4f} | "
                    f"corr={item['pearson']:.4f}"
                ),
            )

        if snapshot.forecast_summary is not None:
            self._plot_forecast_paths(
                current_values=current_values,
                entry_x=entry_x,
                forecast_summary=snapshot.forecast_summary,
                mean_label="Средний future-path",
                median_label="Медианный future-path",
            )

        self._plot_trade_slot_lines(snapshot=snapshot)
        self._plot_entry_line(entry_x=entry_x)

        title_reason = "-"
        if snapshot.decision_result is not None:
            title_reason = snapshot.decision_result["decision"]

        plotted_count = len(ranked_similarity_candidates)

        plt.title(
            f"Trade Entry | {title_reason} | "
            f"analysis CT {snapshot.hour_start_ct} | "
            f"trade slot CT {self._get_snapshot_trade_slot_start_ct(snapshot)} | "
            f"trade_id={trade_id} | forecast_n={plotted_count}"
        )
        plt.xlabel("bar_index внутри 60-минутного окна анализа")
        plt.ylabel("y")
        plt.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=8)
        plt.grid(True)
        plt.tight_layout(rect=[0, 0, 0.80, 1])

        plt.savefig(output_path, dpi=150)
        plt.close()

        return output_path

    def _build_promo_entry_plot(self, *, snapshot, pearson_live_runtime, trade_id):
        if pearson_live_runtime is None:
            return None
        if pearson_live_runtime.current_hour is None:
            return None

        current_values = list(pearson_live_runtime.current_hour.x)
        if not current_values:
            return None

        entry_x = snapshot.current_bar_index
        if entry_x is None or entry_x >= len(current_values):
            return None

        output_path = self.output_dir / (
            f"trade_entry_promo_{trade_id}_{snapshot.hour_start_ts}_{snapshot.current_bar_index}.png"
        )

        plt.figure(figsize=(12, 7))
        current_x = list(range(len(current_values)))

        plt.plot(
            current_x,
            current_values,
            linewidth=2.5,
            label=f"Окно анализа | CT {snapshot.hour_start_ct}",
        )

        entry_y = current_values[entry_x]
        plt.scatter([entry_x], [entry_y], s=60, label="Точка входа")
        self._plot_trade_slot_lines(snapshot=snapshot)
        self._plot_entry_line(entry_x=entry_x, label="Вход")

        if snapshot.forecast_summary is not None:
            self._plot_forecast_paths(
                current_values=current_values,
                entry_x=entry_x,
                forecast_summary=snapshot.forecast_summary,
                mean_label="Прогноз 1",
                median_label="Прогноз 2",
            )

        decision = "-"
        if snapshot.decision_result is not None:
            decision = snapshot.decision_result.get("decision", "-")

        plt.title(
            f"Сделка | {decision} | "
            f"analysis CT {snapshot.hour_start_ct} | "
            f"slot CT {self._get_snapshot_trade_slot_start_ct(snapshot)} | №{trade_id}"
        )
        plt.ylabel("y")
        plt.legend(loc="best", fontsize=9)
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()

        return output_path

    def _build_exit_plot(self, *, snapshot, trade_id, trade_row):
        if trade_row is None:
            return None

        signal_analysis_window_start_ts = self._safe_trade_value(
            trade_row,
            "signal_hour_start_ts",
            fallback=None,
        )
        signal_bar_index = self._safe_trade_value(
            trade_row,
            "signal_bar_index",
            fallback=None,
        )
        signal_analysis_window_start_ct = self._safe_trade_value(
            trade_row,
            "signal_hour_start_ct",
            fallback="-",
        )
        forecast_summary = self._load_trade_json(trade_row, "forecast_summary_json") or {}

        if signal_analysis_window_start_ts is None or signal_bar_index is None:
            return None

        signal_analysis_window_start_ts = int(signal_analysis_window_start_ts)
        signal_bar_index = int(signal_bar_index)

        exit_in_signal_analysis_window = snapshot.hour_start_ts == signal_analysis_window_start_ts
        max_bar_index = None
        fact_label = "Факт окна анализа"
        exit_bar_index = None

        if exit_in_signal_analysis_window:
            if snapshot.current_bar_index is None:
                return None
            exit_bar_index = int(snapshot.current_bar_index)
            max_bar_index = exit_bar_index
            fact_label = "Факт до выхода"

        window_rows = self._load_signal_analysis_window_price_rows(
            signal_analysis_window_start_ts=signal_analysis_window_start_ts,
            max_bar_index=max_bar_index,
        )
        current_values = self._build_normalized_analysis_window_values(window_rows)

        if not current_values:
            return None
        if signal_bar_index >= len(current_values):
            return None

        output_path = self.output_dir / f"trade_exit_{trade_id}_{signal_analysis_window_start_ts}.png"

        plt.figure(figsize=(12, 7))
        current_x = list(range(len(current_values)))

        plt.plot(
            current_x,
            current_values,
            linewidth=2.5,
            label=f"{fact_label} | CT {signal_analysis_window_start_ct}",
        )

        entry_x = signal_bar_index
        entry_y = current_values[entry_x]
        plt.scatter([entry_x], [entry_y], s=60, label="Точка входа")
        self._plot_trade_slot_lines_for_analysis_window()
        self._plot_entry_line(entry_x=entry_x, label="Вход")

        if exit_bar_index is not None and exit_bar_index < len(current_values):
            exit_y = current_values[exit_bar_index]
            plt.scatter([exit_bar_index], [exit_y], s=60, label="Точка выхода")
            plt.axvline(
                x=exit_bar_index,
                linestyle=":",
                linewidth=1.5,
                label="Выход",
            )

        if forecast_summary:
            self._plot_forecast_paths(
                current_values=current_values,
                entry_x=entry_x,
                forecast_summary=forecast_summary,
                mean_label="Прогноз 1",
                median_label="Прогноз 2",
            )

        full_window_text = "yes" if not exit_in_signal_analysis_window else "no"
        plt.title(
            f"Trade Exit | analysis CT {signal_analysis_window_start_ct} | "
            f"trade_id={trade_id} | full_window={full_window_text}"
        )
        plt.xlabel("bar_index внутри 60-минутного окна анализа")
        plt.ylabel("y")
        plt.legend(loc="best", fontsize=9)
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()

        return output_path

    def _load_signal_analysis_window_price_rows(self, *, signal_analysis_window_start_ts, max_bar_index=None):
        upper_ts_exclusive = signal_analysis_window_start_ts + ANALYSIS_WINDOW_SECONDS

        if max_bar_index is not None:
            upper_ts_exclusive = min(
                upper_ts_exclusive,
                signal_analysis_window_start_ts + ((int(max_bar_index) + 1) * self.bar_interval_seconds),
            )

        conn = sqlite3.connect(self.price_db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=5000;")
            sql = f"""
                SELECT
                    bar_time_ts,
                    ask_open,
                    bid_open,
                    ask_close,
                    bid_close
                FROM {self.table_name}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts < ?
                  AND ask_open IS NOT NULL
                  AND bid_open IS NOT NULL
                  AND ask_close IS NOT NULL
                  AND bid_close IS NOT NULL
                ORDER BY bar_time_ts
            """
            return conn.execute(sql, (signal_analysis_window_start_ts, upper_ts_exclusive)).fetchall()
        finally:
            conn.close()

    @staticmethod
    def _build_normalized_analysis_window_values(window_rows):
        if not window_rows:
            return []

        mid_open_0 = None
        values = []

        for row in window_rows:
            ask_open = row["ask_open"]
            bid_open = row["bid_open"]
            ask_close = row["ask_close"]
            bid_close = row["bid_close"]

            if mid_open_0 is None:
                mid_open_0 = (ask_open + bid_open) / 2.0
                if mid_open_0 == 0.0:
                    return []

            mid_close = (ask_close + bid_close) / 2.0
            values.append((mid_close / mid_open_0) - 1.0)

        return values

    @staticmethod
    def _project_future_path(*, entry_y, rel_path):
        return [((1.0 + entry_y) * (1.0 + rel_move)) - 1.0 for rel_move in rel_path]

    def _plot_forecast_paths(self, *, current_values, entry_x, forecast_summary, mean_label, median_label):
        if entry_x is None or entry_x >= len(current_values):
            return

        entry_y = current_values[entry_x]
        mean_future_path = forecast_summary.get("mean_future_path") or []
        median_future_path = forecast_summary.get("median_future_path") or []

        if mean_future_path:
            start_x = entry_x + 1
            future_x = list(range(start_x, start_x + len(mean_future_path)))
            future_y = self._project_future_path(entry_y=entry_y, rel_path=mean_future_path)
            plt.plot(
                future_x,
                future_y,
                linewidth=2.0,
                linestyle="--",
                label=mean_label,
            )

        if median_future_path:
            start_x = entry_x + 1
            future_x = list(range(start_x, start_x + len(median_future_path)))
            future_y = self._project_future_path(entry_y=entry_y, rel_path=median_future_path)
            plt.plot(
                future_x,
                future_y,
                linewidth=2.0,
                linestyle=":",
                label=median_label,
            )

    @staticmethod
    def _plot_entry_line(*, entry_x, label="Точка входа"):
        plt.axvline(
            x=entry_x,
            linestyle="--",
            linewidth=1.5,
            label=f"{label}: bar_index={entry_x}",
        )

    @staticmethod
    def _plot_trade_slot_lines_for_analysis_window():
        plt.axvline(
            x=360,
            linestyle="-.",
            linewidth=1.2,
            label="Начало торгового слота",
        )
        plt.axvline(
            x=600,
            linestyle="-.",
            linewidth=1.2,
            label="Конец окна входа",
        )

    def _plot_trade_slot_lines(self, *, snapshot):
        if snapshot.current_bar_index is None:
            return
        self._plot_trade_slot_lines_for_analysis_window()

    @staticmethod
    def _build_forecast_direction(forecast_summary):
        mean_final_move = forecast_summary["mean_final_move"]
        median_final_move = forecast_summary["median_final_move"]

        if mean_final_move > 0.0 and median_final_move > 0.0:
            return "UP"
        if mean_final_move < 0.0 and median_final_move < 0.0:
            return "DOWN"
        return "MIXED"

    @staticmethod
    def _format_utc_ts(ts):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _format_ct_ts(ts):
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(CHICAGO_TZ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    def _format_ct_from_placement(self, placement):
        ts = (
            int(placement.done.checked_at_utc.timestamp())
            if placement.done is not None
            else int(placement.receipt.placed_at_utc.timestamp())
        )
        return self._format_ct_ts(ts)

    @staticmethod
    def _floor_to_half_hour_ts(ts: int) -> int:
        return (int(ts) // HALF_HOUR_SECONDS) * HALF_HOUR_SECONDS

    def _get_snapshot_trade_slot_start_ts(self, snapshot):
        bar_time_ts = getattr(snapshot, "last_bar_time_ts", None)
        if bar_time_ts is None:
            return None
        return self._floor_to_half_hour_ts(int(bar_time_ts))

    def _get_snapshot_trade_slot_start_ct(self, snapshot):
        trade_slot_start_ts = self._get_snapshot_trade_slot_start_ts(snapshot)
        if trade_slot_start_ts is None:
            return None
        return self._format_ct_ts(trade_slot_start_ts)

    def _load_trade_row(self, trade_id):
        conn = sqlite3.connect(self.trade_db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT * FROM trades WHERE trade_id = ?",
                (trade_id,),
            ).fetchone()
            return row
        finally:
            conn.close()

    @staticmethod
    def _load_trade_json(row, key):
        if row is None:
            return None

        value = row[key]
        if not value:
            return None

        return json.loads(value)

    @staticmethod
    def _safe_trade_value(row, key, fallback="-"):
        if row is None:
            return fallback

        value = row[key]
        if value is None:
            return fallback

        return value

    @staticmethod
    def _parse_bar_interval_seconds(bar_size_setting):
        value = bar_size_setting.strip().lower()

        if value.endswith("secs"):
            return int(value.replace("secs", "").strip())
        if value.endswith("sec"):
            return int(value.replace("sec", "").strip())
        if value.endswith("mins"):
            return int(value.replace("mins", "").strip()) * 60
        if value.endswith("min"):
            return int(value.replace("min", "").strip()) * 60
        if value.endswith("hours"):
            return int(value.replace("hours", "").strip()) * 3600
        if value.endswith("hour"):
            return int(value.replace("hour", "").strip()) * 3600

        raise ValueError(f"Неподдерживаемый barSizeSetting={bar_size_setting}")

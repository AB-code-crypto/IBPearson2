import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt

from core.telegram_sender import TelegramSender

CHICAGO_TZ = ZoneInfo("America/Chicago")


class TradeTelegramNotifier:
    # Отправляет торговые уведомления в Telegram:
    # - подробный common-формат как и раньше
    # - короткий trading-формат
    # - promo-формат в тему группы
    def __init__(self, settings, instrument_code="MNQ"):
        self.settings = settings
        self.instrument_code = instrument_code
        self.sender = TelegramSender(settings)

        self.chat_id_common = settings.telegram_chat_id_common
        self.chat_id_trading = settings.telegram_chat_id_trading
        self.chat_id_promo = settings.telegram_chat_id_promo
        self.thread_id_promo = settings.telegram_thread_id_promo

        self.trade_db_path = settings.trade_db_path
        self.output_dir = Path(settings.trade_db_path).resolve().parent / "telegram_trade_plots"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Текстовый блок пока оставляем компактным.
        self.max_text_candidates = 3

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
        # 1) Подробный common-канал: как и раньше.
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

        # 2) Короткий trading-канал: только факт открытия сделки.
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

        # 3) Promo-группа: упрощённая картинка + короткий текст в тему.
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
        # 1) Подробный common-канал: как и раньше.
        common_text = self._build_exit_text(
            snapshot=snapshot,
            trade_id=trade_id,
            entry_side=entry_side,
            exit_side=exit_side,
            quantity=quantity,
            placement=placement,
        )
        if self.chat_id_common:
            await self.sender.send_text(
                text=common_text,
                chat_id=self.chat_id_common,
            )

        trade_row = self._load_trade_row(trade_id)

        # 2) Короткий trading-канал.
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

        # 3) Promo-группа в тему.
        if self.chat_id_promo:
            promo_text = self._build_promo_exit_text(
                trade_id=trade_id,
                entry_side=entry_side,
                quantity=quantity,
                placement=placement,
                trade_row=trade_row,
            )
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
        best_similarity_score = None
        best_candidate_lines = []
        if snapshot.ranked_similarity_candidates:
            best_similarity_score = snapshot.ranked_similarity_candidates[0]["final_score"]
            for index, item in enumerate(
                    snapshot.ranked_similarity_candidates[: self.max_text_candidates],
                    start=1,
            ):
                best_candidate_lines.append(
                    f"{index}) {item['hour_start_ct']} CT | "
                    f"score={item['final_score']:.4f} | "
                    f"corr={item['pearson']:.4f}"
                )

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

        candidates_block = "\n".join(best_candidate_lines) if best_candidate_lines else "нет"
        best_similarity_text = "-"
        if best_similarity_score is not None:
            best_similarity_text = f"{best_similarity_score:.4f}"
        entry_time = self._format_utc_ts(
            int(placement.done.checked_at_utc.timestamp()) if placement.done is not None
            else int(placement.receipt.placed_at_utc.timestamp())
        )

        return (
            f"ОТКРЫТА СДЕЛКА  №: {trade_id}\n"
            f"Инструмент: {local_symbol}\n"
            f"Сторона: {side}\n"
            f"Количество: {quantity}\n"
            f"Время UTC: {entry_time}\n"
            f"Час CT: {snapshot.hour_start_ct}\n"
            f"bar_index: {snapshot.current_bar_index}\n"
            f"Цена входа: {placement.avg_fill_price}\n"
            f"Комиссия входа: {placement.total_commission}\n"
            f"Лучший similarity-score: {best_similarity_text}\n"
            f"Прогноз: {forecast_text}\n"
            f"Лучшие кандидаты:\n{candidates_block}"
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
            f"ЗАКРЫТА СДЕЛКА №  {trade_id}\n"
            f"Инструмент: {self.instrument_code}\n"
            f"Сторона: {entry_side}\n"
            f"Количество: {quantity}\n"
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
        realized_pnl = self._safe_trade_value(trade_row, "realized_pnl", fallback=placement.realized_pnl)
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

        # fallback: если forecast_summary почему-то пустой, рисуем всё, что есть в snapshot
        if not future_items:
            return ranked_similarity_candidates

        forecast_hour_start_ts = {
            item["hour_start_ts"]
            for item in future_items
            if item.get("hour_start_ts") is not None
        }
        if not forecast_hour_start_ts:
            return ranked_similarity_candidates

        # Очень важно: сохраняем порядок ranked_similarity_candidates,
        # чтобы rank на графике совпадал с similarity-ranking.
        return [
            item
            for item in ranked_similarity_candidates
            if item["hour_start_ts"] in forecast_hour_start_ts
        ]

    def _build_entry_plot(self, *, snapshot, pearson_live_runtime, trade_id):
        if pearson_live_runtime is None:
            return None

        if pearson_live_runtime.current_hour is None:
            return None

        current_values = list(pearson_live_runtime.current_hour.x)
        if not current_values:
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
            label=f"Текущий час | CT {snapshot.hour_start_ct}",
        )

        for rank, item in enumerate(ranked_similarity_candidates, start=1):
            hour_start_ts = item["hour_start_ts"]
            if hour_start_ts not in prepared_hours_map:
                continue

            candidate_y = prepared_hours_map[hour_start_ts]["y"]
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
            mean_future_path = snapshot.forecast_summary.get("mean_future_path") or []
            median_future_path = snapshot.forecast_summary.get("median_future_path") or []

            if mean_future_path:
                start_x = snapshot.current_bar_index + 1
                future_x = list(range(start_x, start_x + len(mean_future_path)))
                future_y = [current_values[-1] + value for value in mean_future_path]

                plt.plot(
                    future_x,
                    future_y,
                    linewidth=2.0,
                    linestyle="--",
                    label="Средний future-path",
                )

            if median_future_path:
                start_x = snapshot.current_bar_index + 1
                future_x = list(range(start_x, start_x + len(median_future_path)))
                future_y = [current_values[-1] + value for value in median_future_path]

                plt.plot(
                    future_x,
                    future_y,
                    linewidth=2.0,
                    linestyle=":",
                    label="Медианный future-path",
                )

        plt.axvline(
            x=snapshot.current_bar_index,
            linestyle="--",
            linewidth=1.5,
            label=f"Точка входа: bar_index={snapshot.current_bar_index}",
        )

        title_reason = "-"
        if snapshot.decision_result is not None:
            title_reason = snapshot.decision_result["decision"]

        plotted_count = len(ranked_similarity_candidates)

        plt.title(
            f"Trade Entry | {title_reason} | CT {snapshot.hour_start_ct} | "
            f"trade_id={trade_id} | forecast_n={plotted_count}"
        )
        plt.xlabel("bar_index")
        plt.ylabel("y")

        # Когда линий много, легенду лучше вынести наружу.
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

        output_path = self.output_dir / (
            f"trade_entry_promo_{trade_id}_{snapshot.hour_start_ts}_{snapshot.current_bar_index}.png"
        )

        plt.figure(figsize=(12, 7))
        current_x = list(range(len(current_values)))
        plt.plot(
            current_x,
            current_values,
            linewidth=2.5,
            label=f"Текущий час | CT {snapshot.hour_start_ct}",
        )

        entry_x = snapshot.current_bar_index
        entry_y = current_values[-1]
        plt.scatter([entry_x], [entry_y], s=60, label="Точка входа")
        plt.axvline(
            x=entry_x,
            linestyle="--",
            linewidth=1.5,
            label=f"Вход",
        )

        if snapshot.forecast_summary is not None:
            mean_future_path = snapshot.forecast_summary.get("mean_future_path") or []
            median_future_path = snapshot.forecast_summary.get("median_future_path") or []

            if mean_future_path:
                start_x = entry_x + 1
                future_x = list(range(start_x, start_x + len(mean_future_path)))
                future_y = [current_values[-1] + value for value in mean_future_path]
                plt.plot(
                    future_x,
                    future_y,
                    linewidth=2.0,
                    linestyle="--",
                    label="Прогноз 1",
                )

            if median_future_path:
                start_x = entry_x + 1
                future_x = list(range(start_x, start_x + len(median_future_path)))
                future_y = [current_values[-1] + value for value in median_future_path]
                plt.plot(
                    future_x,
                    future_y,
                    linewidth=2.0,
                    linestyle=":",
                    label="Прогноз 2",
                )

        decision = "-"
        if snapshot.decision_result is not None:
            decision = snapshot.decision_result.get("decision", "-")
        plt.title(f"Сделка | {decision} | CT {snapshot.hour_start_ct} | №{trade_id}")
        plt.ylabel("y")
        plt.legend(loc="best", fontsize=9)
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()
        return output_path

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
        ts = int(placement.done.checked_at_utc.timestamp()) if placement.done is not None else int(
            placement.receipt.placed_at_utc.timestamp()
        )
        return self._format_ct_ts(ts)

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
    def _safe_trade_value(row, key, fallback="-"):
        if row is None:
            return fallback
        value = row[key]
        if value is None:
            return fallback
        return value

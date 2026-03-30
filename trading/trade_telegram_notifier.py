from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt

from core.telegram_sender import TelegramSender


class TradeTelegramNotifier:
    # Отправляет торговые уведомления в Telegram:
    # - текст при закрытии сделки
    # - фото + текст при открытии сделки
    #
    # Канал назначения берём из settings.telegram_chat_id_common,
    # как и просил пользователь.
    def __init__(self, settings, instrument_code="MNQ"):
        self.settings = settings
        self.instrument_code = instrument_code

        self.sender = TelegramSender(settings)
        self.chat_id = settings.telegram_chat_id_common

        self.output_dir = Path(settings.trade_db_path).resolve().parent / "telegram_trade_plots"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.max_plot_candidates = 5
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
        text = self._build_entry_text(
            snapshot=snapshot,
            trade_id=trade_id,
            local_symbol=local_symbol,
            side=side,
            quantity=quantity,
            placement=placement,
        )

        photo_path = None

        try:
            photo_path = self._build_entry_plot(
                snapshot=snapshot,
                pearson_live_runtime=pearson_live_runtime,
                trade_id=trade_id,
            )
        except Exception:
            photo_path = None

        if photo_path is not None and photo_path.is_file():
            await self.sender.send_photo_with_text(
                photo_path=photo_path,
                text=text,
                chat_id=self.chat_id,
            )
        else:
            await self.sender.send_text(
                text=text,
                chat_id=self.chat_id,
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
        text = self._build_exit_text(
            snapshot=snapshot,
            trade_id=trade_id,
            entry_side=entry_side,
            exit_side=exit_side,
            quantity=quantity,
            placement=placement,
        )

        await self.sender.send_text(
            text=text,
            chat_id=self.chat_id,
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

            for index, item in enumerate(snapshot.ranked_similarity_candidates[: self.max_text_candidates], start=1):
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

        decision_reason = "-"
        if snapshot.decision_result is not None:
            decision_reason = snapshot.decision_result["reason"]

        entry_time = self._format_utc_ts(
            int(placement.done.checked_at_utc.timestamp()) if placement.done is not None
            else int(placement.receipt.placed_at_utc.timestamp())
        )

        candidates_block = "\n".join(best_candidate_lines) if best_candidate_lines else "нет"

        best_similarity_text = "-"
        if best_similarity_score is not None:
            best_similarity_text = f"{best_similarity_score:.4f}"

        return (
            f"ОТКРЫТА СДЕЛКА\n"
            f"trade_id: {trade_id}\n"
            f"Инструмент: {local_symbol}\n"
            f"Сторона: {side}\n"
            f"Количество: {quantity}\n"
            # f"Время UTC: {entry_time}\n"
            f"Час CT: {snapshot.hour_start_ct}\n"
            f"bar_index: {snapshot.current_bar_index}\n"
            f"Цена входа: {placement.avg_fill_price}\n"
            f"Комиссия входа: {placement.total_commission}\n"
            f"Направление: {snapshot.decision_result['decision'] if snapshot.decision_result else '-'}\n"
            # f"Причина: {decision_reason}\n"
            f"Лучший similarity-score: {best_similarity_text}\n"
            f"Forecast: {forecast_text}\n"
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
        exit_time = self._format_utc_ts(
            int(placement.done.checked_at_utc.timestamp()) if placement.done is not None
            else int(placement.receipt.placed_at_utc.timestamp())
        )

        return (
            f"ЗАКРЫТА СДЕЛКА\n"
            f"trade_id: {trade_id}\n"
            f"Инструмент: {self.instrument_code}\n"
            f"Сторона входа: {entry_side}\n"
            f"Сторона выхода: {exit_side}\n"
            f"Количество: {quantity}\n"
            # f"Время UTC: {exit_time}\n"
            f"Час CT: {snapshot.hour_start_ct}\n"
            # f"bar_index: {snapshot.current_bar_index}\n"
            f"Цена выхода: {placement.avg_fill_price}\n"
            f"Комиссия выхода: {placement.total_commission}\n"
            f"Realized PnL: {placement.realized_pnl}"
        )

    def _build_entry_plot(self, *, snapshot, pearson_live_runtime, trade_id):
        if pearson_live_runtime is None:
            return None

        if pearson_live_runtime.current_hour is None:
            return None

        current_values = list(pearson_live_runtime.current_hour.x)
        if not current_values:
            return None

        ranked_similarity_candidates = snapshot.ranked_similarity_candidates[: self.max_plot_candidates]
        prepared_hours_map = pearson_live_runtime.current_hour_prepared_hours_map

        output_path = self.output_dir / (
            f"trade_entry_{trade_id}_{snapshot.hour_start_ts}_{snapshot.current_bar_index}.png"
        )

        plt.figure(figsize=(14, 8))

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
            mean_future_path = snapshot.forecast_summary["mean_future_path"]
            median_future_path = snapshot.forecast_summary["median_future_path"]

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

        plt.title(
            f"Trade Entry | {title_reason} | CT {snapshot.hour_start_ct} | trade_id={trade_id}"
        )
        plt.xlabel("bar_index")
        plt.ylabel("y")
        plt.legend(loc="best", fontsize=8)
        plt.grid(True)

        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()

        return output_path

    def _build_forecast_direction(self, forecast_summary):
        mean_final_move = forecast_summary["mean_final_move"]
        median_final_move = forecast_summary["median_final_move"]

        if mean_final_move > 0.0 and median_final_move > 0.0:
            return "UP"

        if mean_final_move < 0.0 and median_final_move < 0.0:
            return "DOWN"

        return "MIXED"

    def _format_utc_ts(self, ts):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

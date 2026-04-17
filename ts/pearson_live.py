import sqlite3
from dataclasses import dataclass
from typing import Optional

from contracts import Instrument
from core.db_initializer import build_table_name
from core.logger import get_logger, log_warning
from ts.candidate_decision import evaluate_decision_layer
from ts.candidate_forecast import build_group_forecast_from_prepared_candidates
from ts.pearson_runtime import PearsonCurrentHour
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS, StrategyParams

logger = get_logger(__name__)


@dataclass
class PearsonLiveSnapshot:
    # Снимок текущего состояния live-runtime первого шага Пирсона.
    hour_start_ts: Optional[int]
    hour_start_ts_ct: Optional[int]
    hour_start_ct: Optional[str]
    hour_slot_ct: Optional[int]

    last_bar_time_ts: Optional[int]
    current_bar_count: int
    current_bar_index: Optional[int]
    expected_next_bar_time_ts: Optional[int]

    search_window_active: bool
    search_window_start_bar_count: int
    search_window_end_bar_count_exclusive: int

    current_hour_valid: bool
    current_hour_invalid_reason: Optional[str]

    allowed_hour_slots: list[int]
    history_candidate_count: int
    candidates_initialized: bool
    correlation_calculated: bool
    similarity_calculated: bool
    forecast_calculated: bool
    decision_calculated: bool

    ranked_candidates: list[dict]
    ranked_similarity_candidates: list[dict]
    forecast_summary: Optional[dict]
    decision_result: Optional[dict]


def floor_to_hour_ts(ts):
    # Округляем Unix timestamp вниз до точного начала часа.
    return (ts // 3600) * 3600


class PearsonLiveRuntime:
    # Боевой live-runtime для первого шага по корреляции Пирсона.
    #
    # Идея:
    # - живём от потока закрытых 5-секундных баров;
    # - внутри активного часа копим текущий x-ряд;
    # - как только приходит первый бар нового часа,
    #   полностью сбрасываем состояние и начинаем новый час;
    # - historical candidates загружаем сразу в начале часа;
    # - расчёт корреляции включаем только в разрешённом окне.
    def __init__(
            self,
            settings,
            instrument_code="MNQ",
            min_correlation=None,
            top_n=None,
            strategy_params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
    ):
        instrument_row = Instrument[instrument_code]

        self.settings = settings
        self.instrument_code = instrument_code
        self.strategy_params = strategy_params
        self.table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        if min_correlation is None:
            min_correlation = self.strategy_params.pearson_shortlist_min_correlation

        if top_n is None:
            top_n = self.strategy_params.pearson_shortlist_top_n

        self.min_correlation = min_correlation
        self.top_n = top_n

        self.current_hour = None
        self.current_hour_prepared_hours_map = {}
        self.current_hour_valid = True
        self.current_hour_invalid_reason = None
        self.current_hour_last_bar_time_ts = None
        self.current_hour_expected_next_bar_time_ts = None

        self.allowed_hour_slots = []
        self.startup_backfill_completed = False
        self.last_snapshot = self._build_empty_snapshot()

    def on_closed_bar(self, bar):
        # Главная точка входа.
        #
        # Ожидаем bar как mapping с ключами:
        # - bar_time_ts
        # - bar_time_ts_ct
        # - ask_open
        # - bid_open
        # - ask_close
        # - bid_close
        bar_time_ts = bar["bar_time_ts"]
        bar_time_ts_ct = bar["bar_time_ts_ct"]

        bar_hour_start_ts = floor_to_hour_ts(bar_time_ts)
        bar_hour_start_ts_ct = floor_to_hour_ts(bar_time_ts_ct)

        if self.current_hour is None:
            self._start_new_hour(bar_hour_start_ts, bar_hour_start_ts_ct)

        elif bar_hour_start_ts_ct < self.current_hour.hour_start_ts_ct:
            raise ValueError(
                f"Получен бар из прошлого часа: "
                f"bar_time_ts={bar_time_ts}, "
                f"bar_time_ts_ct={bar_time_ts_ct}, "
                f"current_hour_start_ts_ct={self.current_hour.hour_start_ts_ct}"
            )

        elif bar_hour_start_ts_ct > self.current_hour.hour_start_ts_ct:
            self._start_new_hour(bar_hour_start_ts, bar_hour_start_ts_ct)

        self._append_bar_to_current_hour(bar)

        correlation_calculated = False
        similarity_calculated = False
        forecast_calculated = False
        decision_calculated = False
        ranked_candidates = []
        ranked_similarity_candidates = []
        forecast_summary = None
        decision_result = None

        if self.current_hour_valid and self._is_search_window_active():
            if not self.current_hour.candidates_initialized:
                self.current_hour.initialize_candidates()
            else:
                self.current_hour.update_candidates_for_last_bar()

            ranked_candidates = self.current_hour.get_ranked_candidates(
                min_correlation=self.min_correlation,
                top_n=self.top_n,
            )
            correlation_calculated = True

            if not self.strategy_params.pearson_has_enough_shortlist_candidates(
                    len(ranked_candidates)
            ):
                self.last_snapshot = self._build_snapshot(
                    correlation_calculated=correlation_calculated,
                    similarity_calculated=similarity_calculated,
                    forecast_calculated=forecast_calculated,
                    decision_calculated=decision_calculated,
                    ranked_candidates=ranked_candidates,
                    ranked_similarity_candidates=ranked_similarity_candidates,
                    forecast_summary=forecast_summary,
                    decision_result=decision_result,
                )

                return self.last_snapshot

            ranked_similarity_candidates = self._rank_similarity_candidates(
                ranked_candidates=ranked_candidates,
            )
            similarity_calculated = True

            forecast_summary = self._build_forecast_summary(
                ranked_similarity_candidates=ranked_similarity_candidates,
            )
            forecast_calculated = True

            decision_result = self._build_decision_result(
                ranked_similarity_candidates=ranked_similarity_candidates,
                forecast_summary=forecast_summary,
            )
            decision_calculated = True

        self.last_snapshot = self._build_snapshot(
            correlation_calculated=correlation_calculated,
            similarity_calculated=similarity_calculated,
            forecast_calculated=forecast_calculated,
            decision_calculated=decision_calculated,
            ranked_candidates=ranked_candidates,
            ranked_similarity_candidates=ranked_similarity_candidates,
            forecast_summary=forecast_summary,
            decision_result=decision_result,
        )

        return self.last_snapshot

    def get_last_snapshot(self):
        # Возвращаем последний уже построенный snapshot.
        return self.last_snapshot

    def _start_new_hour(self, hour_start_ts, hour_start_ts_ct):
        # Полностью переключаем runtime на новый час.
        self.current_hour = PearsonCurrentHour(hour_start_ts, hour_start_ts_ct)
        self.current_hour_valid = True
        self.current_hour_invalid_reason = None
        self.current_hour_last_bar_time_ts = None
        self.current_hour_expected_next_bar_time_ts = hour_start_ts

        self.allowed_hour_slots = self.strategy_params.resolve_allowed_hour_slots(
            self.current_hour.hour_slot_ct
        )

        prepared_hours = self._load_candidates_for_current_hour()
        self.current_hour_prepared_hours_map = {
            item["hour_start_ts"]: item for item in prepared_hours
        }
        self.current_hour.set_candidates(prepared_hours)

    def _load_candidates_for_current_hour(self):
        # Загружаем всех historical candidates сразу в начале часа.
        #
        # Берём только:
        # - разрешённые hour_slot;
        # - часы строго раньше текущего часа.
        prepared_conn = sqlite3.connect(self.settings.prepared_db_path)

        try:
            prepared_conn.row_factory = sqlite3.Row
            prepared_conn.execute("PRAGMA busy_timeout=5000;")

            prepared_hours = load_prepared_hours_by_slots(
                prepared_conn=prepared_conn,
                table_name=self.table_name,
                hour_slots_ct=self.allowed_hour_slots,
                before_hour_start_ts_ct=self.current_hour.hour_start_ts_ct,
            )

            return prepared_hours

        finally:
            prepared_conn.close()

    def _load_existing_current_hour_bars_from_db(self, from_bar_time_ts, to_bar_time_ts_exclusive):
        # Загружаем уже существующие в price DB полностью собранные бары
        # текущего часа в диапазоне [from_bar_time_ts, to_bar_time_ts_exclusive).
        if to_bar_time_ts_exclusive <= from_bar_time_ts:
            return []

        price_conn = sqlite3.connect(self.settings.price_db_path)
        try:
            price_conn.row_factory = sqlite3.Row
            price_conn.execute("PRAGMA busy_timeout=5000;")

            sql = f"""
                SELECT
                    bar_time_ts,
                    bar_time_ts_ct,
                    bar_time_ct,
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
            return price_conn.execute(sql, (from_bar_time_ts, to_bar_time_ts_exclusive)).fetchall()
        finally:
            price_conn.close()

    def _append_complete_bar_without_gap_checks(self, bar_row):
        # Добавляем уже проверенный полный бар без повторной gap-валидации.
        self.current_hour.add_bar(
            ask_open=bar_row["ask_open"],
            bid_open=bar_row["bid_open"],
            ask_close=bar_row["ask_close"],
            bid_close=bar_row["bid_close"],
        )
        self.current_hour_last_bar_time_ts = bar_row["bar_time_ts"]
        self.current_hour_expected_next_bar_time_ts = (
                bar_row["bar_time_ts"] + self.strategy_params.pearson_bar_interval_seconds
        )

    def _try_hydrate_current_hour_from_db(self, target_bar_time_ts):
        # Пытаемся догрузить в runtime уже существующие в БД бары текущего часа
        # до target_bar_time_ts (не включая его).
        if self.current_hour is None:
            return
        if self.current_hour_expected_next_bar_time_ts is None:
            return
        if target_bar_time_ts <= self.current_hour_expected_next_bar_time_ts:
            return

        db_rows = self._load_existing_current_hour_bars_from_db(
            from_bar_time_ts=self.current_hour_expected_next_bar_time_ts,
            to_bar_time_ts_exclusive=target_bar_time_ts,
        )
        for row in db_rows:
            row_bar_time_ts = row["bar_time_ts"]
            if row_bar_time_ts < self.current_hour_expected_next_bar_time_ts:
                continue
            if row_bar_time_ts > self.current_hour_expected_next_bar_time_ts:
                # В БД всё ещё есть реальная дырка.
                break
            self._append_complete_bar_without_gap_checks(row)

    def mark_startup_backfill_completed(self, sync_ts=None):
        # Фиксируем, что стартовая разовая докачка последнего часа завершилась.
        # После этого gap-check внутри текущего часа снова работает в обычном режиме.
        self.startup_backfill_completed = True

    def _rank_similarity_candidates(self, ranked_candidates):
        # На вход берём уже готовый shortlist после первого Пирсона.
        #
        # Важно:
        # здесь не работаем по всей истории, а только по тем кандидатам,
        # которых уже отобрал и отсортировал первый Пирсон.
        if self.current_hour is None:
            return []

        if not ranked_candidates:
            return []

        shortlist_prepared_hours = []

        for item in ranked_candidates:
            hour_start_ts = item["hour_start_ts"]

            if hour_start_ts not in self.current_hour_prepared_hours_map:
                raise ValueError(
                    f"Не найден prepared-кандидат для hour_start_ts={hour_start_ts}"
                )

            shortlist_prepared_hour = dict(
                self.current_hour_prepared_hours_map[hour_start_ts]
            )
            shortlist_prepared_hour["correlation"] = item["correlation"]
            shortlist_prepared_hours.append(shortlist_prepared_hour)

        ranked_similarity_candidates = rank_prepared_candidates_by_similarity(
            current_values=self.current_hour.x,
            prepared_hours=shortlist_prepared_hours,
            params=self.strategy_params,
        )

        return ranked_similarity_candidates

    def _build_forecast_summary(self, ranked_similarity_candidates):
        # Строим сводный прогноз по лучшим similarity-кандидатам.
        #
        # Здесь тоже не работаем по всей истории:
        # сначала similarity уже отобрал и отсортировал shortlist,
        # а прогноз строится только по его лучшей части.
        if not ranked_similarity_candidates:
            return None

        selected_similarity_candidates = ranked_similarity_candidates[
            : self.strategy_params.forecast_top_n_after_similarity
        ]

        selected_prepared_hours = []

        for item in selected_similarity_candidates:
            hour_start_ts = item["hour_start_ts"]

            if hour_start_ts not in self.current_hour_prepared_hours_map:
                raise ValueError(
                    f"Не найден prepared-кандидат для hour_start_ts={hour_start_ts}"
                )

            selected_prepared_hours.append(
                self.current_hour_prepared_hours_map[hour_start_ts]
            )

        current_bar_index = self.current_hour.current_bar_index()

        if current_bar_index is None:
            return None

        forecast_summary = build_group_forecast_from_prepared_candidates(
            prepared_hours=selected_prepared_hours,
            current_bar_index=current_bar_index,
        )

        return forecast_summary

    def _build_decision_result(self, ranked_similarity_candidates, forecast_summary):
        # Строим formal decision layer поверх similarity и forecast.
        return evaluate_decision_layer(
            ranked_similarity_candidates=ranked_similarity_candidates,
            forecast_summary=forecast_summary,
            params=self.strategy_params,
        )

    def _append_bar_to_current_hour(self, bar):
        # Добавляем очередной бар в текущий runtime-час.
        #
        # Здесь же контролируем:
        # - что внутри часа нет дырок по timestamp;
        # - что в текущем баре нет NULL-цен.
        #
        # Если час становится невалидным, мы больше не считаем по нему
        # корреляцию, но сам runtime продолжаем вести до конца часа.
        if self.current_hour is None:
            raise ValueError("Текущий час ещё не инициализирован")

        bar_time_ts = bar["bar_time_ts"]

        if self.current_hour_expected_next_bar_time_ts is None:
            raise ValueError("current_hour_expected_next_bar_time_ts is None")

        if bar_time_ts < self.current_hour_expected_next_bar_time_ts:
            raise ValueError(
                f"Получен дублирующийся или неупорядоченный бар: "
                f"bar_time_ts={bar_time_ts}, "
                f"expected_ts={self.current_hour_expected_next_bar_time_ts}"
            )

        if bar_time_ts > self.current_hour_expected_next_bar_time_ts:
            self._try_hydrate_current_hour_from_db(target_bar_time_ts=bar_time_ts)

            if bar_time_ts > self.current_hour_expected_next_bar_time_ts:
                # Пока не завершилась стартовая разовая докачка последнего часа,
                # не признаём текущий час невалидным. Это нормальный сценарий
                # рестарта в середине часа: realtime уже пошёл, а backfill ещё
                # не успел заполнить начало текущего часа в БД.
                if not self.startup_backfill_completed:
                    return

                self._mark_current_hour_invalid(
                    f"Обнаружена дырка в текущем часе: "
                    f"ожидался bar_time_ts={self.current_hour_expected_next_bar_time_ts}, "
                    f"получен bar_time_ts={bar_time_ts}"
                )

        ask_open = bar["ask_open"]
        bid_open = bar["bid_open"]
        ask_close = bar["ask_close"]
        bid_close = bar["bid_close"]

        if ask_open is None:
            self._mark_current_hour_invalid("ask_open is NULL в текущем часу")

        if bid_open is None:
            self._mark_current_hour_invalid("bid_open is NULL в текущем часу")

        if ask_close is None:
            self._mark_current_hour_invalid("ask_close is NULL в текущем часу")

        if bid_close is None:
            self._mark_current_hour_invalid("bid_close is NULL в текущем часу")

        # Даже если час уже стал невалидным, всё равно продолжаем копить
        # x-ряд, если у бара есть все нужные цены. Это полезно для отладки и
        # делает поведение runtime более предсказуемым.
        if (
                ask_open is not None
                and bid_open is not None
                and ask_close is not None
                and bid_close is not None
        ):
            self.current_hour.add_bar(
                ask_open=ask_open,
                bid_open=bid_open,
                ask_close=ask_close,
                bid_close=bid_close,
            )

        self.current_hour_last_bar_time_ts = bar_time_ts
        self.current_hour_expected_next_bar_time_ts = (
                bar_time_ts + self.strategy_params.pearson_bar_interval_seconds
        )

    def _mark_current_hour_invalid(self, reason):
        # Помечаем текущий час невалидным.
        #
        # Первый reason сохраняем как основной, чтобы потом было понятно,
        # почему именно этот час был исключён из расчёта.
        if self.current_hour_valid:
            self.current_hour_valid = False
            self.current_hour_invalid_reason = reason

            hour_start_ct = None
            hour_slot_ct = None
            current_bar_count = 0

            if self.current_hour is not None:
                hour_start_ct = self.current_hour.hour_start_ct
                hour_slot_ct = self.current_hour.hour_slot_ct
                current_bar_count = self.current_hour.current_n()

            log_warning(
                logger,
                (
                    "PEARSON LIVE INVALID HOUR | "
                    f"instrument={self.instrument_code} | "
                    f"hour_start_ct={hour_start_ct} | "
                    f"hour_slot_ct={hour_slot_ct} | "
                    f"current_bar_count={current_bar_count} | "
                    f"expected_next_bar_time_ts={self.current_hour_expected_next_bar_time_ts} | "
                    f"reason={reason}"
                ),
                to_telegram=True,
            )

    def _is_search_window_active(self):
        # Проверяем, входит ли текущий уже накопленный префикс
        # в разрешённое окно поиска.
        if self.current_hour is None:
            return False

        current_bar_count = self.current_hour.current_n()

        return (
                current_bar_count >= self.strategy_params.pearson_eval_start_bar_count()
                and current_bar_count < self.strategy_params.pearson_eval_end_bar_count_exclusive()
        )

    def _build_empty_snapshot(self):
        # Пустой snapshot для состояния "ещё ни одного бара не было".
        return PearsonLiveSnapshot(
            hour_start_ts=None,
            hour_start_ts_ct=None,
            hour_start_ct=None,
            hour_slot_ct=None,
            last_bar_time_ts=None,
            current_bar_count=0,
            current_bar_index=None,
            expected_next_bar_time_ts=None,
            search_window_active=False,
            search_window_start_bar_count=self.strategy_params.pearson_eval_start_bar_count(),
            search_window_end_bar_count_exclusive=self.strategy_params.pearson_eval_end_bar_count_exclusive(),
            current_hour_valid=True,
            current_hour_invalid_reason=None,
            allowed_hour_slots=[],
            history_candidate_count=0,
            candidates_initialized=False,
            correlation_calculated=False,
            similarity_calculated=False,
            forecast_calculated=False,
            decision_calculated=False,
            ranked_candidates=[],
            ranked_similarity_candidates=[],
            forecast_summary=None,
            decision_result=None,
        )

    def _build_snapshot(
            self,
            correlation_calculated,
            similarity_calculated,
            forecast_calculated,
            decision_calculated,
            ranked_candidates,
            ranked_similarity_candidates,
            forecast_summary,
            decision_result,
    ):
        # Собираем snapshot по текущему активному часу.
        if self.current_hour is None:
            return self._build_empty_snapshot()

        return PearsonLiveSnapshot(
            hour_start_ts=self.current_hour.hour_start_ts,
            hour_start_ts_ct=self.current_hour.hour_start_ts_ct,
            hour_start_ct=self.current_hour.hour_start_ct,
            hour_slot_ct=self.current_hour.hour_slot_ct,
            last_bar_time_ts=self.current_hour_last_bar_time_ts,
            current_bar_count=self.current_hour.current_n(),
            current_bar_index=self.current_hour.current_bar_index(),
            expected_next_bar_time_ts=self.current_hour_expected_next_bar_time_ts,
            search_window_active=self._is_search_window_active(),
            search_window_start_bar_count=self.strategy_params.pearson_eval_start_bar_count(),
            search_window_end_bar_count_exclusive=self.strategy_params.pearson_eval_end_bar_count_exclusive(),
            current_hour_valid=self.current_hour_valid,
            current_hour_invalid_reason=self.current_hour_invalid_reason,
            allowed_hour_slots=list(self.allowed_hour_slots),
            history_candidate_count=len(self.current_hour.candidates),
            candidates_initialized=self.current_hour.candidates_initialized,
            correlation_calculated=correlation_calculated,
            similarity_calculated=similarity_calculated,
            forecast_calculated=forecast_calculated,
            decision_calculated=decision_calculated,
            ranked_candidates=ranked_candidates,
            ranked_similarity_candidates=ranked_similarity_candidates,
            forecast_summary=forecast_summary,
            decision_result=decision_result,
        )

import sqlite3
from dataclasses import dataclass
from typing import Optional

from contracts import Instrument
from core.db_initializer import build_table_name
from ts.pearson_runtime import PearsonCurrentHour
from ts.candidate_scoring import rank_prepared_candidates_by_similarity
from ts.prepared_reader import load_prepared_hours_by_slots
from ts.ts_config import (
    PEARSON_BAR_INTERVAL_SECONDS,
    PEARSON_SHORTLIST_MIN_CORRELATION,
    PEARSON_SHORTLIST_TOP_N,
    pearson_eval_start_bar_count,
    pearson_eval_end_bar_count_exclusive,
)
from ts.ts_time import resolve_allowed_hour_slots


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

    ranked_candidates: list[dict]
    ranked_similarity_candidates: list[dict]


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
    ):
        instrument_row = Instrument[instrument_code]

        self.settings = settings
        self.instrument_code = instrument_code
        self.table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        if min_correlation is None:
            min_correlation = PEARSON_SHORTLIST_MIN_CORRELATION

        if top_n is None:
            top_n = PEARSON_SHORTLIST_TOP_N

        self.min_correlation = min_correlation
        self.top_n = top_n

        self.current_hour = None
        self.current_hour_prepared_hours_map = {}
        self.current_hour_valid = True
        self.current_hour_invalid_reason = None
        self.current_hour_last_bar_time_ts = None
        self.current_hour_expected_next_bar_time_ts = None

        self.allowed_hour_slots = []
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
        ranked_candidates = []
        ranked_similarity_candidates = []

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

            ranked_similarity_candidates = self._rank_similarity_candidates(
                ranked_candidates=ranked_candidates,
            )
            similarity_calculated = True

        self.last_snapshot = self._build_snapshot(
            correlation_calculated=correlation_calculated,
            similarity_calculated=similarity_calculated,
            ranked_candidates=ranked_candidates,
            ranked_similarity_candidates=ranked_similarity_candidates,
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

        self.allowed_hour_slots = resolve_allowed_hour_slots(
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

            shortlist_prepared_hours.append(
                self.current_hour_prepared_hours_map[hour_start_ts]
            )

        ranked_similarity_candidates = rank_prepared_candidates_by_similarity(
            current_values=self.current_hour.x,
            prepared_hours=shortlist_prepared_hours,
            min_required_pearson=None,
        )

        return ranked_similarity_candidates

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
                bar_time_ts + PEARSON_BAR_INTERVAL_SECONDS
        )

    def _mark_current_hour_invalid(self, reason):
        # Помечаем текущий час невалидным.
        #
        # Первый reason сохраняем как основной, чтобы потом было понятно,
        # почему именно этот час был исключён из расчёта.
        if self.current_hour_valid:
            self.current_hour_valid = False
            self.current_hour_invalid_reason = reason

    def _is_search_window_active(self):
        # Проверяем, входит ли текущий уже накопленный префикс
        # в разрешённое окно поиска.
        if self.current_hour is None:
            return False

        current_bar_count = self.current_hour.current_n()

        return (
                current_bar_count >= pearson_eval_start_bar_count()
                and current_bar_count < pearson_eval_end_bar_count_exclusive()
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
            search_window_start_bar_count=pearson_eval_start_bar_count(),
            search_window_end_bar_count_exclusive=pearson_eval_end_bar_count_exclusive(),
            current_hour_valid=True,
            current_hour_invalid_reason=None,
            allowed_hour_slots=[],
            history_candidate_count=0,
            candidates_initialized=False,
            correlation_calculated=False,
            similarity_calculated=False,
            ranked_candidates=[],
            ranked_similarity_candidates=[],
        )

    def _build_snapshot(
            self,
            correlation_calculated,
            similarity_calculated,
            ranked_candidates,
            ranked_similarity_candidates,
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
            search_window_start_bar_count=pearson_eval_start_bar_count(),
            search_window_end_bar_count_exclusive=pearson_eval_end_bar_count_exclusive(),
            current_hour_valid=self.current_hour_valid,
            current_hour_invalid_reason=self.current_hour_invalid_reason,
            allowed_hour_slots=list(self.allowed_hour_slots),
            history_candidate_count=len(self.current_hour.candidates),
            candidates_initialized=self.current_hour.candidates_initialized,
            correlation_calculated=correlation_calculated,
            similarity_calculated=similarity_calculated,
            ranked_candidates=ranked_candidates,
            ranked_similarity_candidates=ranked_similarity_candidates,
        )

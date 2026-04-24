from datetime import datetime, timezone
from math import sqrt


def text_from_local_axis_ts(local_axis_ts):
    # Преобразуем локальную числовую ось проекта в читаемую строку.
    #
    # Здесь timezone.utc используется только как нейтральная ось вывода,
    # чтобы получить текст "YYYY-MM-DD HH:MM:SS" для уже сдвинутого local ts.
    return datetime.fromtimestamp(local_axis_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class PearsonPreparedCandidate:
    # Один historical prepared-кандидат: 60-минутное окно анализа,
    # подготовленное заранее в prepared DB.
    #
    # Поля hour_* здесь соответствуют текущей схеме prepared DB:
    # в них лежит start 60-минутного analysis window, а не торгового слота.
    def __init__(self, prepared_window_payload):
        self.hour_start_ts = prepared_window_payload["hour_start_ts"]
        self.hour_start_ts_ct = prepared_window_payload["hour_start_ts_ct"]
        self.hour_start_ct = prepared_window_payload["hour_start_ct"]
        self.hour_slot_ct = prepared_window_payload["hour_slot_ct"]
        self.contract = prepared_window_payload["contract"]

        self.y = prepared_window_payload["y"]
        self.sum_y = prepared_window_payload["sum_y"]
        self.sum_y2 = prepared_window_payload["sum_y2"]

        self.sum_xy = 0.0
        self.last_correlation = None

    def initialize_sum_xy(self, current_x):
        # Полностью инициализируем sum_xy по уже накопленному префиксу current_x.
        self.sum_xy = 0.0

        for bar_index, x_value in enumerate(current_x):
            self.sum_xy += x_value * self.y[bar_index]

    def update_sum_xy_for_last_bar(self, x_value, bar_index):
        # Инкрементально добавляем вклад только последнего бара.
        self.sum_xy += x_value * self.y[bar_index]

    def calculate_correlation(self, current_sum_x, current_sum_x2, current_n, current_bar_index):
        # Считаем коэффициент Пирсона на текущем префиксе.
        #
        # По historical prepared-кандидату берём уже готовые префиксные
        # sum_y и sum_y2 ровно на той же длине current_bar_index.
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


class PearsonAnalysisWindow:
    # Runtime-состояние текущего 60-минутного окна анализа.
    #
    # Окно анализа может начинаться:
    # - в HH:00;
    # - в HH:30.
    #
    # Вход в сделку ищется во второй половине этого окна:
    # - 30..50 минута analysis window;
    # - то есть первые 20 минут соответствующего торгового получаса.
    def __init__(self, analysis_window_start_ts, analysis_window_start_ts_ct):
        self.hour_start_ts = analysis_window_start_ts
        self.hour_start_ts_ct = analysis_window_start_ts_ct

        self.hour_start = datetime.fromtimestamp(
            analysis_window_start_ts,
            tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S")
        self.hour_start_ct = text_from_local_axis_ts(analysis_window_start_ts_ct)
        self.hour_slot_ct = (analysis_window_start_ts_ct // 3600) % 24

        self.mid_open_0 = None

        self.x = []
        self.sum_x = 0.0
        self.sum_x2 = 0.0

        self.candidates = []
        self.candidates_initialized = False

    def current_bar_index(self):
        # Индекс последнего уже накопленного бара.
        if not self.x:
            return None

        return len(self.x) - 1

    def current_n(self):
        # Текущее число точек в префиксе.
        return len(self.x)

    def add_bar(self, ask_open, bid_open, ask_close, bid_close):
        # Добавляем один новый завершённый 5-секундный бар
        # текущего 60-минутного окна анализа.
        #
        # Формула:
        # x_i = mid_close_i / mid_open_0 - 1
        #
        # mid_open_0 берём только из первого бара окна анализа.
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

    def set_candidates(self, prepared_windows):
        # Загружаем в runtime все historical prepared-кандидаты.
        #
        # prepared_windows - список payload-объектов из prepared_reader.
        self.candidates = []

        for prepared_window_payload in prepared_windows:
            candidate = PearsonPreparedCandidate(prepared_window_payload)
            self.candidates.append(candidate)

        self.candidates_initialized = False

    def initialize_candidates(self):
        # Один раз инициализируем sum_xy по уже накопленному префиксу
        # текущего окна анализа.
        if not self.x:
            raise ValueError("Нельзя инициализировать кандидатов: текущий x пустой")

        for candidate in self.candidates:
            candidate.initialize_sum_xy(self.x)

        self.candidates_initialized = True

    def update_candidates_for_last_bar(self):
        # После прихода нового бара инкрементально обновляем sum_xy
        # у всех historical-кандидатов.
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
        # Считаем корреляцию для всех загруженных кандидатов
        # на текущем префиксе.
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
        # Возвращаем кандидатов, отсортированных по correlation по убыванию.
        #
        # None-корреляции пропускаем.
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

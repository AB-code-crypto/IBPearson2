from datetime import datetime, timezone
from math import sqrt


class PearsonCandidate:
    # Один исторический prepared-час, который участвует в сравнении
    # с текущим часом.
    #
    # Здесь храним:
    # - метаданные часа;
    # - подготовленные массивы y / sum_y / sum_y2;
    # - текущее runtime-состояние sum_xy и last_correlation.
    def __init__(self, prepared_hour_payload):
        self.hour_start_ts = prepared_hour_payload["hour_start_ts"]
        self.hour_start = prepared_hour_payload["hour_start"]
        self.hour_slot = prepared_hour_payload["hour_slot"]
        self.contract = prepared_hour_payload["contract"]

        self.y = prepared_hour_payload["y"]
        self.sum_y = prepared_hour_payload["sum_y"]
        self.sum_y2 = prepared_hour_payload["sum_y2"]

        self.sum_xy = 0.0
        self.last_correlation = None

    def initialize_sum_xy(self, current_x):
        # Полностью инициализируем sum_xy по уже накопленному префиксу current_x.
        #
        # Используется один раз в момент, когда мы впервые начинаем сравнение
        # текущего часа с историческими кандидатами.
        self.sum_xy = 0.0

        for bar_index, x_value in enumerate(current_x):
            self.sum_xy += x_value * self.y[bar_index]

    def update_sum_xy_for_last_bar(self, x_value, bar_index):
        # Инкрементально добавляем вклад только последнего бара.
        self.sum_xy += x_value * self.y[bar_index]

    def calculate_correlation(self, current_sum_x, current_sum_x2, current_n, current_bar_index):
        # Считаем коэффициент Пирсона на текущем префиксе.
        #
        # По историческому часу берём уже готовые префиксные sum_y и sum_y2
        # ровно на той же длине current_bar_index.
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


class PearsonCurrentHour:
    # Runtime-состояние текущего часа.
    #
    # Здесь храним:
    # - начало часа;
    # - hour_slot;
    # - базовую цену mid_open_0;
    # - текущий ряд x;
    # - sum_x;
    # - sum_x2;
    # - список исторических кандидатов;
    # - флаг, что sum_xy уже инициализирован для кандидатов.
    def __init__(self, hour_start_ts):
        self.hour_start_ts = hour_start_ts
        self.hour_start = datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.hour_slot = datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).hour

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

    def can_start_comparison(self, min_bars=360):
        # Можно ли уже начинать сравнение.
        #
        # По текущей логике:
        # первые 30 минут ждём,
        # после 360 баров начинаем сравнивать.
        return len(self.x) >= min_bars

    def add_bar(self, ask_open, bid_open, ask_close, bid_close):
        # Добавляем один новый завершённый 5-секундный бар текущего часа.
        #
        # Формула:
        # x_i = mid_close_i / mid_open_0 - 1
        #
        # mid_open_0 берём только из первого бара часа.
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
        # Загружаем в runtime все исторические prepared-часы.
        #
        # prepared_hours - это список payload-объектов из prepared_reader.
        self.candidates = []

        for prepared_hour_payload in prepared_hours:
            candidate = PearsonCandidate(prepared_hour_payload)
            self.candidates.append(candidate)

        self.candidates_initialized = False

    def initialize_candidates(self):
        # Один раз инициализируем sum_xy по уже накопленному префиксу текущего часа.
        #
        # Это вызывается в момент первого старта сравнения,
        # когда исторические кандидаты уже загружены.
        if not self.x:
            raise ValueError("Нельзя инициализировать кандидатов: текущий x пустой")

        for candidate in self.candidates:
            candidate.initialize_sum_xy(self.x)

        self.candidates_initialized = True

    def update_candidates_for_last_bar(self):
        # После прихода нового бара инкрементально обновляем sum_xy
        # у всех исторических кандидатов.
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
                    "hour_start": candidate.hour_start,
                    "hour_slot": candidate.hour_slot,
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
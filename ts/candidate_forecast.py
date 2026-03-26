from statistics import median


def build_future_path_from_y(values, current_bar_index):
    # Строим относительный future-path от текущей точки до конца часа.
    #
    # values - это накопленный нормализованный ряд y.
    #
    # Результат:
    # path[k] показывает, на сколько изменился участок относительно
    # точки входа к следующей точке после current_bar_index.
    #
    # Формула:
    # current_rel = 1 + y_current
    # future_rel = 1 + y_future
    # rel_move = future_rel / current_rel - 1
    if not values:
        raise ValueError("values пустой")

    if current_bar_index < 0 or current_bar_index >= len(values):
        raise ValueError(
            f"Некорректный current_bar_index={current_bar_index}, len(values)={len(values)}"
        )

    current_rel = 1.0 + values[current_bar_index]
    future_values = values[current_bar_index + 1 :]

    path = []

    for future_value in future_values:
        future_rel = 1.0 + future_value
        rel_move = (future_rel / current_rel) - 1.0
        path.append(rel_move)

    return path


def build_prepared_candidate_future_path(prepared_hour_payload, current_bar_index):
    # Строим future-path для одного prepared-кандидата.
    return build_future_path_from_y(
        values=prepared_hour_payload["y"],
        current_bar_index=current_bar_index,
    )


def calc_future_final_move(path):
    # Итоговое движение к концу часа.
    if not path:
        return 0.0

    return path[-1]


def calc_future_max_upside(path):
    # Максимальное положительное отклонение пути.
    if not path:
        return 0.0

    return max(path)


def calc_future_max_drawdown(path):
    # Максимальное отрицательное отклонение пути.
    if not path:
        return 0.0

    return min(path)


def build_pointwise_mean_path(paths):
    # Точечное среднее по набору future-path.
    if not paths:
        return []

    path_length = len(paths[0])

    for path in paths:
        if len(path) != path_length:
            raise ValueError("Future-path имеют разную длину")

    mean_path = []

    for index in range(path_length):
        total = 0.0

        for path in paths:
            total += path[index]

        mean_path.append(total / len(paths))

    return mean_path


def build_pointwise_median_path(paths):
    # Точечная медиана по набору future-path.
    if not paths:
        return []

    path_length = len(paths[0])

    for path in paths:
        if len(path) != path_length:
            raise ValueError("Future-path имеют разную длину")

    median_path = []

    for index in range(path_length):
        values = []

        for path in paths:
            values.append(path[index])

        median_path.append(median(values))

    return median_path


def build_future_paths_for_prepared_candidates(prepared_hours, current_bar_index):
    # Строим future-path для списка prepared-кандидатов.
    result = []

    for prepared_hour_payload in prepared_hours:
        future_path = build_prepared_candidate_future_path(
            prepared_hour_payload=prepared_hour_payload,
            current_bar_index=current_bar_index,
        )

        result.append(
            {
                "hour_start_ts": prepared_hour_payload["hour_start_ts"],
                "hour_start_ts_ct": prepared_hour_payload["hour_start_ts_ct"],
                "hour_start_ct": prepared_hour_payload["hour_start_ct"],
                "hour_slot_ct": prepared_hour_payload["hour_slot_ct"],
                "contract": prepared_hour_payload["contract"],
                "future_path": future_path,
                "final_move": calc_future_final_move(future_path),
                "max_upside": calc_future_max_upside(future_path),
                "max_drawdown": calc_future_max_drawdown(future_path),
            }
        )

    return result


def build_group_forecast_from_future_items(future_items):
    # Сводный прогноз по группе кандидатов.
    if not future_items:
        return {
            "candidate_count": 0,
            "positive_count": 0,
            "negative_count": 0,
            "flat_count": 0,
            "positive_ratio": 0.0,
            "negative_ratio": 0.0,
            "flat_ratio": 0.0,
            "mean_final_move": 0.0,
            "median_final_move": 0.0,
            "mean_max_upside": 0.0,
            "median_max_upside": 0.0,
            "mean_max_drawdown": 0.0,
            "median_max_drawdown": 0.0,
            "mean_future_path": [],
            "median_future_path": [],
        }

    paths = [item["future_path"] for item in future_items]
    final_moves = [item["final_move"] for item in future_items]
    max_upsides = [item["max_upside"] for item in future_items]
    max_drawdowns = [item["max_drawdown"] for item in future_items]

    positive_count = sum(1 for value in final_moves if value > 0.0)
    negative_count = sum(1 for value in final_moves if value < 0.0)
    flat_count = sum(1 for value in final_moves if value == 0.0)

    candidate_count = len(future_items)

    return {
        "candidate_count": candidate_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "flat_count": flat_count,
        "positive_ratio": positive_count / candidate_count,
        "negative_ratio": negative_count / candidate_count,
        "flat_ratio": flat_count / candidate_count,
        "mean_final_move": sum(final_moves) / candidate_count,
        "median_final_move": median(final_moves),
        "mean_max_upside": sum(max_upsides) / candidate_count,
        "median_max_upside": median(max_upsides),
        "mean_max_drawdown": sum(max_drawdowns) / candidate_count,
        "median_max_drawdown": median(max_drawdowns),
        "mean_future_path": build_pointwise_mean_path(paths),
        "median_future_path": build_pointwise_median_path(paths),
    }


def build_group_forecast_from_prepared_candidates(prepared_hours, current_bar_index):
    # Полный расчёт forecast-сводки по группе prepared-кандидатов.
    future_items = build_future_paths_for_prepared_candidates(
        prepared_hours=prepared_hours,
        current_bar_index=current_bar_index,
    )

    forecast = build_group_forecast_from_future_items(future_items)
    forecast["future_items"] = future_items

    return forecast
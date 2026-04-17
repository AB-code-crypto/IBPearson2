from ts.candidate_features import (
    build_first_diff,
    calc_mean_abs_diff,
    calc_net_move,
    calc_path_efficiency,
    calc_path_efficiency_from_parts,
    calc_range,
    calc_range_position,
)
from ts.strategy_params import DEFAULT_STRATEGY_PARAMS, StrategyParams


def calc_relative_distance(value_a, value_b, eps=1e-12):
    # Относительное отличие двух значений.
    #
    # Формула:
    # abs(a - b) / max(abs(a), abs(b), eps)
    denominator = max(abs(value_a), abs(value_b), eps)
    return abs(value_a - value_b) / denominator


def calc_absolute_distance(value_a, value_b):
    # Абсолютное отличие двух значений.
    #
    # Используется для признаков, уже нормализованных в диапазон [0, 1].
    return abs(value_a - value_b)


def calc_weighted_average_score(score_items):
    # Считаем взвешенное среднее.
    #
    # score_items:
    # [
    #   (score_value, weight_value),
    #   ...
    # ]
    weighted_sum = 0.0
    total_weight = 0.0

    for score_value, weight_value in score_items:
        weighted_sum += score_value * weight_value
        total_weight += weight_value

    if total_weight == 0.0:
        return 0.0

    return weighted_sum / total_weight


def evaluate_similarity_between_prefixes(
        current_values,
        candidate_values,
        pearson,
        params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
):
    # Сравниваем два префикса и считаем все расстояния, score и итоговый результат.
    if len(current_values) != len(candidate_values):
        raise ValueError(
            f"Длины префиксов не совпадают: "
            f"len(current_values)={len(current_values)}, "
            f"len(candidate_values)={len(candidate_values)}"
        )

    if pearson is None:
        return None

    current_range = None
    candidate_range = None
    range_distance = None
    range_score = 0.0

    if params.similarity_weight_range > 0.0:
        current_range = calc_range(current_values)
        candidate_range = calc_range(candidate_values)
        range_distance = calc_relative_distance(
            current_range,
            candidate_range,
        )
        range_score = 1.0 - range_distance

    current_net_move = None
    candidate_net_move = None
    net_move_distance = None
    net_move_score = 0.0

    if params.similarity_weight_net_move > 0.0:
        current_net_move = calc_net_move(current_values)
        candidate_net_move = calc_net_move(candidate_values)

        if current_net_move * candidate_net_move < 0.0:
            net_move_score = 0.0
        else:
            net_move_distance = calc_relative_distance(
                current_net_move,
                candidate_net_move,
            )
            net_move_score = 1.0 - net_move_distance

    current_range_position = None
    candidate_range_position = None
    range_position_distance = None
    range_position_score = 0.0

    if params.similarity_weight_range_position > 0.0:
        current_range_position = calc_range_position(current_values)
        candidate_range_position = calc_range_position(candidate_values)
        range_position_distance = calc_absolute_distance(
            current_range_position,
            candidate_range_position,
        )
        range_position_score = 1.0 - range_position_distance

    current_mean_abs_diff = None
    candidate_mean_abs_diff = None
    mean_abs_diff_distance = None
    mean_abs_diff_score = 0.0

    current_path_efficiency = None
    candidate_path_efficiency = None
    efficiency_distance = None
    efficiency_score = 0.0

    need_diff_values = (
            params.similarity_weight_mean_abs_diff > 0.0
            or params.similarity_weight_efficiency > 0.0
    )
    current_diff_values = None
    candidate_diff_values = None

    if need_diff_values:
        current_diff_values = build_first_diff(current_values)
        candidate_diff_values = build_first_diff(candidate_values)

    if params.similarity_weight_mean_abs_diff > 0.0:
        current_mean_abs_diff = calc_mean_abs_diff(current_diff_values)
        candidate_mean_abs_diff = calc_mean_abs_diff(candidate_diff_values)
        mean_abs_diff_distance = calc_relative_distance(
            current_mean_abs_diff,
            candidate_mean_abs_diff,
        )
        mean_abs_diff_score = 1.0 - mean_abs_diff_distance

    if params.similarity_weight_efficiency > 0.0:
        if current_net_move is None:
            current_net_move = calc_net_move(current_values)

        if candidate_net_move is None:
            candidate_net_move = calc_net_move(candidate_values)

        current_path_efficiency = calc_path_efficiency_from_parts(
            net_move=current_net_move,
            diff_values=current_diff_values,
        )
        candidate_path_efficiency = calc_path_efficiency_from_parts(
            net_move=candidate_net_move,
            diff_values=candidate_diff_values,
        )
        efficiency_distance = calc_relative_distance(
            current_path_efficiency,
            candidate_path_efficiency,
        )
        efficiency_score = 1.0 - efficiency_distance

    pearson_score = pearson

    final_score = calc_weighted_average_score(
        [
            (pearson_score, params.similarity_weight_pearson),
            (range_score, params.similarity_weight_range),
            (net_move_score, params.similarity_weight_net_move),
            (range_position_score, params.similarity_weight_range_position),
            (mean_abs_diff_score, params.similarity_weight_mean_abs_diff),
            (efficiency_score, params.similarity_weight_efficiency),
        ]
    )

    return {
        "pearson": pearson,
        "current_range": current_range,
        "candidate_range": candidate_range,
        "range_distance": range_distance,
        "current_net_move": current_net_move,
        "candidate_net_move": candidate_net_move,
        "net_move_distance": net_move_distance,
        "current_range_position": current_range_position,
        "candidate_range_position": candidate_range_position,
        "range_position_distance": range_position_distance,
        "current_mean_abs_diff": current_mean_abs_diff,
        "candidate_mean_abs_diff": candidate_mean_abs_diff,
        "mean_abs_diff_distance": mean_abs_diff_distance,
        "current_path_efficiency": current_path_efficiency,
        "candidate_path_efficiency": candidate_path_efficiency,
        "efficiency_distance": efficiency_distance,
        "pearson_score": pearson_score,
        "range_score": range_score,
        "net_move_score": net_move_score,
        "range_position_score": range_position_score,
        "mean_abs_diff_score": mean_abs_diff_score,
        "efficiency_score": efficiency_score,
        "final_score": final_score,
    }


def evaluate_prepared_candidate_similarity(
        current_values,
        prepared_hour_payload,
        params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
):
    # Считаем score похожести для одного prepared-кандидата.
    candidate_values = prepared_hour_payload["y"][: len(current_values)]
    pearson = prepared_hour_payload["correlation"]

    result = evaluate_similarity_between_prefixes(
        current_values=current_values,
        candidate_values=candidate_values,
        pearson=pearson,
        params=params,
    )

    if result is None:
        return None

    result.update(
        {
            "hour_start_ts": prepared_hour_payload["hour_start_ts"],
            "hour_start_ts_ct": prepared_hour_payload["hour_start_ts_ct"],
            "hour_start_ct": prepared_hour_payload["hour_start_ct"],
            "hour_slot_ct": prepared_hour_payload["hour_slot_ct"],
            "contract": prepared_hour_payload["contract"],
        }
    )

    return result


def rank_prepared_candidates_by_similarity(
        current_values,
        prepared_hours,
        min_required_pearson=None,
        params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
):
    # Считаем score похожести для списка prepared-кандидатов и сортируем их.
    ranked = []

    for prepared_hour_payload in prepared_hours:
        item = evaluate_prepared_candidate_similarity(
            current_values=current_values,
            prepared_hour_payload=prepared_hour_payload,
            params=params,
        )

        if item is None:
            continue

        if min_required_pearson is not None and item["pearson"] < min_required_pearson:
            continue

        ranked.append(item)

    ranked.sort(
        key=lambda item: (item["final_score"], item["pearson"]),
        reverse=True,
    )

    return ranked

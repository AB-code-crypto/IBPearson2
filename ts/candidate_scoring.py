from ts.candidate_features import (
    build_first_diff,
    calc_diff_pearson,
    calc_diff_sign_match_ratio,
    calc_mean_abs_diff,
    calc_net_move,
    calc_path_efficiency,
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


def calc_score_from_value(value, score_zero_at, score_one_at):
    # Переводим "чем больше, тем лучше" в score 0..1.
    #
    # Логика:
    # - value <= score_zero_at -> 0
    # - value >= score_one_at -> 1
    # - между ними линейная интерполяция
    if score_one_at <= score_zero_at:
        raise ValueError("score_one_at должен быть строго больше score_zero_at")

    if value <= score_zero_at:
        return 0.0

    if value >= score_one_at:
        return 1.0

    return (value - score_zero_at) / (score_one_at - score_zero_at)


def calc_score_from_distance(distance, distance_zero_at):
    # Переводим "чем меньше, тем лучше" в score 0..1.
    #
    # Логика:
    # - distance <= 0 -> 1
    # - distance >= distance_zero_at -> 0
    # - между ними линейная интерполяция
    if distance_zero_at <= 0.0:
        raise ValueError("distance_zero_at должен быть > 0")

    if distance <= 0.0:
        return 1.0

    if distance >= distance_zero_at:
        return 0.0

    return 1.0 - (distance / distance_zero_at)


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

    diff_pearson = calc_diff_pearson(current_values, candidate_values)
    diff_sign_match_ratio = calc_diff_sign_match_ratio(current_values, candidate_values)

    current_diff_values = build_first_diff(current_values)
    candidate_diff_values = build_first_diff(candidate_values)

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

    current_net_move = calc_net_move(current_values)
    candidate_net_move = calc_net_move(candidate_values)
    net_move_distance = calc_relative_distance(
        current_net_move,
        candidate_net_move,
    )

    current_range_position = calc_range_position(current_values)
    candidate_range_position = calc_range_position(candidate_values)
    range_position_distance = calc_absolute_distance(
        current_range_position,
        candidate_range_position,
    )

    current_mean_abs_diff = calc_mean_abs_diff(current_diff_values)
    candidate_mean_abs_diff = calc_mean_abs_diff(candidate_diff_values)
    mean_abs_diff_distance = calc_relative_distance(
        current_mean_abs_diff,
        candidate_mean_abs_diff,
    )

    current_path_efficiency = calc_path_efficiency(current_values)
    candidate_path_efficiency = calc_path_efficiency(candidate_values)
    efficiency_distance = calc_relative_distance(
        current_path_efficiency,
        candidate_path_efficiency,
    )

    pearson_score = pearson
    diff_pearson_score = 0.0
    if diff_pearson is not None:
        diff_pearson_score = calc_score_from_value(
            value=diff_pearson,
            score_zero_at=params.similarity_diff_pearson_score_zero_at,
            score_one_at=params.similarity_diff_pearson_score_one_at,
        )
    diff_sign_match_score = calc_score_from_value(
        value=diff_sign_match_ratio,
        score_zero_at=params.similarity_diff_sign_match_score_zero_at,
        score_one_at=params.similarity_diff_sign_match_score_one_at,
    )
    net_move_score = calc_score_from_distance(
        distance=net_move_distance,
        distance_zero_at=params.similarity_net_move_distance_zero_at,
    )
    range_position_score = calc_score_from_distance(
        distance=range_position_distance,
        distance_zero_at=params.similarity_range_position_distance_zero_at,
    )
    mean_abs_diff_score = calc_score_from_distance(
        distance=mean_abs_diff_distance,
        distance_zero_at=params.similarity_mean_abs_diff_distance_zero_at,
    )
    efficiency_score = calc_score_from_distance(
        distance=efficiency_distance,
        distance_zero_at=params.similarity_efficiency_distance_zero_at,
    )

    final_score = calc_weighted_average_score(
        [
            (pearson_score, params.similarity_weight_pearson),
            (range_score, params.similarity_weight_range),
            (net_move_score, params.similarity_weight_net_move),
            (range_position_score, params.similarity_weight_range_position),
            (mean_abs_diff_score, params.similarity_weight_mean_abs_diff),
            (efficiency_score, params.similarity_weight_efficiency),
            (diff_pearson_score, params.similarity_weight_diff_pearson),
            (diff_sign_match_score, params.similarity_weight_diff_sign_match),
        ]
    )

    return {
        "pearson": pearson,
        "diff_pearson": diff_pearson,
        "diff_sign_match_ratio": diff_sign_match_ratio,
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
        "diff_pearson_score": diff_pearson_score,
        "diff_sign_match_score": diff_sign_match_score,
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

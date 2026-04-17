from math import sqrt


def build_first_diff(values):
    # Строим первую разность ряда.
    #
    # Пример:
    # [10.0, 10.5, 10.25] -> [0.5, -0.25]
    #
    # Если в ряду меньше 2 точек, возвращаем пустой список.
    diff_values = []

    if len(values) < 2:
        return diff_values

    previous_value = values[0]

    for current_value in values[1:]:
        diff_values.append(current_value - previous_value)
        previous_value = current_value

    return diff_values


def calc_range(values):
    # Считаем диапазон ряда:
    # max(values) - min(values)
    #
    # Если ряд пустой, возвращаем 0.0.
    if not values:
        return 0.0

    return max(values) - min(values)


def calc_net_move(values):
    # Считаем итоговое смещение ряда:
    # last(values) - first(values)
    #
    # Если ряд пустой, возвращаем 0.0.
    if not values:
        return 0.0

    return values[-1] - values[0]


def calc_range_position(values):
    # Считаем положение последней точки внутри уже пройденного диапазона.
    #
    # Формула:
    # (last(values) - min(values)) / (max(values) - min(values))
    #
    # Интерпретация:
    # - 0.0 -> закончили на минимуме диапазона
    # - 1.0 -> закончили на максимуме диапазона
    # - 0.5 -> закончили примерно в середине диапазона
    #
    # Если диапазон нулевой, возвращаем 0.5 как нейтральное значение.
    if not values:
        return 0.5

    min_value = min(values)
    max_value = max(values)
    value_range = max_value - min_value

    if value_range == 0.0:
        return 0.5

    return (values[-1] - min_value) / value_range


def calc_mean_abs_diff(diff_values):
    # Считаем среднее абсолютное приращение.
    #
    # Если список разностей пустой, возвращаем 0.0.
    if not diff_values:
        return 0.0

    abs_sum = 0.0

    for value in diff_values:
        abs_sum += abs(value)

    return abs_sum / len(diff_values)


def calc_path_efficiency_from_parts(net_move, diff_values):
    # Считаем эффективность движения по уже готовым частям:
    #
    # abs(net_move) / sum(abs(first_diff))
    #
    # Это позволяет не строить первую разность повторно, если она уже
    # была нужна для других similarity-фильтров.
    if not diff_values:
        return 0.0

    abs_path_sum = 0.0

    for value in diff_values:
        abs_path_sum += abs(value)

    if abs_path_sum == 0.0:
        return 0.0

    return abs(net_move) / abs_path_sum


def calc_path_efficiency(values):
    # Считаем эффективность движения:
    #
    # abs(net_move) / sum(abs(first_diff))
    #
    # Интерпретация:
    # - ближе к 1.0 -> движение более направленное;
    # - ближе к 0.0 -> движение более "пилообразное".
    #
    # Если длина ряда меньше 2 или сумма абсолютных шагов равна 0,
    # возвращаем 0.0.
    if len(values) < 2:
        return 0.0

    net_move = calc_net_move(values)
    diff_values = build_first_diff(values)

    return calc_path_efficiency_from_parts(
        net_move=net_move,
        diff_values=diff_values,
    )


def calc_pearson_corr(values_a, values_b):
    # Считаем коэффициент корреляции Пирсона для двух рядов одинаковой длины.
    #
    # Возвращаем:
    # - float, если корреляцию можно посчитать
    # - None, если ряд слишком короткий или дисперсия одного из рядов равна нулю
    #
    # Требования:
    # - длины рядов должны совпадать
    # - длина должна быть >= 2
    n = len(values_a)

    if n != len(values_b):
        raise ValueError(
            f"Длины рядов не совпадают: len(values_a)={len(values_a)}, "
            f"len(values_b)={len(values_b)}"
        )

    if n < 2:
        return None

    sum_a = 0.0
    sum_b = 0.0
    sum_a2 = 0.0
    sum_b2 = 0.0
    sum_ab = 0.0

    for value_a, value_b in zip(values_a, values_b):
        sum_a += value_a
        sum_b += value_b
        sum_a2 += value_a * value_a
        sum_b2 += value_b * value_b
        sum_ab += value_a * value_b

    numerator = (n * sum_ab) - (sum_a * sum_b)
    left = (n * sum_a2) - (sum_a * sum_a)
    right = (n * sum_b2) - (sum_b * sum_b)

    if left <= 0.0:
        return None

    if right <= 0.0:
        return None

    denominator = sqrt(left * right)

    if denominator == 0.0:
        return None

    return numerator / denominator

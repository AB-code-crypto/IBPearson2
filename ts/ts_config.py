# Настройки первого шага стратегии по корреляции Пирсона.
#
# Здесь храним именно стратегические параметры:
# - когда внутри часа разрешён поиск корреляции;
# - с каким шагом идут бары;
# - сколько баров в одном часу.
#
# Временное окно задаём минутами часа, а не количеством баров.
# Это делает настройки прозрачнее.
#
# Важно:
# - PEARSON_EVAL_START_MINUTE включается;
# - PEARSON_EVAL_END_MINUTE используется как правая граница "не включая".
#
# Значения по умолчанию:
# - старт поиска после первых 30 минут часа;
# - остановка поиска после первых 50 минут часа.
#
# Итого получаем окно длиной ровно 20 минут.

PEARSON_BAR_INTERVAL_SECONDS = 5
PEARSON_HOUR_SECONDS = 3600
PEARSON_HOUR_BAR_COUNT = PEARSON_HOUR_SECONDS // PEARSON_BAR_INTERVAL_SECONDS

PEARSON_EVAL_START_MINUTE = 30
PEARSON_EVAL_END_MINUTE = 50


def pearson_bars_per_minute():
    # Сколько 5-секундных баров помещается в одной минуте.
    return 60 // PEARSON_BAR_INTERVAL_SECONDS


def pearson_eval_start_bar_count():
    # После какого числа уже накопленных баров можно начинать расчёт.
    #
    # Пример:
    # 30 минут * 12 баров в минуте = 360 баров.
    return PEARSON_EVAL_START_MINUTE * pearson_bars_per_minute()


def pearson_eval_end_bar_count_exclusive():
    # Правая граница окна расчёта по числу уже накопленных баров.
    #
    # Граница не включается.
    #
    # Пример:
    # 50 минут * 12 баров в минуте = 600.
    # Значит поиск активен, пока len(x) < 600.
    return PEARSON_EVAL_END_MINUTE * pearson_bars_per_minute()


# ============================================================
# Настройки второго шага: score похожести кандидатов
# ============================================================
#
# Логика:
# - после первого Пирсона берём уже отобранных кандидатов;
# - считаем для них набор признаков похожести;
# - каждый признак переводим в оценку 0..1;
# - потом считаем итоговый взвешенный score.
#
# Интерпретация оценок:
# - 1.0 = кандидат идеально подходит по этому фильтру;
# - 0.0 = кандидат полностью не подходит по этому фильтру.
#
# Для Пирсона:
# - значение <= SIMILARITY_PEARSON_SCORE_ZERO_AT  -> 0
# - значение >= SIMILARITY_PEARSON_SCORE_ONE_AT   -> 1
# - между ними линейная интерполяция.
#
# Для distance-фильтров:
# - distance = 0                                  -> 1
# - distance >= *_ZERO_AT                         -> 0
# - между ними линейная интерполяция.

SIMILARITY_PEARSON_SCORE_ZERO_AT = 0.50
SIMILARITY_PEARSON_SCORE_ONE_AT = 1.00

SIMILARITY_RANGE_DISTANCE_ZERO_AT = 0.50
SIMILARITY_NET_MOVE_DISTANCE_ZERO_AT = 0.50
SIMILARITY_MEAN_ABS_DIFF_DISTANCE_ZERO_AT = 0.50
SIMILARITY_EFFICIENCY_DISTANCE_ZERO_AT = 0.50

# ============================================================
# Веса фильтров похожести
# ============================================================
#
# Чем больше вес, тем сильнее фильтр влияет на итоговый score.

SIMILARITY_WEIGHT_PEARSON = 4.0
SIMILARITY_WEIGHT_RANGE = 2.0
SIMILARITY_WEIGHT_NET_MOVE = 2.0
SIMILARITY_WEIGHT_MEAN_ABS_DIFF = 2.0
SIMILARITY_WEIGHT_EFFICIENCY = 1.0


def similarity_total_weight():
    # Сумма весов всех фильтров похожести.
    return (
            SIMILARITY_WEIGHT_PEARSON
            + SIMILARITY_WEIGHT_RANGE
            + SIMILARITY_WEIGHT_NET_MOVE
            + SIMILARITY_WEIGHT_MEAN_ABS_DIFF
            + SIMILARITY_WEIGHT_EFFICIENCY
    )

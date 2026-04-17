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
# Настройки shortlist после первого Пирсона
# ============================================================
#
# Сначала первый Пирсон отбирает осмысленный shortlist кандидатов.
# И только потом на этот shortlist накладывается второй шаг:
# similarity score по дополнительным фильтрам.

PEARSON_SHORTLIST_MIN_CORRELATION = 0.70
PEARSON_SHORTLIST_TOP_N = 30
PEARSON_MIN_SHORTLIST = 5

# ============================================================
# Веса фильтров
# ============================================================

SIMILARITY_WEIGHT_PEARSON = 4.0
SIMILARITY_WEIGHT_RANGE = 2.0
SIMILARITY_WEIGHT_NET_MOVE = 2.0
SIMILARITY_WEIGHT_MEAN_ABS_DIFF = 2.0
SIMILARITY_WEIGHT_EFFICIENCY = 2.0
SIMILARITY_WEIGHT_RANGE_POSITION = 0.0


def similarity_total_weight():
    # Сумма весов всех фильтров похожести.
    return (
            SIMILARITY_WEIGHT_PEARSON
            + SIMILARITY_WEIGHT_RANGE
            + SIMILARITY_WEIGHT_NET_MOVE
            + SIMILARITY_WEIGHT_MEAN_ABS_DIFF
            + SIMILARITY_WEIGHT_EFFICIENCY
            + SIMILARITY_WEIGHT_RANGE_POSITION
    )


# ============================================================
# Настройки прогнозного слоя
# ============================================================
#
# После similarity ranking берём только лучшие historical-кандидаты
# и по ним строим сводный прогноз future-path.

FORECAST_TOP_N_AFTER_SIMILARITY = 5

# ============================================================
# Настройки decision layer
# ============================================================
#
# Decision layer получает:
# - ranked_similarity_candidates
# - forecast_summary
#
# И возвращает:
# - LONG
# - SHORT
# - NO_TRADE
#
# Это первый и намеренно простой вариант правил.
# Все границы вынесены сюда, чтобы их можно было спокойно менять
# без переписывания логики.

DECISION_MIN_SIMILARITY_CANDIDATES = 5
DECISION_MIN_FORECAST_CANDIDATES = 5

# Минимальный итоговый similarity-score у лучшего кандидата.
DECISION_MIN_BEST_SIMILARITY_SCORE = 0.3

# Минимальный итоговый similarity-score у последнего кандидата
# внутри top-N, который реально попадает в forecast и decision.
DECISION_MIN_LAST_SIMILARITY_SCORE = 0.3

# Минимальная доля кандидатов, идущих в одну сторону.
DECISION_MIN_DIRECTIONAL_RATIO = 0.60

# Минимальное по модулю ожидаемое движение к концу часа.
# 0.0005 = 0.05%
DECISION_MIN_MEAN_FINAL_MOVE_ABS = 0.0005
DECISION_MIN_MEDIAN_FINAL_MOVE_ABS = 0.0005

# Требовать ли, чтобы mean и median указывали в одну сторону.
DECISION_REQUIRE_MEAN_AND_MEDIAN_SAME_DIRECTION = True

# Если True, то решение разрешается только если:
# - для LONG mean_max_drawdown не глубже порога
# - для SHORT mean_max_upside не выше порога
#
# Порог задаётся по модулю.
DECISION_USE_ADVERSE_MOVE_FILTER = False
DECISION_MAX_MEAN_ADVERSE_MOVE_ABS = 0.0010

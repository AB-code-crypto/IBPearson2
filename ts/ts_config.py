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
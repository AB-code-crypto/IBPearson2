# Tester

Мини-описание порядка запуска файлов в папке `tester`.

## Порядок

1. `current_hour_price_loader.py`  
   Загружает текущий рассматриваемый час из `price DB` целиком.

2. `prepared_candidates_loader.py`  
   Загружает все historical candidates из `prepared DB` для текущего часа.

3. `hour_correlation_runner.py`  
   Считает Pearson-корреляцию текущего часа против historical candidates в окне входа.  
   Сохраняет:
    - полный результат в `json`
    - краткую выжимку в `csv`

4. `hour_similarity_runner.py`  
   Берёт Pearson-shortlist и считает similarity-ranking.  
   Сохраняет:
    - полный результат в `json`
    - краткую выжимку в `csv`

5. `hour_forecast_runner.py`
    - запускаем после `hour_similarity_runner.py`;
    - файл прогоняет текущий час через:
        1) Pearson shortlist
        2) similarity ranking
        3) forecast по top-N после similarity
    - сохраняет:
        - полный результат в `output/json`
        - краткую сводку в `output/csv`
    - показывает в консоли:
        - сколько кандидатов дошло до forecast
        - positive / negative ratio
        - mean / median final move
        - проходят ли данные текущие decision-пороги forecast-слоя
6. `hour_decision_runner.py`
    - запускаем после `hour_forecast_runner.py`;
    - файл прогоняет текущий час через:
        1) Pearson shortlist
        2) similarity ranking
        3) forecast по top-N после similarity
        4) decision layer
    - сохраняет:
        - полный результат в `output/json`
        - краткую сводку в `output/csv`
    - показывает в консоли:
        - итоговое решение `LONG / SHORT / NO_TRADE`
        - причину решения
        - best / last similarity score
        - directional ratio
        - mean / median final move
7. `single_run_tester.py`
    - это первый основной tester-run;
    - запускаем один прогон на одном наборе `StrategyParams`;
    - на вход даём:
        - `instrument_code`
        - `start_utc`
        - `end_utc`
        - один объект `strategy_params_for_run`
    - внутри файл проходит по всем часам диапазона и на каждом часу считает:
        1) Pearson shortlist
        2) similarity
        3) forecast
        4) decision
    - сохраняет:
        - подробный результат в `output/json`
        - почасовой summary в `output/csv`
    - это уже не диагностический runner, а первый основной каркас тестера

## Смысл

Логика запуска идёт снизу вверх:

- сначала загружаем текущий час,
- потом загружаем всех historical candidates,
- потом смотрим Pearson,
- потом смотрим similarity.

Каждый файл можно запускать отдельно и смотреть результат его работы.

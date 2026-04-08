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

## Смысл

Логика запуска идёт снизу вверх:

- сначала загружаем текущий час,
- потом загружаем всех historical candidates,
- потом смотрим Pearson,
- потом смотрим similarity.

Каждый файл можно запускать отдельно и смотреть результат его работы.

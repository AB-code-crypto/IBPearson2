# Decision layer — короткая памятка

## Как сейчас принимается решение о входе

### 1. Текущий час накапливается по 5-секундным барам
Сигнал оценивается только в окне входа: с 30-й по 50-ю минуту часа.

---

### 2. Pearson layer
Для всех исторических кандидатов считается корреляция с текущим префиксом часа.

Дальше:
- отбрасываем кандидатов ниже `PEARSON_SHORTLIST_MIN_CORRELATION`
- берём не больше `PEARSON_SHORTLIST_TOP_N`

Итог: получаем Pearson-shortlist.

---

### 3. Similarity layer
Для Pearson-shortlist считаются similarity-фильтры.

Из них получается `final_score`.

После этого:
- кандидаты сортируются по `final_score`
- дальше берутся лучшие `FORECAST_TOP_N_AFTER_SIMILARITY`

Итог: получаем ядро лучших кандидатов для прогноза.

---

### 4. Forecast layer
Для top-N после similarity смотрим, что эти кандидаты делали дальше до конца часа.

Считаются:
- `final_move`
- `max_upside`
- `max_drawdown`
- `positive_ratio`
- `negative_ratio`
- `mean_final_move`
- `median_final_move`
- и другие forecast-метрики

Итог: получаем прогноз по группе лучших кандидатов.

---

## Как decision принимает решение

### Шаг 1. Хватает ли similarity-кандидатов
Если после similarity кандидатов меньше `DECISION_MIN_SIMILARITY_CANDIDATES`,
то сделки нет.

---

### Шаг 2. Хватает ли forecast-кандидатов
Если в forecast-группе кандидатов меньше `DECISION_MIN_FORECAST_CANDIDATES`,
то сделки нет.

---

### Шаг 3. Достаточно ли качественный similarity
Проверяется:

- лучший similarity score: `best_similarity_score`
- последний similarity score в используемой группе: `last_similarity_score`

Если:
- `best_similarity_score < DECISION_MIN_BEST_SIMILARITY_SCORE`
или
- `last_similarity_score < DECISION_MIN_LAST_SIMILARITY_SCORE`

то сделки нет.

Смысл:
не хотим ситуацию, когда только лидер хороший, а хвост уже слабый.

---

### Шаг 4. Согласованы ли mean и median
Берутся:
- `mean_final_move`
- `median_final_move`

Если они смотрят в разные стороны,
то сделки нет.

Смысл:
прогноз группы должен быть внутренне согласованным.

---

### Шаг 5. Достаточно ли сильный ожидаемый ход
Проверяется модуль:

- `abs(mean_final_move)`
- `abs(median_final_move)`

Если они меньше минимальных порогов,
то сделки нет.

Это и есть проверка, что потенциал движения вообще есть.

---

### Шаг 6. Достаточно ли группа согласована по направлению
Если прогноз вверх, смотрим `positive_ratio`.
Если прогноз вниз, смотрим `negative_ratio`.

Нужная доля должна быть не ниже:
`DECISION_MIN_DIRECTIONAL_RATIO`

Если доля слишком маленькая,
то сделки нет.

Смысл:
нужно, чтобы кандидаты не просто давали движение,
а давали его достаточно согласованно.

---

### Шаг 7. Adverse move filter
Этот фильтр сейчас есть в коде, но обычно выключен.

Если включить:
- для LONG смотрим, не слишком ли большой `mean_max_drawdown`
- для SHORT смотрим, не слишком ли большой `mean_max_upside`

Если adverse move слишком большой,
то сделки нет.

---

## Когда получаем LONG

Получаем `LONG`, если одновременно:

- similarity-кандидатов хватает
- forecast-кандидатов хватает
- similarity score достаточно сильный
- `mean_final_move > 0`
- `median_final_move > 0`
- `abs(mean_final_move)` проходит порог
- `abs(median_final_move)` проходит порог
- `positive_ratio` проходит порог
- adverse filter не запрещает вход

---

## Когда получаем SHORT

Получаем `SHORT`, если одновременно:

- similarity-кандидатов хватает
- forecast-кандидатов хватает
- similarity score достаточно сильный
- `mean_final_move < 0`
- `median_final_move < 0`
- `abs(mean_final_move)` проходит порог
- `abs(median_final_move)` проходит порог
- `negative_ratio` проходит порог
- adverse filter не запрещает вход

---

## Когда получаем NO_TRADE

Получаем `NO_TRADE`, если ломается хотя бы одно из условий выше.

Типовые причины:
- слишком мало Pearson / similarity / forecast кандидатов
- слабый similarity
- mean и median смотрят в разные стороны
- ожидаемое движение слишком маленькое
- directional ratio слишком слабый
- adverse move слишком большой

---

## Очень коротко

Логика сейчас такая:

1. нашли похожие часы
2. уточнили их через similarity
3. взяли лучшие кандидаты
4. посмотрели, что они в среднем делали дальше
5. проверили:
   - есть ли направление
   - достаточно ли оно сильное
   - достаточно ли оно согласованное
6. только после этого открываем сделку

Если один из этих шагов ломается — `NO_TRADE`.

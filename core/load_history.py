import asyncio
import sqlite3
from datetime import datetime, timezone

from ib_async import Contract

# Instrument — это наш реестр инструментов из contracts.py.
# Именно отсюда загрузчик узнаёт:
# - какие инструменты вообще существуют;
# - какие у них параметры;
# - какие у фьючерсов контракты;
# - какие интервалы активности у каждого контракта.
from contracts import Instrument

# Из db_sql мы берём только готовые SQL-шаблоны.
# В этом файле нет SQL-строк "вручную", чтобы структура таблиц и upsert-логика
# были собраны в одном месте.
from core.db_sql import (
    get_create_quotes_table_sql,
    get_upsert_quotes_sql,
    get_create_ohlc_table_sql,
    get_upsert_ohlc_sql,
)

# Из logger берём:
# - get_logger: чтобы создать логгер этого модуля;
# - log_info / log_warning: наши обёртки над logging.
# Важно: эти обёртки ещё и умеют при необходимости слать текст в Telegram.
from core.logger import get_logger, log_info, log_warning

# Логгер именно этого модуля.
# В логах будет видно, что сообщение пришло из load_history.py.
logger = get_logger(__name__)

# Пауза после каждого historical request.
#
# Зачем она нужна:
# - IB ограничивает частоту исторических запросов;
# - если дёргать reqHistoricalDataAsync слишком часто, можно упереться в pacing limits;
# - поэтому после каждого запроса делаем паузу, чтобы качать спокойно и предсказуемо.
#
# Сейчас стоит 11 секунд — это сознательно довольно консервативный режим.
HISTORICAL_REQUEST_DELAY_SECONDS = 11

# Пауза ожидания, пока monitor_task восстановит соединение с IB.
#
# Эта пауза используется только в циклах ожидания реконнекта:
# - если соединение с IB пропало;
# - история не падает сразу;
# - а просто ждёт, пока monitor_task переподключит IB;
# - и раз в RECONNECT_WAIT_SECONDS секунд проверяет, не поднялось ли соединение.
RECONNECT_WAIT_SECONDS = 1


def format_utc(dt, for_ib=False):
    # Универсальный форматтер времени.
    #
    # Используется в двух местах:
    #
    # 1. Для записи времени в БД:
    #    тогда нужен человекочитаемый формат:
    #    YYYY-MM-DD HH:MM:SS
    #
    # 2. Для IB historical request:
    #    тогда нужен формат, который понимает TWS / IB Gateway:
    #    YYYYMMDD HH:MM:SS UTC
    #
    # На вход функция получает datetime.
    # Сначала жёстко приводим его к UTC, чтобы:
    # - не зависеть от локального часового пояса системы;
    # - в БД и запросах всегда работать в одной временной зоне.
    dt = dt.astimezone(timezone.utc)

    # Если формат нужен для IB historical request, возвращаем строку
    # специального вида, который понимает reqHistoricalDataAsync.
    if for_ib:
        return dt.strftime("%Y%m%d %H:%M:%S UTC")

    # Иначе возвращаем удобный для БД и для глаз формат.
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_duration_str(start_dt, end_dt):
    # IB historical request принимает не start и end одновременно,
    # а пару:
    # - endDateTime
    # - durationStr
    #
    # Поэтому durationStr мы вычисляем сами как разницу между концом и началом.
    total_seconds = int((end_dt - start_dt).total_seconds())

    # Если интервал нулевой или отрицательный — это ошибка логики вызова.
    # Такое сразу останавливаем.
    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть больше начала")

    # Возвращаем durationStr в формате, который понимает IB:
    # например "3600 S".
    return f"{total_seconds} S"


def build_table_name(instrument_code, bar_size_setting):
    # Простая функция генерации имени таблицы.
    #
    # Логика очень простая:
    # имя = код инструмента + "_" + таймфрейм
    #
    # Примеры:
    # - MNQ + "5 secs" -> MNQ_5s
    # - VIX + "1 hour" -> VIX_1h
    #
    # Здесь мы не делаем сложных проверок,
    # а просто приводим barSizeSetting к короткому виду без пробелов.
    suffix = (
        bar_size_setting
        .replace(" ", "")
        .replace("secs", "s")
        .replace("sec", "s")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("mins", "m")
        .replace("min", "m")
    )

    return f"{instrument_code}_{suffix}"


def build_futures_contract(instrument_code, instrument_row, contract_row):
    # Собираем полноценный IB Contract для фьючерса.
    #
    # Почему именно так:
    # - мы не хотим каждый раз делать резолв контракта через IB;
    # - все нужные поля уже заранее подготовлены в contracts.py;
    # - значит просто берём их и собираем Contract напрямую.
    #
    # instrument_row содержит общие поля инструмента:
    # - secType
    # - exchange
    # - currency
    # - tradingClass
    # - multiplier
    #
    # contract_row содержит поля конкретного контракта:
    # - conId
    # - localSymbol
    # - lastTradeDateOrContractMonth
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        tradingClass=instrument_row["tradingClass"],
        multiplier=str(instrument_row["multiplier"]),
        conId=contract_row["conId"],
        localSymbol=contract_row["localSymbol"],
        lastTradeDateOrContractMonth=contract_row["lastTradeDateOrContractMonth"],
    )


def build_index_contract(instrument_code, instrument_row):
    # Собираем контракт индекса.
    #
    # Для индекса набор полей проще, чем для фьючерса:
    # - secType
    # - symbol
    # - exchange
    # - currency
    # - conId
    #
    # Этого достаточно для обращения к IB.
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        conId=instrument_row["conId"],
    )


def iter_chunks(start_ts, end_ts, chunk_seconds):
    # Генератор, который разбивает общий интервал [start_ts, end_ts)
    # на последовательность более мелких кусков.
    #
    # Почему это нужно:
    # - для 5-секундных баров мы качаем историю не сразу огромным периодом,
    #   а по одному часу;
    # - так мы лучше укладываемся в рекомендуемый размер historical request;
    # - и это удобнее для контроля процесса.
    #
    # Интервал полуоткрытый:
    # [start, end)
    #
    # Это важно, потому что тогда соседние куски не пересекаются.
    current_start_ts = start_ts

    while current_start_ts < end_ts:
        # Правая граница очередного куска:
        # либо current_start + chunk_seconds,
        # либо общий end_ts, если мы уже подошли к концу диапазона.
        current_end_ts = min(current_start_ts + chunk_seconds, end_ts)

        # Возвращаем очередной кусок.
        yield current_start_ts, current_end_ts

        # Следующий кусок начнётся ровно там, где закончился предыдущий.
        current_start_ts = current_end_ts


def get_chunk_seconds(bar_size_setting, start_ts, end_ts):
    # Определяем размер куска загрузки.
    #
    # Сейчас логика простая:
    # - если бар 5 секунд, качаем по 1 часу;
    # - для остальных таймфреймов пока качаем одним куском целиком.
    #
    # Почему для 5 secs именно 3600 секунд:
    # - это час;
    # - так уже проверяли и договорились использовать именно такой размер куска.
    if bar_size_setting == "5 secs":
        return 3600

    # Для всех остальных случаев пока без дополнительного дробления.
    return end_ts - start_ts


def build_quote_rows(bid_bars, ask_bars, contract_name):
    # Эта функция склеивает два потока баров:
    # - BID
    # - ASK
    #
    # На выходе получается единый набор строк,
    # где в каждой строке лежат:
    # - время бара;
    # - контракт;
    # - ask_open / bid_open;
    # - ask_high / bid_high;
    # - ask_low / bid_low;
    # - ask_close / bid_close;
    # - volume / average / bar_count (пока пустые).
    #
    # Склейка делается по bar_time_ts.
    # Это ключевой момент.
    rows_by_ts = {}

    # Сначала проходим ask-бары.
    for bar in ask_bars:
        # bar.date из IB приводим к UTC.
        dt = bar.date.astimezone(timezone.utc)

        # bar_time_ts — основной технический ключ строки в БД.
        bar_time_ts = int(dt.timestamp())

        # Если строки для этого timestamp ещё нет — создаём пустой каркас.
        if bar_time_ts not in rows_by_ts:
            rows_by_ts[bar_time_ts] = {
                "bar_time_ts": bar_time_ts,
                "bar_time": format_utc(dt),
                "contract": contract_name,
                "ask_open": None,
                "bid_open": None,
                "ask_high": None,
                "bid_high": None,
                "ask_low": None,
                "bid_low": None,
                "ask_close": None,
                "bid_close": None,
                "volume": None,
                "average": None,
                "bar_count": None,
            }

        # Заполняем ask-часть строки.
        rows_by_ts[bar_time_ts]["ask_open"] = bar.open
        rows_by_ts[bar_time_ts]["ask_high"] = bar.high
        rows_by_ts[bar_time_ts]["ask_low"] = bar.low
        rows_by_ts[bar_time_ts]["ask_close"] = bar.close

    # Потом проходим bid-бары.
    # Почему не одновременно:
    # - потому что IB возвращает два отдельных массива;
    # - нам проще сначала положить один поток, потом второй.
    for bar in bid_bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())

        # Если строки ещё нет — создаём такой же пустой каркас.
        # Это позволяет не зависеть от того, что пришло первым:
        # - ask
        # - или bid
        if bar_time_ts not in rows_by_ts:
            rows_by_ts[bar_time_ts] = {
                "bar_time_ts": bar_time_ts,
                "bar_time": format_utc(dt),
                "contract": contract_name,
                "ask_open": None,
                "bid_open": None,
                "ask_high": None,
                "bid_high": None,
                "ask_low": None,
                "bid_low": None,
                "ask_close": None,
                "bid_close": None,
                "volume": None,
                "average": None,
                "bar_count": None,
            }

        # Заполняем bid-часть строки.
        rows_by_ts[bar_time_ts]["bid_open"] = bar.open
        rows_by_ts[bar_time_ts]["bid_high"] = bar.high
        rows_by_ts[bar_time_ts]["bid_low"] = bar.low
        rows_by_ts[bar_time_ts]["bid_close"] = bar.close

    # Дальше преобразуем словарь в список кортежей.
    # Именно такие кортежи потом удобно отдавать в executemany().
    rows = []

    # Идём по timestamp в отсортированном порядке,
    # чтобы строки шли по времени.
    for bar_time_ts in sorted(rows_by_ts.keys()):
        row = rows_by_ts[bar_time_ts]

        rows.append(
            (
                row["bar_time_ts"],
                row["bar_time"],
                row["contract"],
                row["ask_open"],
                row["bid_open"],
                row["ask_high"],
                row["bid_high"],
                row["ask_low"],
                row["bid_low"],
                row["ask_close"],
                row["bid_close"],
                row["volume"],
                row["average"],
                row["bar_count"],
            )
        )

    return rows


def build_ohlc_rows(bars, contract_name):
    # Эта функция нужна для "одиночного" потока OHLC,
    # когда нам не надо склеивать BID и ASK.
    #
    # Пример:
    # - индекс VIX с whatToShow="TRADES"
    #
    # Тогда каждый bar превращается в одну готовую строку.
    rows = []

    for bar in bars:
        dt = bar.date.astimezone(timezone.utc)
        bar_time_ts = int(dt.timestamp())

        rows.append(
            (
                bar_time_ts,
                format_utc(dt),
                contract_name,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.average,
                bar.barCount,
            )
        )

    return rows


def write_quote_rows_to_sqlite(db_path, table_name, rows):
    # Запись BID/ASK-строк в SQLite.
    #
    # Здесь делаем всё максимально просто:
    # 1. берём SQL на CREATE TABLE;
    # 2. берём SQL на UPSERT;
    # 3. открываем соединение;
    # 4. включаем нужные PRAGMA;
    # 5. создаём таблицу если её ещё нет;
    # 6. делаем executemany по всем строкам;
    # 7. commit;
    # 8. закрываем соединение.
    create_sql = get_create_quotes_table_sql(table_name)
    upsert_sql = get_upsert_quotes_sql(table_name)

    conn = sqlite3.connect(db_path)

    try:
        # WAL — чтобы чтения и запись жили спокойнее.
        conn.execute("PRAGMA journal_mode=WAL;")

        # NORMAL — нормальный баланс надёжности и скорости.
        conn.execute("PRAGMA synchronous=NORMAL;")

        # Если БД занята — немного подождать, а не падать мгновенно.
        conn.execute("PRAGMA busy_timeout=5000;")

        # Создаём таблицу, если её ещё нет.
        conn.execute(create_sql)

        # Пишем все строки пачкой.
        conn.executemany(upsert_sql, rows)

        # Фиксируем транзакцию.
        conn.commit()

    finally:
        # Соединение с SQLite закрываем в любом случае.
        conn.close()


def write_ohlc_rows_to_sqlite(db_path, table_name, rows):
    # То же самое, что write_quote_rows_to_sqlite(),
    # но для одиночного OHLC-потока.
    create_sql = get_create_ohlc_table_sql(table_name)
    upsert_sql = get_upsert_ohlc_sql(table_name)

    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")

        conn.execute(create_sql)
        conn.executemany(upsert_sql, rows)
        conn.commit()

    finally:
        conn.close()


def get_last_bar_time_ts(db_path, table_name):
    # Берём максимальный bar_time_ts из таблицы.
    #
    # Зачем это нужно:
    # - чтобы не качать историю всегда с самого начала;
    # - а продолжать с последнего уже записанного бара.
    #
    # Если таблицы нет или она пустая — возвращаем None.
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.execute(f"SELECT MAX(bar_time_ts) FROM {table_name}")
        row = cursor.fetchone()

        # Если таблица существует, но данных нет.
        if row is None or row[0] is None:
            return None

        return int(row[0])

    except sqlite3.OperationalError:
        # Например, если таблица ещё не создана.
        return None

    finally:
        conn.close()


async def wait_for_ib_connection(ib):
    # Эта функция нужна для переживания реконнекта.
    #
    # Если monitor_task уже заметил потерю соединения и начал переподключение,
    # история не должна падать.
    #
    # Вместо этого:
    # - ждём, пока ib.isConnected() снова станет True;
    # - раз в RECONNECT_WAIT_SECONDS секунд проверяем состояние;
    # - один раз пишем warning, что ждём реконнект;
    # - после восстановления пишем info.
    wait_logged = False

    while not ib.isConnected():
        if not wait_logged:
            log_warning(
                logger,
                "Загрузка истории ждёт восстановления соединения с IB...",
                to_telegram=False,
            )
            wait_logged = True

        await asyncio.sleep(RECONNECT_WAIT_SECONDS)

    if wait_logged:
        log_info(
            logger,
            "Соединение с IB восстановлено, загрузка истории продолжается",
            to_telegram=False,
        )


async def request_historical_data_with_reconnect(
        ib,
        contract,
        end_dt,
        start_dt,
        bar_size_setting,
        what_to_show,
        use_rth,
):
    # Это обёртка над reqHistoricalDataAsync().
    #
    # Главная идея:
    # - если соединение живо, делаем запрос;
    # - если соединения нет — ждём реконнект;
    # - если соединение отвалилось прямо во время запроса,
    #   ловим ConnectionError, ждём реконнект и повторяем тот же запрос.
    #
    # Благодаря этому history loader переживает ночные рестарты TWS
    # и временные обрывы связи.
    while True:
        # Перед каждым запросом убеждаемся, что соединение живо.
        await wait_for_ib_connection(ib)

        try:
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=format_utc(end_dt, for_ib=True),
                durationStr=build_duration_str(start_dt, end_dt),
                barSizeSetting=bar_size_setting,
                whatToShow=what_to_show,
                useRTH=use_rth,
                formatDate=2,
                keepUpToDate=False,
            )

            # Если запрос выполнился успешно — сразу возвращаем бары.
            return bars

        except asyncio.CancelledError:
            # Если задачу отменили снаружи — обязательно пробрасываем отмену дальше.
            raise

        except ConnectionError:
            # Если в момент запроса соединение пропало,
            # не падаем, а ждём реконнект и повторяем тот же запрос.
            log_warning(
                logger,
                f"Потеря соединения во время historical request {what_to_show}, "
                f"жду реконнект и повторяю запрос",
                to_telegram=False,
            )
            await asyncio.sleep(RECONNECT_WAIT_SECONDS)


async def request_current_time_with_reconnect(ib):
    # Аналогичная обёртка, но для запроса времени сервера.
    #
    # Нужна в ветке индексов, где end_ts берётся как текущее время сервера IB.
    while True:
        await wait_for_ib_connection(ib)

        try:
            return await ib.reqCurrentTimeAsync()

        except asyncio.CancelledError:
            raise

        except ConnectionError:
            log_warning(
                logger,
                "Потеря соединения во время запроса server time, жду реконнект и повторяю запрос",
                to_telegram=False,
            )
            await asyncio.sleep(RECONNECT_WAIT_SECONDS)


async def load_history_bid_ask_once(
        ib,
        contract,
        db_path,
        table_name,
        start_dt,
        end_dt,
        bar_size_setting,
        use_rth,
):
    # Это одна "атомарная" операция загрузки истории BID+ASK
    # для одного контракта и одного куска времени.
    #
    # Последовательность такая:
    # 1. запрашиваем BID;
    # 2. ждём паузу;
    # 3. запрашиваем ASK;
    # 4. ждём паузу;
    # 5. склеиваем BID и ASK;
    # 6. пишем результат в БД;
    # 7. возвращаем количество записанных строк.

    bid_bars = await request_historical_data_with_reconnect(
        ib=ib,
        contract=contract,
        end_dt=end_dt,
        start_dt=start_dt,
        bar_size_setting=bar_size_setting,
        what_to_show="BID",
        use_rth=use_rth,
    )

    # Пауза после первого historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    ask_bars = await request_historical_data_with_reconnect(
        ib=ib,
        contract=contract,
        end_dt=end_dt,
        start_dt=start_dt,
        bar_size_setting=bar_size_setting,
        what_to_show="ASK",
        use_rth=use_rth,
    )

    # Пауза после второго historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    # Склеиваем два потока в единые строки.
    rows = build_quote_rows(
        bid_bars=bid_bars,
        ask_bars=ask_bars,
        contract_name=contract.localSymbol,
    )

    # Запись в SQLite выносим в отдельный thread,
    # чтобы не блокировать event loop во время дисковой операции.
    await asyncio.to_thread(
        write_quote_rows_to_sqlite,
        db_path,
        table_name,
        rows,
    )

    return len(rows)


async def load_history_single_stream_once(
        ib,
        contract,
        db_path,
        table_name,
        start_dt,
        end_dt,
        bar_size_setting,
        what_to_show,
        use_rth,
        contract_name,
):
    # Аналогичная "атомарная" операция для одиночного потока OHLC.
    #
    # Пример: индекс VIX с whatToShow="TRADES".
    bars = await request_historical_data_with_reconnect(
        ib=ib,
        contract=contract,
        end_dt=end_dt,
        start_dt=start_dt,
        bar_size_setting=bar_size_setting,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )

    # Пауза после historical request.
    await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

    rows = build_ohlc_rows(
        bars=bars,
        contract_name=contract_name,
    )

    await asyncio.to_thread(
        write_ohlc_rows_to_sqlite,
        db_path,
        table_name,
        rows,
    )

    return len(rows)


async def load_history_task(ib, settings):
    # Главная таска загрузки истории.
    #
    # Именно её запускает main.py как отдельную задачу.
    #
    # Что она делает в целом:
    # 1. проходит по реестру Instrument;
    # 2. для каждого инструмента строит имя таблицы;
    # 3. смотрит, докуда уже есть история в БД;
    # 4. для FUT качает BID + ASK;
    # 5. для одиночных потоков качает то, что указано в whatToShow;
    # 6. пишет всё в price_db_path;
    # 7. по завершении пишет итоговое количество строк.
    log_info(logger, "Запускаю задачу первичной загрузки истории", to_telegram=True)

    # Накопительный счётчик — сколько строк суммарно записали за всю задачу.
    total_rows_written = 0

    # Идём по всем инструментам из contracts.py.
    for instrument_code, instrument_row in Instrument.items():
        # Строим имя таблицы по инструменту и таймфрейму.
        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        # Смотрим, есть ли уже история в этой таблице.
        # Если есть — продолжаем с последнего бара.
        last_bar_time_ts = await asyncio.to_thread(
            get_last_bar_time_ts,
            settings.price_db_path,
            table_name,
        )

        if last_bar_time_ts is None:
            log_info(
                logger,
                f"Начинаю загрузку истории: instrument={instrument_code}, table={table_name}, с самого начала",
                to_telegram=False,
            )
        else:
            log_info(
                logger,
                f"Продолжаю загрузку истории: instrument={instrument_code}, table={table_name}, "
                f"с bar_time_ts={last_bar_time_ts}",
                to_telegram=False,
            )

        # Ветка для фьючерсов.
        # Для них сейчас жёстко качаем BID + ASK.
        if instrument_row["secType"] == "FUT":
            # Проходим по всем контрактам этого инструмента.
            for contract_row in instrument_row["contracts"]:
                contract = build_futures_contract(
                    instrument_code=instrument_code,
                    instrument_row=instrument_row,
                    contract_row=contract_row,
                )

                # Интервал активности контракта берём из contracts.py.
                start_ts = contract_row["active_from_ts_utc"]
                end_ts = contract_row["active_to_ts_utc"]

                # Если в таблице уже есть история, стартуем не с самого начала,
                # а с последнего записанного timestamp.
                if last_bar_time_ts is not None:
                    start_ts = max(start_ts, last_bar_time_ts)

                # Если по этому контракту всё уже загружено — пропускаем.
                if start_ts >= end_ts:
                    log_info(
                        logger,
                        f"Пропускаю контракт {contract.localSymbol}, история уже есть",
                        to_telegram=False,
                    )
                    continue

                # Определяем размер куска.
                chunk_seconds = get_chunk_seconds(
                    instrument_row["barSizeSetting"],
                    start_ts,
                    end_ts,
                )

                # Идём по всем кускам этого контракта.
                for chunk_start_ts, chunk_end_ts in iter_chunks(
                        start_ts,
                        end_ts,
                        chunk_seconds,
                ):
                    chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
                    chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

                    # Качаем один кусок BID+ASK.
                    rows_written = await load_history_bid_ask_once(
                        ib=ib,
                        contract=contract,
                        db_path=settings.price_db_path,
                        table_name=table_name,
                        start_dt=chunk_start_dt,
                        end_dt=chunk_end_dt,
                        bar_size_setting=instrument_row["barSizeSetting"],
                        use_rth=instrument_row["useRTH"],
                    )

                    # Увеличиваем общий счётчик.
                    total_rows_written += rows_written

                    # Пишем лог по завершённому куску.
                    log_info(
                        logger,
                        f"История загружена: {instrument_code} {contract.localSymbol} "
                        f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} "
                        f"rows={rows_written}",
                        to_telegram=False,
                    )

            # После обработки всех фьючерсных контрактов этого инструмента
            # переходим к следующему инструменту.
            continue

        # Ветка для индексов и других одиночных потоков.
        contract = build_index_contract(
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )

        # Левая граница — это active_from инструмента.
        start_ts = instrument_row["active_from_ts_utc"]

        # Правая граница — текущее время сервера IB.
        server_time = await request_current_time_with_reconnect(ib)
        end_ts = int(server_time.astimezone(timezone.utc).timestamp())

        # Если история уже есть — продолжаем с последнего бара.
        if last_bar_time_ts is not None:
            start_ts = max(start_ts, last_bar_time_ts)

        # Если качать уже нечего — пропускаем инструмент.
        if start_ts >= end_ts:
            log_info(
                logger,
                f"Пропускаю {instrument_code}, история уже есть",
                to_telegram=False,
            )
            continue

        # Определяем размер куска.
        chunk_seconds = get_chunk_seconds(
            instrument_row["barSizeSetting"],
            start_ts,
            end_ts,
        )

        # Идём по кускам.
        for chunk_start_ts, chunk_end_ts in iter_chunks(
                start_ts,
                end_ts,
                chunk_seconds,
        ):
            chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
            chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

            # Качаем один кусок одиночного OHLC-потока.
            rows_written = await load_history_single_stream_once(
                ib=ib,
                contract=contract,
                db_path=settings.price_db_path,
                table_name=table_name,
                start_dt=chunk_start_dt,
                end_dt=chunk_end_dt,
                bar_size_setting=instrument_row["barSizeSetting"],
                what_to_show=instrument_row["whatToShow"],
                use_rth=instrument_row["useRTH"],
                contract_name=instrument_code,
            )

            total_rows_written += rows_written

            log_info(
                logger,
                f"История загружена: {instrument_code} "
                f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} "
                f"rows={rows_written}",
                to_telegram=False,
            )

    # Финальный лог по всей таске.
    log_info(
        logger,
        f"Первичная загрузка истории завершена. Всего записано строк: {total_rows_written}",
        to_telegram=True,
    )

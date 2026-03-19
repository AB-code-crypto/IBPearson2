import asyncio
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from ib_async import Contract

from contracts import Instrument
from core.db_sql import get_upsert_quotes_ask_sql, get_upsert_quotes_bid_sql
from core.logger import get_logger, log_info, log_warning


logger = get_logger(__name__)

# Код инструмента, по которому хотим получать real-time 5-second бары.
REALTIME_INSTRUMENT_CODE = "MNQ"

# Конкретный фьючерс, с которым сейчас работаем.
# Пока задача узкая: просто подписаться на один известный контракт
# и начать получать real-time бары.
REALTIME_CONTRACT_LOCAL_SYMBOL = "MNQH6"

# reqRealTimeBars у IB поддерживает только 5-секундные бары.
REALTIME_BAR_SIZE_SECONDS = 5

# Какие потоки данных хотим получать в realtime.
# Для нашей схемы нужны отдельные бары по BID и ASK,
# потому что именно так мы уже работаем с историческими данными и БД.
REALTIME_WHAT_TO_SHOW_LIST = ("BID", "ASK")

# Как часто ждём восстановления соединения / market data farm
# перед самой первой подпиской.
REALTIME_READY_WAIT_SECONDS = 1


def build_futures_contract(instrument_code, instrument_row, contract_row):
    # Собираем IB Contract без дополнительного resolve через IB.
    # Все нужные поля уже заранее зафиксированы в contracts.py.
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


def get_realtime_instrument_row(instrument_code):
    # Берём настройки инструмента из нашего реестра.
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code} не найден в contracts.py")

    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] != "FUT":
        raise ValueError(
            f"Realtime loader сейчас поддерживает только FUT, получено: {instrument_row['secType']}"
        )

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Realtime loader ожидает barSizeSetting='5 secs', получено: {instrument_row['barSizeSetting']}"
        )

    return instrument_row


def get_contract_row_by_local_symbol(instrument_row, local_symbol):
    # Ищем в contracts.py строго тот контракт, который указали в настройках.
    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == local_symbol:
            return contract_row

    raise ValueError(
        f"Контракт {local_symbol} не найден в списке contracts для инструмента"
    )


async def wait_for_realtime_ready(ib, ib_health):
    # Для первой подписки ждём нормального состояния соединения и market data.
    wait_reason = ""

    while True:
        if not ib.isConnected():
            if wait_reason != "connection":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления API-соединения с IB...",
                    to_telegram=False,
                )
                wait_reason = "connection"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if not ib_health.ib_backend_ok:
            if wait_reason != "backend":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления backend IB...",
                    to_telegram=False,
                )
                wait_reason = "backend"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if not ib_health.market_data_ok:
            if wait_reason != "market_data":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления market data farm...",
                    to_telegram=False,
                )
                wait_reason = "market_data"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if wait_reason:
            log_info(
                logger,
                "Realtime loader продолжает работу: соединение и market data снова в норме",
                to_telegram=False,
            )

        return


def subscribe_realtime_bars(ib, contract, what_to_show, use_rth):
    # Открываем подписку на 5-секундные real-time бары.
    return ib.reqRealTimeBars(
        contract=contract,
        barSize=REALTIME_BAR_SIZE_SECONDS,
        whatToShow=what_to_show,
        useRTH=use_rth,
    )


def cancel_realtime_bars_safe(ib, realtime_bars):
    # Безопасно отменяем активную подписку, если она была создана.
    if realtime_bars is None:
        return

    try:
        ib.cancelRealTimeBars(realtime_bars)
    except Exception as exc:
        log_warning(
            logger,
            f"Не удалось отменить realtime-подписку: {exc}",
            to_telegram=False,
        )


def format_utc(dt):
    # Универсальный формат времени в UTC для БД и логов.
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def resolve_db_path(db_path_text):
    # Преобразуем путь из config.py в абсолютный путь проекта.
    #
    # main.py лежит в корне проекта, папки core и data тоже лежат в корне.
    # Поэтому относительные пути вида data/price.sqlite3 резолвим от корня проекта,
    # а не от папки core, где находится сам realtime loader.
    db_path = Path(db_path_text)

    if db_path.is_absolute():
        return db_path

    project_root = Path(__file__).resolve().parent.parent
    return (project_root / db_path).resolve()


def build_table_name(instrument_code, bar_size_setting):
    # Простая и предсказуемая схема имени таблицы.
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


def validate_price_value(value, field_name, stream_name, contract_name, bar_time_text):
    # Проверяем одно конкретное ценовое поле realtime-бара.
    #
    # Если значение некорректно, возвращаем готовую строку для лога.
    if value is None:
        return (
            f"Некорректная цена в realtime {stream_name} для {contract_name}, "
            f"bar_time={bar_time_text}, field={field_name}, value={value}"
        )

    if isinstance(value, bool):
        return (
            f"Некорректная цена в realtime {stream_name} для {contract_name}, "
            f"bar_time={bar_time_text}, field={field_name}, value={value}"
        )

    if not isinstance(value, (int, float)):
        return (
            f"Некорректная цена в realtime {stream_name} для {contract_name}, "
            f"bar_time={bar_time_text}, field={field_name}, value={value}"
        )

    numeric_value = float(value)

    if not math.isfinite(numeric_value):
        return (
            f"Некорректная цена в realtime {stream_name} для {contract_name}, "
            f"bar_time={bar_time_text}, field={field_name}, value={value}"
        )

    if numeric_value <= 0:
        return (
            f"Некорректная цена в realtime {stream_name} для {contract_name}, "
            f"bar_time={bar_time_text}, field={field_name}, value={value}"
        )

    return None


def validate_realtime_bar(contract, what_to_show, bar):
    # Проверяем весь realtime-бар целиком.
    bar_time_text = format_utc(bar.time)

    for field_name, field_value in (
            ("open", bar.open_),
            ("high", bar.high),
            ("low", bar.low),
            ("close", bar.close),
    ):
        validation_error = validate_price_value(
            value=field_value,
            field_name=field_name,
            stream_name=what_to_show,
            contract_name=contract.localSymbol,
            bar_time_text=bar_time_text,
        )
        if validation_error is not None:
            return validation_error

    return None


def format_realtime_bar_message(contract, what_to_show, bar):
    # Собираем одну строку лога по новому 5-секундному бару.
    # В лог обязательно добавляем тип потока,
    # чтобы сразу было видно, это BID-бар или ASK-бар.
    bar_time_text = format_utc(bar.time)

    return (
        f"RT BAR {contract.localSymbol} | {what_to_show} | {bar_time_text} | "
        f"O={bar.open_} H={bar.high} L={bar.low} C={bar.close} "
        f"V={bar.volume} WAP={bar.wap} COUNT={bar.count}"
    )


def open_quotes_db(db_path):
    # Открываем только уже существующую SQLite БД.
    #
    # SQLite по умолчанию может создать новый файл, если путь неверный.
    # Для realtime loader это недопустимо, поэтому открываем БД в режиме rw
    # через URI: если файла нет, соединение сразу упадёт с ошибкой.
    db_uri = f"file:{db_path}?mode=rw"

    conn = sqlite3.connect(db_uri, uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def write_realtime_bar_to_sqlite(conn, table_name, contract_name, what_to_show, bar):
    # Записываем одну сторону realtime-бара в SQLite.
    #
    # BID и ASK приходят раздельно, поэтому и пишем их раздельными UPSERT-ами,
    # которые обновляют только свою сторону строки.
    dt = bar.time.astimezone(timezone.utc)
    bar_time_ts = int(dt.timestamp())
    bar_time = format_utc(dt)

    if what_to_show == "ASK":
        sql = get_upsert_quotes_ask_sql(table_name)
        params = (
            bar_time_ts,
            bar_time,
            contract_name,
            bar.open_,
            bar.high,
            bar.low,
            bar.close,
        )
    elif what_to_show == "BID":
        sql = get_upsert_quotes_bid_sql(table_name)
        params = (
            bar_time_ts,
            bar_time,
            contract_name,
            bar.open_,
            bar.high,
            bar.low,
            bar.close,
        )
    else:
        raise ValueError(f"Неподдерживаемый realtime stream: {what_to_show}")

    conn.execute(sql, params)
    conn.commit()


def build_realtime_update_handler(contract, what_to_show, conn, table_name):
    # Для каждой отдельной подписки делаем свой обработчик,
    # чтобы BID и ASK обрабатывались независимо и без догадок по контексту.
    def on_bar_update(bars, has_new_bar):
        # Пишем только когда реально добавился новый бар,
        # а не когда просто обновился последний.
        if not has_new_bar:
            return

        if len(bars) == 0:
            return

        bar = bars[-1]
        validation_error = validate_realtime_bar(contract, what_to_show, bar)

        if validation_error is not None:
            log_warning(
                logger,
                f"Пропускаю некорректный realtime-бар. {validation_error}",
                to_telegram=False,
            )
            return

        write_realtime_bar_to_sqlite(
            conn=conn,
            table_name=table_name,
            contract_name=contract.localSymbol,
            what_to_show=what_to_show,
            bar=bar,
        )

        log_info(
            logger,
            format_realtime_bar_message(contract, what_to_show, bar),
            to_telegram=False,
        )

    return on_bar_update


async def load_realtime_task(ib, ib_health, settings):
    # Текущая realtime-версия loader-а:
    # - берём один заранее выбранный контракт;
    # - открываем отдельные подписки на BID и ASK 5-second bars;
    # - пишем новые бары в SQLite в таблицу вида MNQ_5s;
    # - BID и ASK пишем независимо по мере их прихода;
    # - никакой переподписки и логики ролловера здесь нет.
    instrument_code = REALTIME_INSTRUMENT_CODE
    contract_local_symbol = REALTIME_CONTRACT_LOCAL_SYMBOL

    instrument_row = get_realtime_instrument_row(instrument_code)
    contract_row = get_contract_row_by_local_symbol(instrument_row, contract_local_symbol)
    contract = build_futures_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )

    use_rth = instrument_row["useRTH"]
    table_name = build_table_name(instrument_code, instrument_row["barSizeSetting"])
    db_path = resolve_db_path(settings.price_db_path)
    db_conn = None

    # Храним все открытые подписки и их обработчики,
    # чтобы в finally корректно всё снять и отменить.
    current_subscriptions = []

    try:
        log_info(
            logger,
            f"Запускаю realtime loader для {instrument_code}",
            to_telegram=False,
        )

        db_conn = open_quotes_db(db_path)

        log_info(
            logger,
            f"Realtime loader: запись в БД включена. db={db_path}, table={table_name}",
            to_telegram=False,
        )

        await wait_for_realtime_ready(ib, ib_health)

        for what_to_show in REALTIME_WHAT_TO_SHOW_LIST:
            log_info(
                logger,
                f"Realtime loader: открываю подписку на {contract.localSymbol} "
                f"(conId={contract.conId}), whatToShow={what_to_show}, useRTH={use_rth}",
                to_telegram=False,
            )

            realtime_bars = subscribe_realtime_bars(
                ib=ib,
                contract=contract,
                what_to_show=what_to_show,
                use_rth=use_rth,
            )

            update_handler = build_realtime_update_handler(
                contract=contract,
                what_to_show=what_to_show,
                conn=db_conn,
                table_name=table_name,
            )
            realtime_bars.updateEvent += update_handler

            current_subscriptions.append(
                {
                    "what_to_show": what_to_show,
                    "realtime_bars": realtime_bars,
                    "update_handler": update_handler,
                }
            )

            log_info(
                logger,
                f"Подписался на real-time 5-second bars: {contract.localSymbol} "
                f"(conId={contract.conId}), whatToShow={what_to_show}, useRTH={use_rth}",
                to_telegram=False,
            )

        # Дальше просто держим подписки живыми и ждём новые бары.
        while True:
            await asyncio.sleep(3600)

    finally:
        for subscription_row in current_subscriptions:
            realtime_bars = subscription_row["realtime_bars"]
            update_handler = subscription_row["update_handler"]

            if realtime_bars is not None and update_handler is not None:
                try:
                    realtime_bars.updateEvent -= update_handler
                except Exception as exc:
                    log_warning(
                        logger,
                        f"Не удалось снять обработчик realtime updateEvent "
                        f"для {subscription_row['what_to_show']}: {exc}",
                        to_telegram=False,
                    )

            cancel_realtime_bars_safe(ib, realtime_bars)

        if db_conn is not None:
            try:
                db_conn.close()
            except Exception as exc:
                log_warning(
                    logger,
                    f"Не удалось закрыть SQLite-соединение realtime loader-а: {exc}",
                    to_telegram=False,
                )

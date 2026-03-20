import asyncio
import math
import sqlite3
from datetime import timezone
from pathlib import Path

from ib_async import Contract

from contracts import Instrument
from core.db_initializer import build_table_name
from core.db_sql import upsert_quotes_ask_sql, upsert_quotes_bid_sql
from core.logger import get_logger, log_info, log_warning
from core.recent_gaps_service import (
    note_first_realtime_bar_timestamps,
    is_first_synced_bid_ask_bar_ready,
    get_recent_backfill_sync_ts,
    backfill_recent_hour,
)

logger = get_logger(__name__)

# В realtime сейчас работаем только с одним активным фьючерсом.
# Сам активный контракт приходит снаружи в виде словаря ACTIVE_FUTURES,
# например: {"MNQ": "MNQM6"}.

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


def get_realtime_active_future(active_futures):
    # Для realtime сейчас ожидаем ровно один активный фьючерс.
    # Снаружи нам передают словарь вида {"MNQ": "MNQM6"}.
    if not isinstance(active_futures, dict):
        raise ValueError("ACTIVE_FUTURES должен быть словарём")

    if len(active_futures) == 0:
        raise ValueError("ACTIVE_FUTURES пуст: нет активного фьючерса для realtime")

    instrument_code, contract_local_symbol = next(iter(active_futures.items()))

    if not isinstance(instrument_code, str) or not instrument_code:
        raise ValueError("Некорректный ключ в ACTIVE_FUTURES")

    if not isinstance(contract_local_symbol, str) or not contract_local_symbol:
        raise ValueError("Некорректное значение localSymbol в ACTIVE_FUTURES")

    return instrument_code, contract_local_symbol


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
    # ВАЖНО:
    # 1) settings.price_db_path теперь уже абсолютный путь из config.py,
    #    поэтому здесь не нужно ничего дополнительно resolve-ить.
    #
    # 2) Нам нельзя молча создавать новую пустую БД, если путь ошибочный.
    #    SQLite по умолчанию именно так и делает при обычном connect().
    #    Поэтому перед подключением ЯВНО проверяем, что файл уже существует.
    #
    # 3) Используем обычный sqlite3.connect(db_path), а не URI-режим
    #    file:...?... Это проще и обычно надёжнее на Windows.
    db_path_obj = Path(db_path)

    if not db_path_obj.is_file():
        raise FileNotFoundError(
            f"Файл SQLite БД не найден: {db_path_obj}. "
            f"Realtime loader не должен создавать новую БД автоматически."
        )

    conn = sqlite3.connect(str(db_path_obj))
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
        sql = upsert_quotes_ask_sql(table_name)
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
        sql = upsert_quotes_bid_sql(table_name)
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


def reset_recent_backfill_state(recent_backfill_state):
    # Сбрасываем состояние разовой докачки последнего часа.
    backfill_task = recent_backfill_state["backfill_task"]

    if backfill_task is not None and not backfill_task.done():
        backfill_task.cancel()

    recent_backfill_state["first_bid_ts"] = None
    recent_backfill_state["first_ask_ts"] = None
    recent_backfill_state["last_backfill_completed_sync_ts"] = None
    recent_backfill_state["backfill_task"] = None


def is_realtime_ready_now(ib, ib_health):
    # Считаем realtime ready только если:
    # - локальное API-соединение живо,
    # - backend IB доступен,
    # - market data farm в норме.
    return (
            ib.isConnected()
            and ib_health.ib_backend_ok
            and ib_health.market_data_ok
    )


def maybe_start_recent_backfill_task(
        ib,
        ib_health,
        settings,
        instrument_code,
        contract_local_symbol,
        recent_backfill_state,
        what_to_show,
        bar_time_ts,
):
    # Обновляем состояние первого BID / ASK бара и,
    # когда получен первый синхронный BID/ASK bar_time_ts,
    # один раз запускаем дозагрузку последнего часа.
    first_bid_ts, first_ask_ts = note_first_realtime_bar_timestamps(
        first_bid_ts=recent_backfill_state["first_bid_ts"],
        first_ask_ts=recent_backfill_state["first_ask_ts"],
        what_to_show=what_to_show,
        bar_time_ts=bar_time_ts,
    )

    recent_backfill_state["first_bid_ts"] = first_bid_ts
    recent_backfill_state["first_ask_ts"] = first_ask_ts

    if not is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
        return

    sync_ts = get_recent_backfill_sync_ts(first_bid_ts, first_ask_ts)

    if recent_backfill_state["last_backfill_completed_sync_ts"] == sync_ts:
        return

    backfill_task = recent_backfill_state["backfill_task"]
    if backfill_task is not None and not backfill_task.done():
        return

    async def run_recent_backfill():
        try:
            was_loaded = await backfill_recent_hour(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                contract_local_symbol=contract_local_symbol,
                sync_ts=sync_ts,
            )

            if was_loaded:
                recent_backfill_state["last_backfill_completed_sync_ts"] = sync_ts

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log_warning(
                logger,
                f"Разовая докачка последнего часа завершилась ошибкой: {exc}",
                to_telegram=False,
            )

        finally:
            recent_backfill_state["backfill_task"] = None

    recent_backfill_state["backfill_task"] = asyncio.create_task(run_recent_backfill())


def build_realtime_update_handler(
        ib,
        ib_health,
        settings,
        instrument_code,
        contract_local_symbol,
        recent_backfill_state,
        contract,
        what_to_show,
        conn,
        table_name,
):
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

        bar_time_ts = int(bar.time.astimezone(timezone.utc).timestamp())

        maybe_start_recent_backfill_task(
            ib=ib,
            ib_health=ib_health,
            settings=settings,
            instrument_code=instrument_code,
            contract_local_symbol=contract_local_symbol,
            recent_backfill_state=recent_backfill_state,
            what_to_show=what_to_show,
            bar_time_ts=bar_time_ts,
        )

    return on_bar_update


async def load_realtime_task(ib, ib_health, settings, active_futures, recent_backfill_state):
    # Текущая realtime-версия loader-а:
    # - берём один активный контракт из ACTIVE_FUTURES;
    # - открываем отдельные подписки на BID и ASK 5-second bars;
    # - пишем новые бары в SQLite в таблицу вида MNQ_5s;
    # - BID и ASK пишем независимо по мере их прихода;
    # - никакой переподписки и логики ролловера здесь нет.
    instrument_code, contract_local_symbol = get_realtime_active_future(active_futures)

    instrument_row = get_realtime_instrument_row(instrument_code)
    contract_row = get_contract_row_by_local_symbol(instrument_row, contract_local_symbol)
    contract = build_futures_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )

    use_rth = instrument_row["useRTH"]
    table_name = build_table_name(instrument_code, instrument_row["barSizeSetting"])
    db_path = settings.price_db_path
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
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                contract_local_symbol=contract_local_symbol,
                recent_backfill_state=recent_backfill_state,
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
        #
        # Если realtime ready-состояние пропало, сбрасываем состояние разовой
        # докачки последнего часа. После восстановления и появления нового
        # первого синхронного BID/ASK бара сервис сможет снова один раз
        # дозагрузить последний час.
        was_realtime_ready = is_realtime_ready_now(ib, ib_health)

        while True:
            realtime_ready_now = is_realtime_ready_now(ib, ib_health)

            if was_realtime_ready and not realtime_ready_now:
                reset_recent_backfill_state(recent_backfill_state)

            was_realtime_ready = realtime_ready_now
            await asyncio.sleep(1)

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

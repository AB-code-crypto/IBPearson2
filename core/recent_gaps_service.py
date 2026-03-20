from datetime import datetime, timezone
from pathlib import Path

from contracts import Instrument
from core.load_history import (
    build_futures_contract,
    build_table_name,
    format_utc,
    load_quotes_segment,
    parse_utc_iso_to_ts,
)
from core.logger import get_logger, log_info

logger = get_logger(__name__)

# Разово добираем только последний час.
RECENT_BACKFILL_WINDOW_SECONDS = 3600


def note_first_realtime_bar_timestamps(first_bid_ts, first_ask_ts, what_to_show, bar_time_ts):
    # Запоминаем только первый увиденный timestamp по каждой стороне.
    #
    # Состояние сервис у себя не хранит.
    # Внешний код передаёт текущие значения внутрь и получает обновлённые наружу.
    if what_to_show == "BID":
        if first_bid_ts is None:
            first_bid_ts = bar_time_ts
        return first_bid_ts, first_ask_ts

    if what_to_show == "ASK":
        if first_ask_ts is None:
            first_ask_ts = bar_time_ts
        return first_bid_ts, first_ask_ts

    raise ValueError(f"Неподдерживаемый realtime stream: {what_to_show}")


def is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
    # Первый полноценный realtime-старт считаем подтверждённым,
    # когда обе стороны уже пришли и timestamp у них одинаковый.
    if first_bid_ts is None:
        return False

    if first_ask_ts is None:
        return False

    return first_bid_ts == first_ask_ts


def get_recent_backfill_sync_ts(first_bid_ts, first_ask_ts):
    # Возвращаем timestamp первого синхронного BID/ASK бара.
    if not is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
        raise ValueError("Первый синхронный BID/ASK бар ещё не получен")

    return first_bid_ts


def get_recent_backfill_range(instrument_code, contract_local_symbol, sync_ts):
    # Строим полуоткрытый интервал недавнего добора:
    # [sync_ts - 1 час, sync_ts)
    #
    # Левую границу дополнительно ограничиваем active_from_utc текущего контракта,
    # чтобы не пытаться запрашивать историю левее начала его рабочей зоны.
    instrument_row = Instrument[instrument_code]

    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == contract_local_symbol:
            active_from_ts = parse_utc_iso_to_ts(contract_row["active_from_utc"])
            from_ts = max(active_from_ts, sync_ts - RECENT_BACKFILL_WINDOW_SECONDS)
            to_ts = sync_ts
            return from_ts, to_ts

    raise ValueError(
        f"Контракт {contract_local_symbol} не найден для инструмента {instrument_code}"
    )


def resolve_price_db_path(settings):
    # Относительный путь из config.py считаем относительно корня проекта.
    db_path = Path(settings.price_db_path)

    if db_path.is_absolute():
        return db_path

    project_root = Path(__file__).resolve().parent.parent
    return project_root / db_path


async def backfill_recent_hour(ib, ib_health, settings, instrument_code, contract_local_symbol, sync_ts):
    # Разово догружаем последний час историей до первого синхронного realtime-бара.
    #
    # Никаких проверок существования БД или таблицы здесь не делаем.
    # Считаем, что вся инфраструктура уже подготовлена и работает.
    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] != "FUT":
        raise ValueError(
            f"Сервис добора сейчас поддерживает только FUT. "
            f"instrument={instrument_code}, secType={instrument_row['secType']}"
        )

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Сервис добора ожидает barSizeSetting='5 secs'. "
            f"instrument={instrument_code}, "
            f"barSizeSetting={instrument_row['barSizeSetting']}"
        )

    contract_row = None
    for row in instrument_row["contracts"]:
        if row["localSymbol"] == contract_local_symbol:
            contract_row = row
            break

    if contract_row is None:
        raise ValueError(
            f"Контракт {contract_local_symbol} не найден для инструмента {instrument_code}"
        )

    from_ts, to_ts = get_recent_backfill_range(
        instrument_code=instrument_code,
        contract_local_symbol=contract_local_symbol,
        sync_ts=sync_ts,
    )

    if to_ts <= from_ts:
        return False

    db_path = resolve_price_db_path(settings)
    table_name = build_table_name(instrument_code, instrument_row["barSizeSetting"])
    contract = build_futures_contract(instrument_code, instrument_row, contract_row)

    log_info(
        logger,
        f"Сервис добора: получен первый синхронный BID/ASK бар "
        f"{format_utc(datetime.fromtimestamp(to_ts, tz=timezone.utc))} "
        f"для {contract_local_symbol}. "
        f"Запускаю разовую докачку последнего часа: "
        f"{format_utc(datetime.fromtimestamp(from_ts, tz=timezone.utc))} -> "
        f"{format_utc(datetime.fromtimestamp(to_ts, tz=timezone.utc))}",
        to_telegram=False,
    )

    await load_quotes_segment(
        ib=ib,
        ib_health=ib_health,
        db_path=db_path,
        table_name=table_name,
        contract=contract,
        bar_size_setting=instrument_row["barSizeSetting"],
        use_rth=instrument_row["useRTH"],
        segment_start_ts=from_ts,
        segment_end_ts=to_ts,
        segment_kind="recent-backfill",
    )

    log_info(
        logger,
        f"Сервис добора: разовая докачка последнего часа завершена для "
        f"{contract_local_symbol}",
        to_telegram=False,
    )
    return True
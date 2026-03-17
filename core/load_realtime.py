import asyncio

from ib_async import Contract

from contracts import Instrument
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


def format_realtime_bar_message(contract, bar):
    # Собираем одну строку лога по новому 5-секундному бару.
    bar_time_text = str(bar.time).split("+")[0]

    return (
        f"RT BAR {contract.localSymbol} | {bar_time_text} | "
        f"O={bar.open_} H={bar.high} L={bar.low} C={bar.close} "
        f"V={bar.volume} WAP={bar.wap} COUNT={bar.count}"
    )


async def load_realtime_task(ib, ib_health, settings):
    # Минимальная первая версия realtime-loader:
    # - берём один заранее выбранный контракт;
    # - один раз подписываемся на 5-second real-time bars;
    # - выводим новые бары в консоль;
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

    # Пока задачу держим максимально узкой и явной.
    # Реальные бары хотим получать именно как TRADES.
    what_to_show = "TRADES"
    use_rth = instrument_row["useRTH"]

    current_realtime_bars = None
    current_update_handler = None

    def on_bar_update(bars, has_new_bar):
        # Печатаем только когда реально добавился новый бар,
        # а не когда просто обновился последний.
        if not has_new_bar:
            return

        if len(bars) == 0:
            return

        bar = bars[-1]

        log_info(
            logger,
            format_realtime_bar_message(contract, bar),
            to_telegram=False,
        )

    try:
        log_info(
            logger,
            f"Запускаю realtime loader для {instrument_code}",
            to_telegram=False,
        )

        await wait_for_realtime_ready(ib, ib_health)

        log_info(
            logger,
            f"Realtime loader: открываю подписку на {contract.localSymbol} "
            f"(conId={contract.conId}), whatToShow={what_to_show}, useRTH={use_rth}",
            to_telegram=False,
        )

        current_realtime_bars = subscribe_realtime_bars(
            ib=ib,
            contract=contract,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )

        current_update_handler = on_bar_update
        current_realtime_bars.updateEvent += current_update_handler

        log_info(
            logger,
            f"Подписался на real-time 5-second bars: {contract.localSymbol} "
            f"(conId={contract.conId}), whatToShow={what_to_show}, useRTH={use_rth}",
            to_telegram=False,
        )

        # Дальше просто держим подписку живой и ждём новые бары.
        while True:
            await asyncio.sleep(3600)

    finally:
        if current_realtime_bars is not None and current_update_handler is not None:
            try:
                current_realtime_bars.updateEvent -= current_update_handler
            except Exception as exc:
                log_warning(
                    logger,
                    f"Не удалось снять обработчик realtime updateEvent: {exc}",
                    to_telegram=False,
                )

        cancel_realtime_bars_safe(ib, current_realtime_bars)

from datetime import datetime, timezone

from contracts import Instrument
from core.logger import get_logger, log_warning

logger = get_logger(__name__)

# Как часто обновляем словарь активных фьючерсов.
ACTIVE_FUTURES_REFRESH_SECONDS = 3600


def parse_server_time_text(server_time_text):
    # Разбираем время сервера IB в UTC.
    #
    # Ожидаем строку в формате из get_ib_server_time_text:
    # YYYY-MM-DD HH:MM:SS
    dt = datetime.strptime(server_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_contract_utc_text(utc_text):
    # Разбираем UTC-время контракта из contracts.py.
    #
    # В реестре контрактов время хранится в ISO-формате:
    # YYYY-MM-DDTHH:MM:SSZ
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_active_futures(server_time_text):
    # Возвращаем словарь активных фьючерсов по всем FUT-инструментам.
    #
    # Ключ словаря результата — ключ из Instrument, например MNQ или NQ.
    # Значение — localSymbol текущего активного фьючерса.
    #
    # Индексы здесь полностью пропускаем.
    current_utc = parse_server_time_text(server_time_text)
    active_futures = {}

    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] == "IND":
            continue

        current_local_symbol = None

        for contract_row in instrument_row["contracts"]:
            active_from_utc = parse_contract_utc_text(contract_row["active_from_utc"])
            active_to_utc = parse_contract_utc_text(contract_row["active_to_utc"])

            if active_from_utc <= current_utc < active_to_utc:
                current_local_symbol = contract_row["localSymbol"]
                break

        if current_local_symbol is None:
            error_text = (
                f"Текущий контракт не найден: instrument={instrument_code}, "
                f"server_time_utc={server_time_text}"
            )
            logger.error(error_text)
            log_warning(logger, error_text, to_telegram=True)
            raise RuntimeError(error_text)

        active_futures[instrument_code] = current_local_symbol

    return active_futures

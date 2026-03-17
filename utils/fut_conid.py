import asyncio
import json
import sys

from ib_async import IB, Contract


# =========================
# НАСТРОЙКИ
# =========================

IB_HOST = "127.0.0.1"
IB_PORT = 7496
IB_CLIENT_ID = 55

# Что ищем
LOCAL_SYMBOL = "MNQM4"
SEC_TYPE = "FUT"

# Для старых / истекших фьючерсов должно быть True
INCLUDE_EXPIRED = True

# Необязательные уточнения.
# Обычно можно оставить пустыми.
SYMBOL = ""
EXCHANGE = "CME"
CURRENCY = ""
TRADING_CLASS = ""
MULTIPLIER = ""
EXPIRY = ""

# True - вывести json
# False - обычный человекочитаемый вывод
PRINT_JSON = False


def build_contract():
    kwargs = {
        "secType": SEC_TYPE,
        "localSymbol": LOCAL_SYMBOL,
        "includeExpired": INCLUDE_EXPIRED,
    }

    if SYMBOL:
        kwargs["symbol"] = SYMBOL

    if EXCHANGE:
        kwargs["exchange"] = EXCHANGE

    if CURRENCY:
        kwargs["currency"] = CURRENCY

    if TRADING_CLASS:
        kwargs["tradingClass"] = TRADING_CLASS

    if MULTIPLIER:
        kwargs["multiplier"] = MULTIPLIER

    if EXPIRY:
        kwargs["lastTradeDateOrContractMonth"] = EXPIRY

    return Contract(**kwargs)


def contract_to_dict(contract):
    return {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "tradingClass": getattr(contract, "tradingClass", None),
        "multiplier": getattr(contract, "multiplier", None),
        "exchange": getattr(contract, "exchange", None),
        "primaryExchange": getattr(contract, "primaryExchange", None),
        "currency": getattr(contract, "currency", None),
    }


def print_contract(contract, index=None):
    if index is not None:
        print(f"Инструмент #{index}")

    print(f"  conId       : {getattr(contract, 'conId', None)}")
    print(f"  symbol      : {getattr(contract, 'symbol', None)}")
    print(f"  localSymbol : {getattr(contract, 'localSymbol', None)}")
    print(f"  expiry      : {getattr(contract, 'lastTradeDateOrContractMonth', None)}")
    print(f"  tradingClass: {getattr(contract, 'tradingClass', None)}")
    print(f"  multiplier  : {getattr(contract, 'multiplier', None)}")
    print(f"  exchange    : {getattr(contract, 'exchange', None)}")
    print(f"  primaryExch : {getattr(contract, 'primaryExchange', None)}")
    print(f"  currency    : {getattr(contract, 'currency', None)}")


async def main():
    ib = IB()

    try:
        await ib.connectAsync(
            host=IB_HOST,
            port=IB_PORT,
            clientId=IB_CLIENT_ID,
        )
    except Exception as e:
        print(f"[ERROR] Не удалось подключиться к IB: {e}", file=sys.stderr)
        return 2

    try:
        contract = build_contract()
        details = await ib.reqContractDetailsAsync(contract)

        if not details:
            print("[NOT FOUND] Ничего не найдено.")
            print()
            print("Текущие параметры поиска:")
            print(f"  LOCAL_SYMBOL={LOCAL_SYMBOL}")
            print(f"  SEC_TYPE={SEC_TYPE}")
            print(f"  INCLUDE_EXPIRED={INCLUDE_EXPIRED}")
            print(f"  SYMBOL={SYMBOL}")
            print(f"  EXCHANGE={EXCHANGE}")
            print(f"  CURRENCY={CURRENCY}")
            print(f"  TRADING_CLASS={TRADING_CLASS}")
            print(f"  MULTIPLIER={MULTIPLIER}")
            print(f"  EXPIRY={EXPIRY}")
            return 1

        contracts = [item.contract for item in details]

        if PRINT_JSON:
            data = [contract_to_dict(c) for c in contracts]
            print(json.dumps(data, ensure_ascii=False, indent=2))
            return 0

        print(f"Найдено инструментов: {len(contracts)}")
        print()

        for i, c in enumerate(contracts, start=1):
            print_contract(c, index=i)
            print()

        if len(contracts) == 1:
            print("Найден единственный контракт.")
        else:
            print("Выше выведены все найденные варианты. Можно выбрать нужный conId вручную.")

        return 0

    except Exception as e:
        print(f"[ERROR] Ошибка при запросе контракта: {e}", file=sys.stderr)
        return 4

    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

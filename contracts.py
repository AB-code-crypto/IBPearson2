from decimal import Decimal
from typing import Any, Dict, Literal

# ==============================
# Types
# ==============================

SecType = Literal["FUT", "IND"]
WhatToShow = Literal["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK"]
InstrumentRow = Dict[str, Any]
Registry = Dict[str, InstrumentRow]

FUT_DEFAULTS: InstrumentRow = {
    "secType": "FUT",
    "exchange": "CME",
    "currency": "USD",
    "roll_days": 2,
    "whatToShow": "TRADES",
    "barSizeSetting": "5 secs",
    "useRTH": False,
    "minTick": Decimal("0.25"),
}

IND_DEFAULTS: InstrumentRow = {
    "secType": "IND",
    "currency": "USD",
    "useRTH": False,
}

# ==============================
# Registry
# ==============================

Instrument: Registry = {
    "MNQ": {
        **FUT_DEFAULTS,
        "tradingClass": "MNQ",
        "multiplier": 2.0,
        "contracts": [
            {"conId": 620730945, "localSymbol": "MNQM4", "lastTradeDateOrContractMonth": "20240621",
             "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},

            {"conId": 637533593, "localSymbol": "MNQU4", "lastTradeDateOrContractMonth": "20240920",
             "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},

            {"conId": 654503320, "localSymbol": "MNQZ4", "lastTradeDateOrContractMonth": "20241220",
             "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},

            {"conId": 672387468, "localSymbol": "MNQH5", "lastTradeDateOrContractMonth": "20250321",
             "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},

            {"conId": 691171685, "localSymbol": "MNQM5", "lastTradeDateOrContractMonth": "20250620",
             "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},

            {"conId": 711280073, "localSymbol": "MNQU5", "lastTradeDateOrContractMonth": "20250919",
             "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},

            {"conId": 730283094, "localSymbol": "MNQZ5", "lastTradeDateOrContractMonth": "20251219",
             "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},

            {"conId": 750150193, "localSymbol": "MNQH6", "lastTradeDateOrContractMonth": "20260320",
             "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},

            {"conId": 770561201, "localSymbol": "MNQM6", "lastTradeDateOrContractMonth": "20260618",
             "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},

            {"conId": 793356225, "localSymbol": "MNQU6", "lastTradeDateOrContractMonth": "20260918",
             "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},

            {"conId": 815824267, "localSymbol": "MNQZ6", "lastTradeDateOrContractMonth": "20261218",
             "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
        ]
    },
    # "NQ": {
    #     **FUT_DEFAULTS,
    #     "tradingClass": "NQ",
    #     "multiplier": 20.0,
    #     "contracts": [
    #         {"conId": 666754605, "localSymbol": "NQH5", "lastTradeDateOrContractMonth": "20250321",
    #          "active_from_utc": "2025-01-01T00:00:00Z", "active_to_utc": "2025-03-19T22:00:00Z"},
    #
    #         {"conId": 672387474, "localSymbol": "NQM5", "lastTradeDateOrContractMonth": "20250620",
    #          "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T22:00:00Z"},
    #
    #         {"conId": 691171690, "localSymbol": "NQU5", "lastTradeDateOrContractMonth": "20250919",
    #          "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T22:00:00Z"},
    #
    #         {"conId": 563947738, "localSymbol": "NQZ5", "lastTradeDateOrContractMonth": "20251219",
    #          "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T23:00:00Z"},
    #
    #         {"conId": 730283097, "localSymbol": "NQH6", "lastTradeDateOrContractMonth": "20260320",
    #          "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T22:00:00Z"},
    #
    #         {"conId": 750150196, "localSymbol": "NQM6", "lastTradeDateOrContractMonth": "20260618",
    #          "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T22:00:00Z"},
    #
    #         {"conId": 770561204, "localSymbol": "NQU6", "lastTradeDateOrContractMonth": "20260918",
    #          "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T22:00:00Z"},
    #
    #         {"conId": 563947726, "localSymbol": "NQZ6", "lastTradeDateOrContractMonth": "20261218",
    #          "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T23:00:00Z"},
    #     ],
    # },
    #
    # "VIX": {
    #     **IND_DEFAULTS,
    #     "exchange": "CBOE",
    #     "minTick": Decimal("0.01"),
    #     "whatToShow": "TRADES",
    #     "barSizeSetting": "1 hour",
    #     "conId": 13455763,
    #     "active_from_utc": "2025-01-01T00:00:00Z",
    # },
}

from datetime import datetime, timedelta, timezone
from ib_async import IB, Contract

# =========================
# Настройки
# =========================

HOST = "127.0.0.1"
PORT = 7497          # укажи свой порт TWS / IB Gateway
CLIENT_ID = 17

FUTURE_LOCAL_SYMBOL = "MNQH6"
EXCHANGE = "CME"
CURRENCY = "USD"

# Старт интервала в UTC
# START_DT_UTC = datetime.now(timezone.utc).replace(hour=13, minute=0, second=0, microsecond=0)
START_DT_UTC = datetime(2026, 3, 11, 9, 24, 0, tzinfo=timezone.utc)

# Длина интервала
# DURATION = timedelta(hours=1)
DURATION = timedelta(minutes=20)

BAR_SIZE = "5 secs"
WHAT_TO_SHOW = "TRADES"
USE_RTH = False


def fetch_ib_history(
    future_local_symbol: str,
    start_dt_utc: datetime,
    duration: timedelta,
    host: str = HOST,
    port: int = PORT,
    client_id: int = CLIENT_ID,
    exchange: str = EXCHANGE,
    currency: str = CURRENCY,
) -> list[dict]:
    if start_dt_utc.tzinfo is None:
        raise ValueError("start_dt_utc должен быть timezone-aware и в UTC")

    start_dt_utc = start_dt_utc.astimezone(timezone.utc)
    end_dt_utc = start_dt_utc + duration

    # IB historical data: endDateTime + durationStr назад от конца
    end_date_time = end_dt_utc.strftime("%Y%m%d %H:%M:%S UTC")
    duration_str = f"{int(duration.total_seconds())} S"

    ib = IB()
    ib.connect(host, port, clientId=client_id)

    try:
        contract = Contract()
        contract.secType = "FUT"
        contract.localSymbol = future_local_symbol
        contract.exchange = exchange
        contract.currency = currency

        bars_raw = ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=BAR_SIZE,
            whatToShow=WHAT_TO_SHOW,
            useRTH=USE_RTH,
            formatDate=2,
            keepUpToDate=False,
        )

        bars = []

        for bar in bars_raw:
            bar_date = bar.date

            if isinstance(bar_date, datetime):
                if bar_date.tzinfo is None:
                    dt_utc = bar_date.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = bar_date.astimezone(timezone.utc)
            else:
                ts_utc = int(bar_date)
                dt_utc = datetime.fromtimestamp(ts_utc, tz=timezone.utc)

            bars.append({
                "ts_utc": int(dt_utc.timestamp()),
                "dt_utc": dt_utc,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": float(bar.volume),
                "barCount": bar.barCount,
                "wap": bar.average,
            })

        return bars

    finally:
        ib.disconnect()


if __name__ == "__main__":
    bars = fetch_ib_history(
        future_local_symbol=FUTURE_LOCAL_SYMBOL,
        start_dt_utc=START_DT_UTC,
        duration=DURATION,
    )

    print(f"Загружено баров: {len(bars)}")
    print("Первые 3 бара:")
    for row in bars[:3]:
        print(row)
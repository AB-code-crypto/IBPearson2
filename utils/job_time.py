from dataclasses import dataclass, asdict
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
TZ_CT = ZoneInfo("America/Chicago")
TZ_ET = ZoneInfo("America/New_York")
TZ_MSK = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class MnqIbkrStatus:
    overall_mode: str
    exchange_mode: str
    ibkr_mode: str

    market_open: bool
    can_trade_via_ibkr: bool
    is_primary_cash_session: bool

    dt_utc: str
    dt_chicago: str
    dt_new_york: str
    dt_moscow: str

    note: str


def get_mnq_ibkr_mode(
        dt: datetime | None = None,
        input_tz: str = "UTC",
        include_month_end_halt: bool = True,
        return_dict: bool = True,
) -> MnqIbkrStatus | dict:
    """
    Определяет текущий режим для MNQ / CME через IBKR.

    Логика:
    - CME equity index futures:
        * торговая неделя: Sunday 17:00 CT -> Friday 16:00 CT
        * daily break: 16:00-17:00 CT (Mon-Thu)
    - Основная US cash-сессия:
        * 09:30-16:00 ET = 08:30-15:00 CT
    - После cash close до CME close:
        * 15:00-16:00 CT
    - IBKR daily reset:
        * 00:15-01:45 ET (Sun-Fri)
    - IBKR weekend reset:
        * Friday 23:00 ET -> Saturday 03:00 ET
    - Month-end halt (приближённо):
        * последний рабочий день месяца, 15:15-15:30 CT

    Важно:
    - Если dt не передан, берётся текущее UTC-время.
    - Если dt naive, он трактуется в зоне input_tz.
    - DST обрабатывается через zoneinfo автоматически.
    - Праздники/early close здесь НЕ учтены.
    """

    dt_utc = _normalize_to_utc(dt, input_tz)
    dt_ct = dt_utc.astimezone(TZ_CT)
    dt_et = dt_utc.astimezone(TZ_ET)
    dt_msk = dt_utc.astimezone(TZ_MSK)

    exchange_mode, market_open, is_primary_cash_session, note = _get_exchange_mode(
        dt_ct=dt_ct,
        include_month_end_halt=include_month_end_halt,
    )

    ibkr_mode = _get_ibkr_mode(dt_et)

    # Консервативное правило:
    # can_trade_via_ibkr = True только когда рынок реально открыт
    # и нет reset-окна у IBKR.
    can_trade_via_ibkr = market_open and ibkr_mode == "OK"

    if ibkr_mode == "WEEKEND_RESET":
        overall_mode = "IBKR_WEEKEND_RESET"
    elif exchange_mode in {"CME_WEEKEND_CLOSED", "CME_DAILY_BREAK", "MONTH_END_HALT"}:
        overall_mode = exchange_mode
    elif ibkr_mode == "DAILY_RESET":
        overall_mode = "IBKR_DAILY_RESET"
    else:
        overall_mode = exchange_mode

    result = MnqIbkrStatus(
        overall_mode=overall_mode,
        exchange_mode=exchange_mode,
        ibkr_mode=ibkr_mode,
        market_open=market_open,
        can_trade_via_ibkr=can_trade_via_ibkr,
        is_primary_cash_session=is_primary_cash_session,
        dt_utc=dt_utc.isoformat(),
        dt_chicago=dt_ct.isoformat(),
        dt_new_york=dt_et.isoformat(),
        dt_moscow=dt_msk.isoformat(),
        note=note,
    )

    return asdict(result) if return_dict else result


def _normalize_to_utc(dt: datetime | None, input_tz: str) -> datetime:
    if dt is None:
        return datetime.now(UTC)

    if dt.tzinfo is None:
        tz = ZoneInfo(input_tz)
        dt = dt.replace(tzinfo=tz)

    return dt.astimezone(UTC)


def _get_exchange_mode(
        dt_ct: datetime,
        include_month_end_halt: bool,
) -> tuple[str, bool, bool, str]:
    wd = dt_ct.weekday()  # Mon=0 ... Sun=6
    t = dt_ct.time()

    # 1) Недельное закрытие CME
    # Saturday: закрыто весь день
    if wd == 5:
        return "CME_WEEKEND_CLOSED", False, False, "Суббота по Chicago: рынок закрыт."

    # Sunday до 17:00 CT: закрыто
    if wd == 6 and t < time(17, 0):
        return "CME_WEEKEND_CLOSED", False, False, "Воскресенье до 17:00 CT: рынок ещё не открыт."

    # Friday с 16:00 CT: недельное закрытие
    if wd == 4 and t >= time(16, 0):
        return "CME_WEEKEND_CLOSED", False, False, "Пятница после 16:00 CT: недельное закрытие."

    # 2) Ежедневная пауза CME (Mon-Thu 16:00-17:00 CT)
    if wd in (0, 1, 2, 3) and time(16, 0) <= t < time(17, 0):
        return "CME_DAILY_BREAK", False, False, "Ежедневная пауза CME 16:00-17:00 CT."

    # 3) Month-end halt (приближённо, без holiday calendar)
    if include_month_end_halt and wd < 5 and _is_last_business_day_of_month_approx(dt_ct.date()):
        if time(15, 15) <= t < time(15, 30):
            return "MONTH_END_HALT", False, False, (
                "Приближённый month-end halt: последний рабочий день месяца, 15:15-15:30 CT."
            )

    # 4) Основная cash-сессия США: 08:30-15:00 CT
    if wd < 5 and time(8, 30) <= t < time(15, 0):
        return "PRIMARY_CASH_SESSION", True, True, "Идёт основная cash-сессия США."

    # 5) После cash close, но до CME close: 15:00-16:00 CT
    if wd < 5 and time(15, 0) <= t < time(16, 0):
        return "POST_CASH_PRE_CLOSE", True, False, "Cash-сессия уже закрыта, но фьючерс ещё торгуется."

    # 6) Всё остальное внутри торговой недели — extended / overnight
    return "EXTENDED_HOURS", True, False, "Идёт extended/overnight торговля CME Globex."


def _get_ibkr_mode(dt_et: datetime) -> str:
    wd = dt_et.weekday()  # Mon=0 ... Sun=6
    t = dt_et.time()

    # Weekend reset: Friday 23:00 ET -> Saturday 03:00 ET
    if (wd == 4 and t >= time(23, 0)) or (wd == 5 and t < time(3, 0)):
        return "WEEKEND_RESET"

    # Daily reset: Sunday-Friday 00:15-01:45 ET
    # Исключаем Saturday как день weekly maintenance.
    if wd != 5 and time(0, 15) <= t < time(1, 45):
        return "DAILY_RESET"

    return "OK"


def _is_last_business_day_of_month_approx(d) -> bool:
    """
    Приближённо: последний будний день месяца без учёта биржевых праздников.
    """
    if d.weekday() >= 5:
        return False

    probe = d + timedelta(days=1)
    while probe.weekday() >= 5:
        probe += timedelta(days=1)

    return probe.month != d.month


if __name__ == "__main__":
    # 1) Просто сейчас
    status = get_mnq_ibkr_mode()
    print(f"{status}\n")

    # 2) Конкретное UTC-время
    status = get_mnq_ibkr_mode(datetime(2026, 3, 12, 14, 0), input_tz="UTC")
    print(f"{status["overall_mode"]}, {status["can_trade_via_ibkr"]}\n")

    # 3) Московское naive-время
    status = get_mnq_ibkr_mode(datetime(2026, 3, 12, 18, 30), input_tz="Europe/Moscow")
    print(f"{status}\n")

    # 4) Получить dataclass вместо dict
    status_obj = get_mnq_ibkr_mode(return_dict=False)
    print(f"{status_obj.overall_mode}")

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import settings_live as settings
from core.telegram_sender import TelegramSender
from trading.trade_performance_summary import (
    _build_summary_text,
    _fetch_trade_summary,
    _send_summary_to_targets,
)

MSK_TZ = ZoneInfo("Europe/Moscow")
UTC = timezone.utc

# ============================================================
# Ручные настройки отчёта
# ============================================================

INSTRUMENT_CODE = "MNQ"

# Период задаётся в московском времени.
# Формат: YYYY-MM-DD HH:MM:SS
MANUAL_START_MSK = "2026-04-30 00:00:00"
MANUAL_END_MSK = "2026-05-01 00:00:00"

# Оставил тот же заголовок, что у штатного суточного отчёта.
# При необходимости можно вручную заменить на "ИТОГИ ТОРГОВЛИ ЗА ПЕРИОД".
REPORT_TITLE = "ИТОГИ ТОРГОВЛИ ЗА 24 ЧАСА"

# Если True — только печатает текст отчёта в консоль, но не отправляет в Telegram.
DRY_RUN = False


# ============================================================
# Реализация
# ============================================================


def _parse_msk_datetime(value: str) -> datetime:
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=MSK_TZ)


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _build_manual_period() -> tuple[int, int, str]:
    start_msk = _parse_msk_datetime(MANUAL_START_MSK)
    end_msk = _parse_msk_datetime(MANUAL_END_MSK)

    if end_msk <= start_msk:
        raise ValueError(
            f"MANUAL_END_MSK должен быть больше MANUAL_START_MSK: "
            f"start={MANUAL_START_MSK}, end={MANUAL_END_MSK}"
        )

    start_utc = start_msk.astimezone(UTC)
    end_utc = end_msk.astimezone(UTC)

    period_label = f"MSK {_format_dt(start_msk)} → {_format_dt(end_msk)}"
    return int(start_utc.timestamp()), int(end_utc.timestamp()), period_label


async def main() -> None:
    start_ts, end_ts, period_label = _build_manual_period()

    summary = _fetch_trade_summary(
        db_path=settings.trade_db_path,
        instrument_code=INSTRUMENT_CODE,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    text = _build_summary_text(
        title=REPORT_TITLE,
        instrument_code=INSTRUMENT_CODE,
        period_label=period_label,
        summary=summary,
    )

    print(text)

    if DRY_RUN:
        print("\nDRY_RUN=True — отчёт не отправлен в Telegram.")
        return

    sender = TelegramSender(settings)
    try:
        await _send_summary_to_targets(
            sender=sender,
            settings=settings,
            text=text,
        )
        print("\nОтчёт отправлен в Telegram.")
    finally:
        await sender.close()


if __name__ == "__main__":
    asyncio.run(main())

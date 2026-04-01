import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from core.logger import get_logger, log_info, log_warning
from core.telegram_sender import TelegramSender
from trading.trade_store import ACTIVE_TRADE_STATUSES

logger = get_logger(__name__)

UTC = timezone.utc
MSK_TZ = ZoneInfo("Europe/Moscow")


def _utc_ts(dt: datetime) -> int:
    return int(dt.astimezone(UTC).timestamp())


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _start_of_msk_day(now_msk: datetime) -> datetime:
    return now_msk.replace(hour=0, minute=0, second=0, microsecond=0)


def _is_daily_due(now_msk: datetime) -> bool:
    return now_msk.hour == 0 and now_msk.minute == 0


def _is_weekly_due(now_msk: datetime) -> bool:
    # Суббота 00:01 MSK = итоги за завершившуюся торговую неделю.
    return now_msk.weekday() == 5 and now_msk.hour == 0 and now_msk.minute == 1


def _is_monthly_due(now_msk: datetime) -> bool:
    # 00:02 MSK в первый календарный день после последнего торгового дня месяца.
    if not (now_msk.hour == 0 and now_msk.minute == 2):
        return False

    previous_day = now_msk.date() - timedelta(days=1)

    # Обычный случай: вчера уже был последний календарный день месяца.
    if previous_day.month != now_msk.date().month:
        return True

    # Специальный случай: сейчас суббота, а смена месяца попадает на выходные.
    # Тогда месячный отчёт должен уйти в субботу 00:02 сразу после последней торговой пятницы.
    if now_msk.weekday() == 5:
        saturday = now_msk.date()
        sunday = saturday + timedelta(days=1)
        monday = saturday + timedelta(days=2)

        if sunday.month != saturday.month or monday.month != saturday.month:
            return True

    return False


def _build_daily_period(now_utc: datetime, now_msk: datetime) -> tuple[int, int, str]:
    start_utc = now_utc - timedelta(hours=24)
    end_utc = now_utc
    period_label = (
        f"MSK {_format_dt(start_utc.astimezone(MSK_TZ))} → "
        f"{_format_dt(now_msk)}"
    )
    return _utc_ts(start_utc), _utc_ts(end_utc), period_label


def _build_weekly_period(now_msk: datetime) -> tuple[int, int, str]:
    # В субботу 00:01 MSK берём интервал от прошлой субботы 00:00 до текущей субботы 00:00.
    week_end_msk = _start_of_msk_day(now_msk)
    week_start_msk = week_end_msk - timedelta(days=7)
    period_label = (
        f"MSK {_format_dt(week_start_msk)} → "
        f"{_format_dt(week_end_msk)}"
    )
    return _utc_ts(week_start_msk), _utc_ts(week_end_msk), period_label


def _build_monthly_period(now_msk: datetime) -> tuple[int, int, str]:
    report_end_msk = _start_of_msk_day(now_msk)
    previous_day = report_end_msk.date() - timedelta(days=1)

    if previous_day.month != report_end_msk.date().month:
        # Первый день нового месяца после последнего календарного дня старого месяца.
        month_end_msk = report_end_msk
        month_start_msk = month_end_msk.replace(day=1)
    else:
        # Субботний отчёт после последней торговой пятницы месяца,
        # когда смена месяца попадает на выходные.
        month_end_msk = report_end_msk
        month_start_msk = month_end_msk.replace(day=1)

    period_label = (
        f"MSK {_format_dt(month_start_msk)} → "
        f"{_format_dt(month_end_msk)}"
    )
    return _utc_ts(month_start_msk), _utc_ts(month_end_msk), period_label


def _fetch_trade_summary(*, db_path: str, instrument_code: str, start_ts: int, end_ts: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        closed_row = conn.execute(
            """
            SELECT COUNT(*)                                                                               AS closed_count,
                   COALESCE(SUM(quantity), 0)                                                             AS closed_contracts,
                   COALESCE(SUM(CASE WHEN side = 'LONG' THEN 1 ELSE 0 END), 0)                            AS long_count,
                   COALESCE(SUM(CASE WHEN side = 'SHORT' THEN 1 ELSE 0 END), 0)                           AS short_count,
                   COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0)                         AS win_count,
                   COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0)                         AS loss_count,
                   COALESCE(SUM(CASE WHEN realized_pnl = 0 OR realized_pnl IS NULL THEN 1 ELSE 0 END), 0) AS flat_count,
                   COALESCE(SUM(realized_pnl), 0)                                                         AS total_realized_pnl,
                   COALESCE(SUM(commissions_total), 0)                                                    AS total_commissions,
                   COALESCE(AVG(realized_pnl), 0)                                                         AS avg_realized_pnl,
                   COALESCE(MAX(realized_pnl), 0)                                                         AS best_trade_pnl,
                   COALESCE(MIN(realized_pnl), 0)                                                         AS worst_trade_pnl
            FROM trades
            WHERE instrument_code = ?
              AND status = 'CLOSED'
              AND exit_filled_ts IS NOT NULL
              AND exit_filled_ts >= ?
              AND exit_filled_ts < ?;
            """,
            (instrument_code, start_ts, end_ts),
        ).fetchone()

        opened_row = conn.execute(
            """
            SELECT COUNT(*)                   AS opened_count,
                   COALESCE(SUM(quantity), 0) AS opened_contracts
            FROM trades
            WHERE instrument_code = ?
              AND entry_filled_ts IS NOT NULL
              AND entry_filled_ts >= ?
              AND entry_filled_ts < ?;
            """,
            (instrument_code, start_ts, end_ts),
        ).fetchone()

        placeholders = ", ".join("?" for _ in ACTIVE_TRADE_STATUSES)
        active_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS active_count,
                COALESCE(SUM(quantity), 0) AS active_contracts
            FROM trades
            WHERE instrument_code = ?
              AND status IN ({placeholders});
            """,
            (instrument_code, *ACTIVE_TRADE_STATUSES),
        ).fetchone()
    finally:
        conn.close()

    closed_count = int(closed_row["closed_count"] or 0)
    win_count = int(closed_row["win_count"] or 0)

    win_rate = 0.0
    if closed_count > 0:
        win_rate = (win_count / closed_count) * 100.0

    return {
        "opened_count": int(opened_row["opened_count"] or 0),
        "opened_contracts": int(opened_row["opened_contracts"] or 0),
        "closed_count": closed_count,
        "closed_contracts": int(closed_row["closed_contracts"] or 0),
        "long_count": int(closed_row["long_count"] or 0),
        "short_count": int(closed_row["short_count"] or 0),
        "win_count": win_count,
        "loss_count": int(closed_row["loss_count"] or 0),
        "flat_count": int(closed_row["flat_count"] or 0),
        "win_rate": win_rate,
        "total_realized_pnl": float(closed_row["total_realized_pnl"] or 0.0),
        "total_commissions": float(closed_row["total_commissions"] or 0.0),
        "avg_realized_pnl": float(closed_row["avg_realized_pnl"] or 0.0),
        "best_trade_pnl": float(closed_row["best_trade_pnl"] or 0.0),
        "worst_trade_pnl": float(closed_row["worst_trade_pnl"] or 0.0),
        "active_count": int(active_row["active_count"] or 0),
        "active_contracts": int(active_row["active_contracts"] or 0),
    }


def _build_summary_text(*, title: str, instrument_code: str, period_label: str, summary: dict) -> str:
    return (
        f"{title}\n"
        f"Инструмент: {instrument_code}\n"
        f"Период: {period_label}\n"
        f"Открыто сделок: {summary['opened_count']}\n"
        f"Закрыто сделок: {summary['closed_count']}\n"
        f"Объём открытий: {summary['opened_contracts']}\n"
        f"Объём закрытий: {summary['closed_contracts']}\n"
        f"LONG: {summary['long_count']} | SHORT: {summary['short_count']}\n"
        f"Плюс / минус / 0: {summary['win_count']} / {summary['loss_count']} / {summary['flat_count']}\n"
        f"Win rate: {summary['win_rate']:.1f}%\n"
        f"Суммарный PnL: {summary['total_realized_pnl']:.2f}\n"
        f"Суммарные комиссии: {summary['total_commissions']:.2f}\n"
        f"Средний PnL на сделку: {summary['avg_realized_pnl']:.2f}\n"
        f"Лучшая сделка: {summary['best_trade_pnl']:.2f}\n"
        f"Худшая сделка: {summary['worst_trade_pnl']:.2f}\n"
        f"Активных открытых сделок сейчас: {summary['active_count']}\n"
        f"Активный объём сейчас: {summary['active_contracts']}"
    )


async def _send_summary_to_targets(*, sender: TelegramSender, settings, text: str) -> None:
    if settings.telegram_chat_id_common:
        await sender.send_text(
            text=text,
            chat_id=settings.telegram_chat_id_common,
        )

    if settings.telegram_chat_id_promo:
        await sender.send_text(
            text=text,
            chat_id=settings.telegram_chat_id_promo,
            message_thread_id=settings.telegram_thread_id_promo,
        )


async def trade_performance_summary_task(
        *,
        settings,
        instrument_code: str = "MNQ",
        poll_interval_seconds: float = 20.0,
):
    sender = TelegramSender(settings)

    last_daily_key = None
    last_weekly_key = None
    last_monthly_key = None

    log_info(
        logger,
        f"Запуск фоновой отправки торговых итогов для {instrument_code}",
        to_telegram=False,
    )

    try:
        while True:
            now_utc = datetime.now(UTC)
            now_msk = now_utc.astimezone(MSK_TZ)

            try:
                if _is_daily_due(now_msk):
                    daily_key = now_msk.strftime("%Y-%m-%d")

                    if daily_key != last_daily_key:
                        start_ts, end_ts, period_label = _build_daily_period(now_utc, now_msk)

                        summary = _fetch_trade_summary(
                            db_path=settings.trade_db_path,
                            instrument_code=instrument_code,
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )

                        text = _build_summary_text(
                            title="ИТОГИ ТОРГОВЛИ ЗА 24 ЧАСА",
                            instrument_code=instrument_code,
                            period_label=period_label,
                            summary=summary,
                        )

                        await _send_summary_to_targets(sender=sender, settings=settings, text=text)
                        last_daily_key = daily_key

                if _is_weekly_due(now_msk):
                    weekly_key = _start_of_msk_day(now_msk).strftime("%Y-W%W")

                    if weekly_key != last_weekly_key:
                        start_ts, end_ts, period_label = _build_weekly_period(now_msk)

                        summary = _fetch_trade_summary(
                            db_path=settings.trade_db_path,
                            instrument_code=instrument_code,
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )

                        text = _build_summary_text(
                            title="ИТОГИ ТОРГОВЛИ ЗА НЕДЕЛЮ",
                            instrument_code=instrument_code,
                            period_label=period_label,
                            summary=summary,
                        )

                        await _send_summary_to_targets(sender=sender, settings=settings, text=text)
                        last_weekly_key = weekly_key

                if _is_monthly_due(now_msk):
                    monthly_key = _start_of_msk_day(now_msk).strftime("%Y-%m-%d")

                    if monthly_key != last_monthly_key:
                        start_ts, end_ts, period_label = _build_monthly_period(now_msk)

                        summary = _fetch_trade_summary(
                            db_path=settings.trade_db_path,
                            instrument_code=instrument_code,
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )

                        text = _build_summary_text(
                            title="ИТОГИ ТОРГОВЛИ ЗА МЕСЯЦ",
                            instrument_code=instrument_code,
                            period_label=period_label,
                            summary=summary,
                        )

                        await _send_summary_to_targets(sender=sender, settings=settings, text=text)
                        last_monthly_key = monthly_key

            except Exception as exc:
                log_warning(
                    logger,
                    f"Ошибка в trade_performance_summary_task: {exc}",
                    to_telegram=True,
                )

            await asyncio.sleep(poll_interval_seconds)

    except asyncio.CancelledError:
        log_info(
            logger,
            f"Фоновая отправка торговых итогов для {instrument_code} остановлена",
            to_telegram=False,
        )
        raise

    finally:
        await sender.close()

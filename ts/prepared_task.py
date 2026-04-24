import asyncio
from datetime import datetime, timedelta, timezone

from core.logger import get_logger, log_info
from ts.prepared_sync import sync_recent_prepared_analysis_windows

logger = get_logger(__name__)


def next_prepared_sync_dt_utc(now_utc):
    # Возвращаем момент следующего запуска prepared-sync.
    #
    # Prepared DB теперь хранит 60-минутные analysis windows со стартом каждые 30 минут,
    # поэтому синхронизацию запускаем дважды в час:
    # - HH:01:00 UTC;
    # - HH:31:00 UTC.
    current_hour_run_01 = now_utc.replace(minute=1, second=0, microsecond=0)
    current_hour_run_31 = now_utc.replace(minute=31, second=0, microsecond=0)

    if now_utc < current_hour_run_01:
        return current_hour_run_01

    if now_utc < current_hour_run_31:
        return current_hour_run_31

    return current_hour_run_01 + timedelta(hours=1)


def format_sync_window_text(stats):
    # Формируем компактный текст диапазона prepared-sync для логов.
    if stats.window_start_ts is None:
        start_text = "-inf"
    else:
        start_text = datetime.fromtimestamp(
            stats.window_start_ts,
            tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S")

    if stats.window_end_ts is None:
        end_text = "+inf"
    else:
        end_text = datetime.fromtimestamp(
            stats.window_end_ts,
            tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S")

    return f"[{start_text} .. {end_text}) UTC"


async def run_prepared_sync_once(settings, instrument_code, lookback_days):
    # Выполняем один проход синхронизации prepared DB в отдельном thread.
    #
    # Это принципиально важно: SQLite-скан по месяцу истории нельзя делать
    # синхронно внутри event loop, иначе подвиснут heartbeat, мониторинг и
    # обработка realtime-потока.
    stats = await asyncio.to_thread(
        sync_recent_prepared_analysis_windows,
        settings,
        instrument_code,
        lookback_days,
        None,
        False,
    )

    window_text = format_sync_window_text(stats)

    log_info(
        logger,
        f"Prepared DB sync: instrument={stats.instrument_code}, "
        f"window={window_text}, "
        f"candidate_windows={stats.candidate_windows}, "
        f"inserted={stats.inserted_windows}, "
        f"existing={stats.skipped_existing_windows}, "
        f"invalid={stats.skipped_invalid_windows}",
        to_telegram=False,
    )

    return stats


async def prepared_db_sync_task(settings, instrument_code="MNQ", lookback_days=31, run_immediately=True):
    # Фоновая задача синхронизации prepared DB.
    #
    # Сценарий:
    # - если run_immediately=True, первый проход запускаем сразу;
    # - если run_immediately=False, первый немедленный проход пропускаем;
    # - потом каждые 30 минут в HH:01 и HH:31 UTC.
    log_info(
        logger,
        f"Запуск фоновой синхронизации prepared DB для {instrument_code}",
        to_telegram=False,
    )

    try:
        if run_immediately:
            await run_prepared_sync_once(
                settings=settings,
                instrument_code=instrument_code,
                lookback_days=lookback_days,
            )

        while True:
            now_utc = datetime.now(timezone.utc)
            next_run_utc = next_prepared_sync_dt_utc(now_utc)
            sleep_seconds = (next_run_utc - now_utc).total_seconds()

            log_info(
                logger,
                f"Следующая синхронизация prepared DB: "
                f"{next_run_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC",
                to_telegram=False,
            )

            await asyncio.sleep(sleep_seconds)

            await run_prepared_sync_once(
                settings=settings,
                instrument_code=instrument_code,
                lookback_days=lookback_days,
            )

    except asyncio.CancelledError:
        log_info(
            logger,
            "Фоновая синхронизация prepared DB остановлена",
            to_telegram=False,
        )
        raise

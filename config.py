from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


@dataclass
class Settings:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт 7496 - реальный счёт
    ib_client_id: int = 100

    # Файл SQLite БД
    price_db_path: str = str(BASE_DIR / "data" / "price.sqlite3")
    trade_db_path: str = str(BASE_DIR / "data" / "trade.sqlite3")
    prepared_db_path: str = str(BASE_DIR / "data" / "prepared.sqlite3")

    # ==============================
    # Telegram bot / channels
    # ==============================

    telegram_bot_token: str = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
    # telegram_chat_id_common: int = -1003500510100  # общий лог-канал для тестов

    telegram_chat_id_trading: int = -1002621383506  # торговый канал    -       IB Trade
    telegram_chat_id_common: int = -1003208160378  # лог-канал          -       IB Logs
    telegram_chat_id_tech: int = -1003721167929  # технический канал    -       IB Tech
    telegram_chat_id_promo: int = -1003392998188  # промо группа        -       ФинтехПравда
    telegram_thread_id_promo: int = 34  # тема в промо группе           -       ФинтехПравда/Сигналы в IB


# Набор настроек для "боевого" подключения.
settings_live = Settings()

settings_for_demo = Settings(
    ib_port=7497,
)
settings_for_gap = Settings(
    ib_client_id=105,
)

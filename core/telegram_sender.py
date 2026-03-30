import aiohttp
from pathlib import Path

# Лимит обычного текстового сообщения Telegram.
TELEGRAM_TEXT_LIMIT = 4096

# Лимит подписи к картинке в Telegram.
TELEGRAM_CAPTION_LIMIT = 1024

# Общий timeout на один HTTP-запрос в Bot API.
TELEGRAM_REQUEST_TIMEOUT_SECONDS = 20


class TelegramSender:
    def __init__(self, settings):
        # Токен бота Telegram.
        self.bot_token = settings.telegram_bot_token

        # Канал по умолчанию для технических сообщений и логов.
        self.default_chat_id = settings.telegram_chat_id_tech

        # Если токен или chat_id по умолчанию не заданы,
        # модуль Telegram считаем выключенным.
        self.enabled = bool(self.bot_token and self.default_chat_id)

        # Базовый URL Bot API.
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

        # HTTP-сессия создаётся лениво при первом запросе.
        self.session = None

    async def close(self):
        # Корректно закрываем HTTP-сессию при завершении программы.
        if self.session is not None and not self.session.closed:
            await self.session.close()

    async def _ensure_session(self):
        # Если Telegram отключён в настройках, просто ничего не делаем.
        if not self.enabled:
            return False

        # Создаём ClientSession только один раз.
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=TELEGRAM_REQUEST_TIMEOUT_SECONDS)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return True

    def _resolve_chat_id(self, chat_id):
        # Если chat_id явно передан в вызове, используем его.
        # Иначе отправляем в канал по умолчанию.
        if chat_id:
            return str(chat_id)
        return str(self.default_chat_id)

    def _split_text(self, text, limit):
        # Разрезаем длинный текст на части не длиннее limit.
        # Стараемся резать по переводу строки, потом по пробелу.
        # Если не получилось, режем жёстко по limit.
        text = str(text).replace("\r\n", "\n").strip()
        if not text:
            return []

        chunks = []
        while text:
            if len(text) <= limit:
                chunks.append(text)
                break

            split_pos = text.rfind("\n", 0, limit + 1)
            if split_pos == -1 or split_pos < limit // 2:
                split_pos = text.rfind(" ", 0, limit + 1)
            if split_pos == -1 or split_pos < limit // 2:
                split_pos = limit

            chunk = text[:split_pos].strip()
            text = text[split_pos:].strip()
            if chunk:
                chunks.append(chunk)

        return chunks

    @staticmethod
    def _apply_thread_id(payload, message_thread_id):
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)
        return payload

    async def _post_json(self, method, payload):
        # Делаем простой JSON POST в Bot API.
        # Ничего не валидируем по ответу Telegram, просто отправляем.
        if not await self._ensure_session():
            return False

        url = f"{self.base_url}/{method}"
        try:
            async with self.session.post(url, json=payload) as response:
                await response.read()
                return True
        except Exception:
            # Не валим робота из-за проблем Telegram.
            # Повторных попыток не делаем.
            return False

    async def _post_photo(self, photo_path, chat_id, caption="", message_thread_id=None):
        # Отправка фото через multipart/form-data.
        if not await self._ensure_session():
            return False

        path = Path(photo_path)

        # Если файла нет — это уже локальная ошибка, её не скрываем.
        if not path.is_file():
            raise FileNotFoundError(f"Файл картинки не найден: {path}")

        url = f"{self.base_url}/sendPhoto"
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if message_thread_id is not None:
            form.add_field("message_thread_id", str(int(message_thread_id)))
        if caption:
            form.add_field("caption", caption)

        try:
            with path.open("rb") as file_obj:
                form.add_field(
                    "photo",
                    file_obj,
                    filename=path.name,
                    content_type="application/octet-stream",
                )
                async with self.session.post(url, data=form) as response:
                    await response.read()
                    return True
        except Exception:
            # Telegram не должен валить робота.
            return False

    async def send_text(self, text, chat_id=None, message_thread_id=None):
        # Отправка обычного текста.
        # Если chat_id не передали, идём в канал по умолчанию.
        resolved_chat_id = self._resolve_chat_id(chat_id)

        # Если текст длиннее лимита Telegram, режем на части
        # и отправляем последовательно.
        chunks = self._split_text(text, TELEGRAM_TEXT_LIMIT)
        if not chunks:
            return False

        result = True
        for chunk in chunks:
            payload = {
                "chat_id": resolved_chat_id,
                "text": chunk,
            }
            payload = self._apply_thread_id(payload, message_thread_id)
            ok = await self._post_json("sendMessage", payload)
            if not ok:
                result = False
        return result

    async def send_photo(self, photo_path, chat_id=None, message_thread_id=None):
        # Отправка только картинки без текста.
        resolved_chat_id = self._resolve_chat_id(chat_id)
        return await self._post_photo(
            photo_path,
            resolved_chat_id,
            message_thread_id=message_thread_id,
        )

    async def send_photo_with_text(
            self,
            photo_path,
            text,
            chat_id=None,
            message_thread_id=None,
    ):
        # Если chat_id не передали, используем канал по умолчанию.
        resolved_chat_id = self._resolve_chat_id(chat_id)

        # Если текста нет — просто шлём фото.
        text = str(text).strip()
        if not text:
            return await self.send_photo(
                photo_path,
                chat_id=resolved_chat_id,
                message_thread_id=message_thread_id,
            )

        # Если текст помещается в caption — шлём фото с подписью.
        if len(text) <= TELEGRAM_CAPTION_LIMIT:
            return await self._post_photo(
                photo_path,
                resolved_chat_id,
                caption=text,
                message_thread_id=message_thread_id,
            )

        # Если текст длинный:
        # 1) отправляем картинку без подписи
        # 2) отдельно отправляем текст с автоматической нарезкой
        photo_ok = await self._post_photo(
            photo_path,
            resolved_chat_id,
            message_thread_id=message_thread_id,
        )
        text_ok = await self.send_text(
            text,
            chat_id=resolved_chat_id,
            message_thread_id=message_thread_id,
        )
        return photo_ok and text_ok

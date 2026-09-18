import asyncio
import io
from typing import List, Optional, Tuple, Union

from pyrogram import Client
from pyrogram.errors import FloodWait

from app.core.config import CID, SESSION_STRING, TELEGRAM_API_HASH, TELEGRAM_API_ID, logger
from app.storage import Storage


class TelegramAccountStorage(Storage):
    def __init__(self):
        self._client: Optional[Client] = None
        self._lock = asyncio.Lock()
        self._is_connected = False

    def _create_client(self) -> Client:
        clean_session = SESSION_STRING.strip().strip("'\"") if SESSION_STRING else None
        clean_api_id = int(TELEGRAM_API_ID.strip().strip("'\"")) if TELEGRAM_API_ID else None
        clean_api_hash = TELEGRAM_API_HASH.strip().strip("'\"") if TELEGRAM_API_HASH else None

        if not clean_session:
            logger.error("SESSION_STRING is missing or empty")
        else:
            logger.info("Initializing Telegram client (session length: %d chars)", len(clean_session))

        return Client(
            "telegram",
            api_id=clean_api_id,
            api_hash=clean_api_hash,
            session_string=clean_session,
            in_memory=True,
        )

    async def start(self) -> None:
        async with self._lock:
            if not self._is_connected:
                if not self._client:
                    self._client = self._create_client()
                try:
                    await self._client.start()
                    self._is_connected = True
                    logger.info("Telegram Pyrogram client successfully started.")
                except Exception as e:
                    logger.warning("Could not auto-start Telegram client on startup: %s", e)

    async def stop(self) -> None:
        async with self._lock:
            if self._is_connected and self._client:
                try:
                    await self._client.stop()
                    logger.info("Telegram Pyrogram client stopped.")
                except Exception as e:
                    logger.warning("Error stopping Telegram client: %s", e)
                finally:
                    self._is_connected = False
                    self._client = None

    async def _get_connected_client(self) -> Client:
        if self._is_connected and self._client and self._client.is_connected:
            return self._client

        async with self._lock:
            if not self._client:
                self._client = self._create_client()
            if not self._client.is_connected:
                await self._client.start()
                self._is_connected = True
                logger.info("Telegram Pyrogram client connected on demand.")
            return self._client

    async def put_file(
        self,
        file: Union[bytes, str, io.BytesIO],
        filename: str,
        channel_id: Optional[Union[str, int]] = None,
    ) -> Tuple[str, Optional[int]]:
        if isinstance(file, bytes):
            document = io.BytesIO(file)
        else:
            document = file

        raw_cid = channel_id if channel_id is not None and str(channel_id).strip() else CID
        if not raw_cid:
            raise ValueError("No Telegram Channel ID (CID) specified for bucket or in environment.")

        target_cid = int(str(raw_cid).strip().strip("'\""))

        max_retries = 3
        for attempt in range(max_retries):
            try:
                app = await self._get_connected_client()
                response = await app.send_document(target_cid, document, file_name=filename)
                file_id = str(response.document.file_id)
                message_id = getattr(response, "id", None)
                return file_id, message_id
            except FloodWait as fw:
                logger.warning("Telegram FloodWait encountered: sleeping for %d seconds (attempt %d/%d)", fw.value, attempt + 1, max_retries)
                await asyncio.sleep(fw.value + 1)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.exception("Final attempt failed sending document to Telegram: %s", e)
                    raise
                logger.warning("Telegram send_document retry %d/%d due to: %s", attempt + 1, max_retries, e)
                await asyncio.sleep(1)

        raise RuntimeError("Failed to put_file after max retries")

    async def get_file(self, file_id: str) -> io.BytesIO:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                app = await self._get_connected_client()
                file = await app.download_media(file_id, in_memory=True)
                if file is None:
                    logger.error("Pyrogram download_media returned None for file_id: %s", file_id)
                    raise ValueError(f"Failed to download media for file_id: {file_id}")
                file.seek(0)
                return file
            except FloodWait as fw:
                logger.warning("Telegram FloodWait encountered: sleeping for %d seconds (attempt %d/%d)", fw.value, attempt + 1, max_retries)
                await asyncio.sleep(fw.value + 1)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.exception("Final attempt failed downloading media from Telegram: %s", e)
                    raise
                logger.warning("Telegram download_media retry %d/%d due to: %s", attempt + 1, max_retries, e)
                await asyncio.sleep(1)

        raise RuntimeError("Failed to get_file after max retries")

    async def delete_file(
        self,
        file_id: str,
        channel_id: Optional[Union[str, int]] = None,
        message_id: Optional[int] = None,
        message_ids: Optional[List[int]] = None,
    ) -> bool:
        raw_cid = channel_id if channel_id is not None and str(channel_id).strip() else CID
        if not raw_cid:
            return True

        target_cid = int(str(raw_cid).strip().strip("'\""))
        to_delete = []
        if message_ids:
            to_delete.extend(message_ids)
        elif message_id is not None:
            to_delete.append(message_id)

        if not to_delete:
            return True

        try:
            app = await self._get_connected_client()
            await app.delete_messages(chat_id=target_cid, message_ids=to_delete)
            logger.info("Successfully deleted Telegram message(s) %s from channel %s", to_delete, target_cid)
            return True
        except Exception as e:
            logger.warning("Failed to delete message(s) %s in channel %s: %s", to_delete, target_cid, e)
            return False

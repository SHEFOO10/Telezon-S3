import io

from pyrogram import Client

from app.core.config import CID, SESSION_STRING, TELEGRAM_API_HASH, TELEGRAM_API_ID, logger
from app.storage import Storage


class TelegramAccountStorage(Storage):
    def client(self):
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

    async def put_file(self, file: bytes, filename: str) -> str:
        document = io.BytesIO(file)

        async with self.client() as app:
            async for _ in app.get_dialogs():
                pass
            target_cid = int(str(CID).strip().strip("'\""))
            response = await app.send_document(target_cid, document, file_name=filename)
            return str(response.document.file_id)

    async def get_file(self, file_id: str) -> io.BytesIO:
        async with self.client() as app:
            async for _ in app.get_dialogs():
                pass
            file = await app.download_media(file_id, in_memory=True)
            if file is None:
                logger.error("Pyrogram download_media returned None for file_id: %s", file_id)
                raise ValueError(f"Failed to download media for file_id: {file_id}")
            file.seek(0)
            return file

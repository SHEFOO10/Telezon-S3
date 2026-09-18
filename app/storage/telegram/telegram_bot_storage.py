from typing import Optional, Union

from app.core.config import CID
from app.storage import Storage
from app.storage.telegram.bot import bot


class TelegramBotStorage(Storage):
    async def put_file(
        self, file: bytes, filename: str, channel_id: Optional[Union[str, int]] = None
    ) -> str:
        target_cid = channel_id if channel_id is not None and str(channel_id).strip() else CID
        if not target_cid:
            raise ValueError("No Telegram Channel ID (CID) specified for bucket or in environment.")
        result = await bot.send_document(target_cid, file, filename=filename)
        return str(result.document.file_id)

    async def get_file(self, file_id: str):
        file = await bot.get_file(file_id)
        return file

    async def delete_file(
        self, file_id: str, channel_id: Optional[Union[str, int]] = None
    ) -> bool:
        return True

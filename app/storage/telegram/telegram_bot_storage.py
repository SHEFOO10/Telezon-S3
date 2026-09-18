import io
from typing import List, Optional, Tuple, Union

from app.core.config import CID, logger
from app.storage import Storage
from app.storage.telegram.bot import bot


class TelegramBotStorage(Storage):
    async def put_file(
        self,
        file: Union[bytes, str, io.BytesIO],
        filename: str,
        channel_id: Optional[Union[str, int]] = None,
    ) -> Tuple[str, Optional[int]]:
        target_cid = channel_id if channel_id is not None and str(channel_id).strip() else CID
        if not target_cid:
            raise ValueError("No Telegram Channel ID (CID) specified for bucket or in environment.")
        
        target_cid = int(str(target_cid).strip().strip("'\""))
        result = await bot.send_document(target_cid, file, filename=filename)
        file_id = str(result.document.file_id)
        message_id = getattr(result, "message_id", None)
        return file_id, message_id

    async def get_file(self, file_id: str):
        file = await bot.get_file(file_id)
        return file

    async def delete_file(
        self,
        file_id: str,
        channel_id: Optional[Union[str, int]] = None,
        message_id: Optional[int] = None,
        message_ids: Optional[List[int]] = None,
    ) -> bool:
        target_cid = channel_id if channel_id is not None and str(channel_id).strip() else CID
        if not target_cid:
            return True

        target_cid = int(str(target_cid).strip().strip("'\""))
        to_delete = []
        if message_ids:
            to_delete.extend(message_ids)
        elif message_id is not None:
            to_delete.append(message_id)

        if not to_delete:
            return True

        success = True
        for mid in to_delete:
            try:
                await bot.delete_message(chat_id=target_cid, message_id=mid)
                logger.info("Successfully deleted Telegram message %s from channel %s", mid, target_cid)
            except Exception as e:
                logger.warning("Failed to delete message %s in channel %s: %s", mid, target_cid, e)
                success = False
        return success

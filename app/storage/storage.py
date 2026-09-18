from abc import ABC
import io
from typing import List, Optional, Tuple, Union


class Storage(ABC):
    async def start(self) -> None:
        """Initialize and start any persistent connections / clients."""
        pass

    async def stop(self) -> None:
        """Gracefully disconnect and close storage clients."""
        pass

    async def put_file(
        self,
        file: Union[bytes, str, io.BytesIO],
        filename: str,
        channel_id: Optional[Union[str, int]] = None,
    ) -> Tuple[str, Optional[int]]:
        """
        Upload file to Telegram storage.
        Returns a tuple: (file_id, message_id).
        """
        raise NotImplementedError

    async def get_file(self, file_id: str) -> io.BytesIO:
        """Download file from Telegram storage into memory."""
        raise NotImplementedError

    async def delete_file(
        self,
        file_id: str,
        channel_id: Optional[Union[str, int]] = None,
        message_id: Optional[int] = None,
        message_ids: Optional[List[int]] = None,
    ) -> bool:
        """Delete file/message(s) from Telegram channel."""
        raise NotImplementedError

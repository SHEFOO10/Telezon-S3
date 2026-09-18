from abc import ABC
from typing import Optional, Union


class Storage(ABC):
    async def put_file(
        self, file: bytes, filename: str, channel_id: Optional[Union[str, int]] = None
    ) -> str:
        pass

    async def get_file(self, file_id: str):
        pass

    async def delete_file(
        self, file_id: str, channel_id: Optional[Union[str, int]] = None
    ):
        pass

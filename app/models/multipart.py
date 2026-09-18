from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field


class UploadPart(BaseModel):
    part_number: int
    etag: str
    size: int
    file_id: str = ""
    message_id: Optional[int] = None
    data_path: Optional[str] = None
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"))


class MultipartUploadInDb(BaseModel):
    upload_id: str
    bucket_name: str
    key: str
    content_type: str = "application/octet-stream"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"))
    parts: List[UploadPart] = []

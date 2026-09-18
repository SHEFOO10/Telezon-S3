import os
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import DATABASE_NAME
from app.models.multipart import MultipartUploadInDb, UploadPart

COLLECTION = "multipart_uploads"


async def crud_create_multipart_upload(
    db: AsyncIOMotorClient, upload: MultipartUploadInDb
) -> MultipartUploadInDb:
    await db[DATABASE_NAME][COLLECTION].insert_one(upload.model_dump())
    return upload


async def crud_get_multipart_upload(
    db: AsyncIOMotorClient, bucket_name: str, key: str, upload_id: str
) -> Optional[MultipartUploadInDb]:
    doc = await db[DATABASE_NAME][COLLECTION].find_one(
        {"bucket_name": bucket_name, "key": key, "upload_id": upload_id}
    )
    if doc:
        return MultipartUploadInDb(**doc)
    return None


async def crud_save_part(
    db: AsyncIOMotorClient, bucket_name: str, key: str, upload_id: str, part: UploadPart
) -> bool:
    # Remove existing part with the same part_number if overwriting
    await db[DATABASE_NAME][COLLECTION].update_one(
        {"bucket_name": bucket_name, "key": key, "upload_id": upload_id},
        {"$pull": {"parts": {"part_number": part.part_number}}},
    )
    # Add the new/updated part
    res = await db[DATABASE_NAME][COLLECTION].update_one(
        {"bucket_name": bucket_name, "key": key, "upload_id": upload_id},
        {"$push": {"parts": part.model_dump()}},
    )
    return res.modified_count > 0


async def crud_delete_multipart_upload(
    db: AsyncIOMotorClient, bucket_name: str, key: str, upload_id: str
) -> bool:
    upload = await crud_get_multipart_upload(db, bucket_name, key, upload_id)
    if upload:
        for p in upload.parts:
            if p.data_path and os.path.exists(p.data_path):
                try:
                    os.remove(p.data_path)
                except Exception:
                    pass

    res = await db[DATABASE_NAME][COLLECTION].delete_one(
        {"bucket_name": bucket_name, "key": key, "upload_id": upload_id}
    )
    return res.deleted_count > 0



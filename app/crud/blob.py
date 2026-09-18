import re
from typing import List, Optional, Set, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import DATABASE_NAME
from app.models.blob import Blob, BlobFilterParams, BlobInCreate, BlobInDb

COLLECTION = "blobs"

aggregate_bucket = {
    "$lookup": {
        "from": "buckets",
        "localField": "bucket_name",
        "foreignField": "name",
        "as": "bucket",
    }
}

aggregate_owner = {
    "$lookup": {
        "from": "users",
        "localField": "bucket.owner_username",
        "foreignField": "username",
        "as": "owner",
    }
}


async def crud_get_all_blobs(
    db: AsyncIOMotorClient, filters: BlobFilterParams
) -> List[Blob]:
    blobs: List[Blob] = []
    base_query = {}

    if filters.path:
        paths = filters.path.replace(", ", ",").split(",")
        base_query["path"] = {"$in": paths}

    if filters.bucket_name:
        bucket_names = filters.bucket_name.replace(", ", ",").split(",")
        base_query["bucket_name"] = {"$in": bucket_names}

    blob_docs = db[DATABASE_NAME][COLLECTION].aggregate(
        [
            {"$match": base_query},
            {"$limit": filters.offset + filters.limit},
            {"$skip": filters.offset},
            aggregate_bucket,
            aggregate_owner,
            {"$unwind": {"path": "$bucket"}},
            {"$unwind": {"path": "$owner"}},
        ]
    )

    async for row in blob_docs:
        blobs.append(Blob(**row))

    return blobs


async def crud_create_blob(
    db: AsyncIOMotorClient, blob: BlobInCreate, bucket_name: str, update: bool = False
) -> BlobInDb:
    data_blob = BlobInDb(**blob.model_dump())
    data_blob.bucket_name = bucket_name

    if not update:
        row = await db[DATABASE_NAME][COLLECTION].insert_one(data_blob.model_dump())

        data_blob.created_at = ObjectId(row.inserted_id).generation_time
        data_blob.updated_at = ObjectId(row.inserted_id).generation_time
    else:
        updated_at = await db[DATABASE_NAME][COLLECTION].update_one(
            {"path": data_blob.path, "bucket_name": bucket_name},
            {"$set": data_blob.model_dump()},
        )

        data_blob.updated_at = updated_at

    return data_blob


async def crud_delete_blob(
    db: AsyncIOMotorClient, path: str, bucket_name: str
) -> bool:
    result = await db[DATABASE_NAME][COLLECTION].delete_many(
        {"path": path, "bucket_name": bucket_name}
    )
    return result.deleted_count > 0


async def crud_list_blobs_v2(
    db: AsyncIOMotorClient,
    bucket_name: str,
    prefix: str = "",
    delimiter: str = "",
    max_keys: int = 1000,
    continuation_token: Optional[str] = None,
    start_after: Optional[str] = None,
) -> Tuple[List[Blob], List[str], bool, Optional[str]]:
    base_query = {"bucket_name": bucket_name}

    if prefix:
        base_query["path"] = {"$regex": f"^{re.escape(prefix)}"}

    start_key = continuation_token or start_after
    if start_key:
        if "path" in base_query and isinstance(base_query["path"], dict):
            base_query["path"]["$gt"] = start_key
        else:
            base_query["path"] = {"$gt": start_key}

    pipeline = [
        {"$match": base_query},
        {"$sort": {"path": 1}},
        {"$limit": max_keys + 1},
        aggregate_bucket,
        aggregate_owner,
        {"$unwind": {"path": "$bucket"}},
        {"$unwind": {"path": "$owner"}},
    ]

    cursor = db[DATABASE_NAME][COLLECTION].aggregate(pipeline)
    raw_blobs: List[Blob] = []
    async for row in cursor:
        raw_blobs.append(Blob(**row))

    is_truncated = len(raw_blobs) > max_keys
    if is_truncated:
        raw_blobs = raw_blobs[:max_keys]
        next_token = raw_blobs[-1].path if raw_blobs else None
    else:
        next_token = None

    matched_blobs: List[Blob] = []
    common_prefixes_set: Set[str] = set()

    for blob in raw_blobs:
        if delimiter:
            rel_path = blob.path[len(prefix):] if blob.path.startswith(prefix) else blob.path
            if delimiter in rel_path:
                idx = rel_path.find(delimiter)
                cp = prefix + rel_path[: idx + len(delimiter)]
                common_prefixes_set.add(cp)
            else:
                matched_blobs.append(blob)
        else:
            matched_blobs.append(blob)

    return matched_blobs, sorted(list(common_prefixes_set)), is_truncated, next_token



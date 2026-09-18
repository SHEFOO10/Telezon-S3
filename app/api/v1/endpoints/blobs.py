from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.status import HTTP_404_NOT_FOUND

from app.api.auth.utils import check_role_admin
from app.core.token import get_current_user
from app.crud.blob import crud_delete_blob, crud_get_all_blobs
from app.db.mongodb import get_database
from app.models.blob import Blob, BlobFilterParams
from app.models.user import User

router = APIRouter(prefix="/blobs", tags=["Blobs"])


@router.get("/", response_model=List[Blob])
async def get_all_blobs(
    path: str = "",
    bucket_name: str = "",
    limit: int = Query(20),
    offset: int = Query(0),
    db: AsyncIOMotorClient = Depends(get_database),
    current_user: User = Depends(get_current_user),
):
    check_role_admin(current_user)
    filters = BlobFilterParams(
        path=path, bucket_name=bucket_name, limit=limit, offset=offset
    )

    blobs = await crud_get_all_blobs(db, filters)
    return blobs


@router.delete("/")
async def delete_blob(
    path: str,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
    current_user: User = Depends(get_current_user),
):
    check_role_admin(current_user)
    deleted = await crud_delete_blob(db, path=path, bucket_name=bucket_name)
    if not deleted:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Blob with path '{path}' in bucket '{bucket_name}' not found",
        )
    return {"status": "success", "message": "Blob deleted successfully"}


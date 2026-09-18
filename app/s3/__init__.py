import xml.etree.ElementTree as ET
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.status import HTTP_404_NOT_FOUND

from app.core.config import logger
from app.crud.blob import crud_create_blob, crud_delete_blob, crud_get_all_blobs
from app.crud.bucket import crud_get_bucket_by_name
from app.db.mongodb import get_database
from app.models.blob import BlobFilterParams, BlobInCreate
from app.s3.utils import aws_sig_verify, s3_error_response
from app.storage import storage

router = APIRouter(tags=["S3"])


@router.put("/{bucket_name}/{path:path}")
async def upload_file(
    request: Request,
    bucket_name: str,
    path: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    bucket = await crud_get_bucket_by_name(db, bucket_name)

    if not bucket:
        return s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket '{bucket_name}' does not exist.",
            status_code=404,
            resource=f"/{bucket_name}/{path}",
        )

    is_valid, error_msg = await aws_sig_verify(bucket, request)
    if not is_valid:
        return s3_error_response(
            code="AccessDenied",
            message=f"Access Denied: {error_msg}",
            status_code=403,
            resource=f"/{bucket_name}/{path}",
        )

    filters = BlobFilterParams(path=path, bucket_name=bucket_name)

    blobs = await crud_get_all_blobs(db, filters)

    if len(blobs) == 0:
        update = False
        blob = BlobInCreate(path=path)
    else:
        update = True
        blob = BlobInCreate(**blobs[0].model_dump())

    body = await request.body()
    blob.content_type = request.headers.get("content-type", "application/octet-stream")
    blob.size = int(request.headers.get("content-length", len(body)))

    try:
        file_id = await storage.put_file(body, path, channel_id=bucket.channel_id)
    except Exception as e:
        logger.exception("Storage error uploading file '%s': %s", path, e)
        return s3_error_response(
            code="InternalError",
            message=f"Telegram storage upload failed: {str(e)}",
            status_code=500,
            resource=f"/{bucket_name}/{path}",
        )

    blob.file = file_id

    await crud_create_blob(db, blob, bucket_name, update)

    return Response(
        status_code=200,
        headers={"ETag": f'"{file_id}"'},
    )


@router.get("/{bucket_name}/{path:path}")
async def download_file(
    request: Request,
    bucket_name: str,
    path: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    bucket = await crud_get_bucket_by_name(db, bucket_name)

    if not bucket:
        return s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket '{bucket_name}' does not exist.",
            status_code=404,
            resource=f"/{bucket_name}/{path}",
        )

    is_valid, error_msg = await aws_sig_verify(bucket, request)
    if not is_valid:
        return s3_error_response(
            code="AccessDenied",
            message=f"Access Denied: {error_msg}",
            status_code=403,
            resource=f"/{bucket_name}/{path}",
        )

    filters = BlobFilterParams(path=path, bucket_name=bucket_name)

    blobs = await crud_get_all_blobs(db, filters)

    if len(blobs) == 0:
        return s3_error_response(
            code="NoSuchKey",
            message=f"The specified key '{path}' does not exist.",
            status_code=404,
            resource=f"/{bucket_name}/{path}",
        )

    blob = blobs[0]

    try:
        result_file = await storage.get_file(blob.file)
    except Exception as e:
        logger.exception("Storage error downloading file '%s' (file_id=%s): %s", blob.path, blob.file, e)
        return s3_error_response(
            code="InternalError",
            message=f"Telegram storage download failed: {str(e)}",
            status_code=500,
            resource=f"/{bucket_name}/{path}",
        )

    content_type = blob.content_type or "application/octet-stream"

    async def file_iterator(file_obj, chunk_size=1024 * 1024):
        while chunk := file_obj.read(chunk_size):
            yield chunk

    return StreamingResponse(
        file_iterator(result_file),
        media_type=content_type,
        headers={
            "Content-Length": str(blob.size),
            "Content-Type": content_type,
            "ETag": f'"{blob.file}"',
        },
    )


@router.head("/{bucket_name}/{path:path}")
async def check_file(
    request: Request,
    bucket_name: str,
    path: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    bucket = await crud_get_bucket_by_name(db, bucket_name)

    if not bucket:
        return Response(status_code=HTTP_404_NOT_FOUND)

    is_valid, _ = await aws_sig_verify(bucket, request)
    if not is_valid:
        return Response(status_code=403)

    filters = BlobFilterParams(path=path, bucket_name=bucket_name)

    blobs = await crud_get_all_blobs(db, filters)

    if len(blobs) > 0:
        blob = blobs[0]
        content_type = blob.content_type or "application/octet-stream"
        return Response(
            status_code=200,
            headers={
                "Content-Length": str(blob.size),
                "Content-Type": content_type,
                "ETag": f'"{blob.file}"',
            },
        )
    else:
        return Response(status_code=HTTP_404_NOT_FOUND)


@router.delete("/{bucket_name}/{path:path}")
async def delete_file(
    request: Request,
    bucket_name: str,
    path: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    bucket = await crud_get_bucket_by_name(db, bucket_name)

    if not bucket:
        return s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket '{bucket_name}' does not exist.",
            status_code=404,
            resource=f"/{bucket_name}/{path}",
        )

    is_valid, error_msg = await aws_sig_verify(bucket, request)
    if not is_valid:
        return s3_error_response(
            code="AccessDenied",
            message=f"Access Denied: {error_msg}",
            status_code=403,
            resource=f"/{bucket_name}/{path}",
        )

    filters = BlobFilterParams(path=path, bucket_name=bucket_name)
    blobs = await crud_get_all_blobs(db, filters)

    if len(blobs) > 0:
        blob = blobs[0]
        try:
            await storage.delete_file(blob.file, channel_id=bucket.channel_id)
        except Exception as e:
            logger.warning("Storage warning deleting file '%s': %s", path, e)

        await crud_delete_blob(db, path=path, bucket_name=bucket_name)

    return Response(status_code=204)


@router.post("/{bucket_name}")
async def post_bucket_operations(
    request: Request,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    if "delete" in request.query_params:
        bucket = await crud_get_bucket_by_name(db, bucket_name)

        if not bucket:
            return s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket '{bucket_name}' does not exist.",
                status_code=404,
                resource=f"/{bucket_name}",
            )

        is_valid, error_msg = await aws_sig_verify(bucket, request)
        if not is_valid:
            return s3_error_response(
                code="AccessDenied",
                message=f"Access Denied: {error_msg}",
                status_code=403,
                resource=f"/{bucket_name}",
            )

        body = await request.body()
        deleted_keys = []

        try:
            root = ET.fromstring(body)
            objects = []
            for elem in root.iter():
                if elem.tag.endswith("Object"):
                    objects.append(elem)

            for obj in objects:
                key = None
                for child in obj:
                    if child.tag.endswith("Key") and child.text:
                        key = child.text.strip()
                        break
                if key:
                    await crud_delete_blob(db, path=key, bucket_name=bucket_name)
                    deleted_keys.append(key)
        except Exception as e:
            logger.exception("Error parsing DeleteObjects XML: %s", e)

        deleted_xml = "".join([f"<Deleted><Key>{k}</Key></Deleted>" for k in deleted_keys])
        result_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
            f"{deleted_xml}\n"
            "</DeleteResult>"
        )
        return Response(content=result_xml, status_code=200, media_type="application/xml")

    return s3_error_response(
        code="NotImplemented",
        message="A header or query parameter you provided implies functionality that is not implemented",
        status_code=501,
        resource=f"/{bucket_name}",
    )


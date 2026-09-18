import hashlib
import os
import tempfile
import uuid
import xml.etree.ElementTree as ET
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.status import HTTP_404_NOT_FOUND

from app.core.config import logger
from app.crud.blob import (
    crud_create_blob,
    crud_delete_blob,
    crud_get_all_blobs,
    crud_list_blobs_v2,
)
from app.crud.bucket import crud_get_bucket_by_name
from app.crud.multipart import (
    crud_create_multipart_upload,
    crud_delete_multipart_upload,
    crud_get_multipart_upload,
    crud_save_part,
)
from app.db.mongodb import get_database
from app.models.blob import BlobFilterParams, BlobInCreate
from app.models.multipart import MultipartUploadInDb, UploadPart
from app.s3.utils import (
    aws_sig_verify,
    build_complete_multipart_upload_result_xml,
    build_initiate_multipart_upload_result_xml,
    build_list_objects_v2_xml,
    build_list_parts_result_xml,
    s3_error_response,
)
from app.storage import storage

router = APIRouter(tags=["S3"])


@router.get("/{bucket_name}")
async def get_bucket_objects(
    request: Request,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    """
    ListObjects / ListObjectsV2 handler
    """
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

    prefix = request.query_params.get("prefix", "")
    delimiter = request.query_params.get("delimiter", "")
    max_keys_str = request.query_params.get("max-keys") or request.query_params.get("max_keys", "1000")
    try:
        max_keys = min(int(max_keys_str), 1000)
    except ValueError:
        max_keys = 1000

    continuation_token = request.query_params.get("continuation-token") or request.query_params.get("continuation_token")
    start_after = request.query_params.get("start-after") or request.query_params.get("start_after") or request.query_params.get("marker")

    blobs, common_prefixes, is_truncated, next_token = await crud_list_blobs_v2(
        db=db,
        bucket_name=bucket_name,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        continuation_token=continuation_token,
        start_after=start_after,
    )

    xml_content = build_list_objects_v2_xml(
        bucket_name=bucket_name,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        blobs=blobs,
        common_prefixes=common_prefixes,
        is_truncated=is_truncated,
        continuation_token=continuation_token,
        next_continuation_token=next_token,
    )

    return Response(content=xml_content, status_code=200, media_type="application/xml")


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

    # 1. Handle UploadPart in Multipart Upload
    upload_id = request.query_params.get("uploadId")
    part_number_str = request.query_params.get("partNumber")
    if upload_id and part_number_str:
        try:
            part_number = int(part_number_str)
        except ValueError:
            return s3_error_response(
                code="InvalidArgument",
                message="Part number must be an integer",
                status_code=400,
                resource=f"/{bucket_name}/{path}",
            )

        upload = await crud_get_multipart_upload(db, bucket_name, path, upload_id)
        if not upload:
            return s3_error_response(
                code="NoSuchUpload",
                message="The specified multipart upload does not exist.",
                status_code=404,
                resource=f"/{bucket_name}/{path}",
            )

        body = await request.body()
        etag = hashlib.md5(body).hexdigest()

        # Save part data to a temporary file
        temp_dir = tempfile.gettempdir()
        part_file_path = os.path.join(temp_dir, f"telezon_{upload_id}_{part_number}.part")
        with open(part_file_path, "wb") as f:
            f.write(body)

        part_data = UploadPart(
            part_number=part_number,
            etag=etag,
            size=len(body),
            data_path=part_file_path,
        )
        await crud_save_part(db, bucket_name, path, upload_id, part_data)

        return Response(status_code=200, headers={"ETag": f'"{etag}"'})

    # 2. Standard PutObject
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

    # 1. Handle ListParts in Multipart Upload
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        upload = await crud_get_multipart_upload(db, bucket_name, path, upload_id)
        if not upload:
            return s3_error_response(
                code="NoSuchUpload",
                message="The specified multipart upload does not exist.",
                status_code=404,
                resource=f"/{bucket_name}/{path}",
            )

        parts_list = [p.model_dump() for p in sorted(upload.parts, key=lambda x: x.part_number)]
        xml_content = build_list_parts_result_xml(bucket_name, path, upload_id, parts_list)
        return Response(content=xml_content, status_code=200, media_type="application/xml")

    # 2. Standard GetObject
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

    # 1. Handle AbortMultipartUpload
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        await crud_delete_multipart_upload(db, bucket_name, path, upload_id)
        return Response(status_code=204)

    # 2. Standard DeleteObject
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


@router.post("/{bucket_name}/{path:path}")
async def post_object_operations(
    request: Request,
    bucket_name: str,
    path: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    """
    Handle CreateMultipartUpload (?uploads) and CompleteMultipartUpload (?uploadId=...)
    """
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

    # 1. CreateMultipartUpload (?uploads)
    if "uploads" in request.query_params:
        upload_id = uuid.uuid4().hex
        content_type = request.headers.get("content-type", "application/octet-stream")

        record = MultipartUploadInDb(
            upload_id=upload_id,
            bucket_name=bucket_name,
            key=path,
            content_type=content_type,
        )
        await crud_create_multipart_upload(db, record)

        xml_content = build_initiate_multipart_upload_result_xml(bucket_name, path, upload_id)
        return Response(content=xml_content, status_code=200, media_type="application/xml")

    # 2. CompleteMultipartUpload (?uploadId=...)
    upload_id = request.query_params.get("uploadId")
    if upload_id:
        upload = await crud_get_multipart_upload(db, bucket_name, path, upload_id)
        if not upload:
            return s3_error_response(
                code="NoSuchUpload",
                message="The specified multipart upload does not exist.",
                status_code=404,
                resource=f"/{bucket_name}/{path}",
            )

        # Sort parts by part number
        sorted_parts = sorted(upload.parts, key=lambda p: p.part_number)
        if not sorted_parts:
            return s3_error_response(
                code="InvalidPart",
                message="No parts found for this multipart upload.",
                status_code=400,
                resource=f"/{bucket_name}/{path}",
            )

        # Concatenate parts
        combined_bytes = bytearray()
        for p in sorted_parts:
            if p.data_path and os.path.exists(p.data_path):
                with open(p.data_path, "rb") as f:
                    combined_bytes.extend(f.read())

        # Upload assembled file to Telegram storage
        try:
            file_id = await storage.put_file(bytes(combined_bytes), path, channel_id=bucket.channel_id)
        except Exception as e:
            logger.exception("Storage error completing multipart upload for '%s': %s", path, e)
            return s3_error_response(
                code="InternalError",
                message=f"Telegram storage upload failed: {str(e)}",
                status_code=500,
                resource=f"/{bucket_name}/{path}",
            )

        # Update Blob in MongoDB
        filters = BlobFilterParams(path=path, bucket_name=bucket_name)
        existing_blobs = await crud_get_all_blobs(db, filters)
        update = len(existing_blobs) > 0

        blob_in = BlobInCreate(
            path=path,
            file=file_id,
            content_type=upload.content_type,
            size=len(combined_bytes),
        )
        await crud_create_blob(db, blob_in, bucket_name, update)

        # Clean up multipart temporary files and DB record
        await crud_delete_multipart_upload(db, bucket_name, path, upload_id)

        location = f"{request.url.scheme}://{request.url.netloc}/{bucket_name}/{path}"
        xml_content = build_complete_multipart_upload_result_xml(bucket_name, path, location, file_id)
        return Response(content=xml_content, status_code=200, media_type="application/xml")

    return s3_error_response(
        code="NotImplemented",
        message="A header or query parameter you provided implies functionality that is not implemented",
        status_code=501,
        resource=f"/{bucket_name}/{path}",
    )


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



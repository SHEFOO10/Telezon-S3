import asyncio
from datetime import datetime, timezone
import hashlib
import os
import tempfile
from urllib.parse import unquote
import uuid
import xml.etree.ElementTree as ET
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.status import HTTP_404_NOT_FOUND

from app.core.config import CID, MULTIPART_MODE, logger
from app.crud.blob import (
    crud_create_blob,
    crud_delete_blob,
    crud_get_all_blobs,
    crud_list_blobs_v2,
)
from app.crud.bucket import (
    crud_create_bucket,
    crud_delete_bucket,
    crud_get_all_buckets,
    crud_get_bucket_by_name,
)
from app.crud.multipart import (
    crud_create_multipart_upload,
    crud_delete_multipart_upload,
    crud_get_multipart_upload,
    crud_save_part,
)
from app.db.mongodb import get_database
from app.models.blob import BlobFilterParams, BlobInCreate
from app.models.bucket import BucketFilterParams, BucketInCreate
from app.models.multipart import MultipartUploadInDb, UploadPart
from app.s3.utils import (
    authenticate_request_user,
    aws_sig_verify,
    build_complete_multipart_upload_result_xml,
    build_copy_object_result_xml,
    build_initiate_multipart_upload_result_xml,
    build_list_all_my_buckets_result_xml,
    build_list_objects_v2_xml,
    build_list_parts_result_xml,
    parse_range_header,
    s3_error_response,
)
from app.storage import storage

router = APIRouter(tags=["S3"])


# ---------------------------------------------------------
# S3 Root Operations: ListBuckets (GET /)
# ---------------------------------------------------------
@router.get("/")
async def list_buckets(
    request: Request,
    db: AsyncIOMotorClient = Depends(get_database),
):
    user, err = await authenticate_request_user(db, request)
    if not user:
        return s3_error_response(
            code="AccessDenied",
            message=f"Access Denied: {err}",
            status_code=403,
            resource="/",
        )

    buckets = await crud_get_all_buckets(db, BucketFilterParams(owner_username=user.username, limit=1000))
    xml_content = build_list_all_my_buckets_result_xml(
        buckets=buckets,
        owner_id=user.access_key_id,
        owner_name=user.username,
    )
    return Response(content=xml_content, status_code=200, media_type="application/xml")


# ---------------------------------------------------------
# Bucket Level Operations: ListObjectsV2 (GET /{bucket_name})
# ---------------------------------------------------------
@router.get("/{bucket_name}")
async def get_bucket_objects(
    request: Request,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
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


# ---------------------------------------------------------
# S3 CreateBucket (PUT /{bucket_name})
# ---------------------------------------------------------
@router.put("/{bucket_name}")
async def create_bucket_handler(
    request: Request,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
    user, err = await authenticate_request_user(db, request)
    if not user:
        return s3_error_response(
            code="AccessDenied",
            message=f"Access Denied: {err}",
            status_code=403,
            resource=f"/{bucket_name}",
        )

    existing_bucket = await crud_get_bucket_by_name(db, bucket_name)
    if existing_bucket:
        if existing_bucket.owner and existing_bucket.owner.username == user.username:
            return Response(status_code=200, headers={"Location": f"/{bucket_name}"})
        return s3_error_response(
            code="BucketAlreadyExists",
            message="The requested bucket name is not available. The bucket namespace is shared by all users of the system.",
            status_code=409,
            resource=f"/{bucket_name}",
        )

    bucket_create = BucketInCreate(
        name=bucket_name,
        channel_id=CID,
        owner_username=user.username,
    )
    await crud_create_bucket(db, bucket_create, user)
    return Response(status_code=200, headers={"Location": f"/{bucket_name}"})


# ---------------------------------------------------------
# S3 DeleteBucket (DELETE /{bucket_name})
# ---------------------------------------------------------
@router.delete("/{bucket_name}")
async def delete_bucket_handler(
    request: Request,
    bucket_name: str,
    db: AsyncIOMotorClient = Depends(get_database),
):
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

    blobs = await crud_get_all_blobs(db, BlobFilterParams(bucket_name=bucket_name, limit=1))
    if len(blobs) > 0:
        return s3_error_response(
            code="BucketNotEmpty",
            message="The bucket you tried to delete is not empty.",
            status_code=409,
            resource=f"/{bucket_name}",
        )

    await crud_delete_bucket(db, bucket_name)
    return Response(status_code=204)


# ---------------------------------------------------------
# Object PUT: UploadPart, CopyObject, or PutObject
# ---------------------------------------------------------
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

        if MULTIPART_MODE == "assembled":
            # Assembled mode: Save part temporarily to disk
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
        else:
            # Diskless mode: Upload part directly to Telegram storage
            try:
                part_file_id, part_msg_id = await storage.put_file(
                    body, f"{path}.part{part_number}", channel_id=bucket.channel_id
                )
            except Exception as e:
                logger.exception("Storage error uploading part %d for '%s': %s", part_number, path, e)
                return s3_error_response(
                    code="InternalError",
                    message=f"Telegram storage upload failed: {str(e)}",
                    status_code=500,
                    resource=f"/{bucket_name}/{path}",
                )

            part_data = UploadPart(
                part_number=part_number,
                etag=etag,
                size=len(body),
                file_id=part_file_id,
                message_id=part_msg_id,
            )

        await crud_save_part(db, bucket_name, path, upload_id, part_data)
        return Response(status_code=200, headers={"ETag": f'"{etag}"'})

    # 2. Handle S3 CopyObject (x-amz-copy-source)
    copy_source = request.headers.get("x-amz-copy-source") or request.headers.get("X-Amz-Copy-Source")
    if copy_source:
        copy_source = unquote(copy_source).lstrip("/")
        if "/" not in copy_source:
            return s3_error_response(
                code="InvalidArgument",
                message="Invalid x-amz-copy-source format. Expected bucket/key",
                status_code=400,
                resource=f"/{bucket_name}/{path}",
            )
        src_bucket_name, src_key = copy_source.split("/", 1)

        src_blobs = await crud_get_all_blobs(db, BlobFilterParams(path=src_key, bucket_name=src_bucket_name))
        if not src_blobs:
            return s3_error_response(
                code="NoSuchKey",
                message=f"The specified source key '{src_key}' does not exist in bucket '{src_bucket_name}'.",
                status_code=404,
                resource=f"/{bucket_name}/{path}",
            )
        src_blob = src_blobs[0]

        # Clone metadata into target blob
        existing_target = await crud_get_all_blobs(db, BlobFilterParams(path=path, bucket_name=bucket_name))
        update = len(existing_target) > 0

        target_blob = BlobInCreate(
            path=path,
            file=src_blob.file,
            parts=src_blob.parts,
            message_id=src_blob.message_id,
            message_ids=src_blob.message_ids,
            content_type=src_blob.content_type,
            size=src_blob.size,
        )
        await crud_create_blob(db, target_blob, bucket_name, update)

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        etag = src_blob.file or "copied"
        xml_content = build_copy_object_result_xml(etag, now_str)
        return Response(content=xml_content, status_code=200, media_type="application/xml")

    # 3. Standard PutObject
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
    blob.parts = []
    blob.message_ids = []

    try:
        file_id, message_id = await storage.put_file(body, path, channel_id=bucket.channel_id)
    except Exception as e:
        logger.exception("Storage error uploading file '%s': %s", path, e)
        return s3_error_response(
            code="InternalError",
            message=f"Telegram storage upload failed: {str(e)}",
            status_code=500,
            resource=f"/{bucket_name}/{path}",
        )

    blob.file = file_id
    blob.message_id = message_id
    await crud_create_blob(db, blob, bucket_name, update)

    return Response(
        status_code=200,
        headers={"ETag": f'"{file_id}"'},
    )


# ---------------------------------------------------------
# Object GET: ListParts or GetObject (with Range support & Prefetching)
# ---------------------------------------------------------
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
    content_type = blob.content_type or "application/octet-stream"
    range_header = request.headers.get("range") or request.headers.get("Range")
    range_bounds = parse_range_header(range_header, blob.size)

    # Base response headers
    response_headers = {
        "Accept-Ranges": "bytes",
        "ETag": f'"{blob.file}"',
        "Content-Type": content_type,
    }

    # Multipart file with multiple Telegram part documents
    if blob.parts and len(blob.parts) > 0:
        async def multi_parts_prefetched_iterator(part_ids, start_byte=0, max_bytes=None, chunk_size=1024 * 1024):
            # Prefetch pipeline for parts
            bytes_sent = 0
            current_offset = 0

            for i, pid in enumerate(part_ids):
                # Download part
                try:
                    part_file = await storage.get_file(pid)
                except Exception as e:
                    logger.exception("Error downloading part %s: %s", pid, e)
                    return

                part_data = part_file.read()
                part_len = len(part_data)
                part_start = current_offset
                part_end = current_offset + part_len

                current_offset += part_len

                # Check if this part falls in the requested range
                if part_end <= start_byte:
                    continue
                if max_bytes is not None and bytes_sent >= max_bytes:
                    break

                slice_start = max(0, start_byte - part_start)
                slice_data = part_data[slice_start:]

                if max_bytes is not None:
                    remaining = max_bytes - bytes_sent
                    slice_data = slice_data[:remaining]

                for pos in range(0, len(slice_data), chunk_size):
                    chunk = slice_data[pos : pos + chunk_size]
                    bytes_sent += len(chunk)
                    yield chunk

        if range_bounds:
            start, end = range_bounds
            length = end - start + 1
            response_headers["Content-Range"] = f"bytes {start}-{end}/{blob.size}"
            response_headers["Content-Length"] = str(length)
            return StreamingResponse(
                multi_parts_prefetched_iterator(blob.parts, start_byte=start, max_bytes=length),
                status_code=206,
                media_type=content_type,
                headers=response_headers,
            )

        response_headers["Content-Length"] = str(blob.size)
        return StreamingResponse(
            multi_parts_prefetched_iterator(blob.parts),
            status_code=200,
            media_type=content_type,
            headers=response_headers,
        )

    # Single Telegram file
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

    if range_bounds:
        start, end = range_bounds
        length = end - start + 1
        result_file.seek(start)

        async def range_file_iterator(file_obj, bytes_to_read, chunk_size=1024 * 1024):
            remaining = bytes_to_read
            while remaining > 0:
                chunk = file_obj.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

        response_headers["Content-Range"] = f"bytes {start}-{end}/{blob.size}"
        response_headers["Content-Length"] = str(length)
        return StreamingResponse(
            range_file_iterator(result_file, length),
            status_code=206,
            media_type=content_type,
            headers=response_headers,
        )

    async def file_iterator(file_obj, chunk_size=1024 * 1024):
        while chunk := file_obj.read(chunk_size):
            yield chunk

    response_headers["Content-Length"] = str(blob.size)
    return StreamingResponse(
        file_iterator(result_file),
        status_code=200,
        media_type=content_type,
        headers=response_headers,
    )


# ---------------------------------------------------------
# Object HEAD: HeadObject
# ---------------------------------------------------------
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
                "Accept-Ranges": "bytes",
                "Content-Length": str(blob.size),
                "Content-Type": content_type,
                "ETag": f'"{blob.file}"',
            },
        )
    else:
        return Response(status_code=HTTP_404_NOT_FOUND)


# ---------------------------------------------------------
# Object DELETE: AbortMultipartUpload or DeleteObject
# ---------------------------------------------------------
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
        upload = await crud_get_multipart_upload(db, bucket_name, path, upload_id)
        if upload:
            part_msg_ids = [p.message_id for p in upload.parts if p.message_id]
            if part_msg_ids:
                try:
                    await storage.delete_file("", channel_id=bucket.channel_id, message_ids=part_msg_ids)
                except Exception as e:
                    logger.warning("Error deleting multipart part messages for '%s': %s", path, e)

        await crud_delete_multipart_upload(db, bucket_name, path, upload_id)
        return Response(status_code=204)

    # 2. Standard DeleteObject
    filters = BlobFilterParams(path=path, bucket_name=bucket_name)
    blobs = await crud_get_all_blobs(db, filters)

    if len(blobs) > 0:
        blob = blobs[0]
        # Clean up physical Telegram messages
        all_msg_ids = []
        if blob.message_ids:
            all_msg_ids.extend(blob.message_ids)
        elif blob.message_id:
            all_msg_ids.append(blob.message_id)

        try:
            await storage.delete_file(blob.file, channel_id=bucket.channel_id, message_ids=all_msg_ids)
        except Exception as e:
            logger.warning("Storage warning deleting file '%s' (%s): %s", path, blob.file, e)

        await crud_delete_blob(db, path=path, bucket_name=bucket_name)

    return Response(status_code=204)


# ---------------------------------------------------------
# Object POST: CreateMultipartUpload / CompleteMultipartUpload
# ---------------------------------------------------------
@router.post("/{bucket_name}/{path:path}")
async def post_object_operations(
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

        sorted_parts = sorted(upload.parts, key=lambda p: p.part_number)
        if not sorted_parts:
            return s3_error_response(
                code="InvalidPart",
                message="No parts found for this multipart upload.",
                status_code=400,
                resource=f"/{bucket_name}/{path}",
            )

        if MULTIPART_MODE == "assembled":
            # Assembled mode: Merge parts on disk and upload as 1 Telegram document
            temp_dir = tempfile.gettempdir()
            combined_file_path = os.path.join(temp_dir, f"telezon_assembled_{upload_id}.tmp")
            total_size = 0
            with open(combined_file_path, "wb") as outfile:
                for p in sorted_parts:
                    if p.data_path and os.path.exists(p.data_path):
                        with open(p.data_path, "rb") as infile:
                            while chunk := infile.read(8 * 1024 * 1024):
                                outfile.write(chunk)
                                total_size += len(chunk)

            try:
                primary_file_id, primary_msg_id = await storage.put_file(
                    combined_file_path, path, channel_id=bucket.channel_id
                )
            except Exception as e:
                logger.exception("Storage error completing assembled upload for '%s': %s", path, e)
                return s3_error_response(
                    code="InternalError",
                    message=f"Telegram storage upload failed: {str(e)}",
                    status_code=500,
                    resource=f"/{bucket_name}/{path}",
                )
            finally:
                if os.path.exists(combined_file_path):
                    try:
                        os.remove(combined_file_path)
                    except Exception:
                        pass
            part_file_ids = []
            part_message_ids = []
            combined_etag = primary_file_id
        else:
            # Diskless mode: Save parts manifest in MongoDB
            total_size = sum(p.size for p in sorted_parts)
            part_file_ids = [p.file_id for p in sorted_parts]
            part_message_ids = [p.message_id for p in sorted_parts if p.message_id]
            primary_file_id = part_file_ids[0] if part_file_ids else ""
            primary_msg_id = part_message_ids[0] if part_message_ids else None
            combined_etag = f"{hashlib.md5(''.join(p.etag for p in sorted_parts).encode()).hexdigest()}-{len(sorted_parts)}"

        # Update Blob in MongoDB
        filters = BlobFilterParams(path=path, bucket_name=bucket_name)
        existing_blobs = await crud_get_all_blobs(db, filters)
        update = len(existing_blobs) > 0

        blob_in = BlobInCreate(
            path=path,
            file=primary_file_id,
            parts=part_file_ids,
            message_id=primary_msg_id,
            message_ids=part_message_ids,
            content_type=upload.content_type,
            size=total_size,
        )
        await crud_create_blob(db, blob_in, bucket_name, update)

        # Clean up multipart DB record
        await crud_delete_multipart_upload(db, bucket_name, path, upload_id)

        location = f"{request.url.scheme}://{request.url.netloc}/{bucket_name}/{path}"
        xml_content = build_complete_multipart_upload_result_xml(bucket_name, path, location, combined_etag)
        return Response(content=xml_content, status_code=200, media_type="application/xml")

    return s3_error_response(
        code="NotImplemented",
        message="A header or query parameter you provided implies functionality that is not implemented",
        status_code=501,
        resource=f"/{bucket_name}/{path}",
    )


# ---------------------------------------------------------
# Bucket POST: Batch DeleteObjects (?delete)
# ---------------------------------------------------------
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
                    blobs = await crud_get_all_blobs(db, BlobFilterParams(path=key, bucket_name=bucket_name))
                    if blobs:
                        blob = blobs[0]
                        msg_ids = blob.message_ids or ([blob.message_id] if blob.message_id else [])
                        if msg_ids:
                            try:
                                await storage.delete_file(blob.file, channel_id=bucket.channel_id, message_ids=msg_ids)
                            except Exception as e:
                                logger.warning("Error deleting physical messages for %s: %s", key, e)

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




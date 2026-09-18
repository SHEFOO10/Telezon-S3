from datetime import datetime, timedelta, timezone
import re
from typing import List, Optional, Tuple
from xml.sax.saxutils import escape
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import logger
from app.crud.user import crud_get_user_by_access_key_id
from app.models.blob import Blob
from app.models.bucket import Bucket
from app.models.user import UserInDb
from app.s3.awssig import AWSSigV4Verifier, InvalidSignatureError


def s3_error_response(code: str, message: str, status_code: int = 403, resource: str = "") -> Response:
    xml_content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<Error>\n"
        f"  <Code>{escape(code)}</Code>\n"
        f"  <Message>{escape(message)}</Message>\n"
        f"  <Resource>{escape(resource)}</Resource>\n"
        "</Error>"
    )
    return Response(content=xml_content, status_code=status_code, media_type="application/xml")


def extract_access_key_id(request: Request) -> Optional[str]:
    # 1. Authorization header: AWS4-HMAC-SHA256 Credential=AKIA.../2026...
    auth_header = request.headers.get("authorization", "")
    if auth_header:
        cred_match = re.search(r"Credential=([A-Za-z0-9_-]+)/", auth_header)
        if cred_match:
            return cred_match.group(1)
        aws_match = re.search(r"AWS\s+([A-Za-z0-9_-]+):", auth_header)
        if aws_match:
            return aws_match.group(1)

    # 2. Query parameters for presigned URLs
    x_amz_cred = request.query_params.get("X-Amz-Credential") or request.query_params.get("x-amz-credential")
    if x_amz_cred:
        parts = x_amz_cred.split("/")
        if parts:
            return parts[0]

    aws_access_key = request.query_params.get("AWSAccessKeyId") or request.query_params.get("awsaccesskeyid")
    if aws_access_key:
        return aws_access_key

    return None


async def aws_sig_verify_user(user: UserInDb, request: Request) -> Tuple[bool, str]:
    body = await request.body()
    base_headers = dict(**request.headers)

    if "x-amz-date" in base_headers:
        base_headers["X-Amz-Date"] = base_headers["x-amz-date"]

    path = request.url.path

    # Check presigned URL expiration if X-Amz-Expires is provided
    expires_str = request.query_params.get("X-Amz-Expires") or request.query_params.get("x-amz-expires")
    if expires_str:
        try:
            expires_sec = int(expires_str)
            date_str = (
                request.query_params.get("X-Amz-Date")
                or request.query_params.get("x-amz-date")
                or base_headers.get("x-amz-date")
                or base_headers.get("X-Amz-Date")
            )
            if date_str:
                req_time = datetime.strptime(date_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                if now > req_time + timedelta(seconds=expires_sec):
                    err = f"Request has expired. (Expires: {expires_sec}s)"
                    logger.warning(err)
                    return False, err
        except Exception as e:
            logger.warning("Error verifying presigned URL expiration: %s", e)

    key_mapping = {user.access_key_id: user.secret_key}

    candidate_hosts = []
    if "x-forwarded-host" in base_headers:
        candidate_hosts.append(base_headers["x-forwarded-host"])
    if "host" in base_headers and base_headers["host"] not in candidate_hosts:
        candidate_hosts.append(base_headers["host"])
    if not candidate_hosts:
        candidate_hosts.append("")

    last_error = ""
    for host_candidate in candidate_hosts:
        test_headers = dict(base_headers)
        if host_candidate:
            test_headers["host"] = host_candidate

        v = AWSSigV4Verifier(
            request_method=request.method,
            uri_path=path,
            query_string=str(request.query_params),
            headers=test_headers,
            body=body,
            region="us-east-1",
            service="s3",
            key_mapping=key_mapping,
            timestamp_mismatch=None,
        )
        try:
            v.verify()
            return True, ""
        except InvalidSignatureError as e:
            last_error = str(e)
        except Exception as e:
            last_error = str(e)

    logger.warning("AWS SigV4 user verification failed for %s %s: %s", request.method, path, last_error)
    return False, last_error


async def authenticate_request_user(db: AsyncIOMotorClient, request: Request) -> Tuple[Optional[UserInDb], str]:
    access_key_id = extract_access_key_id(request)
    if not access_key_id:
        return None, "Missing AWS credentials in request Authorization header or query parameters"

    user = await crud_get_user_by_access_key_id(db, access_key_id)
    if not user:
        return None, f"Invalid AccessKeyId '{access_key_id}'"

    is_valid, err = await aws_sig_verify_user(user, request)
    if not is_valid:
        return None, err

    return user, ""


async def aws_sig_verify(bucket: Bucket, request: Request) -> Tuple[bool, str]:
    body = await request.body()
    base_headers = dict(**request.headers)
    
    if "x-amz-date" in base_headers:
        base_headers["X-Amz-Date"] = base_headers["x-amz-date"]

    path = request.url.path

    if not bucket.owner or not bucket.owner.access_key_id:
        err = f"Bucket '{bucket.name}' has no owner or access_key_id configured in database"
        logger.error(err)
        return False, err

    # Check presigned URL expiration if X-Amz-Expires is provided
    expires_str = request.query_params.get("X-Amz-Expires") or request.query_params.get("x-amz-expires")
    if expires_str:
        try:
            expires_sec = int(expires_str)
            date_str = (
                request.query_params.get("X-Amz-Date")
                or request.query_params.get("x-amz-date")
                or base_headers.get("x-amz-date")
                or base_headers.get("X-Amz-Date")
            )
            if date_str:
                req_time = datetime.strptime(date_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                if now > req_time + timedelta(seconds=expires_sec):
                    err = f"Request has expired. (Expires: {expires_sec}s)"
                    logger.warning(err)
                    return False, err
        except Exception as e:
            logger.warning("Error verifying presigned URL expiration: %s", e)

    key_mapping = {bucket.owner.access_key_id: bucket.owner.secret_key}

    # Host variants to try (proxy x-forwarded-host vs incoming host header)
    candidate_hosts = []
    if "x-forwarded-host" in base_headers:
        candidate_hosts.append(base_headers["x-forwarded-host"])
    if "host" in base_headers and base_headers["host"] not in candidate_hosts:
        candidate_hosts.append(base_headers["host"])
    if not candidate_hosts:
        candidate_hosts.append("")

    last_error = ""

    for host_candidate in candidate_hosts:
        test_headers = dict(base_headers)
        if host_candidate:
            test_headers["host"] = host_candidate

        v = AWSSigV4Verifier(
            request_method=request.method,
            uri_path=path,
            query_string=str(request.query_params),
            headers=test_headers,
            body=body,
            region="us-east-1",
            service="s3",
            key_mapping=key_mapping,
            timestamp_mismatch=None,
        )
        try:
            v.verify()
            return True, ""
        except InvalidSignatureError as e:
            last_error = str(e)
        except Exception as e:
            last_error = str(e)

    logger.warning(
        "AWS SigV4 verification failed for %s %s: %s (Bucket owner: %s, Key: %s...)",
        request.method,
        path,
        last_error,
        bucket.owner.username if bucket.owner else "unknown",
        bucket.owner.access_key_id[:4] if bucket.owner and bucket.owner.access_key_id else "None",
    )
    return False, last_error


def parse_range_header(range_header: Optional[str], total_size: int) -> Optional[Tuple[int, int]]:
    """
    Parses HTTP Range header, e.g.:
    - 'bytes=0-499' -> (0, 499)
    - 'bytes=500-' -> (500, total_size - 1)
    - 'bytes=-500' -> (total_size - 500, total_size - 1)
    Returns (start, end) inclusive, or None if invalid or not provided.
    """
    if not range_header or total_size <= 0:
        return None

    range_header = range_header.strip()
    if not range_header.startswith("bytes="):
        return None

    spec = range_header[6:].strip()
    # If multiple ranges e.g. bytes=0-50, 100-150, handle the first one
    if "," in spec:
        spec = spec.split(",")[0].strip()

    if "-" not in spec:
        return None

    start_str, end_str = spec.split("-", 1)
    start_str = start_str.strip()
    end_str = end_str.strip()

    try:
        if not start_str and end_str:
            # Suffix range: -500
            suffix_len = int(end_str)
            if suffix_len <= 0:
                return None
            start = max(0, total_size - suffix_len)
            end = total_size - 1
            return start, end

        if start_str and not end_str:
            # Range start: 500-
            start = int(start_str)
            if start >= total_size:
                return None
            end = total_size - 1
            return start, end

        if start_str and end_str:
            start = int(start_str)
            end = int(end_str)
            if start > end or start >= total_size:
                return None
            end = min(end, total_size - 1)
            return start, end
    except ValueError:
        return None

    return None


def build_list_all_my_buckets_result_xml(buckets: List[Bucket], owner_id: str = "telezon", owner_name: str = "telezon") -> str:
    buckets_xml = []
    for b in buckets:
        created = b.created_at.strftime("%Y-%m-%dT%H:%M:%S.000Z") if b.created_at else "2026-01-01T00:00:00.000Z"
        buckets_xml.append(
            f"    <Bucket>\n"
            f"      <Name>{escape(b.name)}</Name>\n"
            f"      <CreationDate>{created}</CreationDate>\n"
            f"    </Bucket>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        "  <Owner>\n"
        f"    <ID>{escape(owner_id)}</ID>\n"
        f"    <DisplayName>{escape(owner_name)}</DisplayName>\n"
        "  </Owner>\n"
        "  <Buckets>\n"
        + "\n".join(buckets_xml) + "\n"
        "  </Buckets>\n"
        "</ListAllMyBucketsResult>"
    )


def build_copy_object_result_xml(etag: str, last_modified: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <LastModified>{last_modified}</LastModified>\n"
        f"  <ETag>&quot;{escape(etag)}&quot;</ETag>\n"
        "</CopyObjectResult>"
    )


def build_list_objects_v2_xml(
    bucket_name: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    blobs: List[Blob],
    common_prefixes: List[str],
    is_truncated: bool = False,
    continuation_token: Optional[str] = None,
    next_continuation_token: Optional[str] = None,
) -> str:
    contents_xml = []
    for blob in blobs:
        last_mod = blob.updated_at.strftime("%Y-%m-%dT%H:%M:%S.000Z") if blob.updated_at else "2026-01-01T00:00:00.000Z"
        contents_xml.append(
            f"  <Contents>\n"
            f"    <Key>{escape(blob.path)}</Key>\n"
            f"    <LastModified>{last_mod}</LastModified>\n"
            f"    <ETag>&quot;{blob.file}&quot;</ETag>\n"
            f"    <Size>{blob.size}</Size>\n"
            f"    <StorageClass>STANDARD</StorageClass>\n"
            f"  </Contents>"
        )

    common_prefixes_xml = []
    for cp in common_prefixes:
        common_prefixes_xml.append(
            f"  <CommonPrefixes>\n"
            f"    <Prefix>{escape(cp)}</Prefix>\n"
            f"  </CommonPrefixes>"
        )

    key_count = len(blobs) + len(common_prefixes)

    xml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"  <Name>{escape(bucket_name)}</Name>",
        f"  <Prefix>{escape(prefix)}</Prefix>",
        f"  <MaxKeys>{max_keys}</MaxKeys>",
        f"  <KeyCount>{key_count}</KeyCount>",
        f"  <IsTruncated>{'true' if is_truncated else 'false'}</IsTruncated>",
    ]
    if delimiter:
        xml_lines.append(f"  <Delimiter>{escape(delimiter)}</Delimiter>")
    if continuation_token:
        xml_lines.append(f"  <ContinuationToken>{escape(continuation_token)}</ContinuationToken>")
    if next_continuation_token:
        xml_lines.append(f"  <NextContinuationToken>{escape(next_continuation_token)}</NextContinuationToken>")

    xml_lines.extend(contents_xml)
    xml_lines.extend(common_prefixes_xml)
    xml_lines.append("</ListBucketResult>")
    return "\n".join(xml_lines)


def build_initiate_multipart_upload_result_xml(bucket_name: str, key: str, upload_id: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Bucket>{escape(bucket_name)}</Bucket>\n"
        f"  <Key>{escape(key)}</Key>\n"
        f"  <UploadId>{escape(upload_id)}</UploadId>\n"
        "</InitiateMultipartUploadResult>"
    )


def build_complete_multipart_upload_result_xml(bucket_name: str, key: str, location: str, etag: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Location>{escape(location)}</Location>\n"
        f"  <Bucket>{escape(bucket_name)}</Bucket>\n"
        f"  <Key>{escape(key)}</Key>\n"
        f"  <ETag>&quot;{escape(etag)}&quot;</ETag>\n"
        "</CompleteMultipartUploadResult>"
    )


def build_list_parts_result_xml(bucket_name: str, key: str, upload_id: str, parts: list) -> str:
    parts_xml = []
    for p in parts:
        last_mod = p.get("updated_at", "2026-01-01T00:00:00.000Z")
        parts_xml.append(
            f"  <Part>\n"
            f"    <PartNumber>{p.get('part_number')}</PartNumber>\n"
            f"    <LastModified>{last_mod}</LastModified>\n"
            f"    <ETag>&quot;{escape(p.get('etag', ''))}&quot;</ETag>\n"
            f"    <Size>{p.get('size', 0)}</Size>\n"
            f"  </Part>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Bucket>{escape(bucket_name)}</Bucket>\n"
        f"  <Key>{escape(key)}</Key>\n"
        f"  <UploadId>{escape(upload_id)}</UploadId>\n"
        f"  <StorageClass>STANDARD</StorageClass>\n"
        + "\n".join(parts_xml) + "\n"
        "</ListPartsResult>"
    )


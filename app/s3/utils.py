from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from xml.sax.saxutils import escape
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import logger
from app.models.blob import Blob
from app.models.bucket import Bucket
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


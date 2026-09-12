from typing import Tuple
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import logger
from app.models.bucket import Bucket
from app.s3.awssig import AWSSigV4Verifier, InvalidSignatureError


def s3_error_response(code: str, message: str, status_code: int = 403, resource: str = "") -> Response:
    xml_content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<Error>\n"
        f"  <Code>{code}</Code>\n"
        f"  <Message>{message}</Message>\n"
        f"  <Resource>{resource}</Resource>\n"
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

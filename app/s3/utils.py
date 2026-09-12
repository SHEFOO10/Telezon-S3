from starlette.requests import Request

from app.core.config import logger
from app.models.bucket import Bucket
from app.s3.awssig import AWSSigV4Verifier, InvalidSignatureError


async def aws_sig_verify(bucket: Bucket, request: Request) -> bool:
    body = await request.body()
    headers = dict(**request.headers)
    
    # SigV4 requires title-cased X-Amz-Date in headers dict for internal lookup
    if "x-amz-date" in headers:
        headers["X-Amz-Date"] = headers["x-amz-date"]

    # Handle reverse proxy forwarded host (Render, Cloudflare, Nginx)
    if "x-forwarded-host" in headers:
        headers["host"] = headers["x-forwarded-host"]

    path = request.url.path

    if not bucket.owner or not bucket.owner.access_key_id:
        logger.error("Bucket %s has no owner or access_key_id configured", bucket.name)
        return False

    key_mapping = {bucket.owner.access_key_id: bucket.owner.secret_key}

    v = AWSSigV4Verifier(
        request_method=request.method,
        uri_path=path,
        query_string=str(request.query_params),
        headers=headers,
        body=body,
        region="us-east-1",
        service="s3",
        key_mapping=key_mapping,
        timestamp_mismatch=None,
    )
    try:
        v.verify()
        return True
    except InvalidSignatureError as e:
        logger.warning(
            "AWS SigV4 verification failed for %s %s: %s (Bucket owner key: %s...)",
            request.method,
            path,
            e,
            bucket.owner.access_key_id[:4] if bucket.owner.access_key_id else "None",
        )
    except Exception as e:
        logger.error("Unable to verify request for %s %s: %s", request.method, path, e)

    return False

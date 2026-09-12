from starlette.requests import Request

from app.core.config import logger
from app.models.bucket import Bucket
from app.s3.awssig import AWSSigV4Verifier, InvalidSignatureError


async def aws_sig_verify(bucket: Bucket, request: Request):
    body = await request.body()
    headers = dict(**request.headers)
    headers["X-Amz-Date"] = headers.get("x-amz-date", "")
    
    # Handle reverse proxy forwarded host (Render, Cloudflare, Nginx)
    if "x-forwarded-host" in headers:
        headers["host"] = headers["x-forwarded-host"]

    # Use exact path component instead of string replacement of base_url
    path = request.url.path

    v = AWSSigV4Verifier(
        request_method=request.method,
        uri_path=path,
        query_string=str(request.query_params),
        headers=headers,
        body=body,
        region="us-east-1",
        service="s3",
        key_mapping={bucket.owner.access_key_id: bucket.owner.secret_key},
        timestamp_mismatch=None,
    )
    try:
        v.verify()
        return True
    except InvalidSignatureError as e:
        logger.warning("Invalid signature for %s %s: %s", request.method, path, e)
    except Exception as e:
        logger.error("Unable to verify request for %s %s: %s", request.method, path, e)
    return False

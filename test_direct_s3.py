import sys
from urllib.parse import urlparse
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import httpx
import typer

app = typer.Typer()


@app.command()
def upload(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    input_path: str = typer.Option(..., prompt=True),
    output_path: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    with open(input_path, "rb") as f:
        data = f.read()

    target_url = f"{url}/{bucket_name}/{output_path}"
    print(f"Target URL: {target_url}")

    # Prepare and sign request with SigV4
    headers = {
        "Host": host,
        "Content-Type": "application/octet-stream",
    }
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(
        method="PUT",
        url=target_url,
        data=data,
        headers=headers,
    )
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    signed_headers = dict(req.headers)
    print("Signed Headers:", {k: v for k, v in signed_headers.items() if k.lower() != "authorization"})

    with httpx.Client(timeout=30.0) as client:
        r = client.put(target_url, content=data, headers=signed_headers)
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Headers: {dict(r.headers)}")
        print(f"Response Body:\n{r.text}")


@app.command()
def download(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    input_path: str = typer.Option(..., prompt=True),
    output_path: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/{bucket_name}/{input_path}"
    print(f"Target URL: {target_url}")

    headers = {
        "Host": host,
    }
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(
        method="GET",
        url=target_url,
        headers=headers,
    )
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.get(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        if r.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(r.content)
        else:
            print(f"Error Response Body:\n{r.text}")


@app.command()
def delete(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    path: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/{bucket_name}/{path}"
    print(f"Target URL: {target_url}")

    headers = {
        "Host": host,
    }
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(
        method="DELETE",
        url=target_url,
        headers=headers,
    )
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.delete(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Headers: {dict(r.headers)}")
        print(f"Response Body:\n{r.text}")


@app.command()
def list_objects(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    prefix: str = typer.Option("", help="Object prefix filter"),
    delimiter: str = typer.Option("", help="Delimiter for grouping directories (e.g. '/')"),
    max_keys: int = typer.Option(1000, help="Max keys to return"),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    params = []
    if prefix:
        params.append(f"prefix={prefix}")
    if delimiter:
        params.append(f"delimiter={delimiter}")
    if max_keys != 1000:
        params.append(f"max-keys={max_keys}")
    params.append("list-type=2")

    query_str = "&".join(params)
    target_url = f"{url}/{bucket_name}?{query_str}"
    print(f"Target URL: {target_url}")

    headers = {"Host": host}
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(
        method="GET",
        url=target_url,
        headers=headers,
    )
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.get(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Body:\n{r.text}")


@app.command()
def presign(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    path: str = typer.Option(..., prompt=True),
    method: str = typer.Option("GET", help="GET or PUT"),
    expires_in: int = typer.Option(3600, help="Expiration in seconds"),
):
    import botocore.session
    session = botocore.session.get_session()
    client = session.create_client(
        "s3",
        region_name="us-east-1",
        endpoint_url=s3_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
    )
    if method.upper() == "GET":
        presigned_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": path},
            ExpiresIn=expires_in,
        )
    else:
        presigned_url = client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket_name, "Key": path},
            ExpiresIn=expires_in,
        )

    print(f"\nGenerated Presigned {method.upper()} URL:")
    print(presigned_url)

    # Test downloading or uploading using the presigned URL with no auth headers
    print(f"\nTesting request without Authorization header...")
    with httpx.Client(timeout=30.0) as http_client:
        if method.upper() == "GET":
            r = http_client.get(presigned_url)
            print(f"Response Status: {r.status_code}")
            print(f"Content Length: {len(r.content)} bytes")
        else:
            r = http_client.put(presigned_url, content=b"Presigned URL upload test content")
            print(f"Response Status: {r.status_code}")
            print(f"Response Body: {r.text}")


@app.command()
def list_buckets(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/"
    headers = {"Host": host}
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(method="GET", url=target_url, headers=headers)
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.get(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Body:\n{r.text}")


@app.command()
def create_bucket(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/{bucket_name}"
    headers = {"Host": host}
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(method="PUT", url=target_url, headers=headers)
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.put(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Headers: {dict(r.headers)}")


@app.command()
def delete_bucket(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/{bucket_name}"
    headers = {"Host": host}
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(method="DELETE", url=target_url, headers=headers)
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.delete(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Body:\n{r.text}")


@app.command()
def copy(
    s3_url: str = typer.Argument(..., help="Render app URL (e.g. https://telezon-s3.onrender.com)"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    source_bucket: str = typer.Option(..., prompt=True),
    source_path: str = typer.Option(..., prompt=True),
    dest_bucket: str = typer.Option(..., prompt=True),
    dest_path: str = typer.Option(..., prompt=True),
):
    url = s3_url.rstrip("/")
    parsed = urlparse(url)
    host = parsed.netloc

    target_url = f"{url}/{dest_bucket}/{dest_path}"
    headers = {
        "Host": host,
        "x-amz-copy-source": f"/{source_bucket}/{source_path}",
    }
    credentials = Credentials(access_key_id, secret_key)
    req = AWSRequest(method="PUT", url=target_url, headers=headers)
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(req)

    with httpx.Client(timeout=30.0) as client:
        r = client.put(target_url, headers=dict(req.headers))
        print(f"\nResponse Status: {r.status_code}")
        print(f"Response Body:\n{r.text}")


if __name__ == "__main__":
    app()



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
            print(f"Downloaded successfully to {output_path} ({len(r.content)} bytes)")
        else:
            print(f"Error Response Body:\n{r.text}")


if __name__ == "__main__":
    app()

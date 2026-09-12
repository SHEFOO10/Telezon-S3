import boto3
from botocore.client import Config
import typer


def main(
    s3_url: str = typer.Argument("http://127.0.0.1:8000"),
    access_key_id: str = typer.Option(..., prompt=True),
    secret_key: str = typer.Option(..., prompt=True),
    bucket_name: str = typer.Option(..., prompt=True),
    input_path: str = typer.Option(..., prompt=True),
    output_path: str = typer.Option(..., prompt=True),
):
    session = boto3.Session()

    def remove_expect_100_continue(request, **kwargs):
        request.headers.pop("Expect", None)

    session.events.register("before-send.s3.*", remove_expect_100_continue)

    s3 = session.client(
        "s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
        endpoint_url=s3_url,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Telezon-Client/1.0",
        ),
    )

    print(f"Downloading {s3_url}/{bucket_name}/{input_path} to {output_path}...")
    response = s3.get_object(
        Bucket=bucket_name,
        Key=input_path,
    )

    with open(output_path, "wb") as f:
        f.write(response["Body"].read())

    print(f"Download successful! Saved to {output_path}")


if __name__ == "__main__":
    typer.run(main)

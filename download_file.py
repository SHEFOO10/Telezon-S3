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
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
        endpoint_url=s3_url,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    s3.download_file(bucket_name, input_path, output_path)


if __name__ == "__main__":
    typer.run(main)

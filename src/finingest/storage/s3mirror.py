from __future__ import annotations

import os
from pathlib import Path

from finingest.storage.lake import s3_key


def mirror_enabled() -> bool:
    return bool(os.environ.get("FININGEST_S3_ENDPOINT") and os.environ.get("FININGEST_S3_BUCKET"))


def mirror_file(local_path: Path, dataset: str, batch_id: int | str) -> str | None:
    """Upload one lake file to the S3-compatible bucket. Local files stay in place for dbt."""
    if not mirror_enabled():
        return None
    import boto3

    bucket = os.environ["FININGEST_S3_BUCKET"]
    key = s3_key(dataset, batch_id)
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["FININGEST_S3_ENDPOINT"],
        aws_access_key_id=os.environ.get("FININGEST_S3_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.environ.get("FININGEST_S3_SECRET_KEY", "minioadmin"),
        region_name=os.environ.get("FININGEST_S3_REGION", "us-east-1"),
    )
    client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"

"""
S3 I/O helpers for flexible output (local path or S3 URI).

Used by scrapers when running in Lambda or with --output s3://...
"""

from pathlib import Path
from typing import Optional

S3_PREFIX = "s3://"


def is_s3_path(path: str) -> bool:
    """Return True if path is an S3 URI (s3://bucket/key)."""
    return path.strip().lower().startswith(S3_PREFIX)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """
    Parse s3://bucket/key into (bucket, key).

    Raises:
        ValueError: If URI format is invalid.
    """
    uri = uri.strip()
    if not uri.lower().startswith(S3_PREFIX):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    rest = uri[len(S3_PREFIX) :]
    if "/" not in rest:
        raise ValueError(f"Invalid S3 URI (missing key): {uri!r}")
    bucket, key = rest.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, key


def output_exists(output_path: str) -> bool:
    """
    Check if output already exists (local file or S3 object).

    Returns True if the path exists and should be skipped (unless override).
    """
    if is_s3_path(output_path):
        try:
            import boto3

            bucket, key = parse_s3_uri(output_path)
            s3 = boto3.client("s3")
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
    return Path(output_path).exists()


def write_output(content: bytes | str, output_path: str) -> None:
    """
    Write content to output_path (local file or S3).

    For S3, content is uploaded. For local, directory is created if needed.
    """
    if isinstance(content, str):
        content = content.encode("utf-8")
    if is_s3_path(output_path):
        import boto3

        bucket, key = parse_s3_uri(output_path)
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=content)
    else:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def build_s3_uri(bucket: str, prefix: str, filename: str) -> str:
    """
    Build S3 URI from bucket, prefix (e.g. 'data' or 'runs/dev-123'), and filename.

    Trailing slashes in prefix are normalized.
    """
    prefix = prefix.rstrip("/")
    key = f"{prefix}/{filename}" if prefix else filename
    return f"{S3_PREFIX}{bucket}/{key}"

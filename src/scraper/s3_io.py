"""
S3 I/O helpers for flexible output (local path or S3 URI).

Used by scrapers when running in Lambda or with --output s3://...
"""

import json
from pathlib import Path
from typing import Optional

S3_PREFIX = "s3://"

VALID_RUN_TYPES = ("prod", "custom", "test")


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


def download_to_file(s3_uri: str, local_path: str | Path) -> Path:
    """
    Download S3 object to a local file.

    Returns:
        Path to the local file.
    """
    import boto3

    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    path = Path(local_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(path))
    return path


def build_run_base(run_type: str, run_name: str | None) -> str:
    """
    Return key prefix like 'prod/2024-01' or 'test'. Validates run_type.
    """
    if run_type not in VALID_RUN_TYPES:
        raise ValueError(f"run_type must be one of {VALID_RUN_TYPES}")
    if run_type in ("prod", "custom") and not run_name:
        raise ValueError("run_name required when run_type is prod or custom")
    if run_type == "test":
        return "test"
    return f"{run_type}/{run_name}"


def build_s3_uri_for_run(
    bucket: str,
    run_type: str,
    run_name: str | None,
    subfolder: str,
    *path_parts: str,
) -> str:
    """Build s3://bucket/key for run. subfolder is data, sample, or reports."""
    base = build_run_base(run_type, run_name)
    key = "/".join([base, subfolder] + list(path_parts))
    return f"{S3_PREFIX}{bucket}/{key}"


def build_local_path_for_run(
    local_root: str | Path,
    run_type: str,
    run_name: str | None,
    subfolder: str,
    *path_parts: str,
) -> Path:
    """
    Build local path for run. local_root = data (bucket equivalent); mirrors S3 key structure.
    """
    base = build_run_base(run_type, run_name)
    parts = [base, subfolder] + list(path_parts)
    return Path(local_root) / "/".join(parts)


def write_run_metadata(
    base_path: str | Path,
    metadata: dict,
    merge: bool = True,
) -> None:
    """
    Write run_metadata.json at run root (not in data/sample/reports).
    base_path: S3 URI (s3://bucket/prod/2024-01) or local path (data/prod/2024-01).
    merge: If True and file exists, merge metadata into existing; else overwrite.
    """
    if is_s3_path(str(base_path)):
        bucket, key_prefix = parse_s3_uri(str(base_path))
        uri = f"{S3_PREFIX}{bucket}/{key_prefix}/run_metadata.json"
        if merge:
            try:
                import boto3

                s3 = boto3.client("s3")
                obj = s3.get_object(
                    Bucket=bucket, Key=f"{key_prefix}/run_metadata.json"
                )
                existing = json.loads(obj["Body"].read().decode("utf-8"))
                metadata = {**existing, **metadata}
            except Exception:
                pass
        write_output(json.dumps(metadata, indent=2), uri)
    else:
        path = Path(base_path) / "run_metadata.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if merge and path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                metadata = {**existing, **metadata}
            except Exception:
                pass
        path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

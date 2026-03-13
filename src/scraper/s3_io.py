"""
S3 I/O helpers for flexible output (local path or S3 URI).

Used by scrapers when running in Lambda or with --output s3://...
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

S3_PREFIX = "s3://"

VALID_RUN_TYPES = ("prod", "custom", "test")

# Shared paths for federations and player list (all run types share these)
FEDERATIONS_DATA_PREFIX = "federations/data"
PLAYER_LISTS_DATA_PREFIX = "player_lists/data"
PLAYER_LISTS_RAW_PREFIX = "player_lists/raw"
PLAYER_LISTS_SAMPLE_PREFIX = "player_lists/sample"
PLAYER_LISTS_REPORTS_PREFIX = "player_lists/reports"
STALE_DAYS = 14  # Only re-fetch if latest is older than this


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
    """Build s3://bucket/key for run. subfolder is data, sample, reports, or raw."""
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


def list_s3_objects(bucket: str, prefix: str) -> list[tuple[str, datetime]]:
    """List objects under prefix, return [(key, last_modified), ...]. Keys are full keys."""
    import boto3

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            lm = obj.get("LastModified")
            if lm:
                results.append((key, lm))
    return results


def get_latest_in_s3_prefix(
    bucket: str, prefix: str
) -> tuple[Optional[str], Optional[datetime]]:
    """
    Get the latest object in prefix by key name (assumes timestamp format in filename).
    Returns (s3_uri, last_modified) or (None, None) if empty.
    """
    objects = list_s3_objects(bucket, prefix)
    if not objects:
        return None, None
    # Sort by key descending (latest timestamp first)
    objects.sort(key=lambda x: x[0], reverse=True)
    key, last_modified = objects[0]
    return f"{S3_PREFIX}{bucket}/{key}", last_modified


def is_stale(last_modified: datetime | float) -> bool:
    """Return True if last_modified is more than STALE_DAYS ago."""
    if isinstance(last_modified, (int, float)):
        dt = datetime.fromtimestamp(last_modified, tz=timezone.utc)
    elif last_modified.tzinfo is None:
        dt = last_modified.replace(tzinfo=timezone.utc)
    else:
        dt = last_modified
    age = datetime.now(timezone.utc) - dt
    return age.days >= STALE_DAYS


def build_federations_data_uri(bucket: str, timestamp: str) -> str:
    """Build s3://bucket/federations/data/federations_{timestamp}.csv."""
    return f"{S3_PREFIX}{bucket}/{FEDERATIONS_DATA_PREFIX}/federations_{timestamp}.csv"


def build_player_lists_data_uri(bucket: str, timestamp: str) -> str:
    """Build s3://bucket/player_lists/data/player_list_{timestamp}.parquet."""
    return f"{S3_PREFIX}{bucket}/{PLAYER_LISTS_DATA_PREFIX}/player_list_{timestamp}.parquet"


def build_player_lists_raw_uri(bucket: str, timestamp: str) -> str:
    """Build s3://bucket/player_lists/raw/player_list_{timestamp}.xml.gz."""
    return (
        f"{S3_PREFIX}{bucket}/{PLAYER_LISTS_RAW_PREFIX}/player_list_{timestamp}.xml.gz"
    )


def resolve_latest_federations_uri(bucket: str) -> Optional[str]:
    """Return URI of latest federations file, or None if none exist."""
    uri, _ = get_latest_in_s3_prefix(bucket, FEDERATIONS_DATA_PREFIX + "/")
    return uri


def resolve_latest_players_list_uri(bucket: str) -> Optional[str]:
    """Return URI of latest players list parquet, or None if none exist."""
    uri, _ = get_latest_in_s3_prefix(bucket, PLAYER_LISTS_DATA_PREFIX + "/")
    return uri


def resolve_latest_federations_local(local_root: str | Path) -> Optional[Path]:
    """Return path of latest federations CSV for local mode, or None."""
    path, _ = get_latest_in_local_prefix(local_root, FEDERATIONS_DATA_PREFIX)
    return path


def resolve_latest_players_list_local(local_root: str | Path) -> Optional[Path]:
    """Return path of latest players list parquet for local mode, or None."""
    path, _ = get_latest_in_local_prefix(local_root, PLAYER_LISTS_DATA_PREFIX)
    return path


def list_local_shared_files(
    local_root: str | Path, prefix: str
) -> list[tuple[Path, float]]:
    """List files under local_root/prefix, return [(path, mtime), ...]."""
    base = Path(local_root) / prefix
    if not base.exists():
        return []
    results = []
    for p in base.iterdir():
        if p.is_file():
            results.append((p, p.stat().st_mtime))
    return results


def get_latest_in_local_prefix(
    local_root: str | Path, prefix: str
) -> tuple[Optional[Path], Optional[float]]:
    """Get latest file by name (timestamp) in prefix. Returns (path, mtime) or (None, None)."""
    files = list_local_shared_files(local_root, prefix)
    if not files:
        return None, None
    files.sort(key=lambda x: x[0].name, reverse=True)
    return files[0][0], files[0][1]


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

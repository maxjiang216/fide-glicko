#!/usr/bin/env python3
"""
Split tournament IDs into even chunks for fan-out processing.

Reads IDs from a file (S3 or local), splits into N chunks, writes each chunk.
Can optionally invoke the tournaments Lambda first to produce the IDs file.
"""

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _is_s3(path: str) -> bool:
    try:
        from s3_io import is_s3_path

        return is_s3_path(path)
    except ImportError:
        return path.strip().lower().startswith("s3://")


def _read_ids(path: str) -> List[str]:
    """Read tournament IDs from file (local or S3)."""
    if _is_s3(path):
        from s3_io import download_to_file

        local = Path(tempfile.gettempdir()) / "tournament_ids_split.txt"
        download_to_file(path, local)
        path = str(local)
    ids = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            tid = line.strip()
            if tid:
                ids.append(tid)
    return ids


def _write_chunk(content: str, path: str) -> None:
    """Write chunk content to path (local or S3)."""
    if _is_s3(path):
        from s3_io import write_output

        write_output(content, path)
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")


def _output_exists(path: str) -> bool:
    """Check if path exists (local or S3)."""
    if _is_s3(path):
        try:
            from s3_io import output_exists

            return output_exists(path)
        except ImportError:
            return False
    return Path(path).exists()


def even_split(items: List[str], n: int) -> List[List[str]]:
    """Split list into n chunks as evenly as possible."""
    if n <= 0:
        raise ValueError("n must be positive")
    if not items:
        return []
    total = len(items)
    base_size = total // n
    remainder = total % n
    chunks = []
    start = 0
    for i in range(n):
        size = base_size + (1 if i < remainder else 0)
        chunks.append(items[start : start + size])
        start += size
    return chunks


def run(
    ids_path: str,
    chunk_count: int,
    bucket: str = "fide-glicko",
    output_prefix: str = "data",
    year: Optional[int] = None,
    month: Optional[int] = None,
    chunk_prefix: str = "chunk",
    output_part_prefix: str = "part",
    override: bool = False,
    quiet: bool = False,
) -> List[dict]:
    """
    Read IDs, split into even chunks, write chunk files.

    Args:
        ids_path: Path to tournament IDs file (one per line). S3 or local.
        chunk_count: Number of chunks for even split.
        bucket: S3 bucket (for building chunk/output paths if using S3).
        output_prefix: S3 prefix (e.g. "data" or "runs/dev-123").
        year: Year for path building (optional; extracted from ids_path if missing).
        month: Month for path building (optional).
        chunk_prefix: Prefix for chunk input files (e.g. "chunk" -> YYYY_MM_chunk_0).
        output_part_prefix: Prefix for details output paths (e.g. "part" -> YYYY_MM_part_0).
        override: If True, overwrite existing chunk files.
        quiet: Reduce log output.

    Returns:
        List of dicts {"input_path": str, "output_path": str} for each chunk.
    """
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    ids = _read_ids(ids_path)
    if not ids:
        logger.error("No tournament IDs found in %s", ids_path)
        return []

    if chunk_count <= 0:
        logger.error("chunk_count must be positive")
        return []

    # Derive year_month from ids_path if not provided (e.g. "2024_01" from ".../2024_01")
    year_month = None
    if year is not None and month is not None:
        year_month = f"{year}_{month:02d}"
    else:
        base = Path(ids_path).name
        # Match YYYY_MM (e.g. 2024_01) in filename
        for part in base.replace(".", "_").split("_"):
            if len(part) == 6 and part[:4].isdigit() and part[4:6].isdigit():
                year_month = part
                break
        if not year_month:
            year_month = "unknown"

    chunks = even_split(ids, chunk_count)
    logger.info(
        "Split %d IDs into %d chunks (sizes %s)",
        len(ids),
        len(chunks),
        [len(c) for c in chunks],
    )

    result = []
    for i, chunk_ids in enumerate(chunks):
        chunk_content = "\n".join(chunk_ids) + "\n"
        if _is_s3(ids_path):
            chunk_input_path = f"s3://{bucket}/{output_prefix}/tournament_ids/{year_month}_{chunk_prefix}_{i}"
            chunk_output_path = f"s3://{bucket}/{output_prefix}/tournament_details/{year_month}_{output_part_prefix}_{i}"
        else:
            base = Path(ids_path)
            chunks_dir = base.parent
            chunk_input_path = str(chunks_dir / f"{year_month}_{chunk_prefix}_{i}")
            # Match S3 layout: .../tournament_details/YYYY_MM_part_N (flat)
            ids_dir_str = str(chunks_dir)
            details_dir = Path(
                ids_dir_str.replace("tournament_ids", "tournament_details")
            )
            chunk_output_path = str(
                details_dir / f"{year_month}_{output_part_prefix}_{i}"
            )

        if not override and _output_exists(chunk_input_path):
            logger.info("Chunk %d already exists, skipping write", i)
        else:
            _write_chunk(chunk_content, chunk_input_path)
            logger.info(
                "Wrote chunk %d (%d IDs) -> %s", i, len(chunk_ids), chunk_input_path
            )

        result.append(
            {
                "input_path": chunk_input_path,
                "output_path": chunk_output_path,
                "tournament_count": len(chunk_ids),
                "chunk_index": i,
            }
        )

    return result


def invoke_tournaments_lambda(
    year: int,
    month: int,
    bucket: str = "fide-glicko",
    output_prefix: str = "data",
    function_name: str = "fide-glicko-tournaments",
    federations_s3_uri: Optional[str] = None,
) -> bool:
    """
    Invoke the tournaments Lambda and wait for completion.

    Returns True on success, False on failure.
    """
    import boto3

    payload = {
        "year": year,
        "month": month,
        "bucket": bucket,
        "output_prefix": output_prefix,
        "override": False,
    }
    if federations_s3_uri:
        payload["federations_s3_uri"] = federations_s3_uri

    client = boto3.client("lambda")
    try:
        resp = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("statusCode") == 200 and result.get("success"):
            return True
        logger.error("Tournaments Lambda failed: %s", result)
        return False
    except Exception as e:
        logger.error("Failed to invoke tournaments Lambda: %s", e)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Split tournament IDs into even chunks for fan-out",
    )
    parser.add_argument(
        "--ids",
        required=True,
        help="Path to tournament IDs file (S3 URI or local). One ID per line.",
    )
    parser.add_argument(
        "--chunk-count",
        "-n",
        type=int,
        default=50,
        help="Number of chunks (default: 50)",
    )
    parser.add_argument(
        "--bucket",
        default="fide-glicko",
        help="S3 bucket for output paths (default: fide-glicko)",
    )
    parser.add_argument(
        "--output-prefix",
        default="data",
        help="S3 prefix for output (default: data)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year for path building (optional; inferred from ids path)",
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Month for path building (optional)",
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Overwrite existing chunk files",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Reduce log output",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print chunks as JSON to stdout",
    )
    args = parser.parse_args()

    chunks = run(
        ids_path=args.ids,
        chunk_count=args.chunk_count,
        bucket=args.bucket,
        output_prefix=args.output_prefix,
        year=args.year,
        month=args.month,
        override=args.override,
        quiet=args.quiet,
    )

    if not chunks:
        return 1

    if args.json:
        print(json.dumps({"chunks": chunks}, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())

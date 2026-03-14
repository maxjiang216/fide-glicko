#!/usr/bin/env python3
"""
Split tournament IDs into even chunks for fan-out processing.

Reads IDs from a file (S3 or local), splits into N chunks, writes each chunk.
Fails fast if the IDs file does not exist.
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
    chunk_count: Optional[int] = None,
    chunk_size: int = 225,
    bucket: str = "fide-glicko",
    output_prefix: str = "data",
    chunk_prefix: str = "chunk",
    output_part_prefix: str = "part",
    override: bool = False,
    quiet: bool = False,
) -> List[dict]:
    """
    Read IDs, split into even chunks, write chunk files.

    Args:
        ids_path: Path to tournament IDs file (one per line). S3 or local.
        chunk_count: Number of chunks (optional). If not set, derived from chunk_size.
        chunk_size: Max tournaments per chunk when chunk_count not set (default: 225).
        bucket: S3 bucket (for building chunk/output paths if using S3).
        output_prefix: S3 prefix when ids_path does not match standard structure.
        chunk_prefix: Prefix for chunk input files (unused with standard structure).
        output_part_prefix: Prefix for details output paths (unused with standard structure).
        override: If True, overwrite existing chunk files.
        quiet: Reduce log output.

    Returns:
        List of dicts {"input_path": str, "output_path": str, "tournament_count": int, "chunk_index": int}.
    """
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    if not _output_exists(ids_path):
        raise RuntimeError(
            f"Tournament IDs file not found: {ids_path}. Run get_tournaments first."
        )

    ids = _read_ids(ids_path)
    if not ids:
        logger.error("No tournament IDs found in %s", ids_path)
        return []

    if chunk_count is not None and chunk_count <= 0:
        logger.error("chunk_count must be positive")
        return []
    if chunk_size <= 0:
        logger.error("chunk_size must be positive")
        return []

    n_chunks = (
        chunk_count
        if chunk_count is not None
        else max(1, (len(ids) + chunk_size - 1) // chunk_size)
    )

    # Derive run_base from ids_path: .../data/tournament_ids.txt -> ... (run root)
    if _is_s3(ids_path):
        # s3://bucket/prod/2024-01/data/tournament_ids.txt -> s3://bucket/prod/2024-01
        if "/data/tournament_ids.txt" in ids_path:
            run_base = ids_path.replace("/data/tournament_ids.txt", "").rstrip("/")
        else:
            run_base = (
                f"s3://{bucket}/{output_prefix}"
                if output_prefix
                else ids_path.rsplit("/", 2)[0]
            )
    else:
        # data/prod/2024-01/data/tournament_ids.txt -> data/prod/2024-01
        ids_p = Path(ids_path)
        if ids_p.name == "tournament_ids.txt" and ids_p.parent.name == "data":
            run_base = str(ids_p.parent.parent)
        else:
            run_base = str(ids_p.parent)

    chunks = even_split(ids, n_chunks)
    logger.info(
        "Split %d IDs into %d chunks (sizes %s)",
        len(ids),
        len(chunks),
        [len(c) for c in chunks],
    )

    result = []
    for i, chunk_ids in enumerate(chunks):
        chunk_content = "\n".join(chunk_ids) + "\n"
        # New structure: tournament_id_chunks/chunk_{i}.txt, tournament_details_chunks/chunk_{i}
        if _is_s3(run_base):
            chunk_input_path = f"{run_base}/data/tournament_id_chunks/chunk_{i}.txt"
            chunk_output_path = f"{run_base}/data/tournament_details_chunks/chunk_{i}"
        else:
            chunk_input_path = str(
                Path(run_base) / "data" / "tournament_id_chunks" / f"chunk_{i}.txt"
            )
            chunk_output_path = str(
                Path(run_base) / "data" / "tournament_details_chunks" / f"chunk_{i}"
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
        default=None,
        help="Number of chunks (overrides --chunk-size if set)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=225,
        help="Max tournaments per chunk when chunk-count not set (default: 225)",
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
        chunk_size=args.chunk_size,
        bucket=args.bucket,
        output_prefix=args.output_prefix,
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

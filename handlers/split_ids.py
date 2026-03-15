"""
Lambda handler for splitting tournament IDs into chunks.

Event shape:
{
    "run_type": "custom",
    "run_name": "2024-01",
    "bucket": "fide-glicko",
    "ids_uri": null,
    "chunk_size": 400,
    "override": false
}

- run_type: prod, custom, or test (default: custom).
- run_name: Required for prod/custom. Ignored for test. Used to locate
  {base}/data/tournament_ids.txt.
- bucket: S3 bucket (default: fide-glicko).
- ids_uri: Path to tournament IDs file. If not set, uses {base}/data/tournament_ids.txt.
  Must exist; Step Function runs tournaments Lambda first.
- chunk_size: Max tournaments per chunk (default: 400).
- override: Overwrite existing chunk files (default: false).

Returns: { statusCode, success, chunks: [{ input_path, output_path, tournament_count, chunk_index }, ...] }
"""

import logging

from .lambda_logging import configure
from s3_io import (
    build_run_base,
    build_s3_uri_for_run,
    output_exists,
    write_run_metadata,
)
from split_tournament_ids import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for splitting tournament IDs into chunks."""
    configure()
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")
    ids_uri = event.get("ids_uri")
    chunk_count = event.get("chunk_count")
    chunk_size = event.get("chunk_size", 400)
    override = event.get("override", False)

    if run_type not in ("prod", "custom", "test"):
        return {
            "statusCode": 400,
            "success": False,
            "error": f"run_type must be one of prod, custom, test (got {run_type!r})",
        }
    if run_type in ("prod", "custom") and not run_name:
        return {
            "statusCode": 400,
            "success": False,
            "error": "run_name required when run_type is prod or custom",
        }

    if ids_uri is None:
        ids_uri = build_s3_uri_for_run(
            bucket, run_type, run_name, "data", "tournament_ids.txt"
        )

    if not output_exists(ids_uri):
        logger.error(
            "Tournament IDs file not found: %s (run tournaments Lambda first)", ids_uri
        )
        return {
            "statusCode": 404,
            "success": False,
            "error": "Tournament IDs file not found; run tournaments Lambda first",
            "ids_uri": ids_uri,
        }

    logger.info(
        "Splitting IDs from %s (chunk_size=%s, chunk_count=%s) (bucket=%s run_type=%s run_name=%s)",
        ids_uri,
        chunk_size,
        chunk_count,
        bucket,
        run_type,
        run_name,
    )

    chunks = run(
        ids_path=ids_uri,
        chunk_count=chunk_count,
        chunk_size=chunk_size,
        bucket=bucket,
        output_prefix=build_run_base(run_type, run_name),
        override=override,
        quiet=False,
    )

    base_key = build_run_base(run_type, run_name)
    base_uri = f"s3://{bucket}/{base_key}"
    write_run_metadata(
        base_uri,
        {
            "step": "split_ids",
            "chunk_count": len(chunks),
            "chunk_size": chunk_size,
            "run_type": run_type,
            "run_name": run_name or "",
        },
        merge=True,
    )

    if not chunks:
        return {
            "statusCode": 500,
            "success": False,
            "error": "Split produced no chunks",
            "ids_uri": ids_uri,
        }

    # Include run_type/run_name in each chunk so Map ItemSelector can use them
    # (Execution.Input may omit run_name when defaulted by AddDefaultRunName)
    for ch in chunks:
        ch["run_type"] = run_type
        ch["run_name"] = run_name or ""

    logger.info("Produced %d chunks for details Lambda fan-out", len(chunks))
    return {
        "statusCode": 200,
        "success": True,
        "chunk_count": len(chunks),
        "chunks": chunks,
    }

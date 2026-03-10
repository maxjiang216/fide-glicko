"""
Lambda handler for splitting tournament IDs into chunks.

Event shape:
{
    "year": 2024,
    "month": 1,
    "bucket": "fide-glicko",
    "output_prefix": "data",
    "ids_uri": "s3://fide-glicko/data/tournament_ids/2024_01",
    "invoke_tournaments": false,
    "tournaments_function_name": "fide-glicko-tournaments",
    "chunk_count": 50,
    "override": false
}

- year, month: Required. Used for path building and optionally invoking tournaments Lambda.
- bucket: S3 bucket (default: fide-glicko).
- output_prefix: S3 prefix (default: data).
- ids_uri: Path to tournament IDs file. If not set, uses s3://{bucket}/{prefix}/tournament_ids/{year}_{month}.
- invoke_tournaments: If true, invokes tournaments Lambda first to produce ids_uri (if not set or file missing).
- tournaments_function_name: Lambda to invoke when invoke_tournaments=true (default: fide-glicko-tournaments).
- chunk_count: Number of even chunks (default: 50).
- override: Overwrite existing chunk files (default: false).

Returns: { statusCode, success, chunks: [{ input_path, output_path, tournament_count, chunk_index }, ...] }
"""

import json
import logging

from s3_io import build_s3_uri, output_exists
from split_tournament_ids import run, invoke_tournaments_lambda

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for splitting tournament IDs into chunks."""
    year = event.get("year")
    month = event.get("month")
    bucket = event.get("bucket", "fide-glicko")
    output_prefix = event.get("output_prefix", "data")
    ids_uri = event.get("ids_uri")
    invoke_tournaments = event.get("invoke_tournaments", False)
    tournaments_function_name = event.get("tournaments_function_name", "fide-glicko-tournaments")
    chunk_count = event.get("chunk_count", 50)
    override = event.get("override", False)

    if year is None or month is None:
        logger.error("year and month are required")
        return {
            "statusCode": 400,
            "success": False,
            "error": "year and month are required",
        }

    if ids_uri is None:
        ids_uri = build_s3_uri(
            bucket, f"{output_prefix}/tournament_ids", f"{year}_{month:02d}"
        )

    if invoke_tournaments or not output_exists(ids_uri):
        logger.info(
            "Invoking tournaments Lambda for %d-%02d (invoke_tournaments=%s, ids_exist=%s)",
            year, month, invoke_tournaments, output_exists(ids_uri),
        )
        if not invoke_tournaments_lambda(
            year=year,
            month=month,
            bucket=bucket,
            output_prefix=output_prefix,
            function_name=tournaments_function_name,
            federations_s3_uri=event.get("federations_s3_uri"),
        ):
            return {
                "statusCode": 500,
                "success": False,
                "error": "Tournaments Lambda failed",
                "year": year,
                "month": month,
            }

    logger.info(
        "Splitting IDs from %s into %d chunks (bucket=%s prefix=%s)",
        ids_uri, chunk_count, bucket, output_prefix,
    )

    chunks = run(
        ids_path=ids_uri,
        chunk_count=chunk_count,
        bucket=bucket,
        output_prefix=output_prefix,
        year=year,
        month=month,
        override=override,
        quiet=False,
    )

    if not chunks:
        return {
            "statusCode": 500,
            "success": False,
            "error": "Split produced no chunks",
            "ids_uri": ids_uri,
        }

    logger.info("Produced %d chunks for details Lambda fan-out", len(chunks))
    return {
        "statusCode": 200,
        "success": True,
        "year": year,
        "month": month,
        "chunk_count": len(chunks),
        "chunks": chunks,
    }

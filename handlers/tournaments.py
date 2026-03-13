"""
Lambda handler for tournaments scraper.

Event shape:
{
    "year": 2025,
    "month": 3,
    "run_type": "custom",
    "run_name": "2024-01",
    "bucket": "fide-glicko",
    "override": false,
    "federations_s3_uri": "s3://fide-glicko/prod/2024-01/data/federations.csv"
}

- year, month: Required
- run_type: prod, custom, or test (default: custom)
- run_name: Required for prod/custom. Ignored for test.
- bucket: S3 bucket (default: fide-glicko)
- override: If true, overwrite existing output
- federations_s3_uri: Optional. Defaults to {base}/data/federations.csv

Outputs: {base}/data/tournament_ids.txt, {base}/sample/tournament_ids_sample.json,
{base}/raw/tournaments.json.gz (raw API JSON, all federations concatenated, gzip-9)
"""

import logging

from .lambda_logging import configure
from s3_io import (
    build_run_base,
    build_s3_uri_for_run,
    output_exists,
    write_run_metadata,
)
from get_tournaments import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for tournaments scraper."""
    configure()
    year = event.get("year")
    month = event.get("month")
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)
    federations_s3_uri = event.get("federations_s3_uri")

    if year is None or month is None:
        logger.error("year and month are required")
        return {
            "statusCode": 400,
            "success": False,
            "error": "year and month are required",
        }
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

    ids_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_ids.txt"
    )
    json_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "sample", "tournament_ids_sample.json"
    )

    if not override and output_exists(ids_uri):
        return {
            "statusCode": 409,
            "success": False,
            "error": "Output already exists; pass override=true to replace",
            "output_path": ids_uri,
        }

    if federations_s3_uri is None:
        federations_s3_uri = build_s3_uri_for_run(
            bucket, run_type, run_name, "data", "federations.csv"
        )

    logger.info(
        "Starting tournaments scrape: year=%s month=%s bucket=%s run_type=%s run_name=%s override=%s -> %s",
        year,
        month,
        bucket,
        run_type,
        run_name,
        override,
        ids_uri,
    )

    base_key = build_run_base(run_type, run_name)
    base_uri = f"s3://{bucket}/{base_key}"
    write_run_metadata(
        base_uri,
        {
            "step": "tournaments",
            "year": int(year),
            "month": int(month),
            "run_type": run_type,
            "run_name": run_name or "",
        },
        merge=True,
    )

    exit_code = run(
        year=int(year),
        month=int(month),
        federations_s3_uri=federations_s3_uri,
        override=override,
        quiet=False,
        ids_uri=ids_uri,
        json_uri=json_uri,
    )

    if exit_code != 0:
        logger.error("Tournaments scrape failed with exit code %d", exit_code)
        return {
            "statusCode": 500,
            "success": False,
            "output_path": ids_uri,
            "error": "Scrape failed",
        }

    logger.info("Tournaments scrape completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "output_path": ids_uri,
    }

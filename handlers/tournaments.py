"""
Lambda handler for tournaments scraper.

Event shape:
{
    "year": 2025,
    "month": 3,
    "bucket": "fide-glicko",
    "output_prefix": "data",
    "override": false,
    "federations_s3_uri": "s3://fide-glicko/data/federations.csv"
}

- year: Year to scrape (required)
- month: Month to scrape 1-12 (required)
- bucket: S3 bucket name (default: fide-glicko)
- output_prefix: Path prefix under bucket, e.g. "data" or "runs/dev-20250308-abc"
- override: If true, overwrite existing output
- federations_s3_uri: Optional S3 URI for federations.csv. Defaults to s3://{bucket}/data/federations.csv

Outputs: s3://{bucket}/{output_prefix}/tournament_ids/YYYY_MM, tournament_ids_json/YYYY_MM.json

Logs go to CloudWatch Logs (/aws/lambda/<function-name>).
"""

import logging

from s3_io import build_s3_uri
from get_tournaments import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for tournaments scraper."""
    year = event.get("year")
    month = event.get("month")
    bucket = event.get("bucket", "fide-glicko")
    output_prefix = event.get("output_prefix", "data")
    override = event.get("override", False)
    federations_s3_uri = event.get("federations_s3_uri")

    if year is None or month is None:
        logger.error("year and month are required")
        return {
            "statusCode": 400,
            "success": False,
            "error": "year and month are required",
        }

    ids_uri = build_s3_uri(
        bucket, f"{output_prefix}/tournament_ids", f"{year}_{month:02d}"
    )
    logger.info(
        "Starting tournaments scrape: year=%s month=%s bucket=%s prefix=%s override=%s -> %s",
        year,
        month,
        bucket,
        output_prefix,
        override,
        ids_uri,
    )

    exit_code = run(
        year=int(year),
        month=int(month),
        bucket=bucket,
        output_prefix=output_prefix,
        federations_s3_uri=federations_s3_uri,
        override=override,
        quiet=False,
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

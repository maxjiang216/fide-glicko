"""
Lambda handler for federations scraper.

Event shape:
{
    "run_type": "custom",
    "run_name": "2024-01",
    "bucket": "fide-glicko",
    "override": false
}

- run_type: prod, custom, or test (default: custom)
- run_name: Required for prod/custom (e.g. "2024-01"). Ignored for test.
- bucket: S3 bucket (default: fide-glicko)
- override: If true, overwrite existing file

Output: s3://{bucket}/{run_type}/{run_name}/data/federations.csv
"""

import logging

from .lambda_logging import configure
from s3_io import (
    build_run_base,
    build_s3_uri_for_run,
    output_exists,
    write_run_metadata,
)
from get_federations import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for federations scraper."""
    configure()
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")
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

    output_path = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "federations.csv"
    )

    if not override and output_exists(output_path):
        return {
            "statusCode": 409,
            "success": False,
            "error": "Output already exists; pass override=true to replace",
            "output_path": output_path,
        }

    logger.info(
        "Starting federations scrape: bucket=%s run_type=%s run_name=%s override=%s -> %s",
        bucket,
        run_type,
        run_name,
        override,
        output_path,
    )

    base_key = build_run_base(run_type, run_name)
    base_uri = f"s3://{bucket}/{base_key}"
    write_run_metadata(
        base_uri,
        {"step": "federations", "run_type": run_type, "run_name": run_name or ""},
        merge=False,
    )

    exit_code = run(
        output_path=output_path,
        override=override,
        quiet=False,
    )

    if exit_code != 0:
        logger.error("Federations scrape failed with exit code %d", exit_code)
        return {
            "statusCode": 500,
            "success": False,
            "output_path": output_path,
            "error": "Scrape failed",
        }

    logger.info("Federations scrape completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "output_path": output_path,
    }

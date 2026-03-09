"""
Lambda handler for federations scraper.

Event shape:
{
    "bucket": "fide-glicko",
    "output_prefix": "data",
    "override": false
}

- bucket: S3 bucket name (default: fide-glicko)
- output_prefix: Path prefix under bucket, e.g. "data" or "runs/dev-20250308-abc"
- override: If true, overwrite existing file

Output path: s3://{bucket}/{output_prefix}/federations.csv

Logs go to CloudWatch Logs (/aws/lambda/<function-name>) - all logging output
is captured. Use INFO level for verbose output (default).
"""

import logging

from s3_io import build_s3_uri
from get_federations import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for federations scraper."""
    bucket = event.get("bucket", "fide-glicko")
    output_prefix = event.get("output_prefix", "data")
    override = event.get("override", False)

    output_path = build_s3_uri(bucket, output_prefix, "federations.csv")
    logger.info(
        "Starting federations scrape: bucket=%s prefix=%s override=%s -> %s",
        bucket,
        output_prefix,
        override,
        output_path,
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

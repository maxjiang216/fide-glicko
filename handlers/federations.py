"""
Lambda handler for federations scraper.

Event shape:
{
    "bucket": "fide-glicko",
    "override": false
}

- bucket: S3 bucket (default: fide-glicko)
- override: If true, always fetch and write new. Else skip if latest < 2 weeks old,
  and only write if content changed (order-independent comparison).

Output: s3://{bucket}/federations/data/federations_{timestamp}.csv (shared across all run types)
Returns: federations_uri in response body
"""

import logging

from .lambda_logging import configure
from get_federations import run_shared

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for federations scraper."""
    configure()
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)

    logger.info(
        "Starting federations scrape: bucket=%s override=%s",
        bucket,
        override,
    )

    try:
        federations_uri = run_shared(bucket=bucket, override=override, quiet=False)
    except RuntimeError as e:
        logger.error("Federations scrape failed: %s", e)
        return {
            "statusCode": 500,
            "success": False,
            "error": str(e),
        }

    logger.info("Federations scrape completed successfully: %s", federations_uri)
    return {
        "statusCode": 200,
        "success": True,
        "federations_uri": federations_uri,
    }

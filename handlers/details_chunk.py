"""
Lambda handler for tournament details chunk scraper.

Event shape:
{
    "input_path": "s3://bucket/path/to/tournament_ids.txt",
    "output_path": "s3://bucket/path/to/tournament_details/2025_03_part_0"
}

- input_path: Path to tournament IDs file (one ID per line). S3 URI or local.
- output_path: Base output path. Writes .parquet, _sample.json, _report.json, _failures.json.

The orchestrator builds these paths; this handler just runs the scrape.
"""

import logging

from get_tournament_details import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for tournament details chunk scraper."""
    input_path = event.get("input_path")
    output_path = event.get("output_path")

    if not input_path or not output_path:
        logger.error("input_path and output_path are required")
        return {
            "statusCode": 400,
            "success": False,
            "error": "input_path and output_path are required",
        }

    logger.info(
        "Starting tournament details scrape: input=%s output=%s",
        input_path,
        output_path,
    )

    exit_code = run(
        input_path=input_path,
        output_path=output_path,
        rate_limit=0.5,
        max_retries=3,
        checkpoint=0,
        quiet=False,
    )

    if exit_code != 0:
        logger.error("Tournament details scrape failed with exit code %d", exit_code)
        return {
            "statusCode": 500,
            "success": False,
            "input_path": input_path,
            "output_path": output_path,
            "error": "Scrape failed",
        }

    logger.info("Tournament details scrape completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "input_path": input_path,
        "output_path": output_path,
    }

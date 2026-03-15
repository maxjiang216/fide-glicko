"""
Lambda handler for merging tournament details and reports chunks.

Event shape:
{
    "run_type": "prod",
    "run_name": "2024-01",
    "bucket": "fide-glicko",
    "override": false
}

- run_type: prod | custom | test (default: custom)
- run_name: Required for prod/custom. Ignored for test.
- bucket: S3 bucket (default: fide-glicko)
- override: If true, overwrite existing merged files (default: false)

Inputs: {base}/data/tournament_details_chunks/details_chunk_*.parquet,
        {base}/data/tournament_reports_chunks/reports_chunk_*_players.parquet, reports_chunk_*_games.parquet
Outputs: {base}/data/tournament_details.parquet,
         {base}/data/tournament_reports_players.parquet,
         {base}/data/tournament_reports_games.parquet
Returns: details_uri, reports_players_uri, reports_games_uri
"""

import logging

from .lambda_logging import configure
from merge_chunks import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for merge chunks."""
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

    logger.info(
        "Starting merge chunks: bucket=%s run_type=%s run_name=%s override=%s",
        bucket,
        run_type,
        run_name,
        override,
    )

    try:
        result = run(
            bucket=bucket,
            run_type=run_type,
            run_name=run_name,
            override=override,
            quiet=False,
        )
    except RuntimeError as e:
        logger.error("Merge chunks failed: %s", e)
        return {
            "statusCode": 500,
            "success": False,
            "error": str(e),
        }

    logger.info("Merge chunks completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        **result,
    }

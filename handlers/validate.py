"""
Lambda handler for pipeline validation.

Event shape:
{
    "run_type": "prod",
    "run_name": "2024-01",
    "bucket": "fide-glicko"
}

- run_type: prod | custom | test (default: custom)
- run_name: Required for prod/custom. Ignored for test.
- bucket: S3 bucket (default: fide-glicko)
- All paths inferred from run_type and run_name.

Inputs: {base}/data/tournament_details.parquet, {base}/data/tournament_reports_games.parquet,
        latest player_lists/data/player_list_*.parquet
Output: {base}/reports/validation_report.json
Returns: report_uri, has_issues, player_list_vs_reports, details_vs_reports
"""

import logging

from .lambda_logging import configure
from validate_pipeline import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for pipeline validation."""
    configure()
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")

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
        "Starting validation: bucket=%s run_type=%s run_name=%s",
        bucket,
        run_type,
        run_name,
    )

    try:
        result = run(
            bucket=bucket,
            run_type=run_type,
            run_name=run_name,
            quiet=False,
        )
    except RuntimeError as e:
        logger.error("Validation failed: %s", e)
        return {
            "statusCode": 500,
            "success": False,
            "error": str(e),
        }

    logger.info("Validation completed (has_issues=%s)", result["has_issues"])
    return {
        "statusCode": 200,
        "success": True,
        **result,
    }

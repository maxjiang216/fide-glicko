"""
Lambda handler for FIDE player list download.

Event shape:
{
    "bucket": "fide-glicko",
    "override": false,
    "federations_uri": null
}

- bucket: S3 bucket (default: fide-glicko)
- override: If true, always fetch and write new. Else skip if latest < 2 weeks old.
- federations_uri: Optional. For report's fed check. Defaults to latest in federations/data/.

Output: s3://{bucket}/player_lists/data/player_list_{timestamp}.parquet (shared across all run types)
Returns: players_list_uri in response body
"""

import logging

from .lambda_logging import configure
from get_player_list import run_shared

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for player list download."""
    configure()
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)
    federations_uri = event.get("federations_uri")

    logger.info(
        "Starting player list download: bucket=%s override=%s",
        bucket,
        override,
    )

    try:
        players_list_uri = run_shared(
            bucket=bucket,
            override=override,
            quiet=False,
            federations_uri=federations_uri,
        )
    except RuntimeError as e:
        logger.error("Player list download failed: %s", e)
        return {
            "statusCode": 500,
            "success": False,
            "error": str(e),
        }

    logger.info("Player list download completed successfully: %s", players_list_uri)
    return {
        "statusCode": 200,
        "success": True,
        "players_list_uri": players_list_uri,
    }

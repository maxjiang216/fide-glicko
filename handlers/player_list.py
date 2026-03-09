"""
Lambda handler for FIDE player list download.

Event shape:
{
    "bucket": "fide-glicko",
    "output_prefix": "data",
    "override": false,
    "federations_s3_uri": "s3://fide-glicko/data/federations.csv"
}

- bucket: S3 bucket name (default: fide-glicko)
- output_prefix: Path prefix under bucket, e.g. "data" or "runs/dev-20250308-abc"
- override: If true, overwrite existing files
- federations_s3_uri: Optional S3 URI for federations.csv (for report's fed check).
  Defaults to s3://{bucket}/data/federations.csv when output_prefix is "data".

Outputs: players_list.parquet, players_list_sample.json, players_list.xml,
players_list_report.json under s3://{bucket}/{output_prefix}/

Logs go to CloudWatch Logs (/aws/lambda/<function-name>).
"""

import logging

from s3_io import build_s3_uri
from get_player_list import run

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for player list download."""
    bucket = event.get("bucket", "fide-glicko")
    output_prefix = event.get("output_prefix", "data")
    override = event.get("override", False)
    federations_s3_uri = event.get("federations_s3_uri")
    if federations_s3_uri is None and output_prefix == "data":
        federations_s3_uri = build_s3_uri(bucket, "data", "federations.csv")

    parquet_uri = build_s3_uri(bucket, output_prefix, "players_list.parquet")
    logger.info(
        "Starting player list download: bucket=%s prefix=%s override=%s -> %s",
        bucket,
        output_prefix,
        override,
        parquet_uri,
    )

    exit_code = run(
        output_prefix=output_prefix,
        bucket=bucket,
        override=override,
        quiet=False,
        federations_s3_uri=federations_s3_uri,
    )

    if exit_code != 0:
        logger.error("Player list download failed with exit code %d", exit_code)
        return {
            "statusCode": 500,
            "success": False,
            "output_path": parquet_uri,
            "error": "Download failed",
        }

    logger.info("Player list download completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "output_path": parquet_uri,
    }

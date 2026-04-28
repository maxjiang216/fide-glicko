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
- tournaments_max_concurrency: Optional int (default 1). Parallel federation requests to FIDE.
  Increase if Sandbox.Timedout at 900s; lower reduces throttling risk.

Outputs: {base}/data/tournament_ids.txt, {base}/sample/tournament_ids_sample.json,
{base}/raw/tournaments.json.gz (raw API JSON, all federations concatenated, gzip-9)
"""

import json
import logging

import boto3
from botocore.exceptions import ClientError

from .lambda_logging import configure
from s3_io import (
    build_run_base,
    build_s3_uri_for_run,
    output_exists,
    resolve_latest_federations_uri,
    write_run_metadata,
)
from get_tournaments import run

COUNTRY_MONTHS_KEY = "metadata/country_months.json"

logger = logging.getLogger(__name__)


def _load_federation_filter(bucket: str, year: int, month: int) -> frozenset | None:
    """
    Load country-months lookup from S3 and return the set of federation codes
    that have tournament data for the given year/month. Returns None if the
    lookup file doesn't exist or fails to load (falls back to querying all feds).
    """
    year_month = f"{year}-{month:02d}"
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=COUNTRY_MONTHS_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        country_months = data.get("country_months", {})
        codes = frozenset(
            code for code, months in country_months.items() if year_month in months
        )
        logger.info(
            "Country-month lookup loaded: %d/%d federations have data for %s",
            len(codes),
            len(country_months),
            year_month,
        )
        return codes
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.info(
                "No country-months lookup at %s/%s; querying all federations",
                bucket,
                COUNTRY_MONTHS_KEY,
            )
        else:
            logger.warning(
                "Failed to load country-months lookup (%s); querying all federations", e
            )
        return None
    except Exception as e:
        logger.warning(
            "Failed to load country-months lookup: %s; querying all federations", e
        )
        return None


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
            "statusCode": 200,
            "success": True,
            "skipped": True,
            "output_path": ids_uri,
            "message": "Output already exists; left as-is (pass override=true to replace)",
        }

    if federations_s3_uri is None:
        federations_s3_uri = resolve_latest_federations_uri(bucket)
        if not federations_s3_uri:
            return {
                "statusCode": 404,
                "success": False,
                "error": "No federations found; run federations Lambda first",
                "ids_uri": ids_uri,
            }

    tm = event.get("tournaments_max_concurrency")
    max_concurrency = int(tm) if tm is not None else 1
    if max_concurrency < 1:
        max_concurrency = 1

    federation_filter = _load_federation_filter(bucket, int(year), int(month))

    logger.info(
        "Starting tournaments scrape: year=%s month=%s bucket=%s run_type=%s run_name=%s "
        "override=%s tournaments_max_concurrency=%s -> %s",
        year,
        month,
        bucket,
        run_type,
        run_name,
        override,
        max_concurrency,
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
            "federations_uri": federations_s3_uri,
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
        max_concurrency=max_concurrency,
        lambda_context=context,
        federation_filter=federation_filter,
    )

    if exit_code != 0:
        logger.error("Tournaments scrape failed with exit code %d", exit_code)
        raise RuntimeError(
            f"Tournaments scrape failed for {year}-{month:02d}: one or more federations "
            "could not be fetched (fail fast)"
        )

    logger.info("Tournaments scrape completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "output_path": ids_uri,
    }

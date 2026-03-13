"""
Lambda handler for tournament details chunk scraper.

Event shape:
{
    "run_type": "prod",
    "run_name": "2024-01",
    "chunk_index": 0,
    "bucket": "fide-glicko",
    "override": false,
    "save_raw": false
}

- run_type: prod | custom | test (default: custom)
- run_name: Required for prod/custom. Ignored for test.
- chunk_index: Chunk index (0-based). Paths inferred as
  {base}/data/tournament_id_chunks/chunk_{i}.txt and
  {base}/data/tournament_details_chunks/chunk_{i}.
- bucket: S3 bucket (default: fide-glicko)
- override: If true, overwrite existing output (default: false)
- save_raw: If true, save raw HTML to raw/details/chunk_{i}/{id}.html.gz (default: false)
"""

import logging

from .lambda_logging import configure
from s3_io import build_s3_uri_for_run, output_exists
from get_tournament_details import run

logger = logging.getLogger(__name__)


def _derive_sample_and_reports_paths(output_path: str) -> tuple[str | None, str | None]:
    """
    Derive output_sample_path and output_reports_base from output_path.
    output_path should contain /data/ (e.g. .../data/tournament_details_chunks/chunk_0).
    Returns (sample_path, reports_base) or (None, None) if /data/ not present.
    """
    if "/data/" not in output_path:
        return None, None
    sample_base = output_path.replace("/data/", "/sample/", 1)
    reports_base = output_path.replace("/data/", "/reports/", 1)
    output_sample_path = sample_base + "_sample.json"
    return output_sample_path, reports_base


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for tournament details chunk scraper."""
    configure()
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    chunk_index = event.get("chunk_index")
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)
    save_raw = event.get("save_raw", False)

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
    if chunk_index is None:
        return {
            "statusCode": 400,
            "success": False,
            "error": "chunk_index is required",
        }

    input_path = build_s3_uri_for_run(
        bucket,
        run_type,
        run_name,
        "data",
        "tournament_id_chunks",
        f"chunk_{chunk_index}.txt",
    )
    output_path = build_s3_uri_for_run(
        bucket,
        run_type,
        run_name,
        "data",
        "tournament_details_chunks",
        f"chunk_{chunk_index}",
    )

    output_sample_path, output_reports_base = _derive_sample_and_reports_paths(
        output_path
    )

    parquet_uri = output_path + ".parquet"
    if not override and output_exists(parquet_uri):
        return {
            "statusCode": 409,
            "success": False,
            "error": "Output already exists; pass override=true to replace",
            "output_path": parquet_uri,
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
        output_sample_path=output_sample_path,
        output_reports_base=output_reports_base,
        save_raw=save_raw,
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

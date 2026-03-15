"""
Lambda handler for tournament reports chunk scraper.

Event shape:
{
    "run_type": "prod",
    "run_name": "2024-01",
    "chunk_index": 0,
    "bucket": "fide-glicko",
    "override": false,
    "save_raw": true,
    "details_path": null
}

- run_type: prod | custom | test (default: custom)
- run_name: Required for prod/custom. Ignored for test.
- chunk_index: Chunk index (0-based). Paths: ids_chunk_{i}.txt, reports_chunk_{i}_*.parquet
- bucket: S3 bucket (default: fide-glicko)
- override: If true, overwrite existing output (default: false)
- save_raw: If true, save raw HTML to raw/reports/reports_chunk_{i}.html.gz (default: true)
- details_path: Optional S3 URI to details chunk parquet for date inference.

Outputs: parquet, plus reports_chunk_{i}_verbose_sample.json and reports_chunk_{i}_games_sample.csv.
"""

import logging

from .lambda_logging import configure
from s3_io import build_s3_uri_for_run, output_exists
from get_tournament_reports import run

logger = logging.getLogger(__name__)


def _derive_sample_paths(output_path: str) -> tuple[str, str]:
    """Derive sample JSON and CSV paths from output_path (data/ -> sample/)."""
    if "/data/" not in output_path:
        return "", ""
    sample_base = output_path.replace("/data/", "/sample/", 1)
    return sample_base + "_verbose_sample.json", sample_base + "_games_sample.csv"


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for tournament reports chunk scraper."""
    configure()
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    chunk_index = event.get("chunk_index")
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)
    save_raw = event.get("save_raw", True)
    details_path = event.get("details_path")

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
        f"ids_chunk_{chunk_index}.txt",
    )
    output_path = build_s3_uri_for_run(
        bucket,
        run_type,
        run_name,
        "data",
        "tournament_reports_chunks",
        f"reports_chunk_{chunk_index}",
    )

    output_sample_json, output_sample_csv = _derive_sample_paths(output_path)

    games_uri = output_path + "_games.parquet"
    if not override and output_exists(games_uri):
        return {
            "statusCode": 200,
            "success": True,
            "skipped": True,
            "output_path": games_uri,
            "message": "Output already exists; left as-is (pass override=true to replace)",
        }

    if details_path is None:
        details_path = build_s3_uri_for_run(
            bucket,
            run_type,
            run_name,
            "data",
            "tournament_details_chunks",
            f"details_chunk_{chunk_index}.parquet",
        )

    logger.info(
        "Starting tournament reports scrape: input=%s output=%s",
        input_path,
        output_path,
    )

    exit_code = run(
        input_path=input_path,
        output_path=output_path,
        details_path=details_path,
        rate_limit=0,
        quiet=False,
        save_raw=save_raw,
        output_sample_json=output_sample_json,
        output_sample_csv=output_sample_csv,
    )

    if exit_code != 0:
        logger.error("Tournament reports scrape failed with exit code %d", exit_code)
        return {
            "statusCode": 500,
            "success": False,
            "input_path": input_path,
            "output_path": output_path,
            "error": "Scrape failed",
        }

    logger.info("Tournament reports scrape completed successfully")
    return {
        "statusCode": 200,
        "success": True,
        "input_path": input_path,
        "output_path": output_path,
    }

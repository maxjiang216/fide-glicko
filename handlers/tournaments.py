"""
Lambda handler for the tournaments step: fetch the month's tournament list,
one federation at a time, resumably (see src/scraper/tournament_list.py).

Event shape:
{
    "year": 2025,
    "month": 3,
    "run_type": "prod",
    "run_name": "2025-03",
    "bucket": "fide-glicko",
    "override": false,
    "federations_s3_uri": null,
    "tournaments_rate_limit": 0.3
}

- override: refetch even if tournament_ids.txt exists.
- federations_s3_uri: defaults to the latest federations list in S3.

Returns {"remaining": N, "progress": K, "finalized": bool, ...}. The Step
Function calls it again while remaining > 0 and progress > 0. Raises
FideBlocked on a block (saved federations are kept) and ValueError on a bad
event. tournament_ids.txt is only ever written complete.
"""

import json
import logging
import tempfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .chunk_common import (
    deadline_from_context,
    hard_deadline_from_context,
    rate_limit_req_s,
)
from .lambda_logging import configure
from s3_io import (
    build_run_base,
    build_s3_uri_for_run,
    download_to_file,
    list_s3_objects,
    output_exists,
    parse_s3_uri,
    resolve_latest_federations_uri,
    write_output,
    write_run_metadata,
)

COUNTRY_MONTHS_KEY = "metadata/country_months.json"

logger = logging.getLogger(__name__)


def _load_federation_filter(bucket: str, year: int, month: int) -> frozenset | None:
    """
    Federation codes with tournament data for year/month, from the
    country-months lookup in S3. None (query all federations) if the lookup is
    missing, unreadable or has no entry for the month.
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
        if not codes:
            logger.warning(
                "Country-month lookup has no federations for %s; querying all",
                year_month,
            )
            return None
        logger.info(
            "Country-month lookup: %d/%d federations have data for %s",
            len(codes),
            len(country_months),
            year_month,
        )
        return codes
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.info("No country-months lookup; querying all federations")
        else:
            logger.warning("Country-months lookup failed (%s); querying all", e)
        return None
    except Exception as e:
        logger.warning("Country-months lookup failed: %s; querying all", e)
        return None


def _s3_io(bucket: str, base: str):
    from tournament_list import PARTS_DIR

    s3 = boto3.client("s3")
    parts_prefix = f"{base}/{PARTS_DIR}/"

    def list_parts() -> set[str]:
        return {
            k.rsplit("/", 1)[-1][: -len(".json")]
            for k, _ in list_s3_objects(bucket, parts_prefix)
            if k.endswith(".json")
        }

    def read_text(uri: str) -> str:
        b, k = parse_s3_uri(uri)
        return s3.get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8")

    def delete_parts(codes) -> None:
        # Delete every part under the prefix, not just this federation set:
        # leftovers from an interrupted refresh with a different set would
        # otherwise make every later run look like a refresh in progress.
        keys = [{"Key": k} for k, _ in list_s3_objects(bucket, parts_prefix)]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})

    return {
        "list_parts": list_parts,
        "read_text": read_text,
        "write": lambda path, content: write_output(content, path),
        "exists": output_exists,
        "delete_parts": delete_parts,
    }


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for the tournaments step."""
    from chunk_worker import make_session
    from fide_http import AdaptiveRateLimiter, FideBlocked, PoliteFetcher
    from get_tournaments import read_federations
    from metrics import chunk_summary, emit
    from tournament_list import run_tournament_list

    configure()
    year = event.get("year")
    month = event.get("month")
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")
    override = event.get("override", False)

    if year is None or month is None:
        raise ValueError("year and month are required")
    if run_type not in ("prod", "custom", "test"):
        raise ValueError(
            f"run_type must be one of prod, custom, test (got {run_type!r})"
        )
    if run_type in ("prod", "custom") and not run_name:
        raise ValueError("run_name required when run_type is prod or custom")
    year, month = int(year), int(month)

    ids_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_ids.txt"
    )
    base = build_run_base(run_type, run_name)
    month_root = f"s3://{bucket}/{base}"
    io = _s3_io(bucket, base)

    # A list already exists and no refresh asked for (and no refresh in
    # progress, i.e. no saved parts): nothing to do.
    if not override and output_exists(ids_uri) and not io["list_parts"]():
        return {
            "statusCode": 200,
            "success": True,
            "skipped": True,
            "remaining": 0,
            "progress": 0,
            "output_path": ids_uri,
        }

    federations_s3_uri = event.get(
        "federations_s3_uri"
    ) or resolve_latest_federations_uri(bucket)
    if not federations_s3_uri:
        raise RuntimeError("No federations found; run federations Lambda first")
    fed_path = Path(tempfile.gettempdir()) / "federations.csv"
    download_to_file(federations_s3_uri, fed_path)
    federations = [code for code, _ in read_federations(fed_path)]
    fed_filter = _load_federation_filter(bucket, year, month)
    if fed_filter is not None:
        federations = [c for c in federations if c in fed_filter]

    write_run_metadata(
        month_root,
        {
            "step": "tournaments",
            "year": year,
            "month": month,
            "run_type": run_type,
            "run_name": run_name or "",
            "federations_uri": federations_s3_uri,
        },
        merge=True,
    )

    rate = rate_limit_req_s(event, "tournaments_rate_limit", 0.3)
    fetcher = PoliteFetcher(
        AdaptiveRateLimiter(rate), hard_deadline=hard_deadline_from_context(context)
    )
    logger.info(
        "Tournament list %d-%02d: %d federations at %.2f req/s",
        year,
        month,
        len(federations),
        rate,
    )

    def summarize(result, blocked, error=None):
        emit(
            chunk_summary(
                kind="tournaments",
                run_name=run_name,
                chunk=f"{year}-{month:02d}",
                fetch_stats=fetcher.stats(),
                chunk_result=result,
                blocked=blocked,
                error=error,
            )
        )

    try:
        result = run_tournament_list(
            month_root=month_root,
            year=year,
            month=month,
            federations=federations,
            fetcher=fetcher,
            session=make_session(),
            deadline=deadline_from_context(context),
            expect_data=fed_filter or frozenset(),
            **io,
        )
    except FideBlocked as e:
        summarize(None, True, str(e))
        raise
    except Exception as e:
        summarize(None, False, f"{type(e).__name__}: {e}")
        raise
    summarize(result, False)
    return {
        "statusCode": 200,
        "success": True,
        "output_path": ids_uri,
        "rate_limit": rate,
        **result,
    }

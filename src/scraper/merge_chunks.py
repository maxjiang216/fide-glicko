"""
Merge tournament details and reports chunk parquets into single files.

Reads every chunk under {base}/data/tournament_details_chunks/ and
{base}/data/tournament_reports_chunks/, from all chunk sets ever written for the
month, keeps the newest rows per tournament, and writes to {base}/data/:
  - tournament_details.parquet
  - tournament_reports_players.parquet
  - tournament_reports_games.parquet

The merge refuses to write anything unless every tournament in
tournament_ids.txt has details and either reports or a page-structure skip
(see coverage.py). A partial month raises IncompleteRunError instead of being
merged and marked done.
"""

import io
import logging

from coverage import IncompleteRunError, load_month  # noqa: F401 (re-export)

logger = logging.getLogger(__name__)


def run(
    bucket: str,
    run_type: str,
    run_name: str | None,
    override: bool = False,
    quiet: bool = False,
    allow_incomplete: bool = False,
) -> dict:
    """
    Merge details and reports chunks into single parquet files in {base}/data/.

    Args:
        bucket: S3 bucket.
        run_type: prod | custom | test.
        run_name: Required for prod/custom. Ignored for test.
        override: Ignored. Merged files are always rewritten from the chunks,
            since earlier merges may have been partial. Kept for event compat.
        quiet: Reduce log output.
        allow_incomplete: Merge even if tournaments are missing. Only for
            offline inspection; the pipeline never sets it.

    Returns:
        Dict with merged URIs, row counts, file counts and coverage summary.

    Raises:
        IncompleteRunError: some tournaments lack details or reports.
    """
    from s3_io import (
        build_run_base,
        build_s3_uri_for_run,
        parse_s3_uri,
        write_run_metadata,
    )

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = build_run_base(run_type, run_name)
    details_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_details.parquet"
    )
    players_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_reports_players.parquet"
    )
    games_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_reports_games.parquet"
    )

    month = load_month(bucket, base)
    cov = month.coverage()
    logger.info("Coverage for %s: %s", base, cov.summary(sample=5))
    if not allow_incomplete:
        cov.raise_if_incomplete(f"Refusing to merge s3://{bucket}/{base}")

    result = {
        "details_uri": details_uri,
        "reports_players_uri": players_uri,
        "reports_games_uri": games_uri,
        "files": month.files,
        "rows": {
            "details": month.details.num_rows,
            "players": month.players.num_rows,
            "games": month.games.num_rows,
        },
        "coverage": cov.summary(),
    }

    import boto3
    import pyarrow.parquet as pq

    s3 = boto3.client("s3")

    def _write_parquet(table, uri: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        b, k = parse_s3_uri(uri)
        s3.put_object(Bucket=b, Key=k, Body=buf.getvalue())
        logger.info("Wrote %d rows -> %s", table.num_rows, uri)

    _write_parquet(month.details, details_uri)
    _write_parquet(month.players, players_uri)
    _write_parquet(month.games, games_uri)

    write_run_metadata(
        f"s3://{bucket}/{base}",
        {"step": "merge_chunks", "files": month.files, "coverage": cov.summary(0)},
        merge=True,
    )
    return result

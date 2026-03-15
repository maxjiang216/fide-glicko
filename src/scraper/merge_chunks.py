"""
Merge tournament details and reports chunk parquets into single files.

Reads from {base}/data/tournament_details_chunks/details_chunk_*.parquet and
{base}/data/tournament_reports_chunks/reports_chunk_*_players.parquet, reports_chunk_*_games.parquet,
concatenates, and writes to {base}/data/:
  - tournament_details.parquet
  - tournament_reports_players.parquet
  - tournament_reports_games.parquet
"""

import io
import logging
import re

logger = logging.getLogger(__name__)

# Chunk filenames: details_chunk_0.parquet, reports_chunk_0_players.parquet, ...
DETAILS_CHUNK_RE = re.compile(r"details_chunk_(\d+)\.parquet$")
REPORTS_PLAYERS_RE = re.compile(r"reports_chunk_(\d+)_players\.parquet$")
REPORTS_GAMES_RE = re.compile(r"reports_chunk_(\d+)_games\.parquet$")


def _parse_chunk_index(key: str, pattern: re.Pattern) -> int | None:
    """Extract chunk index from key like 'prod/2024-01/data/tournament_details_chunks/details_chunk_3.parquet'."""
    parts = key.split("/")
    if not parts:
        return None
    name = parts[-1]
    m = pattern.match(name)
    return int(m.group(1)) if m else None


def _sorted_chunk_keys(keys: list[str], pattern: re.Pattern) -> list[str]:
    """Filter keys matching pattern, sort by chunk index ascending."""
    indexed = []
    for k in keys:
        idx = _parse_chunk_index(k, pattern)
        if idx is not None:
            indexed.append((idx, k))
    indexed.sort(key=lambda x: x[0])
    return [k for _, k in indexed]


def run(
    bucket: str,
    run_type: str,
    run_name: str | None,
    override: bool = False,
    quiet: bool = False,
) -> dict:
    """
    Merge details and reports chunks into single parquet files in {base}/data/.

    Args:
        bucket: S3 bucket.
        run_type: prod | custom | test.
        run_name: Required for prod/custom. Ignored for test.
        override: If True, overwrite existing merged files.
        quiet: Reduce log output.

    Returns:
        Dict with details_uri, reports_players_uri, reports_games_uri, details_chunks, reports_chunks.
    """
    from s3_io import (
        build_run_base,
        build_s3_uri_for_run,
        list_s3_objects,
        output_exists,
        parse_s3_uri,
        write_run_metadata,
    )

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = build_run_base(run_type, run_name)
    details_prefix = f"{base}/data/tournament_details_chunks/"
    reports_prefix = f"{base}/data/tournament_reports_chunks/"

    # List chunk files
    details_objs = list_s3_objects(bucket, details_prefix)
    reports_objs = list_s3_objects(bucket, reports_prefix)

    details_keys = _sorted_chunk_keys([k for k, _ in details_objs], DETAILS_CHUNK_RE)
    players_keys = _sorted_chunk_keys([k for k, _ in reports_objs], REPORTS_PLAYERS_RE)
    games_keys = _sorted_chunk_keys([k for k, _ in reports_objs], REPORTS_GAMES_RE)

    if not details_keys:
        raise RuntimeError(
            f"No details chunks found under s3://{bucket}/{details_prefix}"
        )
    if not players_keys or not games_keys:
        raise RuntimeError(
            f"No reports chunks found under s3://{bucket}/{reports_prefix} "
            "(need reports_chunk_*_players.parquet and reports_chunk_*_games.parquet)"
        )

    details_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_details.parquet"
    )
    players_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_reports_players.parquet"
    )
    games_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_reports_games.parquet"
    )

    if not override:
        if (
            output_exists(details_uri)
            and output_exists(players_uri)
            and output_exists(games_uri)
        ):
            logger.info(
                "Merged files already exist (override=false), skipping: %s",
                details_uri,
            )
            return {
                "details_uri": details_uri,
                "reports_players_uri": players_uri,
                "reports_games_uri": games_uri,
                "details_chunks": len(details_keys),
                "reports_chunks": len(players_keys),
            }

    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = boto3.client("s3")

    def _read_parquet(key: str) -> pa.Table:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pq.read_table(io.BytesIO(obj["Body"].read()))

    def _write_parquet(table: pa.Table, uri: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        b, k = parse_s3_uri(uri)
        s3.put_object(Bucket=b, Key=k, Body=buf.getvalue())

    # Merge details
    logger.info("Merging %d details chunks -> %s", len(details_keys), details_uri)
    details_tables = [_read_parquet(k) for k in details_keys]
    details_merged = pa.concat_tables(details_tables)
    _write_parquet(details_merged, details_uri)
    del details_tables, details_merged

    # Merge reports players
    logger.info(
        "Merging %d reports players chunks -> %s", len(players_keys), players_uri
    )
    players_tables = [_read_parquet(k) for k in players_keys]
    players_merged = pa.concat_tables(players_tables)
    _write_parquet(players_merged, players_uri)
    del players_tables, players_merged

    # Merge reports games
    logger.info("Merging %d reports games chunks -> %s", len(games_keys), games_uri)
    games_tables = [_read_parquet(k) for k in games_keys]
    games_merged = pa.concat_tables(games_tables)
    _write_parquet(games_merged, games_uri)

    base_uri = f"s3://{bucket}/{base}"
    write_run_metadata(
        base_uri,
        {
            "step": "merge_chunks",
            "details_chunks": len(details_keys),
            "reports_chunks": len(players_keys),
        },
        merge=True,
    )

    logger.info(
        "Merge completed: details=%s, reports_players=%s, reports_games=%s",
        details_uri,
        players_uri,
        games_uri,
    )

    return {
        "details_uri": details_uri,
        "reports_players_uri": players_uri,
        "reports_games_uri": games_uri,
        "details_chunks": len(details_keys),
        "reports_chunks": len(players_keys),
    }

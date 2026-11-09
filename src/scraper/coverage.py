"""
Month completeness: which tournaments in tournament_ids.txt have details and
reports stored, and merging every stored chunk into one deduplicated table.

A tournament counts as done for details if a details row with success=True
exists. It counts as done for reports if it has player rows, or if a
*_skipped.json records it with a page-structure reason (SKIPPABLE_ERRORS).
Network errors never count as done.

Chunk files from every chunk set are considered (details_chunk_3_of_15,
details_chunk_3_of_22, legacy details_chunk_3, ...), because earlier runs wrote
the same month under different chunk counts. When a tournament appears in more
than one file, rows from the newest file win.
"""

from __future__ import annotations

import io
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

# Reasons a tournament legitimately has no report (FIDE page structure, not
# network). Must match get_tournament_reports.SKIPPABLE_ERRORS.
SKIPPABLE_ERRORS = frozenset(
    ["report_updated_or_replaced", "no data found", "no players found"]
)

DETAILS_CHUNK_RE = re.compile(r"details_chunk_\d+(_of_\d+)?\.parquet$")
PLAYERS_CHUNK_RE = re.compile(r"reports_chunk_\d+(_of_\d+)?_players\.parquet$")
GAMES_CHUNK_RE = re.compile(r"reports_chunk_\d+(_of_\d+)?_games\.parquet$")
SKIPPED_RE = re.compile(r"reports_chunk_\d+(_of_\d+)?_skipped\.json$")


class IncompleteRunError(RuntimeError):
    """A month is missing details or reports for some tournaments."""


@dataclass
class Coverage:
    expected: int
    missing_details: list[str] = field(default_factory=list)
    missing_reports: list[str] = field(default_factory=list)
    skipped_reports: int = 0

    @property
    def complete(self) -> bool:
        return not self.missing_details and not self.missing_reports

    def summary(self, sample: int = 20) -> dict:
        return {
            "expected": self.expected,
            "missing_details": len(self.missing_details),
            "missing_reports": len(self.missing_reports),
            "skipped_reports": self.skipped_reports,
            "missing_details_sample": self.missing_details[:sample],
            "missing_reports_sample": self.missing_reports[:sample],
        }

    def raise_if_incomplete(self, what: str) -> None:
        if not self.complete:
            raise IncompleteRunError(
                f"{what}: {len(self.missing_details)} of {self.expected} tournaments "
                f"missing details, {len(self.missing_reports)} missing reports "
                f"(e.g. details {self.missing_details[:5]}, "
                f"reports {self.missing_reports[:5]})"
            )


def parse_ids(text: str) -> list[str]:
    """Tournament IDs from tournament_ids.txt contents, order kept, duplicates dropped."""
    seen: dict[str, None] = {}
    for line in text.splitlines():
        tid = line.strip()
        if tid:
            seen.setdefault(tid, None)
    return list(seen)


def tournament_ids(table: pa.Table, col: str = "tournament_id") -> set[str]:
    if col not in table.column_names or table.num_rows == 0:
        return set()
    return {str(v) for v in table.column(col).to_pylist() if v is not None}


def successful_details(table: pa.Table) -> pa.Table:
    """Drop details rows that record a failed fetch."""
    if "success" not in table.column_names or table.num_rows == 0:
        return table
    mask = pc.fill_null(pc.equal(table.column("success"), True), False)
    return table.filter(mask)


def skippable_ids(skipped_entries: Iterable[dict]) -> set[str]:
    return {
        str(e.get("tournament_code"))
        for e in skipped_entries
        if e.get("error") in SKIPPABLE_ERRORS and e.get("tournament_code") is not None
    }


def compute_coverage(
    expected_ids: list[str],
    details_done: set[str],
    reports_done: set[str],
    skipped_ok: set[str],
) -> Coverage:
    return Coverage(
        expected=len(expected_ids),
        missing_details=[t for t in expected_ids if t not in details_done],
        missing_reports=[
            t for t in expected_ids if t not in reports_done and t not in skipped_ok
        ],
        skipped_reports=sum(1 for t in expected_ids if t in skipped_ok),
    )


def concat_unified(tables: list[pa.Table]) -> pa.Table:
    """Concat tables, unifying schemas (e.g. n_players int64 vs double)."""
    tables = [t for t in tables if t.num_columns > 0]
    if not tables:
        return pa.table({})
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except pa.ArrowInvalid:
        import pandas as pd

        dfs = [t.to_pandas() for t in tables]
        return pa.Table.from_pandas(
            pd.concat(dfs, ignore_index=True), preserve_index=False
        )


def merge_newest_first(
    tables_newest_first: list[pa.Table], col: str = "tournament_id"
) -> pa.Table:
    """
    Concatenate tables so each tournament's rows come from exactly one table:
    the newest one that contains that tournament.
    """
    claimed: set[str] = set()
    kept: list[pa.Table] = []
    for t in tables_newest_first:
        if t.num_rows == 0 or col not in t.column_names:
            continue
        ids = pc.cast(t.column(col), pa.string())
        if claimed:
            mask = pc.invert(
                pc.fill_null(
                    pc.is_in(ids, value_set=pa.array(sorted(claimed), pa.string())),
                    False,
                )
            )
            t = t.filter(mask)
            ids = pc.cast(t.column(col), pa.string())
        if t.num_rows == 0:
            continue
        claimed.update(v for v in ids.to_pylist() if v is not None)
        kept.append(t)
    return concat_unified(kept)


@dataclass
class MonthData:
    """Every stored chunk for one month, merged and deduplicated."""

    expected_ids: list[str]
    details: pa.Table
    players: pa.Table
    games: pa.Table
    skipped: list[dict]
    files: dict[str, int]

    def coverage(self) -> Coverage:
        reports_done = tournament_ids(self.players) | tournament_ids(self.games)
        return compute_coverage(
            self.expected_ids,
            tournament_ids(self.details),
            reports_done,
            skippable_ids(self.skipped),
        )


def _newest_first(objs: list[tuple[str, object]], pattern: re.Pattern) -> list[str]:
    matching = [(k, lm) for k, lm in objs if pattern.search(k.rsplit("/", 1)[-1])]
    matching.sort(key=lambda x: x[1], reverse=True)
    return [k for k, _ in matching]


def load_month(
    bucket: str,
    base: str,
    *,
    list_objects: Callable[[str, str], list[tuple[str, object]]] | None = None,
    read_bytes: Callable[[str, str], bytes] | None = None,
) -> MonthData:
    """
    Load tournament_ids.txt plus every details/reports chunk and skipped file
    under s3://{bucket}/{base}/ and merge them (newest file wins per tournament).

    list_objects / read_bytes are injectable for tests.
    """
    import pyarrow.parquet as pq

    if list_objects is None:
        from s3_io import list_s3_objects

        list_objects = list_s3_objects
    if read_bytes is None:
        import boto3

        s3 = boto3.client("s3")

        def read_bytes(b: str, k: str) -> bytes:
            return s3.get_object(Bucket=b, Key=k)["Body"].read()

    ids_key = f"{base}/data/tournament_ids.txt"
    try:
        ids_text = read_bytes(bucket, ids_key).decode("utf-8")
    except Exception as e:
        raise IncompleteRunError(
            f"Cannot read s3://{bucket}/{ids_key}: {e}; tournament list missing"
        ) from e
    expected = parse_ids(ids_text)

    details_objs = list_objects(bucket, f"{base}/data/tournament_details_chunks/")
    reports_objs = list_objects(bucket, f"{base}/data/tournament_reports_chunks/")
    skipped_objs = list_objects(bucket, f"{base}/reports/tournament_reports_chunks/")

    def read_tables(keys: list[str]) -> list[pa.Table]:
        return [pq.read_table(io.BytesIO(read_bytes(bucket, k))) for k in keys]

    details_keys = _newest_first(details_objs, DETAILS_CHUNK_RE)
    players_keys = _newest_first(reports_objs, PLAYERS_CHUNK_RE)
    games_keys = _newest_first(reports_objs, GAMES_CHUNK_RE)
    skipped_keys = _newest_first(skipped_objs, SKIPPED_RE)

    details = merge_newest_first(
        [successful_details(t) for t in read_tables(details_keys)]
    )
    players = merge_newest_first(read_tables(players_keys))
    games = merge_newest_first(read_tables(games_keys))

    skipped: list[dict] = []
    for k in skipped_keys:
        try:
            skipped.extend(json.loads(read_bytes(bucket, k)))
        except (ValueError, TypeError) as e:
            logger.warning("Unreadable skipped file %s: %s", k, e)

    files = {
        "details": len(details_keys),
        "players": len(players_keys),
        "games": len(games_keys),
        "skipped": len(skipped_keys),
    }
    logger.info(
        "Loaded %s: %d expected tournaments, files=%s", base, len(expected), files
    )
    return MonthData(expected, details, players, games, skipped, files)

"""
Month completeness: which tournaments in tournament_ids.txt have details and
reports stored, and merging every stored file into one deduplicated table.

Stored files come in two shapes, both under {base}/:
  - chunk files from the old fan-out (details_chunk_3_of_15.parquet, legacy
    details_chunk_3.parquet, ...), possibly from several chunk sets;
  - part files appended by resumable workers
    (data/tournament_details_parts/details_<stamp>.parquet, ...).

A tournament counts as done for details if a details row with success=True
exists, or a details *_skipped.json records it with a page-structure reason.
It counts as done for reports if it has player rows, or a reports
*_skipped.json records it with a page-structure reason. Network errors never
count as done. Player rows are the commit marker for reports: workers write
games and skipped files first and players last.

When a tournament appears in more than one file, rows from the newest file win.
"""

from __future__ import annotations

import io
import json
import logging
import re
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

# Reasons a tournament legitimately has no report (FIDE page structure, not
# network). Must match get_tournament_reports.SKIPPABLE_ERRORS.
SKIPPABLE_ERRORS = frozenset(
    ["report_updated_or_replaced", "no data found", "no players found"]
)
# Details page exists but has no details table.
DETAILS_SKIPPABLE_ERRORS = frozenset(["no data found"])

_STAMP = r"\d{8}T\d{6}_[0-9a-f]{6}"
DETAILS_FILE_RE = re.compile(rf"details_(chunk_\d+(_of_\d+)?|{_STAMP})\.parquet$")
PLAYERS_FILE_RE = re.compile(
    rf"reports_(chunk_\d+(_of_\d+)?|{_STAMP})_players\.parquet$"
)
GAMES_FILE_RE = re.compile(rf"reports_(chunk_\d+(_of_\d+)?|{_STAMP})_games\.parquet$")
REPORTS_SKIPPED_RE = re.compile(
    rf"reports_(chunk_\d+(_of_\d+)?|{_STAMP})_skipped\.json$"
)
DETAILS_SKIPPED_RE = re.compile(rf"details_{_STAMP}_skipped\.json$")

# Prefixes (relative to the month base) and the files expected there.
DETAILS_PREFIXES = ("data/tournament_details_chunks/", "data/tournament_details_parts/")
REPORTS_PREFIXES = ("data/tournament_reports_chunks/", "data/tournament_reports_parts/")
REPORTS_SKIPPED_PREFIXES = (
    "reports/tournament_reports_chunks/",
    "reports/tournament_reports_parts/",
)
DETAILS_SKIPPED_PREFIXES = ("reports/tournament_details_parts/",)

ListObjects = Callable[[str, str], list[tuple[str, object]]]
ReadBytes = Callable[[str, str], bytes]


class IncompleteRunError(RuntimeError):
    """A month is missing details or reports for some tournaments."""


def part_stamp() -> str:
    """Unique, time-sortable part name: 20261109T143000_a1b2c3."""
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{now}_{secrets.token_hex(3)}"


@dataclass
class Coverage:
    expected: int
    missing_details: list[str] = field(default_factory=list)
    missing_reports: list[str] = field(default_factory=list)
    skipped_details: int = 0
    skipped_reports: int = 0

    @property
    def complete(self) -> bool:
        return not self.missing_details and not self.missing_reports

    def summary(self, sample: int = 20) -> dict:
        return {
            "expected": self.expected,
            "missing_details": len(self.missing_details),
            "missing_reports": len(self.missing_reports),
            "skipped_details": self.skipped_details,
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


def skippable_ids(
    skipped_entries: Iterable[dict], reasons: frozenset = SKIPPABLE_ERRORS
) -> set[str]:
    return {
        str(e.get("tournament_code"))
        for e in skipped_entries
        if e.get("error") in reasons and e.get("tournament_code") is not None
    }


def compute_coverage(
    expected_ids: list[str],
    details_done: set[str],
    reports_done: set[str],
    skipped_ok: set[str],
    details_skipped_ok: set[str] = frozenset(),
) -> Coverage:
    return Coverage(
        expected=len(expected_ids),
        missing_details=[
            t
            for t in expected_ids
            if t not in details_done and t not in details_skipped_ok
        ],
        missing_reports=[
            t for t in expected_ids if t not in reports_done and t not in skipped_ok
        ],
        skipped_details=sum(1 for t in expected_ids if t in details_skipped_ok),
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


# ---------------------------------------------------------------------------
# S3 access
# ---------------------------------------------------------------------------


def _default_io(
    list_objects: ListObjects | None, read_bytes: ReadBytes | None
) -> tuple[ListObjects, ReadBytes]:
    if list_objects is None:
        from s3_io import list_s3_objects

        list_objects = list_s3_objects
    if read_bytes is None:
        import boto3

        s3 = boto3.client("s3")

        def read_bytes(b: str, k: str) -> bytes:
            return s3.get_object(Bucket=b, Key=k)["Body"].read()

    return list_objects, read_bytes


def _keys_newest_first(
    list_objects: ListObjects,
    bucket: str,
    base: str,
    prefixes: Iterable[str],
    pattern: re.Pattern,
) -> list[str]:
    found: list[tuple[str, object]] = []
    for p in prefixes:
        for k, lm in list_objects(bucket, f"{base}/{p}"):
            if pattern.search(k.rsplit("/", 1)[-1]):
                found.append((k, lm))
    found.sort(key=lambda x: x[1], reverse=True)
    return [k for k, _ in found]


@dataclass
class MonthFiles:
    details: list[str]
    players: list[str]
    games: list[str]
    reports_skipped: list[str]
    details_skipped: list[str]

    def counts(self) -> dict[str, int]:
        return {
            "details": len(self.details),
            "players": len(self.players),
            "games": len(self.games),
            "skipped": len(self.reports_skipped),
            "details_skipped": len(self.details_skipped),
        }


def list_month_files(
    bucket: str, base: str, list_objects: ListObjects | None = None
) -> MonthFiles:
    list_objects, _ = _default_io(list_objects, lambda b, k: b"")
    k = lambda prefixes, pat: _keys_newest_first(  # noqa: E731
        list_objects, bucket, base, prefixes, pat
    )
    return MonthFiles(
        details=k(DETAILS_PREFIXES, DETAILS_FILE_RE),
        players=k(REPORTS_PREFIXES, PLAYERS_FILE_RE),
        games=k(REPORTS_PREFIXES, GAMES_FILE_RE),
        reports_skipped=k(REPORTS_SKIPPED_PREFIXES, REPORTS_SKIPPED_RE),
        details_skipped=k(DETAILS_SKIPPED_PREFIXES, DETAILS_SKIPPED_RE),
    )


def _read_json_entries(read_bytes: ReadBytes, bucket: str, keys: list[str]) -> list:
    entries: list[dict] = []
    for k in keys:
        try:
            entries.extend(json.loads(read_bytes(bucket, k)))
        except (ValueError, TypeError) as e:
            logger.warning("Unreadable skipped file %s: %s", k, e)
    return entries


def _read_tables(
    read_bytes: ReadBytes, bucket: str, keys: list[str], columns=None
) -> list[pa.Table]:
    import pyarrow.parquet as pq

    out = []
    for k in keys:
        f = pq.ParquetFile(io.BytesIO(read_bytes(bucket, k)))
        cols = None
        if columns is not None:
            cols = [c for c in columns if c in f.schema_arrow.names]
        out.append(f.read(columns=cols))
    return out


def read_expected_ids(bucket: str, base: str, read_bytes: ReadBytes) -> list[str]:
    ids_key = f"{base}/data/tournament_ids.txt"
    try:
        return parse_ids(read_bytes(bucket, ids_key).decode("utf-8"))
    except Exception as e:
        raise IncompleteRunError(
            f"Cannot read s3://{bucket}/{ids_key}: {e}; tournament list missing"
        ) from e


def done_ids(
    bucket: str,
    base: str,
    kind: str,
    *,
    list_objects: ListObjects | None = None,
    read_bytes: ReadBytes | None = None,
) -> set[str]:
    """
    Tournaments already done for this month in any chunk or part file.
    kind: "details" or "reports". Reads only the ID columns it needs.
    """
    list_objects, read_bytes = _default_io(list_objects, read_bytes)
    files = list_month_files(bucket, base, list_objects)
    if kind == "details":
        tables = _read_tables(
            read_bytes, bucket, files.details, ["tournament_id", "success"]
        )
        done = set().union(*(tournament_ids(successful_details(t)) for t in tables))
        skipped = _read_json_entries(read_bytes, bucket, files.details_skipped)
        return done | skippable_ids(skipped, DETAILS_SKIPPABLE_ERRORS)
    if kind == "reports":
        tables = _read_tables(read_bytes, bucket, files.players, ["tournament_id"])
        done = set().union(*(tournament_ids(t) for t in tables))
        skipped = _read_json_entries(read_bytes, bucket, files.reports_skipped)
        return done | skippable_ids(skipped)
    raise ValueError(f"kind must be details or reports (got {kind!r})")


def load_skipped(
    bucket: str,
    base: str,
    *,
    list_objects: ListObjects | None = None,
    read_bytes: ReadBytes | None = None,
) -> tuple[list[dict], list[dict]]:
    """(details skipped entries, reports skipped entries) for the month."""
    list_objects, read_bytes = _default_io(list_objects, read_bytes)
    files = list_month_files(bucket, base, list_objects)
    return (
        _read_json_entries(read_bytes, bucket, files.details_skipped),
        _read_json_entries(read_bytes, bucket, files.reports_skipped),
    )


@dataclass
class MonthData:
    """Every stored chunk and part for one month, merged and deduplicated."""

    expected_ids: list[str]
    details: pa.Table
    players: pa.Table
    games: pa.Table
    skipped: list[dict]
    details_skipped: list[dict]
    files: dict[str, int]

    def coverage(self) -> Coverage:
        return compute_coverage(
            self.expected_ids,
            tournament_ids(self.details),
            tournament_ids(self.players),
            skippable_ids(self.skipped),
            skippable_ids(self.details_skipped, DETAILS_SKIPPABLE_ERRORS),
        )


def load_details(
    bucket: str,
    base: str,
    *,
    list_objects: ListObjects | None = None,
    read_bytes: ReadBytes | None = None,
) -> pa.Table:
    """Successful details rows for the month, newest file wins per tournament."""
    list_objects, read_bytes = _default_io(list_objects, read_bytes)
    files = list_month_files(bucket, base, list_objects)
    return merge_newest_first(
        [successful_details(t) for t in _read_tables(read_bytes, bucket, files.details)]
    )


def load_month(
    bucket: str,
    base: str,
    *,
    list_objects: ListObjects | None = None,
    read_bytes: ReadBytes | None = None,
) -> MonthData:
    """
    Load tournament_ids.txt plus every details/reports chunk, part and skipped
    file under s3://{bucket}/{base}/ and merge them (newest file wins per
    tournament). list_objects / read_bytes are injectable for tests.
    """
    list_objects, read_bytes = _default_io(list_objects, read_bytes)
    expected = read_expected_ids(bucket, base, read_bytes)
    files = list_month_files(bucket, base, list_objects)

    details = merge_newest_first(
        [successful_details(t) for t in _read_tables(read_bytes, bucket, files.details)]
    )
    players = merge_newest_first(_read_tables(read_bytes, bucket, files.players))
    games = merge_newest_first(_read_tables(read_bytes, bucket, files.games))
    skipped = _read_json_entries(read_bytes, bucket, files.reports_skipped)
    details_skipped = _read_json_entries(read_bytes, bucket, files.details_skipped)

    logger.info(
        "Loaded %s: %d expected tournaments, files=%s",
        base,
        len(expected),
        files.counts(),
    )
    return MonthData(
        expected,
        details,
        players,
        games,
        skipped,
        details_skipped,
        files.counts(),
    )

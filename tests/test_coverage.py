"""Tests for month completeness checks and the union/dedupe merge (coverage.py)."""

from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import coverage  # noqa: E402
from coverage import (  # noqa: E402
    IncompleteRunError,
    compute_coverage,
    load_month,
    merge_newest_first,
    parse_ids,
    successful_details,
)

BASE = "prod/2024-01"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _pq(rows: list[dict]) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


class FakeS3:
    """Minimal bucket: key -> (bytes, last_modified)."""

    def __init__(self):
        self.objs: dict[str, tuple[bytes, datetime]] = {}
        self._t = T0

    def put(self, key: str, body: bytes | str) -> None:
        if isinstance(body, str):
            body = body.encode()
        self._t += timedelta(minutes=1)
        self.objs[key] = (body, self._t)

    def list_objects(self, bucket: str, prefix: str):
        return [(k, lm) for k, (_, lm) in self.objs.items() if k.startswith(prefix)]

    def read_bytes(self, bucket: str, key: str) -> bytes:
        return self.objs[key][0]


def _load(s3: FakeS3):
    return load_month("b", BASE, list_objects=s3.list_objects, read_bytes=s3.read_bytes)


def test_skippable_errors_match_reports_scraper():
    from get_tournament_reports import SKIPPABLE_ERRORS

    assert coverage.SKIPPABLE_ERRORS == SKIPPABLE_ERRORS


def test_parse_ids_strips_blanks_and_duplicates():
    assert parse_ids("1\n 2 \n\n1\n3\n") == ["1", "2", "3"]


def test_successful_details_drops_failed_rows():
    t = pa.Table.from_pylist(
        [
            {"tournament_id": "1", "success": True},
            {"tournament_id": "2", "success": False},
            {"tournament_id": "3", "success": None},
        ]
    )
    assert successful_details(t).column("tournament_id").to_pylist() == ["1"]


def test_merge_newest_first_takes_each_tournament_from_one_file():
    newer = pa.Table.from_pylist(
        [{"tournament_id": "1", "v": "new"}, {"tournament_id": "1", "v": "new2"}]
    )
    older = pa.Table.from_pylist(
        [
            {"tournament_id": "1", "v": "old"},
            {"tournament_id": "2", "v": "old"},
        ]
    )
    out = merge_newest_first([newer, older]).to_pylist()
    assert out == [
        {"tournament_id": "1", "v": "new"},
        {"tournament_id": "1", "v": "new2"},
        {"tournament_id": "2", "v": "old"},
    ]


def test_compute_coverage_network_skip_is_not_done():
    cov = compute_coverage(
        ["1", "2", "3"],
        details_done={"1", "2", "3"},
        reports_done={"1"},
        skipped_ok={"2"},
    )
    assert cov.missing_reports == ["3"]
    assert cov.skipped_reports == 1
    assert not cov.complete
    with pytest.raises(IncompleteRunError):
        cov.raise_if_incomplete("x")


def _month_with_two_chunk_sets() -> FakeS3:
    """IDs 1-4. Old 2-chunk set covers 1,2; newer 3-chunk set covers 3,4 (and 1 again)."""
    s3 = FakeS3()
    s3.put(f"{BASE}/data/tournament_ids.txt", "1\n2\n3\n4\n")
    d = f"{BASE}/data/tournament_details_chunks/"
    r = f"{BASE}/data/tournament_reports_chunks/"
    s3.put(
        d + "details_chunk_0_of_2.parquet",
        _pq(
            [
                {"tournament_id": "1", "success": True, "name": "old"},
                {"tournament_id": "2", "success": True, "name": "old"},
            ]
        ),
    )
    s3.put(r + "reports_chunk_0_of_2_players.parquet", _pq([{"tournament_id": "1"}]))
    s3.put(r + "reports_chunk_0_of_2_games.parquet", _pq([{"tournament_id": "1"}]))
    s3.put(
        f"{BASE}/reports/tournament_reports_chunks/reports_chunk_0_of_2_skipped.json",
        json.dumps([{"tournament_code": "2", "error": "report_updated_or_replaced"}]),
    )
    s3.put(
        d + "details_chunk_1_of_3.parquet",
        _pq(
            [
                {"tournament_id": "1", "success": True, "name": "new"},
                {"tournament_id": "3", "success": True, "name": "new"},
                {"tournament_id": "4", "success": False, "name": None},
            ]
        ),
    )
    s3.put(
        r + "reports_chunk_1_of_3_players.parquet",
        _pq([{"tournament_id": "3"}, {"tournament_id": "4"}]),
    )
    s3.put(r + "reports_chunk_1_of_3_games.parquet", _pq([{"tournament_id": "3"}]))
    return s3


def test_load_month_unions_chunk_sets_and_reports_gaps():
    month = _load(_month_with_two_chunk_sets())
    cov = month.coverage()
    # 4 has a failed details row only -> missing details. Reports all covered
    # (2 via page-structure skip).
    assert cov.missing_details == ["4"]
    assert cov.missing_reports == []
    assert month.files == {
        "details": 2,
        "players": 2,
        "games": 2,
        "skipped": 1,
        "details_skipped": 0,
    }
    names = {r["tournament_id"]: r["name"] for r in month.details.to_pylist()}
    assert names == {"1": "new", "2": "old", "3": "new"}


def test_load_month_complete_after_rescrape():
    s3 = _month_with_two_chunk_sets()
    s3.put(
        f"{BASE}/data/tournament_details_chunks/details_chunk_0_of_1.parquet",
        _pq([{"tournament_id": "4", "success": True, "name": "fixed"}]),
    )
    cov = _load(s3).coverage()
    assert cov.complete


def test_load_month_ignores_network_skips():
    s3 = FakeS3()
    s3.put(f"{BASE}/data/tournament_ids.txt", "1\n")
    s3.put(
        f"{BASE}/data/tournament_details_chunks/details_chunk_0_of_1.parquet",
        _pq([{"tournament_id": "1", "success": True}]),
    )
    s3.put(
        f"{BASE}/reports/tournament_reports_chunks/reports_chunk_0_of_1_skipped.json",
        json.dumps(
            [{"tournament_code": "1", "error": "max retries exceeded: timeout"}]
        ),
    )
    assert _load(s3).coverage().missing_reports == ["1"]


def test_load_month_without_id_list_raises():
    with pytest.raises(IncompleteRunError):
        _load(FakeS3())


def test_merge_refuses_incomplete_month(monkeypatch):
    import merge_chunks

    s3 = _month_with_two_chunk_sets()
    monkeypatch.setattr(
        merge_chunks,
        "load_month",
        lambda bucket, base: load_month(
            bucket, base, list_objects=s3.list_objects, read_bytes=s3.read_bytes
        ),
    )
    with pytest.raises(IncompleteRunError, match="1 of 4 tournaments missing details"):
        merge_chunks.run("b", "prod", "2024-01")


@pytest.mark.parametrize(
    "module", ["details_chunk", "reports_chunk", "merge_chunks", "validate"]
)
def test_handlers_raise_on_bad_input(module):
    mod = __import__(f"handlers.{module}", fromlist=["lambda_handler"])
    with pytest.raises(ValueError, match="run_type"):
        mod.lambda_handler({"run_type": "nope"}, None)

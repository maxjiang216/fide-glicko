"""Tests for resumable, time-budgeted chunk scraping (chunk_worker.py)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from chunk_worker import (
    FAIL,
    OK,
    SKIP,
    Outcome,
    details_flusher,
    details_processor,
    reports_flusher,
    reports_processor,
    run_chunk,
)
from coverage import done_ids, load_month
from fide_http import FideBlocked


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def _ok(tid):
    return Outcome(tid, OK, {"tid": tid})


def test_skips_done_and_flushes_in_parts():
    flushed = []
    res = run_chunk(
        ["1", "2", "3", "4", "5"],
        done={"2"},
        process_one=_ok,
        flush=lambda outs: flushed.append([o.tid for o in outs]) or "p",
        deadline=float("inf"),
        flush_every=2,
    )
    assert flushed == [["1", "3"], ["4", "5"]]
    assert res.already_done == 1 and res.progress == 4 and res.remaining == 0


def test_time_budget_stops_and_saves():
    clock = Clock()
    flushed = []

    def process(tid):
        clock.t += 10
        return _ok(tid)

    res = run_chunk(
        [str(i) for i in range(10)],
        set(),
        process,
        lambda outs: flushed.append(len(outs)) or "p",
        deadline=25,
        clock=clock,
    )
    assert res.stopped == "time_budget"
    assert res.progress == 3 and res.remaining == 7
    assert sum(flushed) == 3


def test_block_saves_work_before_raising():
    flushed = []

    def process(tid):
        if tid == "3":
            raise FideBlocked("blocked")
        return _ok(tid)

    with pytest.raises(FideBlocked):
        run_chunk(
            ["1", "2", "3", "4"],
            set(),
            process,
            lambda outs: flushed.append([o.tid for o in outs]) or "p",
            deadline=float("inf"),
        )
    assert flushed == [["1", "2"]]


def test_failures_count_as_remaining_not_progress():
    res = run_chunk(
        ["1", "2", "3"],
        set(),
        lambda t: Outcome(t, FAIL, error="HTTP 503") if t == "2" else _ok(t),
        lambda outs: "p",
        deadline=float("inf"),
    )
    assert res.progress == 2 and res.remaining == 1
    assert res.failed == [("2", "HTTP 503")]


def test_final_skips_count_as_done():
    res = run_chunk(
        ["1"],
        set(),
        lambda t: Outcome(
            t, SKIP, {"tournament_code": t, "error": "report_updated_or_replaced"}
        ),
        lambda outs: "p",
        deadline=float("inf"),
    )
    assert res.progress == 1 and res.remaining == 0


def test_weak_skip_saved_but_still_remaining():
    """
    A weak skip needs a second, separate run to confirm it (coverage). It is
    saved (progress), but stays in remaining so the caller loops once more;
    regression: prod 2024-04 ended with remaining=0 and merge refused 365437.
    """
    res = run_chunk(
        ["1", "2"],
        set(),
        lambda t: (
            Outcome(
                t, SKIP, {"tournament_code": t, "error": "no data found"}, final=False
            )
            if t == "2"
            else _ok(t)
        ),
        lambda outs: "p",
        deadline=float("inf"),
    )
    assert res.progress == 2 and res.unconfirmed == 1 and res.remaining == 1


@pytest.mark.parametrize(
    "error,final", [("no data found", False), ("report_updated_or_replaced", True)]
)
def test_reports_processor_marks_weak_skips(error, final):
    class Fetcher:
        def fetch(self, label, fn):
            return None, error, None, None

    out = reports_processor(Fetcher(), None)("7")
    assert out.status == SKIP and out.final is final


def test_details_processor_marks_weak_skips():
    class Fetcher:
        def fetch(self, label, fn):
            return None, "no data found", None, None

    out = details_processor(Fetcher(), None)("7")
    assert out.status == SKIP and out.final is False


# --- part files round-trip through coverage -------------------------------


def _dir_io(root: Path):
    """list_objects/read_bytes over a local dir laid out like the bucket."""

    def list_objects(bucket, prefix):
        out = []
        for p in root.rglob("*"):
            if p.is_file():
                key = p.relative_to(root).as_posix()
                if key.startswith(prefix):
                    lm = datetime.fromtimestamp(
                        p.stat().st_mtime_ns / 1e9, timezone.utc
                    )
                    out.append((key, lm))
        return out

    def read_bytes(bucket, key):
        return (root / key).read_bytes()

    return {"list_objects": list_objects, "read_bytes": read_bytes}


def test_parts_round_trip(tmp_path, monkeypatch):
    import chunk_worker
    from pipeline_common import stamp_time

    stamps = iter(
        [
            "20261101T000000_aaaaaa",  # details: 1 ok, 3 skipped
            "20261101T010000_bbbbbb",  # 3 skipped again, 1h later
            "20261102T010000_cccccc",  # 3 skipped again, 25h after the first
            "20261102T020000_dddddd",  # reports
        ]
    )
    monkeypatch.setattr(chunk_worker, "part_stamp", lambda: next(stamps))
    t0 = stamp_time("20261101T000000_aaaaaa")
    base = "prod/2024-01"
    month = tmp_path / base
    (month / "data").mkdir(parents=True)
    (month / "data" / "tournament_ids.txt").write_text("1\n2\n3\n")
    io_ = _dir_io(tmp_path)

    details_flush = details_flusher(str(month))
    details_flush(
        [
            Outcome(
                "1",
                OK,
                {
                    "tournament_id": "1",
                    "success": True,
                    "details": {"event_code": "1", "start_date": "2024-01-02"},
                },
                b"<html>1</html>",
            ),
            Outcome("3", SKIP, {"tournament_code": "3", "error": "no data found"}),
        ]
    )
    skip3 = [Outcome("3", SKIP, {"tournament_code": "3", "error": "no data found"})]
    # A single "no data found" is provisional (could be an error page): not
    # refetched right away, but not counted as covered either...
    assert done_ids("b", base, "details", now=t0 + 3600, **io_) == {"1", "3"}
    assert "3" in load_month("b", base, **io_).coverage().missing_details
    # ...and refetched once the confirmation gap has passed.
    assert done_ids("b", base, "details", now=t0 + 25 * 3600, **io_) == {"1"}
    # A second sighting within the gap does not confirm it (regression:
    # 2026-11-11 FIDE served empty report pages for days)...
    details_flush(skip3)
    assert "3" in load_month("b", base, **io_).coverage().missing_details
    # ...one a day later does.
    details_flush(skip3)
    assert done_ids("b", base, "details", now=t0 + 30 * 3600, **io_) == {"1", "3"}

    report = {
        "tournament_code": "1",
        "success": True,
        "players": [
            {"id": "100", "name": "A", "rounds": []},
            {"id": "200", "name": "B", "rounds": []},
        ],
    }
    reports_flush = reports_flusher(str(month), details_map={})
    reports_flush(
        [
            Outcome("1", OK, report, b"<html>r1</html>"),
            Outcome(
                "2",
                SKIP,
                {"tournament_code": "2", "error": "report_updated_or_replaced"},
            ),
        ]
    )
    assert done_ids("b", base, "reports", **io_) == {"1", "2"}

    cov = load_month("b", base, **io_).coverage()
    assert cov.missing_details == ["2"]
    assert cov.missing_reports == ["3"]
    assert (month / "raw" / "details").exists() and (month / "raw" / "reports").exists()


def test_block_reports_partial_progress():
    def process(tid):
        if tid == "3":
            raise FideBlocked("blocked")
        return _ok(tid)

    with pytest.raises(FideBlocked) as ei:
        run_chunk(
            ["1", "2", "3"], set(), process, lambda outs: "p", deadline=float("inf")
        )
    assert ei.value.chunk_result["progress"] == 2
    assert ei.value.chunk_result["remaining"] == 1

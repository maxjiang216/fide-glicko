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
    reports_flusher,
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


def test_skips_count_as_progress():
    res = run_chunk(
        ["1"],
        set(),
        lambda t: Outcome(t, SKIP, {"tournament_code": t, "error": "no data found"}),
        lambda outs: "p",
        deadline=float("inf"),
    )
    assert res.progress == 1 and res.remaining == 0


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


def test_parts_round_trip(tmp_path):
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
    assert done_ids("b", base, "details", **io_) == {"1", "3"}

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

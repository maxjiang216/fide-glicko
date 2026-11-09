"""Tests for the chunk_summary EMF line (metrics.py)."""

import io
import json

from metrics import METRICS, NAMESPACE, chunk_summary, emit

STATS = {
    "requests": 120,
    "block_signals": 0,
    "strain_errors": 2,
    "slowdowns": 1,
    "target_rate": 0.3,
    "final_rate": 0.15,
    "latency_p50_s": 1.2,
    "latency_p90_s": 3.4,
}


def test_metric_count_stays_in_free_tier():
    assert len(METRICS) <= 10


def test_emf_line_is_valid_and_complete():
    rec = chunk_summary(
        kind="reports",
        run_name="2024-01",
        chunk="3_of_22",
        fetch_stats=STATS,
        chunk_result={"progress": 50, "remaining": 0, "stopped": "complete"},
        blocked=False,
    )
    buf = io.StringIO()
    emit(rec, buf)
    line = buf.getvalue()
    assert line.endswith("\n") and line.count("\n") == 1
    out = json.loads(line)
    cw = out["_aws"]["CloudWatchMetrics"][0]
    assert cw["Namespace"] == NAMESPACE
    assert cw["Dimensions"] == [[]]
    names = {m["Name"] for m in cw["Metrics"]}
    assert names == {n for n, _ in METRICS}
    for n in names:  # every metric must be a top-level numeric field
        assert isinstance(out[n], (int, float))
    assert out["event"] == "chunk_summary" and out["kind"] == "reports"


def test_blocked_invocation_omits_missing_fields():
    rec = chunk_summary(
        kind="details",
        run_name="2024-01",
        chunk="0_of_1",
        fetch_stats={**STATS, "latency_p50_s": None},
        chunk_result=None,
        blocked=True,
        error="FideBlocked",
    )
    names = {m["Name"] for m in rec["_aws"]["CloudWatchMetrics"][0]["Metrics"]}
    assert "blocked" in names and rec["blocked"] == 1
    assert "progress" not in names and "latency_p50_s" not in names
    assert rec["stopped"] == "blocked"

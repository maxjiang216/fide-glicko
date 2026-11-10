"""
One summary line per chunk invocation, in CloudWatch Embedded Metric Format.

Printing this JSON to stdout in Lambda does two things at once:
  - it is a normal log line, queryable with Logs Insights
    (filter event = "chunk_summary");
  - CloudWatch extracts the listed fields as custom metrics in the FideGlicko
    namespace. No API calls and no extra IAM permissions.

Metrics are published without dimensions to stay inside CloudWatch's 10 free
custom metrics. kind (details/reports), month and chunk are plain properties,
so Logs Insights can still split by them.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Optional

NAMESPACE = "FideGlicko"

# (field, unit). Keep <= 10 to stay in the free tier.
METRICS = [
    ("requests", "Count"),
    ("block_signals", "Count"),
    ("blocked", "Count"),
    ("strain_errors", "Count"),
    ("slowdowns", "Count"),
    ("final_rate", "None"),
    ("latency_p50_s", "Seconds"),
    ("progress", "Count"),
    ("remaining", "Count"),
]


def chunk_summary(
    *,
    kind: str,
    run_name: Optional[str],
    chunk: str,
    fetch_stats: dict,
    chunk_result: Optional[dict],
    blocked: bool,
    error: Optional[str] = None,
) -> dict:
    """Build the EMF record. chunk_result is ChunkResult.to_dict() (None if it raised)."""
    cr = chunk_result or {}
    record = {
        "event": "chunk_summary",
        "kind": kind,
        "run_name": run_name,
        "chunk": chunk,
        "blocked": int(blocked),
        "error": error,
        **fetch_stats,
        "progress": cr.get("progress"),
        "remaining": cr.get("remaining"),
        "already_done": cr.get("already_done"),
        "failed": cr.get("failed"),
        "stopped": "blocked" if blocked else cr.get("stopped", "error"),
    }
    present = [(n, u) for n, u in METRICS if record.get(n) is not None]
    record["_aws"] = {
        "Timestamp": int(time.time() * 1000),
        "CloudWatchMetrics": [
            {
                "Namespace": NAMESPACE,
                "Dimensions": [[]],
                "Metrics": [{"Name": n, "Unit": u} for n, u in present],
            }
        ],
    }
    return record


def emit(record: dict, stream=None) -> None:
    """Write one EMF line. Must be a bare JSON line on stdout for extraction."""
    stream = stream or sys.stdout
    stream.write(json.dumps(record, default=str) + "\n")
    stream.flush()

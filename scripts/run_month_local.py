#!/usr/bin/env python3
"""
Run the pipeline for one or more months from this machine (no Step Functions).

Same code and the same rules as the deployed state machine, driven locally:
  Tournaments (resumable list fetch; skipped if the list exists, unless
  --refresh-list) -> SplitIds -> per chunk: DetailsChunk, ReportsChunk (each
  called again while remaining > 0 and progress > 0) -> MergeChunks -> Validate.

Use it when FIDE blocks AWS addresses: requests go out from this machine's IP,
at the same polite rate (SSM-free defaults: 0.3 req/s), one request at a time.
All output goes to S3 exactly as a Lambda run would write it, and every step
resumes from what is already stored, so stopping and restarting is safe.

On FideBlocked it waits like the state machine (30m, 1h, 2h, 4h, 4h) and then
stops with an error. A stuck chunk (no progress with work left) stops too.

Examples:
  uv run scripts/run_month_local.py --month 2024-04
  uv run scripts/run_month_local.py --month 2024-01 --month 2024-02 --refresh-list
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(REPO), str(REPO / "src" / "scraper")]

from fide_http import FideBlocked  # noqa: E402

logger = logging.getLogger("run_month_local")

BLOCK_WAITS_S = [1800, 3600, 7200, 14400, 14400]
CALL_BUDGET_S = 1800  # per worker call; parts are flushed every 50 / 5 min anyway


class Budget:
    """Stands in for the Lambda context: how long one worker call may run."""

    def __init__(self, seconds: float):
        self.end = time.monotonic() + seconds

    def get_remaining_time_in_millis(self) -> int:
        return int(max(0, self.end - time.monotonic()) * 1000)


def call_with_block_waits(name: str, fn, event: dict) -> dict:
    for attempt, wait in enumerate([0] + BLOCK_WAITS_S):
        if wait:
            logger.warning(
                "%s blocked; waiting %d min (attempt %d of %d)",
                name,
                wait // 60,
                attempt + 1,
                len(BLOCK_WAITS_S) + 1,
            )
            time.sleep(wait)
        try:
            return fn(dict(event), Budget(CALL_BUDGET_S))
        except FideBlocked as e:
            last = e
    raise SystemExit(f"{name}: still blocked after all waits: {last}")


def loop_until_done(name: str, fn, event: dict) -> dict:
    """Call fn again while remaining > 0 and progress > 0 (the ASL Choice)."""
    while True:
        r = call_with_block_waits(name, fn, event)
        rem, prog = r.get("remaining", 0), r.get("progress", 0)
        logger.info(
            "%s: progress=%s remaining=%s failed=%s stopped=%s",
            name,
            prog,
            rem,
            r.get("failed"),
            r.get("stopped"),
        )
        if rem <= 0:
            return r
        if prog <= 0:
            raise SystemExit(
                f"{name} stuck: {rem} remaining, no progress. "
                f"Failures: {r.get('failed_sample')}"
            )


def run_month(year: int, month: int, args) -> dict:
    from handlers import details_chunk, ensure_run_name, reports_chunk, split_ids
    from handlers import tournaments
    import merge_chunks
    import validate_pipeline

    run_type = args.run_type
    run_name = f"{year}-{month:02d}" if run_type == "prod" else args.run_name
    base_event = ensure_run_name.lambda_handler(
        {
            "input": {
                "year": year,
                "month": month,
                "run_type": run_type,
                "run_name": run_name,
                "federations_s3_uri": args.federations_s3_uri,
                "bucket": args.bucket,
                "override": args.refresh_list,
                "details_rate_limit": args.rate,
                "reports_rate_limit": args.rate,
                "tournaments_rate_limit": args.rate,
            }
        },
        None,
    )
    t0 = time.time()
    logger.info("=== %s: tournament list", run_name)
    loop_until_done(f"{run_name} tournaments", tournaments.lambda_handler, base_event)

    logger.info("=== %s: split", run_name)
    split = split_ids.lambda_handler(dict(base_event), None)
    if not split.get("success", True) or not split.get("chunks"):
        raise SystemExit(f"split_ids failed: {split}")
    chunks = split["chunks"]

    for c in chunks:
        ev = {
            **base_event,
            "chunk_index": c["chunk_index"],
            "chunk_count": c["chunk_count"],
        }
        label = f"{run_name} chunk {c['chunk_index']}/{c['chunk_count']}"
        loop_until_done(f"{label} details", details_chunk.lambda_handler, ev)
        loop_until_done(f"{label} reports", reports_chunk.lambda_handler, ev)

    logger.info("=== %s: merge + validate", run_name)
    m = merge_chunks.run(args.bucket, run_type, run_name)
    v = validate_pipeline.run(args.bucket, run_type, run_name)
    out = {
        "month": run_name,
        "minutes": round((time.time() - t0) / 60, 1),
        "rows": m["rows"],
        "coverage": {
            k: v
            for k, v in m["coverage"].items()
            if not k.endswith("_sample") and not k.endswith("_ids")
        },
        "report": v["report_uri"],
    }
    logger.info("=== %s done: %s", run_name, json.dumps(out))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument(
        "--month", action="append", required=True, help="YYYY-MM (repeatable)"
    )
    ap.add_argument("--bucket", default="fide-glicko")
    ap.add_argument("--rate", type=float, default=0.3, help="requests/s per step")
    ap.add_argument(
        "--refresh-list",
        action="store_true",
        help="Refetch the month's tournament list even if one exists",
    )
    ap.add_argument("--run-type", default="prod", choices=["prod", "custom"])
    ap.add_argument("--run-name", help="required for --run-type custom")
    ap.add_argument(
        "--federations-s3-uri", help="federations CSV to use (e.g. a test subset)"
    )
    args = ap.parse_args()
    if args.run_type == "custom" and not args.run_name:
        raise SystemExit("--run-name is required with --run-type custom")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.rate > 0.5:
        raise SystemExit("--rate above 0.5 req/s is not allowed (FIDE blocks per IP)")

    results = []
    for m in args.month:
        y, mo = (int(x) for x in m.split("-"))
        results.append(run_month(y, mo, args))
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

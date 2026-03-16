#!/usr/bin/env python3
"""
Run FIDE pipeline Step Function for multiple prod months with controlled concurrency.

Starts executions for each month in [start, end] that exists in FIDE's periods API.
Limits concurrent runs, polls status, and starts new executions when slots open.

Example:
  uv run scripts/run_prod_backfill.py --start 2024-01 --end 2024-12
  uv run scripts/run_prod_backfill.py --start 2024-01 --end 2024-06 --concurrency 2

Requires AWS credentials (e.g. aws configure). Uses default region unless --region.
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

import boto3
import requests
from botocore.exceptions import ClientError

# FIDE periods API (same as pipeline_historical); RUS has long history
PERIODS_URL = "https://ratings.fide.com/a_tournaments_panel.php"
REFERENCE_FED = "RUS"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Per infra/step-function/README.md
AVG_RUN_MINUTES = 30
POLL_INTERVAL_SEC = 60
STATE_MACHINE_NAME = "fide-glicko-pipeline"


def parse_month(s: str) -> tuple[int, int]:
    """Parse YYYY-MM to (year, month)."""
    try:
        parts = s.split("-")
        if len(parts) != 2:
            raise ValueError()
        year, month = int(parts[0]), int(parts[1])
        if not (1 <= month <= 12):
            raise ValueError("month must be 1-12")
        return year, month
    except (ValueError, IndexError) as e:
        raise argparse.ArgumentTypeError(
            f"Invalid month '{s}': expected YYYY-MM (e.g. 2024-01)"
        ) from e


def month_range(
    start: tuple[int, int], end: tuple[int, int]
) -> Iterator[tuple[int, int]]:
    """Yield (year, month) for each month from start (incl) to end (incl)."""
    sy, sm = start
    ey, em = end
    if (sy, sm) > (ey, em):
        raise ValueError("start month must be <= end month")
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def fetch_available_periods() -> list[tuple[int, int]]:
    """
    Fetch available (year, month) from FIDE periods API.
    Returns empty list on failure (caller falls back to unfiltered range).
    """
    url = f"{PERIODS_URL}?country={REFERENCE_FED}&periods_tab=1"
    headers = {"X-Requested-With": "XMLHttpRequest"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []

    periods: list[tuple[int, int]] = []
    for item in data:
        pub = item.get("frl_publish", "")
        if not pub:
            continue
        parts = pub.split("-")
        if len(parts) >= 2:
            try:
                y, m = int(parts[0]), int(parts[1])
                if 1 <= m <= 12:
                    periods.append((y, m))
            except ValueError:
                continue
    return list(set(periods))  # dedupe


@dataclass
class RunState:
    execution_arn: str
    year: int
    month: int
    started_at: datetime
    status: str = "RUNNING"


def get_state_machine_arn(client, name: str) -> str | None:
    """Find state machine ARN by name."""
    paginator = client.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm.get("name") == name:
                return sm["stateMachineArn"]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run prod pipeline for a range of months with controlled concurrency",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start",
        type=parse_month,
        required=True,
        metavar="YYYY-MM",
        help="Start month (e.g. 2024-01)",
    )
    parser.add_argument(
        "--end",
        type=parse_month,
        required=True,
        metavar="YYYY-MM",
        help="End month (e.g. 2024-12)",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=1,
        help="Max concurrent Step Function executions (default: 1)",
    )
    parser.add_argument(
        "--state-machine-arn",
        help="Step Function state machine ARN (default: lookup fide-glicko-pipeline)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region (default: from config)",
    )
    parser.add_argument(
        "--bucket",
        default="fide-glicko",
        help="S3 bucket (default: fide-glicko)",
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Pass override=true to pipeline (refetch/overwrite cached)",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=8,
        help="Map state concurrency per execution (default: 8)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Max tournaments per chunk (default: 300; use smaller if reports_chunk times out)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print months that would run, do not start",
    )
    args = parser.parse_args()

    if args.concurrency < 1:
        logger.error("--concurrency must be >= 1")
        return 1

    try:
        range_months = list(month_range(args.start, args.end))
    except ValueError as e:
        logger.error("%s", e)
        return 1
    if not range_months:
        logger.error("No months in range")
        return 1

    # Fetch FIDE periods and intersect with range; fall back to range if fetch fails
    available = set(fetch_available_periods())
    if available:
        today = datetime.now(timezone.utc).date()
        months = [
            (y, m)
            for (y, m) in range_months
            if (y, m) in available
            and (y < today.year or (y == today.year and m <= today.month))
        ]
        if months != range_months:
            excluded = set(range_months) - set(months)
            logger.info(
                "FIDE periods: %d in range, %d excluded (not in FIDE or future)",
                len(months),
                len(excluded),
            )
    else:
        logger.warning(
            "Could not fetch FIDE periods; using full range without period filter"
        )
        today = datetime.now(timezone.utc).date()
        months = [
            (y, m)
            for (y, m) in range_months
            if y < today.year or (y == today.year and m <= today.month)
        ]

    if not months:
        logger.error(
            "No months to run (range %04d-%02d to %04d-%02d; none in FIDE periods)",
            *args.start,
            *args.end,
        )
        return 1

    logger.info(
        "Backfill %d months from %04d-%02d to %04d-%02d (concurrency=%d)",
        len(months),
        *args.start,
        *args.end,
        args.concurrency,
    )

    if args.dry_run:
        for y, m in months:
            logger.info("  Would run: %04d-%02d", y, m)
        return 0

    client = boto3.client("stepfunctions", region_name=args.region or None)
    arn = args.state_machine_arn or get_state_machine_arn(client, STATE_MACHINE_NAME)
    if not arn:
        logger.error(
            "State machine %r not found. Deploy the stack or pass --state-machine-arn",
            STATE_MACHINE_NAME,
        )
        return 1

    pending: list[tuple[int, int]] = list(months)
    running: dict[str, RunState] = {}
    completed: list[tuple[int, int, str, str]] = []  # (y, m, status, execution_arn)
    failed: list[tuple[int, int, str, str]] = []
    total = len(months)

    def start_next() -> bool:
        if not pending or len(running) >= args.concurrency:
            return False
        y, m = pending.pop(0)
        name = f"backfill-{y:04d}-{m:02d}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        input_dict = {
            "year": y,
            "month": m,
            "run_type": "prod",
            "bucket": args.bucket,
            "override": args.override,
            "max_concurrency": args.max_concurrency,
        }
        if args.chunk_size is not None:
            input_dict["chunk_size"] = args.chunk_size
        try:
            resp = client.start_execution(
                stateMachineArn=arn,
                name=name,
                input=json.dumps(input_dict),
            )
            exec_arn = resp["executionArn"]
            running[exec_arn] = RunState(
                execution_arn=exec_arn,
                year=y,
                month=m,
                started_at=datetime.now(timezone.utc),
                status="RUNNING",
            )
            logger.info("Started %04d-%02d -> %s", y, m, exec_arn.split(":")[-1])
            return True
        except ClientError as e:
            logger.error("Failed to start %04d-%02d: %s", y, m, e)
            failed.append((y, m, str(e), ""))
            return True  # try next

    def poll_running() -> None:
        done = []
        for exec_arn, state in list(running.items()):
            try:
                desc = client.describe_execution(executionArn=exec_arn)
                status = desc["status"]
                if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                    done.append(exec_arn)
                    elapsed = (
                        datetime.now(timezone.utc) - state.started_at
                    ).total_seconds()
                    if status == "SUCCEEDED":
                        completed.append((state.year, state.month, status, exec_arn))
                        logger.info(
                            "Completed %04d-%02d (%.1f min) -> %s",
                            state.year,
                            state.month,
                            elapsed / 60,
                            status,
                        )
                    else:
                        failed.append((state.year, state.month, status, exec_arn))
                        logger.warning(
                            "Failed %04d-%02d: %s",
                            state.year,
                            state.month,
                            status,
                        )
            except ClientError as e:
                logger.warning("Poll failed for %s: %s", exec_arn, e)
        for arn in done:
            del running[arn]

    # Start initial batch
    while start_next():
        pass

    while running or pending:
        poll_running()
        # Start more if we have capacity
        while start_next():
            pass

        if running or pending:
            n_done = len(completed) + len(failed)
            # Rough estimate: remaining runs = pending + running; each ~30 min
            remaining_runs = len(pending) + len(running)
            # Serialized time for remaining at current concurrency
            est_min = (remaining_runs * AVG_RUN_MINUTES) / args.concurrency
            logger.info(
                "Progress: %d/%d done | %d running | %d pending | est. %.0f min remaining",
                n_done,
                total,
                len(running),
                len(pending),
                est_min,
            )
            if running:
                for s in running.values():
                    logger.info(
                        "  - %04d-%02d running (%s)",
                        s.year,
                        s.month,
                        s.execution_arn.split(":")[-1],
                    )
            time.sleep(POLL_INTERVAL_SEC)

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info(
        "Backfill complete: %d succeeded, %d failed", len(completed), len(failed)
    )
    for y, m, status, _ in completed:
        logger.info("  SUCCESS %04d-%02d", y, m)
    for y, m, status, exec_arn in failed:
        logger.warning("  FAILED  %04d-%02d %s %s", y, m, status, exec_arn or "")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

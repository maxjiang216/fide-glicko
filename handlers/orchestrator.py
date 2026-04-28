"""
Backfill orchestrator Lambda.

Triggered hourly by EventBridge (disabled by default — enable in AWS Console
or via `aws events enable-rule` when ready to start the historical backfill).

Scans for incomplete prod months (2006-01 to 2024-12) and starts the Step
Functions pipeline for the earliest one, subject to cooldown and DLQ logic.

State persisted at s3://{bucket}/metadata/orchestrator_state.json:
  {
    "last_updated": "ISO-8601",
    "remaining_months": ["2006-01", ...],   // hint queue; rebuilt from S3 when empty
    "last_scan": "ISO-8601",
    "deferred_months": {"2006-01": 3, ...}, // failure counts (DLQ)
    "last_processed_execution_arn": "arn:..."
  }

Execution names follow the pattern  orch-{YYYY}-{MM}-{YYYYMMDDHHmmss}
so the month can always be parsed back from the name.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .lambda_logging import configure
from s3_io import output_exists

logger = logging.getLogger(__name__)

BACKFILL_START = "2006-01"
BACKFILL_END = "2024-12"
COOLDOWN_HOURS = 2
DEFERRED_THRESHOLD = 3  # failures before a month moves to the deferred queue
STATE_KEY = "metadata/orchestrator_state.json"
COUNTRY_MONTHS_KEY = "metadata/country_months.json"
EXEC_NAME_RE = re.compile(r"^orch-(\d{4})-(\d{2})-\d{14}$")


def _active_backfill_months(bucket: str) -> list[str]:
    """
    Return sorted list of months that have at least one federation with tournament
    data, filtered to [BACKFILL_START, BACKFILL_END].

    Derived from the country_months.json lookup so we never queue months like
    2006-02 that are empty for every country (FIDE used quarterly updates pre-2009).
    Falls back to every calendar month in the range if the lookup is unavailable.
    """
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=COUNTRY_MONTHS_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        all_months: set[str] = set()
        for months in data.get("country_months", {}).values():
            all_months.update(months)
        return sorted(
            m for m in all_months if BACKFILL_START <= m <= BACKFILL_END
        )
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            logger.warning("Could not load country_months.json: %s", e)
    except Exception as e:
        logger.warning("Could not load country_months.json: %s", e)

    # Fallback: every calendar month in range
    logger.info("Falling back to all calendar months in backfill range")
    months = []
    y, mo = int(BACKFILL_START[:4]), int(BACKFILL_START[5:])
    end_y, end_mo = int(BACKFILL_END[:4]), int(BACKFILL_END[5:])
    while (y, mo) <= (end_y, end_mo):
        months.append(f"{y}-{mo:02d}")
        mo += 1
        if mo > 12:
            mo, y = 1, y + 1
    return months


def _scan_incomplete_months(bucket: str, all_months: list[str]) -> list[str]:
    """
    List all prod/*/reports/validation_report.json keys in one S3 paginated call,
    then return the months that do NOT have a validation report.
    """
    s3 = boto3.client("s3")
    found: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="prod/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/reports/validation_report.json"):
                # key looks like prod/2006-01/reports/validation_report.json
                parts = key.split("/")
                if len(parts) >= 2:
                    found.add(parts[1])
    return [m for m in all_months if m not in found]


def _load_state(bucket: str) -> dict:
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=STATE_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise
    except Exception:
        return {}


def _save_state(bucket: str, state: dict) -> None:
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=STATE_KEY,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def _parse_month_from_execution_name(name: str) -> Optional[str]:
    """Return 'YYYY-MM' from 'orch-YYYY-MM-YYYYMMDDHHmmss', or None if not parseable."""
    m = EXEC_NAME_RE.match(name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def _get_running_and_last_execution(
    state_machine_arn: str,
) -> tuple[bool, Optional[dict]]:
    """
    Return (is_running, last_completed_execution).
    last_completed_execution is the most recent non-RUNNING execution dict, or None.
    """
    sfn = boto3.client("stepfunctions")
    # Fetch recent executions (all statuses) — sorted newest first
    resp = sfn.list_executions(stateMachineArn=state_machine_arn, maxResults=20)
    executions = resp.get("executions", [])

    is_running = any(e["status"] == "RUNNING" for e in executions)
    last_completed = next((e for e in executions if e["status"] != "RUNNING"), None)
    return is_running, last_completed


def lambda_handler(event: dict, context) -> dict:
    configure()
    bucket = os.environ.get("DATA_BUCKET", "fide-glicko")
    state_machine_arn = os.environ.get("PIPELINE_STATE_MACHINE_ARN")

    if not state_machine_arn:
        logger.error("PIPELINE_STATE_MACHINE_ARN env var not set")
        return {
            "status": "error",
            "message": "PIPELINE_STATE_MACHINE_ARN not configured",
        }

    # 1. Check for running execution — nothing to do if pipeline is active
    is_running, last_exec = _get_running_and_last_execution(state_machine_arn)
    if is_running:
        logger.info("Pipeline execution is currently RUNNING — nothing to do")
        return {"status": "running"}

    # 2. Load persisted state
    state = _load_state(bucket)
    state.setdefault("remaining_months", [])
    state.setdefault("deferred_months", {})
    state.setdefault("last_scan", None)
    state.setdefault("last_processed_execution_arn", None)

    # 3. Process the last completed execution (once per unique ARN)
    if last_exec and last_exec["executionArn"] != state["last_processed_execution_arn"]:
        exec_status = last_exec["status"]
        exec_name = last_exec["name"]
        exec_month = _parse_month_from_execution_name(exec_name)
        state["last_processed_execution_arn"] = last_exec["executionArn"]

        if exec_month:
            if exec_status == "SUCCEEDED":
                # Remove from hint queue — this month is done
                if exec_month in state["remaining_months"]:
                    state["remaining_months"].remove(exec_month)
                logger.info(
                    "Execution SUCCEEDED for %s; removed from queue", exec_month
                )
            elif exec_status in ("FAILED", "TIMED_OUT", "ABORTED"):
                state["deferred_months"][exec_month] = (
                    state["deferred_months"].get(exec_month, 0) + 1
                )
                fail_count = state["deferred_months"][exec_month]
                logger.info(
                    "Execution %s for %s (failure #%d)",
                    exec_status,
                    exec_month,
                    fail_count,
                )

                # Cooldown: if the failure was recent, wait before trying again
                stopped_at = last_exec.get("stopDate")
                if stopped_at:
                    if stopped_at.tzinfo is None:
                        stopped_at = stopped_at.replace(tzinfo=timezone.utc)
                    hours_ago = (
                        datetime.now(timezone.utc) - stopped_at
                    ).total_seconds() / 3600
                    if hours_ago < COOLDOWN_HOURS:
                        logger.info(
                            "Last failure was %.1fh ago (cooldown=%.0fh) — waiting",
                            hours_ago,
                            COOLDOWN_HOURS,
                        )
                        _save_state(bucket, state)
                        return {"status": "cooldown", "hours_ago": round(hours_ago, 1)}
        else:
            logger.info(
                "Last execution '%s' was not started by orchestrator — ignoring for tracking",
                exec_name,
            )

    # 4. Determine the work queue using the hint file
    all_months = _active_backfill_months(bucket)

    if state["remaining_months"]:
        # Fast path: trust the hint queue
        queue = state["remaining_months"]
        logger.info("Using hint queue: %d months remaining", len(queue))
    else:
        # Hint queue empty or first run — do a full S3 scan to (re)build it
        logger.info("Hint queue empty; scanning S3 for incomplete months...")
        incomplete = _scan_incomplete_months(bucket, all_months)
        if not incomplete:
            logger.info("ALL DONE — no incomplete months found in S3 scan")
            state["remaining_months"] = []
            state["last_scan"] = datetime.now(timezone.utc).isoformat()
            _save_state(bucket, state)
            return {"status": "complete"}
        state["remaining_months"] = incomplete
        state["last_scan"] = datetime.now(timezone.utc).isoformat()
        logger.info(
            "S3 scan found %d incomplete months; rebuilt hint queue", len(incomplete)
        )
        queue = incomplete

    # 5. Apply DLQ: normal queue first, fall back to deferred when normal is empty
    deferred = state["deferred_months"]
    normal_queue = [m for m in queue if deferred.get(m, 0) < DEFERRED_THRESHOLD]
    deferred_queue = [m for m in queue if deferred.get(m, 0) >= DEFERRED_THRESHOLD]

    if normal_queue:
        next_month = normal_queue[0]
    elif deferred_queue:
        next_month = deferred_queue[0]
        logger.info(
            "All remaining months are deferred (≥%d failures); retrying earliest: %s",
            DEFERRED_THRESHOLD,
            next_month,
        )
    else:
        logger.info("No months eligible to run")
        _save_state(bucket, state)
        return {"status": "idle"}

    # 6. Start the Step Function execution
    year_str, month_str = next_month.split("-")
    year_int = int(year_str)
    month_int = int(month_str)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    exec_name = f"orch-{next_month}-{ts}"

    execution_input = {
        "year": year_int,
        "month": month_int,
        "run_type": "prod",
        "bucket": bucket,
        "override": False,
    }

    sfn = boto3.client("stepfunctions")
    sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=exec_name,
        input=json.dumps(execution_input),
    )
    logger.info("Started execution %s for %s", exec_name, next_month)

    _save_state(bucket, state)
    return {"status": "started", "month": next_month, "execution_name": exec_name}

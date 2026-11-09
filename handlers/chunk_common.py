"""
Shared logic for the details_chunk and reports_chunk Lambdas: resumable,
time-budgeted scraping of one ID chunk (see chunk_worker.py).

Event shape:
{
    "run_type": "prod",
    "run_name": "2024-01",
    "chunk_index": 0,
    "chunk_count": 15,
    "bucket": "fide-glicko",
    "details_rate_limit": 0.3,   # details only
    "reports_rate_limit": 0.3    # reports only
}

Returns {"remaining": N, "progress": K, ...}. The Step Function invokes the
chunk again while remaining > 0 and progress > 0. Raises FideBlocked when FIDE
blocks us (work done so far is saved first) and ValueError on a bad event.
"""

from __future__ import annotations

import logging
import time

from s3_io import build_run_base, build_s3_uri_for_run

logger = logging.getLogger(__name__)

# Stop this long before the Lambda timeout: room for one slow fetch (15s
# connect + 45s read), a block wait (30s) and writing the final part.
STOP_MARGIN_S = 150
LOCAL_BUDGET_S = 780


def rate_limit_req_s(event: dict, key: str, default: float) -> float:
    v = event.get(key, default)
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if f >= 0 else default


# Time kept back at the very end for writing the final part file.
SAVE_MARGIN_S = 30


def _remaining_s(context) -> float:
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        return context.get_remaining_time_in_millis() / 1000
    return LOCAL_BUDGET_S + STOP_MARGIN_S


def deadline_from_context(context) -> float:
    """Soft deadline: no new tournament is started after this."""
    return time.monotonic() + _remaining_s(context) - STOP_MARGIN_S


def hard_deadline_from_context(context) -> float:
    """Hard deadline: no fetch attempt (or retry) may run past this."""
    return time.monotonic() + _remaining_s(context) - SAVE_MARGIN_S


def validate_event(event: dict) -> None:
    run_type = event.get("run_type", "custom")
    if run_type not in ("prod", "custom", "test"):
        raise ValueError(
            f"run_type must be one of prod, custom, test (got {run_type!r})"
        )
    if run_type in ("prod", "custom") and not event.get("run_name"):
        raise ValueError("run_name required when run_type is prod or custom")
    if event.get("chunk_index") is None:
        raise ValueError("chunk_index is required")
    if event.get("chunk_count") is None:
        raise ValueError("chunk_count is required")


def run_chunk_lambda(kind: str, event: dict, context) -> dict:
    """kind: "details" or "reports"."""
    from chunk_worker import (
        details_flusher,
        details_processor,
        make_session,
        reports_flusher,
        reports_processor,
        run_chunk,
    )
    from coverage import done_ids, load_details
    from fide_http import AdaptiveRateLimiter, FideBlocked, PoliteFetcher
    from get_tournament_details import _read_ids_from_path

    validate_event(event)
    run_type = event.get("run_type", "custom")
    run_name = event.get("run_name")
    bucket = event.get("bucket", "fide-glicko")
    i, n = event["chunk_index"], event["chunk_count"]
    if event.get("override"):
        logger.warning(
            "override is ignored: chunks resume from stored parts. Delete parts "
            "to force a rescrape."
        )

    base = build_run_base(run_type, run_name)
    month_root = f"s3://{bucket}/{base}"
    ids_uri = build_s3_uri_for_run(
        bucket,
        run_type,
        run_name,
        "data",
        "tournament_id_chunks",
        f"ids_chunk_{i}_of_{n}.txt",
    )
    ids = [str(t) for t in _read_ids_from_path(ids_uri)]
    if not ids:
        raise ValueError(f"No tournament IDs in {ids_uri}")

    deadline = deadline_from_context(context)
    done = done_ids(bucket, base, kind)
    rate = rate_limit_req_s(event, f"{kind}_rate_limit", 0.3)
    fetcher = PoliteFetcher(
        AdaptiveRateLimiter(rate), hard_deadline=hard_deadline_from_context(context)
    )
    session = make_session()

    if kind == "details":
        process = details_processor(fetcher, session)
        flush = details_flusher(month_root)
    elif kind == "reports":
        from get_tournament_reports import details_map_from_df

        details = load_details(bucket, base)
        details_map = (
            details_map_from_df(details.to_pandas()) if details.num_rows else {}
        )
        process = reports_processor(fetcher, session)
        flush = reports_flusher(month_root, details_map)
    else:
        raise ValueError(f"kind must be details or reports (got {kind!r})")

    logger.info("Starting %s chunk %d/%d of %s at %.2f req/s", kind, i, n, base, rate)
    from metrics import chunk_summary, emit

    def summarize(chunk_result, blocked, error=None):
        # One EMF line per invocation: Logs Insights row + CloudWatch metrics.
        emit(
            chunk_summary(
                kind=kind,
                run_name=run_name,
                chunk=f"{i}_of_{n}",
                fetch_stats=fetcher.stats(),
                chunk_result=chunk_result,
                blocked=blocked,
                error=error,
            )
        )

    try:
        result = run_chunk(ids, done, process, flush, deadline=deadline)
    except FideBlocked as e:
        summarize(getattr(e, "chunk_result", None), True, str(e))
        raise
    except Exception as e:
        summarize(getattr(e, "chunk_result", None), False, f"{type(e).__name__}: {e}")
        raise
    summarize(result.to_dict(), False)
    if result.failed:
        # One record per invocation; a tournament failing in several separate
        # runs is given up on (coverage.gave_up_ids) instead of blocking the
        # month forever.
        import json

        from coverage import FAILURES_PREFIX
        from pipeline_common import part_stamp
        from s3_io import write_output

        write_output(
            json.dumps(
                [{"tournament_code": t, "error": e} for t, e in result.failed],
                indent=2,
            ),
            f"{month_root}/{FAILURES_PREFIX[kind]}{kind}_{part_stamp()}_failures.json",
        )
    return {
        "statusCode": 200,
        "success": True,
        "kind": kind,
        "ids_uri": ids_uri,
        "rate_limit": rate,
        "fetch_stats": fetcher.stats(),
        **result.to_dict(),
    }

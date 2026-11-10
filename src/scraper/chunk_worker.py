"""
Resumable, time-budgeted scraping of one chunk of tournament IDs.

Each invocation:
  1. skips every tournament already done anywhere in the month (old chunk sets
     or earlier part files; see coverage.done_ids),
  2. fetches the rest through one PoliteFetcher (rate limit, slowdown, stop on
     block),
  3. appends a part file every `flush_every` tournaments or `flush_secs`
     seconds, and always before returning or raising (FideBlocked included),
  4. stops cleanly once `deadline` passes, so a Lambda timeout never loses
     work.

It returns how many tournaments remain. The Step Function calls it again
while remaining > 0 and progress > 0; zero progress with work remaining fails
the run (ChunkStuck) instead of looping forever.

Part files (relative to the month base), written in commit order:
  details: raw/details/details_<stamp>.html.gz
           reports/tournament_details_parts/details_<stamp>_skipped.json
           data/tournament_details_parts/details_<stamp>.parquet      (commit)
  reports: raw/reports/reports_<stamp>.html.gz
           data/tournament_reports_parts/reports_<stamp>_games.parquet
           reports/tournament_reports_parts/reports_<stamp>_skipped.json
           data/tournament_reports_parts/reports_<stamp>_players.parquet (commit)
"""

from __future__ import annotations

import io
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from pipeline_common import (
    DETAILS_SKIPPABLE_ERRORS,
    SKIPPABLE_ERRORS,
    STRONG_SKIP_REASONS,
    part_stamp,
)

logger = logging.getLogger(__name__)

OK, SKIP, FAIL = "ok", "skip", "fail"


@dataclass
class Outcome:
    tid: str
    status: str  # OK | SKIP | FAIL
    result: Optional[dict] = None  # OK: row(s) source; SKIP: skipped entry
    raw: Optional[bytes] = None
    error: Optional[str] = None
    # SKIP only: False for weak reasons, which coverage accepts only once seen
    # in SKIP_CONFIRMATIONS separate parts. Such a skip is saved but still
    # counted as remaining, so the caller runs again and confirms it.
    final: bool = True


@dataclass
class ChunkResult:
    chunk_size: int
    already_done: int
    progress: int = 0
    unconfirmed: int = 0  # saved weak skips, still counted in remaining
    failed: list[tuple[str, str]] = field(default_factory=list)
    parts: list[str] = field(default_factory=list)
    stopped: str = "complete"  # complete | time_budget

    @property
    def remaining(self) -> int:
        return self.chunk_size - self.already_done - self.progress + self.unconfirmed

    def to_dict(self) -> dict:
        return {
            "chunk_size": self.chunk_size,
            "already_done": self.already_done,
            "progress": self.progress,
            "unconfirmed": self.unconfirmed,
            "remaining": self.remaining,
            "failed": len(self.failed),
            "failed_sample": [f"{t}: {e}" for t, e in self.failed[:10]],
            "parts": self.parts,
            "stopped": self.stopped,
        }


def run_chunk(
    ids: list[str],
    done: set[str],
    process_one: Callable[[str], Outcome],
    flush: Callable[[list[Outcome]], str],
    *,
    deadline: float,
    flush_every: int = 50,
    flush_secs: float = 300.0,
    clock: Callable[[], float] = time.monotonic,
) -> ChunkResult:
    """Process ids not in done; flush OK/SKIP outcomes in parts. See module doc."""
    todo = [t for t in dict.fromkeys(ids) if t not in done]
    res = ChunkResult(chunk_size=len(set(ids)), already_done=len(set(ids)) - len(todo))
    buffer: list[Outcome] = []
    last_flush = clock()

    def do_flush() -> None:
        nonlocal buffer, last_flush
        if buffer:
            res.parts.append(flush(buffer))
            res.progress += len(buffer)
            res.unconfirmed += sum(
                1 for o in buffer if o.status == SKIP and not o.final
            )
        buffer = []
        last_flush = clock()

    logger.info(
        "Chunk: %d ids, %d already done, %d to do",
        res.chunk_size,
        res.already_done,
        len(todo),
    )
    try:
        for tid in todo:
            if clock() >= deadline:
                res.stopped = "time_budget"
                logger.info("Time budget reached; stopping with work left")
                break
            out = process_one(tid)
            if out.status == FAIL:
                res.failed.append((tid, out.error or "unknown"))
                logger.warning("Not done this round: %s (%s)", tid, out.error)
            else:
                buffer.append(out)
            if len(buffer) >= flush_every or clock() - last_flush >= flush_secs:
                do_flush()
    except BaseException as exc:
        # FideBlocked or any other error: save fetched work first, then attach
        # what was done so the caller can still report progress.
        try:
            do_flush()
        finally:
            exc.chunk_result = res.to_dict()  # type: ignore[attr-defined]
        raise
    do_flush()
    logger.info("Chunk result: %s", res.to_dict())
    return res


# ---------------------------------------------------------------------------
# Details / reports adapters
# ---------------------------------------------------------------------------


def _write(path: str, content: bytes | str) -> None:
    from get_tournament_details import _write_to_path

    _write_to_path(path, content)


def details_processor(fetcher, session) -> Callable[[str], Outcome]:
    from get_tournament_details import fetch_tournament_details

    def process(tid: str) -> Outcome:
        details, error, _, raw = fetcher.fetch(
            f"details {tid}",
            lambda **kw: fetch_tournament_details(tid, session, return_raw=True, **kw),
        )
        if details is not None:
            return Outcome(
                tid,
                OK,
                {"tournament_id": tid, "success": True, "details": details},
                raw,
            )
        if error in DETAILS_SKIPPABLE_ERRORS:
            return Outcome(
                tid,
                SKIP,
                {"tournament_code": tid, "error": error},
                final=error in STRONG_SKIP_REASONS,
            )
        return Outcome(tid, FAIL, error=error)

    return process


def details_flusher(month_root: str) -> Callable[[list[Outcome]], str]:
    """month_root: s3://bucket/prod/2024-01 (or a local dir)."""
    from get_tournament_details import results_to_parquet_bytes
    from raw_utils import build_concatenated_gzip

    def flush(outs: list[Outcome]) -> str:
        stamp = part_stamp()
        name = f"details_{stamp}"
        raws = [(o.tid, o.raw) for o in outs if o.status == OK and o.raw]
        if raws:
            _write(
                f"{month_root}/raw/details/{name}.html.gz",
                build_concatenated_gzip(raws),
            )
        skipped = [o.result for o in outs if o.status == SKIP]
        if skipped:
            _write(
                f"{month_root}/reports/tournament_details_parts/{name}_skipped.json",
                json.dumps(skipped, indent=2, ensure_ascii=False),
            )
        rows = [o.result for o in outs if o.status == OK]
        part = f"{month_root}/data/tournament_details_parts/{name}.parquet"
        if rows:
            _write(part, results_to_parquet_bytes(rows))
        logger.info(
            "Wrote details part %s: %d ok, %d skipped", name, len(rows), len(skipped)
        )
        return name

    return flush


def reports_processor(fetcher, session) -> Callable[[str], Outcome]:
    from get_tournament_reports import fetch_tournament_report

    def process(code: str) -> Outcome:
        report, error, _, raw = fetcher.fetch(
            f"report {code}",
            lambda **kw: fetch_tournament_report(code, session, return_raw=True, **kw),
        )
        if report is not None:
            return Outcome(
                code, OK, {"tournament_code": code, "success": True, **report}, raw
            )
        if error in SKIPPABLE_ERRORS:
            return Outcome(
                code,
                SKIP,
                {"tournament_code": code, "error": error},
                final=error in STRONG_SKIP_REASONS,
            )
        return Outcome(code, FAIL, error=error)

    return process


def reports_flusher(
    month_root: str, details_map: dict
) -> Callable[[list[Outcome]], str]:
    from get_tournament_reports import (
        results_to_games_dataframe,
        results_to_players_dataframe,
    )
    from raw_utils import build_concatenated_gzip

    def to_bytes(df) -> bytes:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        return buf.getvalue()

    def flush(outs: list[Outcome]) -> str:
        stamp = part_stamp()
        name = f"reports_{stamp}"
        data = f"{month_root}/data/tournament_reports_parts/{name}"
        raws = [(o.tid, o.raw) for o in outs if o.status == OK and o.raw]
        if raws:
            _write(
                f"{month_root}/raw/reports/{name}.html.gz",
                build_concatenated_gzip(raws),
            )
        results = [o.result for o in outs if o.status == OK]
        if results:
            _write(
                data + "_games.parquet",
                to_bytes(results_to_games_dataframe(results, details_map=details_map)),
            )
        skipped = [o.result for o in outs if o.status == SKIP]
        if skipped:
            _write(
                f"{month_root}/reports/tournament_reports_parts/{name}_skipped.json",
                json.dumps(skipped, indent=2, ensure_ascii=False),
            )
        if results:
            # Commit marker: written last.
            _write(
                data + "_players.parquet",
                to_bytes(results_to_players_dataframe(results)),
            )
        logger.info(
            "Wrote reports part %s: %d ok, %d skipped", name, len(results), len(skipped)
        )
        return name

    return flush


def make_session():
    import requests

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

"""
Small shared definitions with no third-party dependencies.

Kept separate from coverage.py (which needs pyarrow) so the lightweight ZIP
Lambdas (e.g. tournaments) can use the resumable worker without pulling in
pyarrow/pandas.
"""

from __future__ import annotations

import re
import secrets
from datetime import datetime, timezone

# Reasons a tournament legitimately has no report (FIDE page structure, not
# network). Must match get_tournament_reports.SKIPPABLE_ERRORS.
SKIPPABLE_ERRORS = frozenset(
    ["report_updated_or_replaced", "no data found", "no players found"]
)
# Details page exists but has no details table.
DETAILS_SKIPPABLE_ERRORS = frozenset(["no data found"])
# FIDE's explicit "report was updated or replaced" wording: trusted on sight.
STRONG_SKIP_REASONS = frozenset(["report_updated_or_replaced"])
# Any other skip reason must be seen in this many separate runs to count,
# and the sightings must be at least SKIP_CONFIRM_GAP_S apart, so one bad
# period at FIDE (e.g. a backend outage serving empty pages) cannot confirm
# itself. Until then a recent sighting is provisional: not refetched, but the
# month is not complete.
SKIP_CONFIRMATIONS = 2
SKIP_CONFIRM_GAP_S = 24 * 3600


# A tournament that fails (non-skip, non-block error) in this many separate
# worker invocations is given up on: reported as an exception in the coverage
# summary instead of blocking its month forever.
GIVE_UP_AFTER_RUNS = 3


def part_stamp() -> str:
    """Unique, time-sortable part name: 20261109T143000_a1b2c3."""
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{now}_{secrets.token_hex(3)}"


_STAMP_RE = re.compile(r"(\d{8}T\d{6})_[0-9a-f]{6}")


def stamp_time(name: str):
    """UTC epoch seconds of the part_stamp in a file name, or None (legacy
    chunk files, written by the old pipeline long before any part file)."""
    m = _STAMP_RE.search(name or "")
    if not m:
        return None
    return (
        datetime.strptime(m.group(1), "%Y%m%dT%H%M%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )


def parse_ids(text: str) -> list[str]:
    """Tournament IDs from tournament_ids.txt contents, order kept, duplicates dropped."""
    seen: dict[str, None] = {}
    for line in text.splitlines():
        tid = line.strip()
        if tid:
            seen.setdefault(tid, None)
    return list(seen)

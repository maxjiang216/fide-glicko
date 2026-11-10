"""
Small shared definitions with no third-party dependencies.

Kept separate from coverage.py (which needs pyarrow) so the lightweight ZIP
Lambdas (e.g. tournaments) can use the resumable worker without pulling in
pyarrow/pandas.
"""

from __future__ import annotations

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
# Any other skip reason must be seen in this many separate runs to count.
SKIP_CONFIRMATIONS = 2


# A tournament that fails (non-skip, non-block error) in this many separate
# worker invocations is given up on: reported as an exception in the coverage
# summary instead of blocking its month forever.
GIVE_UP_AFTER_RUNS = 3


def part_stamp() -> str:
    """Unique, time-sortable part name: 20261109T143000_a1b2c3."""
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{now}_{secrets.token_hex(3)}"


def parse_ids(text: str) -> list[str]:
    """Tournament IDs from tournament_ids.txt contents, order kept, duplicates dropped."""
    seen: dict[str, None] = {}
    for line in text.splitlines():
        tid = line.strip()
        if tid:
            seen.setdefault(tid, None)
    return list(seen)

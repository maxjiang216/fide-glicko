"""
Resumable tournament-list fetch for one month (the Tournaments pipeline step).

One request per federation, sequential, through the shared PoliteFetcher
(rate limit, slowdown on strain, FideBlocked on block). Each federation's raw
JSON response is saved as its own part:

    {base}/data/tournament_list_parts/{FED}.json

so a timeout, block or retry never refetches a federation already saved.
tournament_ids.txt is written only once every federation in the list has a
part (parts are then deleted, see run_tournament_list): a month can never start from a partial ID list. If an older
tournament_ids.txt exists and differs, it is kept as
tournament_ids_prev_<stamp>.txt and the differences are logged.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from chunk_worker import FAIL, OK, Outcome, run_chunk
from coverage import parse_ids, part_stamp

logger = logging.getLogger(__name__)

PARTS_DIR = "data/tournament_list_parts"


def fetch_federation(
    code: str, year: int, month: int, session, *, max_attempts: int = 1
) -> tuple[Optional[str], Optional[str], int, Optional[bytes]]:
    """
    Fetch one federation's tournament list (FIDE's a_tournaments.php JSON).
    Returns (text, error, attempts, raw) like the other fetch functions;
    text is the JSON body on success. max_attempts is accepted for
    PoliteFetcher compatibility; retries happen there.
    """
    import requests

    from get_tournaments import TOURNAMENTS_URL

    url = f"{TOURNAMENTS_URL}?country={code}&period={year}-{month:02d}-01"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    try:
        resp = session.get(url, headers=headers, timeout=(15, 45))
    except requests.exceptions.Timeout as e:
        return None, f"timeout: {e}", 1, None
    except requests.exceptions.ConnectionError as e:
        return None, f"connection error: {e}", 1, None
    except requests.exceptions.RequestException as e:
        return None, f"network error: {e}", 1, None
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}", 1, None
    # FIDE prefixes this JSON with a UTF-8 BOM; json.loads rejects it.
    text = resp.content.decode("utf-8-sig", errors="replace")
    if text.strip().startswith("<"):
        return (
            None,
            f"server returned HTML instead of JSON ({len(text)} chars)",
            1,
            None,
        )
    try:
        json.loads(text)
    except ValueError as e:
        return None, f"invalid JSON: {e}", 1, None
    return text, None, 1, resp.content


def parse_part(code: str, text: str) -> list:
    """Tournament objects from one federation's saved JSON."""
    from get_tournaments import parse_tournament_row

    data = json.loads(text.lstrip("\ufeff"))
    rows = data.get("data") if isinstance(data, dict) else None
    out = []
    for row in rows or []:
        t = parse_tournament_row(row, code)
        if t:
            out.append(t)
    return out


def run_tournament_list(
    *,
    month_root: str,
    year: int,
    month: int,
    federations: list[str],
    fetcher,
    session,
    deadline: float,
    list_parts: callable,
    read_text: callable,
    write: callable,
    exists: callable,
    delete_parts: callable,
) -> dict:
    """
    Fetch missing federation parts, then finalize tournament_ids.txt if all
    are present. I/O is injected (list_parts() -> set of saved codes;
    read_text(path); write(path, content); exists(path); delete_parts(codes)).
    Parts are deleted after a successful finalize (their content is archived in
    raw/tournaments.json.gz), so the next refresh fetches fresh lists.

    Raises RuntimeError if finalizing would produce zero tournaments (FIDE
    returning empty is treated as a failure, not an empty month).
    """
    if not federations:
        raise RuntimeError(
            f"No federations to query for {year}-{month:02d}; refusing to write "
            "an empty tournament list"
        )
    parts_root = f"{month_root}/{PARTS_DIR}"
    done = list_parts() & set(federations)

    def process(code: str) -> Outcome:
        text, error, _, _ = fetcher.fetch(
            f"tournaments {code}",
            lambda **kw: fetch_federation(code, year, month, session, **kw),
        )
        if text is not None:
            return Outcome(code, OK, {"text": text})
        return Outcome(code, FAIL, error=error)

    def flush(outs: list[Outcome]) -> str:
        for o in outs:
            write(f"{parts_root}/{o.tid}.json", o.result["text"])
        return f"{len(outs)} federations"

    res = run_chunk(
        federations, done, process, flush, deadline=deadline, flush_every=10
    )
    out = res.to_dict()
    if res.remaining > 0:
        return {**out, "finalized": False}
    result = finalize(month_root, federations, read_text, write, exists)
    delete_parts(federations)
    return {**out, **result}


def finalize(month_root, federations, read_text, write, exists) -> dict:
    from raw_utils import build_concatenated_gzip

    parts_root = f"{month_root}/{PARTS_DIR}"
    texts = {code: read_text(f"{parts_root}/{code}.json") for code in federations}
    seen: dict[str, object] = {}
    for code in federations:
        for t in parse_part(code, texts[code]):
            seen.setdefault(t.tournament_id, t)
    if not seen:
        raise RuntimeError(
            f"{len(federations)} federations returned 0 tournaments; FIDE may be "
            "blocking or returning empty. Not writing tournament_ids.txt"
        )
    tournaments = sorted(seen.values(), key=lambda t: t.tournament_id)
    ids = [t.tournament_id for t in tournaments]
    ids_path = f"{month_root}/data/tournament_ids.txt"

    changes = {}
    if exists(ids_path):
        old = parse_ids(read_text(ids_path))
        added = sorted(set(ids) - set(old))
        dropped = sorted(set(old) - set(ids))
        changes = {
            "previous_count": len(old),
            "added": len(added),
            "dropped": len(dropped),
            "added_sample": added[:20],
            "dropped_sample": dropped[:20],
        }
        if added or dropped:
            backup = f"{month_root}/data/tournament_ids_prev_{part_stamp()}.txt"
            write(backup, "\n".join(old) + "\n")
            changes["previous_backup"] = backup
            logger.warning(
                "Tournament list changed: %d added, %d dropped (old list kept at %s)",
                len(added),
                len(dropped),
                backup,
            )

    write(
        f"{month_root}/raw/tournaments.json.gz",
        build_concatenated_gzip([(c, texts[c].encode("utf-8")) for c in federations]),
    )
    sample = [
        {
            "tournament_id": t.tournament_id,
            "name": t.name,
            "location": t.location,
            "time_control": t.time_control,
            "start_date": t.start_date,
            "end_date": t.end_date,
            "federation": t.federation,
        }
        for t in tournaments[:50]
    ]
    write(
        f"{month_root}/sample/tournament_ids_sample.json",
        json.dumps(sample, indent=2, ensure_ascii=False),
    )
    # Written last: its presence means the list is complete.
    write(ids_path, "\n".join(ids) + "\n")
    logger.info("Wrote %d tournament IDs to %s", len(ids), ids_path)
    return {"finalized": True, "tournaments": len(ids), "ids_path": ids_path, **changes}

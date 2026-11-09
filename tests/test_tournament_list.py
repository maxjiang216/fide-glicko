"""Tests for the resumable tournament-list step (tournament_list.py) and the
no-partial-list guard in get_tournaments.scrape_month."""

from __future__ import annotations

import asyncio
import json

import pytest

import get_tournaments
import tournament_list
from fide_http import FideBlocked
from tournament_list import PARTS_DIR, run_tournament_list

ROOT = "s3://b/prod/2024-01"


def _body(*ids):
    return json.dumps(
        {"data": [[i, f"T{i}", "City", "s", "2024-01-05", "2024-01-09"] for i in ids]}
    )


class Store:
    def __init__(self):
        self.files: dict[str, str] = {}
        self.deleted: list[str] = []

    def io(self):
        parts = f"{ROOT}/{PARTS_DIR}/"
        return {
            "list_parts": lambda: {
                k[len(parts) : -5] for k in self.files if k.startswith(parts)
            },
            "read_text": lambda p: self.files[p],
            "write": lambda p, c: self.files.__setitem__(
                p, c if isinstance(c, str) else c.decode("latin-1")
            ),
            "exists": lambda p: p in self.files,
            "delete_parts": lambda codes: [
                self.files.pop(f"{parts}{c}.json", None) for c in codes
            ],
        }


class StubFetcher:
    """Calls the fetch directly; stands in for PoliteFetcher."""

    def fetch(self, label, fn):
        return fn(max_attempts=1)


def _patch_fetch(monkeypatch, responses: dict, calls: list):
    def fake(code, year, month, session, *, max_attempts=1):
        calls.append(code)
        r = responses[code]
        if isinstance(r, Exception):
            raise r
        if r.startswith("ERR:"):
            return None, r[4:], 1, None
        return r, None, 1, r.encode()

    monkeypatch.setattr(tournament_list, "fetch_federation", fake)


def _run(store, feds, deadline=float("inf")):
    return run_tournament_list(
        month_root=ROOT,
        year=2024,
        month=1,
        federations=feds,
        fetcher=StubFetcher(),
        session=None,
        deadline=deadline,
        **store.io(),
    )


def test_complete_run_writes_ids_and_deletes_parts(monkeypatch):
    calls = []
    _patch_fetch(
        monkeypatch, {"AAA": _body("300", "100"), "BBB": _body("100", "200")}, calls
    )
    store = Store()
    res = _run(store, ["AAA", "BBB"])
    assert res["finalized"] and res["tournaments"] == 3
    assert store.files[f"{ROOT}/data/tournament_ids.txt"] == "100\n200\n300\n"
    assert not any(PARTS_DIR in k for k in store.files)
    assert f"{ROOT}/raw/tournaments.json.gz" in store.files


def test_failed_federation_never_writes_partial_list(monkeypatch):
    calls = []
    _patch_fetch(monkeypatch, {"AAA": _body("1"), "BBB": "ERR:HTTP 404"}, calls)
    store = Store()
    res = _run(store, ["AAA", "BBB"])
    assert not res["finalized"] and res["remaining"] == 1 and res["progress"] == 1
    assert f"{ROOT}/data/tournament_ids.txt" not in store.files
    # Next call resumes: only BBB is fetched.
    calls.clear()
    _patch_fetch(monkeypatch, {"AAA": _body("1"), "BBB": _body("2")}, calls)
    res = _run(store, ["AAA", "BBB"])
    assert calls == ["BBB"] and res["finalized"]
    assert store.files[f"{ROOT}/data/tournament_ids.txt"] == "1\n2\n"


def test_block_keeps_saved_federations(monkeypatch):
    calls = []
    _patch_fetch(monkeypatch, {"AAA": _body("1"), "BBB": FideBlocked("blocked")}, calls)
    store = Store()
    with pytest.raises(FideBlocked):
        _run(store, ["AAA", "BBB"])
    assert f"{ROOT}/{PARTS_DIR}/AAA.json" in store.files
    assert f"{ROOT}/data/tournament_ids.txt" not in store.files


def test_zero_tournaments_is_an_error(monkeypatch):
    _patch_fetch(monkeypatch, {"AAA": "{}", "BBB": _body()}, [])
    store = Store()
    with pytest.raises(RuntimeError, match="0 tournaments"):
        _run(store, ["AAA", "BBB"])
    assert f"{ROOT}/data/tournament_ids.txt" not in store.files


def test_changed_list_keeps_previous(monkeypatch):
    _patch_fetch(monkeypatch, {"AAA": _body("1", "3")}, [])
    store = Store()
    store.files[f"{ROOT}/data/tournament_ids.txt"] = "1\n2\n"
    res = _run(store, ["AAA"])
    assert res["added"] == 1 and res["dropped"] == 1
    assert store.files[res["previous_backup"]] == "1\n2\n"
    assert store.files[f"{ROOT}/data/tournament_ids.txt"] == "1\n3\n"


def test_no_federations_is_an_error():
    with pytest.raises(RuntimeError, match="No federations"):
        _run(Store(), [])


def test_scrape_month_does_not_write_partial_list(tmp_path, monkeypatch):
    feds = tmp_path / "federations.csv"
    feds.write_text("code,name\nAAA,A\nBBB,B\n")

    async def fake(session, semaphore, code, name, year, month, *a):
        if code == "BBB":
            return (code, name, [], "HTTP 503", None)
        return (code, name, [], None, "{}")

    monkeypatch.setattr(get_tournaments, "fetch_federation_tournaments", fake)
    out = tmp_path / "data" / "tournament_ids.txt"
    tournaments, n_errors, n_total = asyncio.run(
        get_tournaments.scrape_month(2024, 1, feds, out, save_raw=False)
    )
    assert n_errors >= 1 and not out.exists()


def test_fetch_federation_strips_bom():
    class Resp:
        status_code = 200
        content = "﻿".encode("utf-8") + _body("7").encode()

    class Session:
        def get(self, *a, **kw):
            return Resp()

    text, err, _, _ = tournament_list.fetch_federation("AAA", 2024, 1, Session())
    assert err is None
    assert [t.tournament_id for t in tournament_list.parse_part("AAA", text)] == ["7"]


def test_handler_write_adapter_argument_order(monkeypatch):
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import handlers.tournaments as h

    written = []
    monkeypatch.setattr(
        h, "write_output", lambda content, path: written.append((path, content))
    )
    monkeypatch.setattr(h.boto3, "client", lambda *a, **k: None)
    h._s3_io("b", "prod/2024-01")["write"]("s3://b/x.json", "{}")
    assert written == [("s3://b/x.json", "{}")]

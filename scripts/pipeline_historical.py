#!/usr/bin/env python3
"""
Historical pipeline run: sample tournaments across months to detect format changes.

Uses discovered available months (from FIDE periods API for federation RUS - no browser),
samples N random federations per month with 1 tournament each, runs details+reports,
and validates against the player list. Writes a report with anomalies and time estimates.

Month discovery: a_tournaments_panel.php?country=RUS&periods_tab=1 returns JSON with
frl_publish (YYYY-MM-01). If that fails, falls back to fixed range 2002-04 to now.

Usage:
  python scripts/pipeline_historical.py [--countries 10] [--limit-months 5]

  # Quick smoke run (2 months, 3 countries):
  python scripts/pipeline_historical.py --limit-months 2 --countries 3
"""

import argparse
import asyncio
import csv
import json
import random
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import aiohttp
import pandas as pd

# Paths
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPER = REPO_ROOT / "src" / "scraper"
PERIODS_URL = "https://ratings.fide.com/a_tournaments_panel.php"
REFERENCE_FED = "RUS"  # Long history for month discovery


@dataclass
class MonthResult:
    """Result for one month."""
    year: int
    month: int
    period: str
    tournaments_fetched: int
    tournaments_with_details: int
    tournaments_with_reports: int
    games_count: int
    players_in_reports: int
    players_missing_from_list: int
    sample_missing_ids: list[str] = field(default_factory=list)
    player_list_anomalies: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    tournaments_failed: bool = False
    details_failed: bool = False
    reports_failed: bool = False
    tournaments_error: str = ""
    details_error: str = ""
    reports_error: str = ""
    elapsed_seconds: float = 0.0


async def fetch_available_periods(session: aiohttp.ClientSession, code: str = REFERENCE_FED) -> list[tuple[int, int]]:
    """Fetch available (year, month) from periods API. Returns sorted desc (most recent first)."""
    url = f"{PERIODS_URL}?country={code}&periods_tab=1"
    headers = {"X-Requested-With": "XMLHttpRequest"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
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

    # Sort descending (most recent first)
    periods.sort(reverse=True)
    return periods


def fetch_tournaments_via_script(
    federations: list[tuple[str, str]],
    year: int,
    month: int,
    count: int,
    test_dir: Path,
    data_dir: str,
) -> tuple[list[str], bool, str]:
    """
    Use get_tournaments.py with a limited federations subset. Same logic as src/scraper.
    Returns (tournament_ids, success, error_msg).
    """
    # Use more federations than count to handle feds with 0 tournaments for the month
    subset_size = min(len(federations), max(count * 3, 30))
    shuffled = list(federations)
    random.shuffle(shuffled)
    subset = shuffled[:subset_size]

    # Write temp federations CSV
    temp_fed_path = test_dir / f"federations_{year}_{month:02d}.csv"
    temp_fed_path.parent.mkdir(parents=True, exist_ok=True)
    with open(temp_fed_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code", "name"])
        for code, name in subset:
            w.writerow([code, name])

    month_key = f"{year}_{month:02d}"
    ids_file = test_dir / f"tournament_ids_{month_key}"
    json_file = test_dir.parent / "tournament_ids_json" / f"tournament_ids_{month_key}.json"

    ok, err = run_cmd([
        sys.executable,
        str(SCRAPER / "get_tournaments.py"),
        "--year", str(year),
        "--month", str(month),
        "--federations", str(temp_fed_path),
        "--output", str(ids_file),
        "--quiet",
    ])

    if not ok:
        return [], False, err

    # Read result: 1 tournament per federation, up to count total
    tournament_ids: list[str] = []
    if json_file.exists():
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)
        by_fed: dict[str, list[str]] = {}
        for t in data:
            tid = str(t.get("tournament_id", "")).strip()
            fed = t.get("federation", "")
            if tid and tid.isdigit() and fed:
                by_fed.setdefault(fed, []).append(tid)
        for fed, tids in by_fed.items():
            tournament_ids.append(tids[0])  # 1 per federation
            if len(tournament_ids) >= count:
                break
    elif ids_file.exists():
        # Fallback: read IDs file, take first count
        with open(ids_file, encoding="utf-8") as f:
            tournament_ids = [line.strip() for line in f if line.strip()][:count]

    return tournament_ids, True, ""


def read_federations(path: Path) -> list[tuple[str, str]]:
    """Read (code, name) from federations CSV."""
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [(r["code"].strip(), r["name"].strip()) for r in reader if r.get("code")]


def check_player_list_anomalies(players_path: Path) -> list[str]:
    """Check player list for data anomalies. Returns list of anomaly descriptions."""
    anomalies: list[str] = []
    if not players_path.exists():
        return ["Player list file not found"]

    df = pd.read_parquet(players_path)
    if "id" not in df.columns:
        anomalies.append("Missing 'id' column")
    else:
        null_ids = df["id"].isna().sum()
        if null_ids > 0:
            anomalies.append(f"{null_ids} rows with null id")

    if "fed" in df.columns:
        null_fed = df["fed"].isna().sum()
        if null_fed > 0:
            anomalies.append(f"{null_fed} rows with null fed")
        invalid_fed = df[df["fed"].notna() & (df["fed"].str.len() != 3)]
        if len(invalid_fed) > 0:
            anomalies.append(f"{len(invalid_fed)} rows with fed not 3 chars")

    if "name" in df.columns:
        empty_name = (df["name"].isna() | (df["name"].astype(str).str.strip() == "")).sum()
        if empty_name > 0:
            anomalies.append(f"{empty_name} rows with empty name")

    return anomalies


def validate_players_in_list(players_path: Path, reports_path: Path) -> tuple[int, int, list[str]]:
    """Returns (players_in_reports, missing_count, sample_missing_ids)."""
    if not players_path.exists() or not reports_path.exists():
        return 0, 0, []

    pl = pd.read_parquet(players_path)
    rp = pd.read_parquet(reports_path)
    white_col = "white_player_id" if "white_player_id" in rp.columns else "white_id"
    black_col = "black_player_id" if "black_player_id" in rp.columns else "black_id"
    if "id" not in pl.columns or white_col not in rp.columns or black_col not in rp.columns:
        return 0, 0, []

    player_ids = set(pl["id"].astype(str).dropna())
    white = set(rp[white_col].astype(str).dropna())
    black = set(rp[black_col].astype(str).dropna())
    report_ids = white | black
    report_ids.discard("")
    report_ids.discard("nan")
    missing = report_ids - player_ids
    return len(report_ids), len(missing), sorted(missing)[:20]


def run_cmd(cmd: list[str], timeout: int = 600) -> tuple[bool, str]:
    """Run command, return (success, stderr_or_msg)."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(REPO_ROOT))
        if r.returncode != 0:
            return False, (r.stderr or r.stdout or f"exit {r.returncode}").strip()[:500]
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline across historical months")
    parser.add_argument(
        "--countries",
        type=int,
        default=10,
        help="Number of random countries to sample per month (default 10)",
    )
    parser.add_argument(
        "--limit-months",
        type=int,
        default=0,
        help="Limit to N months (0 = all, default 0)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Data directory (default data)",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="",
        help="Report output path (default: data/pipeline_test_report.txt)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default 42)",
    )
    parser.add_argument(
        "--start-from",
        type=str,
        default="",
        help="Resume from this month (YYYY-MM). Process this month and older only. E.g. --start-from 2007-01",
    )
    args = parser.parse_args()
    random.seed(args.seed)

    report_path = Path(args.report) if args.report else REPO_ROOT / args.data_dir / "pipeline_test_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("FIDE Pipeline Historical Run")
    print("=" * 80)
    sys.stdout.flush()

    # 1. Get federations
    fed_path = REPO_ROOT / args.data_dir / "federations.csv"
    if not fed_path.exists():
        print("Fetching federations...")
        ok, err = run_cmd([sys.executable, str(SCRAPER / "get_federations.py"), "--directory", args.data_dir])
        if not ok:
            print(f"ERROR: {err}")
            return 1
        print("  Done.")
    federations = read_federations(fed_path)
    if not federations:
        print("ERROR: No federations loaded")
        return 1
    print(f"Federations: {len(federations)}")

    # 2. Get player list
    players_path = REPO_ROOT / "src" / "data" / "players_list.parquet"
    if not players_path.exists():
        print("Fetching player list...")
        ok, err = run_cmd([sys.executable, str(SCRAPER / "get_player_list.py")], timeout=300)
        if not ok:
            print(f"ERROR: {err}")
            return 1
        print("  Done.")
    print(f"Player list: {players_path}")

    # 3. Discover months
    print("Discovering available months...")
    async def _discover():
        async with aiohttp.ClientSession() as session:
            return await fetch_available_periods(session)

    periods = asyncio.run(_discover())
    if not periods:
        print("ERROR: Could not fetch periods. Using fallback range (2002-04 to now).")
        from datetime import date
        today = date.today()
        periods = []
        for y in range(today.year, 2001, -1):
            for m in range(12, 0, -1):
                if y == today.year and m > today.month:
                    continue
                if y == 2002 and m < 4:
                    continue
                periods.append((y, m))

    # Skip future months (API may return them but data not yet available)
    today = datetime.now().date()
    periods = [(y, m) for y, m in periods if y < today.year or (y == today.year and m <= today.month)]
    print(f"  Found {len(periods)} periods (most recent first, future months excluded)")

    if args.start_from:
        try:
            sy, sm = map(int, args.start_from.split("-"))
            if 1 <= sm <= 12:
                periods = [(y, m) for y, m in periods if (y, m) <= (sy, sm)]
                print(f"  Starting from {args.start_from} ({len(periods)} months)")
        except ValueError:
            print(f"  Warning: invalid --start-from {args.start_from}, ignoring")

    if args.limit_months > 0:
        periods = periods[: args.limit_months]
        print(f"  Limited to first {args.limit_months} months")

    # 4. Run pipeline for each month
    results: list[MonthResult] = []
    total_start = time.perf_counter()
    times_per_month: list[float] = []

    for idx, (year, month) in enumerate(periods):
        t0 = time.perf_counter()
        month_key = f"{year}_{month:02d}"
        print(f"\n--- [{idx+1}/{len(periods)}] {year}-{month:02d} ---", flush=True)

        res = MonthResult(year=year, month=month, period=f"{year}-{month:02d}", tournaments_fetched=0,
                          tournaments_with_details=0, tournaments_with_reports=0, games_count=0,
                          players_in_reports=0, players_missing_from_list=0)

        # Fetch tournaments via get_tournaments.py (same logic as src/scraper, limited federations)
        test_dir = REPO_ROOT / args.data_dir / "pipeline_test"
        test_dir.mkdir(parents=True, exist_ok=True)
        print(f"  Running get_tournaments.py ({args.countries} federations)...", flush=True)
        tournament_ids, ok_fetch, err_fetch = fetch_tournaments_via_script(
            federations, year, month, args.countries, test_dir, args.data_dir
        )
        res.tournaments_fetched = len(tournament_ids)
        print(f"  Found {len(tournament_ids)} tournaments", flush=True)

        if not ok_fetch:
            res.tournaments_failed = True
            res.tournaments_error = err_fetch
            res.errors.append(f"get_tournaments: {err_fetch}")
            results.append(res)
            res.elapsed_seconds = time.perf_counter() - t0
            times_per_month.append(res.elapsed_seconds)
            print(f"  FAILED get_tournaments: {err_fetch[:120]}", flush=True)
            continue

        if not tournament_ids:
            res.errors.append("No tournaments found for sampled federations")
            results.append(res)
            res.elapsed_seconds = time.perf_counter() - t0
            times_per_month.append(res.elapsed_seconds)
            print(f"  No tournaments, skipping", flush=True)
            continue

        # Write sampled IDs (1 per fed, up to count) for details/reports - overwrite full output
        ids_file = test_dir / f"tournament_ids_{month_key}"
        ids_file.write_text("\n".join(tournament_ids), encoding="utf-8")
        print(f"  Using {len(tournament_ids)} tournament IDs (1 per federation)", flush=True)

        details_out = test_dir / "details" / f"{month_key}.parquet"
        details_out.parent.mkdir(parents=True, exist_ok=True)
        reports_base = test_dir / "reports" / month_key
        reports_base.parent.mkdir(parents=True, exist_ok=True)
        reports_games_out = test_dir / "reports" / f"{month_key}_games.parquet"

        # Run details
        print(f"  Running get_tournament_details.py...", flush=True)
        ok, err = run_cmd([
            sys.executable, str(SCRAPER / "get_tournament_details.py"),
            "--input", str(ids_file),
            "--output", str(details_out.with_suffix("")),
            "--rate-limit", "0.5",
        ])
        if not ok:
            res.details_failed = True
            res.details_error = err
            res.errors.append(f"get_tournament_details: {err}")
            print(f"  FAILED get_tournament_details: {err[:120]}", flush=True)
        elif details_out.exists():
            df = pd.read_parquet(details_out)
            res.tournaments_with_details = int((df["success"] == True).sum())
            print(f"  get_tournament_details OK ({res.tournaments_with_details} tournaments)", flush=True)

        # Run reports
        print(f"  Running get_tournament_reports.py...", flush=True)
        details_path_arg = str(details_out) if details_out.exists() else ""
        reports_cmd = [
            sys.executable, str(SCRAPER / "get_tournament_reports.py"),
            "--input", str(ids_file),
            "--output", str(reports_base),
            "--no-samples",
        ]
        if details_path_arg:
            reports_cmd.extend(["--details-path", details_path_arg])
        ok, err = run_cmd(reports_cmd)
        if not ok:
            res.reports_failed = True
            res.reports_error = err
            res.errors.append(f"get_tournament_reports: {err}")
            print(f"  FAILED get_tournament_reports: {err[:120]}", flush=True)
        elif reports_games_out.exists():
            rp = pd.read_parquet(reports_games_out)
            tc_col = "tournament_id" if "tournament_id" in rp.columns else "tournament_code"
            if tc_col in rp.columns:
                res.tournaments_with_reports = rp[tc_col].nunique()
            res.games_count = len(rp)
            print(f"  get_tournament_reports OK ({res.games_count} games)", flush=True)

        # Validate: players in list
        print(f"  Validating players...", flush=True)
        res.players_in_reports, res.players_missing_from_list, res.sample_missing_ids = validate_players_in_list(
            players_path, reports_games_out
        )

        # Player list anomalies (check once per month - same list)
        res.player_list_anomalies = check_player_list_anomalies(players_path)

        res.elapsed_seconds = time.perf_counter() - t0
        times_per_month.append(res.elapsed_seconds)
        results.append(res)

        # Time estimate
        avg_time = sum(times_per_month) / len(times_per_month)
        remaining = len(periods) - idx - 1
        est_remaining = avg_time * remaining
        print(f"  Summary: {res.tournaments_fetched} fetched, {res.tournaments_with_details} details, {res.tournaments_with_reports} reports", flush=True)
        print(f"  Games: {res.games_count} | Players: {res.players_in_reports} in reports, {res.players_missing_from_list} missing from list", flush=True)
        if res.errors:
            print(f"  FAILURES: {res.errors}", flush=True)
        print(f"  Elapsed: {res.elapsed_seconds:.1f}s | Est. remaining: {est_remaining/60:.1f}m", flush=True)

    total_elapsed = time.perf_counter() - total_start

    # 5. Write report
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("FIDE Pipeline Historical Report\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Countries per month: {args.countries}\n")
        f.write(f"Total months: {len(periods)}\n")
        f.write(f"Total time: {total_elapsed/60:.1f} minutes\n")
        f.write("=" * 80 + "\n\n")

        anomalies_section = []
        for r in results:
            failed_steps = []
            if r.tournaments_failed:
                failed_steps.append("get_tournaments")
            if r.details_failed:
                failed_steps.append("get_tournament_details")
            if r.reports_failed:
                failed_steps.append("get_tournament_reports")
            has_issues = (
                failed_steps
                or r.players_missing_from_list > 0
                or r.player_list_anomalies
                or r.errors
                or (r.tournaments_fetched > 0 and r.tournaments_with_reports == 0)
            )
            if has_issues:
                parts = []
                if failed_steps:
                    parts.append(f"FAILED({','.join(failed_steps)})")
                parts.append(f"missing_players={r.players_missing_from_list}")
                if r.player_list_anomalies:
                    parts.extend(r.player_list_anomalies)
                parts.extend(r.errors)
                anomalies_section.append(f"{r.period}: " + "; ".join(parts))

        f.write("FAILURES BY SCRIPT\n")
        f.write("-" * 80 + "\n")
        tournaments_failures = [(r.period, r.tournaments_error) for r in results if r.tournaments_failed]
        details_failures = [(r.period, r.details_error) for r in results if r.details_failed]
        reports_failures = [(r.period, r.reports_error) for r in results if r.reports_failed]
        if tournaments_failures:
            f.write(f"get_tournaments failed ({len(tournaments_failures)} months):\n")
            for period, err in tournaments_failures:
                f.write(f"  {period}: {err[:150]}\n")
        else:
            f.write("get_tournaments: none\n")
        if details_failures:
            f.write(f"get_tournament_details failed ({len(details_failures)} months):\n")
            for period, err in details_failures:
                f.write(f"  {period}: {err[:150]}\n")
        else:
            f.write("get_tournament_details: none\n")
        if reports_failures:
            f.write(f"get_tournament_reports failed ({len(reports_failures)} months):\n")
            for period, err in reports_failures:
                f.write(f"  {period}: {err[:150]}\n")
        else:
            f.write("get_tournament_reports: none\n")

        f.write("\nANOMALIES SUMMARY\n")
        f.write("-" * 80 + "\n")
        if anomalies_section:
            for line in anomalies_section:
                f.write(line + "\n")
        else:
            f.write("None\n")

        f.write("\n\nDETAILED RESULTS (per month)\n")
        f.write("-" * 80 + "\n")
        for r in results:
            f.write(f"\n{r.period}:\n")
            f.write(f"  Tournaments: fetched={r.tournaments_fetched} details={r.tournaments_with_details} reports={r.tournaments_with_reports}\n")
            f.write(f"  Games: {r.games_count} | Players: {r.players_in_reports} in reports, {r.players_missing_from_list} missing from list\n")
            if r.tournaments_failed or r.details_failed or r.reports_failed:
                f.write(f"  FAILED SCRIPTS:\n")
                if r.tournaments_failed:
                    f.write(f"    get_tournaments: {r.tournaments_error}\n")
                if r.details_failed:
                    f.write(f"    get_tournament_details: {r.details_error}\n")
                if r.reports_failed:
                    f.write(f"    get_tournament_reports: {r.reports_error}\n")
            if r.sample_missing_ids:
                f.write(f"  Sample missing IDs: {r.sample_missing_ids[:10]}\n")
            if r.player_list_anomalies:
                f.write(f"  Player list anomalies: {r.player_list_anomalies}\n")
            if r.errors:
                f.write(f"  All errors: {r.errors}\n")

    print("\n" + "=" * 80)
    print(f"Report written to: {report_path}")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())

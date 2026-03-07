#!/usr/bin/env python3
"""
Full FIDE data pipeline for a given month.

Runs the complete pipeline:
1. Get federations -> data/federations.csv
2. Get tournaments (for each federation) -> data/tournament_ids/YYYY_MM
3. Get tournament details -> data/tournament_details/YYYY_MM.parquet
4. Get tournament reports (games) -> data/tournament_reports/YYYY_MM.parquet
5. Get player list -> src/data/players_list.parquet

Optionally runs validation to compare:
- Player list vs reports: which player IDs in reports are missing from the player list
- Tournament details vs reports: event_code alignment, player counts, date consistency
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Paths relative to repo root
SCRAPER_DIR = Path(__file__).resolve().parent.parent / "src" / "scraper"


def run(cmd: list[str], cwd: Path, desc: str) -> bool:
    """Run a command; return True on success."""
    print("\n" + "=" * 80)
    print(desc)
    print("-" * 80)
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        print(f"ERROR: {desc} failed (exit code {result.returncode})")
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run full FIDE data pipeline for a month",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year (e.g. 2025)",
    )
    parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Month 1-12",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--skip-federations",
        action="store_true",
        help="Skip federation fetch (use existing data/federations.csv)",
    )
    parser.add_argument(
        "--skip-player-list",
        action="store_true",
        help="Skip player list download (use existing src/data/players_list.parquet)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step (player list vs reports, details vs reports)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit tournaments (for details/reports) for testing (0 = no limit)",
    )
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        print("Error: month must be 1-12")
        return 1

    base_dir = Path(__file__).resolve().parent.parent
    month_key = f"{args.year}_{args.month:02d}"
    data_dir = args.data_dir

    # Step 1: Federations
    if not args.skip_federations:
        if not run(
            [
                sys.executable,
                str(SCRAPER_DIR / "get_federations.py"),
                "--directory",
                data_dir,
            ],
            base_dir,
            "STEP 1: Fetch federations",
        ):
            return 1

    # Step 2: Tournaments
    if not run(
        [
            sys.executable,
            str(SCRAPER_DIR / "get_tournaments.py"),
            "--year",
            str(args.year),
            "--month",
            str(args.month),
            "--federations",
            f"{data_dir}/federations.csv",
            "--output",
            f"{data_dir}/tournament_ids/{month_key}",
        ],
        base_dir,
        "STEP 2: Get tournaments (by federation)",
    ):
        return 1

    # Step 3: Tournament details
    details_cmd = [
        sys.executable,
        str(SCRAPER_DIR / "get_tournament_details.py"),
        "--year",
        str(args.year),
        "--month",
        str(args.month),
        "--data-dir",
        args.data_dir,
    ]
    if args.limit > 0:
        details_cmd.extend(["--limit", str(args.limit)])
    if not run(details_cmd, base_dir, "STEP 3: Get tournament details"):
        return 1

    # Step 4: Tournament reports
    reports_cmd = [
        sys.executable,
        str(SCRAPER_DIR / "get_tournament_reports.py"),
        "--year",
        str(args.year),
        "--month",
        str(args.month),
        "--data-dir",
        args.data_dir,
    ]
    if args.limit > 0:
        reports_cmd.extend(["--limit", str(args.limit)])
    if not run(reports_cmd, base_dir, "STEP 4: Get tournament reports (games)"):
        return 1

    # Step 5: Player list
    if not args.skip_player_list:
        if not run(
            [sys.executable, str(SCRAPER_DIR / "get_player_list.py")],
            base_dir,
            "STEP 5: Get player list",
        ):
            return 1

    # Step 6: Validation
    if not args.skip_validation:
        if not run(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "validate_pipeline.py"),
                "--year",
                str(args.year),
                "--month",
                str(args.month),
                "--data-dir",
                args.data_dir,
            ],
            base_dir,
            "STEP 6: Validate (player list vs reports, details vs reports)",
        ):
            return 1

    print("\n" + "=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

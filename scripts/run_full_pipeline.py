#!/usr/bin/env python3
"""
Full FIDE data pipeline for a given month.

Runs the complete pipeline in order:
1. Get federations -> data/federations.csv
2. Get tournaments -> data/tournament_ids/YYYY_MM
3. Get tournament details -> data/tournament_details/YYYY_MM.parquet
4. Get player list -> src/data/players_list.parquet
5. Get tournament reports -> data/tournament_reports/YYYY_MM_players.parquet, _games.parquet

Optionally runs validation (player list vs reports, details vs reports).

Use --test for a quick smoke run with limited sampling on the slower steps.
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SCRAPER_DIR = Path(__file__).resolve().parent.parent / "src" / "scraper"
SCRIPTS_DIR = Path(__file__).resolve().parent

# Test mode defaults: limit sampling for slow scripts
TEST_LIMIT_TOURNAMENTS = 5
TEST_LIMIT_DETAILS = 5
TEST_LIMIT_REPORTS = 5


def run(cmd: list[str], cwd: Path, desc: str) -> bool:
    """Run a command; return True on success."""
    logger.info("")
    logger.info("=" * 80)
    logger.info("%s", desc)
    logger.info("-" * 80)
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        logger.error("%s failed (exit code %d)", desc, result.returncode)
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
        "--test",
        action="store_true",
        help="Test run with limited sampling: limit tournaments/details/reports to %d each, skip JSON/CSV samples"
        % TEST_LIMIT_REPORTS,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Override limit for details/reports (overrides --test defaults when set)",
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
        "--no-validation",
        dest="no_validation",
        action="store_true",
        help="Pass --no-validation to tournament reports (skip pairing/player checks)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Pass --quiet to scripts that support it",
    )
    parser.add_argument(
        "--override",
        "-o",
        action="store_true",
        help="Overwrite existing outputs (federations, player list) instead of skipping",
    )
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        logger.error("Month must be 1-12")
        return 1

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base_dir = Path(__file__).resolve().parent.parent
    month_key = f"{args.year}_{args.month:02d}"
    data_dir = args.data_dir

    # Determine limits for test mode
    limit_details = args.limit or (TEST_LIMIT_DETAILS if args.test else 0)
    limit_reports = args.limit or (TEST_LIMIT_REPORTS if args.test else 0)
    limit_tournaments = args.limit or (TEST_LIMIT_TOURNAMENTS if args.test else 0)

    if args.test:
        logger.info(
            "[TEST MODE] Limiting: tournaments=%d, details=%d, reports=%d",
            limit_tournaments,
            limit_details,
            limit_reports,
        )

    common_quiet = ["--quiet"] if args.quiet else []

    # Step 1: Federations
    if not args.skip_federations:
        fed_cmd = [
            sys.executable,
            str(SCRAPER_DIR / "get_federations.py"),
            "--directory",
            data_dir,
        ] + common_quiet
        if args.override:
            fed_cmd.append("--override")
        if not run(fed_cmd, base_dir, "STEP 1: Fetch federations"):
            return 1

    # Step 2: Tournaments
    tournaments_cmd = [
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
    ] + common_quiet
    if limit_tournaments > 0:
        tournaments_cmd.extend(["--limit", str(limit_tournaments)])
    if not run(tournaments_cmd, base_dir, "STEP 2: Get tournaments (by federation)"):
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
        data_dir,
    ]
    if limit_details > 0:
        details_cmd.extend(["--limit", str(limit_details)])
    if not run(details_cmd, base_dir, "STEP 3: Get tournament details"):
        return 1

    # Step 4: Player list (before reports so default validation has data)
    if not args.skip_player_list:
        player_cmd = [
            sys.executable,
            str(SCRAPER_DIR / "get_player_list.py"),
        ] + common_quiet
        if args.override:
            player_cmd.append("--override")
        if not run(player_cmd, base_dir, "STEP 4: Get player list"):
            return 1

    # Step 5: Tournament reports
    reports_cmd = [
        sys.executable,
        str(SCRAPER_DIR / "get_tournament_reports.py"),
        "--year",
        str(args.year),
        "--month",
        str(args.month),
        "--data-dir",
        data_dir,
    ]
    if limit_reports > 0:
        reports_cmd.extend(["--limit", str(limit_reports)])
    if args.test:
        reports_cmd.append("--no-samples")
    if args.no_validation:
        reports_cmd.append("--no-validation")
    if not run(reports_cmd, base_dir, "STEP 5: Get tournament reports (games)"):
        return 1

    # Step 6: Validation
    if not args.skip_validation:
        validate_cmd = [
            sys.executable,
            str(SCRIPTS_DIR.parent / "exploratory" / "validate_pipeline.py"),
            "--year",
            str(args.year),
            "--month",
            str(args.month),
            "--data-dir",
            data_dir,
        ]
        if args.quiet:
            validate_cmd.append("--quiet")
        if not run(
            validate_cmd,
            base_dir,
            "STEP 6: Validate (player list vs reports, details vs reports)",
        ):
            return 1

    logger.info("")
    logger.info("=" * 80)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

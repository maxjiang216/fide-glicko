#!/usr/bin/env python3
"""
Validate FIDE pipeline data consistency.

Compares:
1. Player list vs reports: Which player IDs in reports are missing from the player list?
2. Tournament details vs reports: event_code alignment, player counts, date consistency

Writes a report file by default. Can be run standalone or invoked by run_full_pipeline.py.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Ensure scraper modules are importable
_SCRAPER_DIR = Path(__file__).resolve().parent.parent / "src" / "scraper"
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

import pandas as pd

from validate_pipeline import validate_details_vs_reports, validate_player_list_vs_reports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _format_report(
    base: Path,
    args,
    pl_result: dict,
    dt_result: dict,
    has_error: bool,
) -> tuple[str, dict]:
    """Build human-readable report string and machine-readable dict."""
    month_key = f"{args.year}_{args.month:02d}"
    players_path = (
        Path(args.players_path)
        if args.players_path
        else base / "src" / "data" / "players_list.parquet"
    )
    details_path = base / args.data_dir / "tournament_details" / f"{month_key}.parquet"
    reports_path = base / args.data_dir / "tournament_reports" / f"{month_key}_games.parquet"

    lines = [
        "=" * 80,
        "FIDE Pipeline Validation",
        "=" * 80,
        f"Year: {args.year}, Month: {args.month}",
        f"Details: {details_path}",
        f"Reports: {reports_path}",
        f"Players: {players_path}",
        "",
        "-" * 80,
        "1. Player list vs reports",
        "-" * 80,
    ]
    if "error" in pl_result:
        lines.append(f"  ERROR: {pl_result['error']}")
    else:
        lines.append(f"  Players in list: {pl_result['total_in_player_list']}")
        lines.append(f"  Unique players in reports: {pl_result['total_in_reports']}")
        lines.append(f"  Missing from player list: {pl_result['missing_in_player_list']}")
        if pl_result["missing_in_player_list"] > 0:
            lines.append(f"  Sample missing IDs: {pl_result['sample_missing']}")

    lines.extend([
        "",
        "-" * 80,
        "2. Tournament details vs reports",
        "-" * 80,
    ])
    if "error" in dt_result:
        lines.append(f"  ERROR: {dt_result['error']}")
    else:
        lines.append(f"  Tournaments in details: {dt_result['details_tournaments']}")
        lines.append(f"  Tournaments in reports: {dt_result['reports_tournaments']}")
        lines.append(f"  In reports, not in details: {dt_result['in_reports_not_details']}")
        if dt_result["in_reports_not_details"] > 0:
            lines.append(f"    Sample: {dt_result['sample_in_reports_not_details']}")
        lines.append(f"  In details, not in reports: {dt_result['in_details_not_reports']}")
        if dt_result["in_details_not_reports"] > 0:
            lines.append(f"    Sample: {dt_result['sample_in_details_not_reports']}")
        lines.append(f"  Player count mismatches: {dt_result['player_count_mismatches']}")
        if dt_result["player_count_mismatches"] > 0:
            for m in dt_result["sample_count_mismatches"]:
                lines.append(
                    f"    {m['tournament_code']}: details={m['details_count']} "
                    f"reports={m['reports_count']} (diff={m['diff']})"
                )
        lines.append(f"  Date consistency issues: {dt_result['date_issues']}")
        if dt_result["date_issues"] > 0:
            for d in dt_result["sample_date_issues"]:
                lines.append(f"    {d['tournament_code']}: {d['issue']}")

    lines.extend([
        "",
        "=" * 80,
        "Validation found issues (see above)" if has_error else "Validation completed - no issues found",
        "=" * 80,
    ])
    text = "\n".join(lines)

    report_dict = {
        "year": args.year,
        "month": args.month,
        "has_issues": has_error,
        "player_list_vs_reports": pl_result,
        "details_vs_reports": dt_result,
    }
    return text, report_dict


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate FIDE pipeline data consistency"
    )
    parser.add_argument("--year", type=int, required=True, help="Year")
    parser.add_argument("--month", type=int, required=True, help="Month 1-12")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Base data directory",
    )
    parser.add_argument(
        "--local-root",
        type=str,
        default="data",
        help="Local bucket root for run structure",
    )
    parser.add_argument(
        "--run-type",
        type=str,
        choices=("prod", "custom", "test"),
        default=None,
        help="Run type; with --run-name uses run path structure",
    )
    parser.add_argument(
        "--run-name",
        type=str,
        default=None,
        help="Run name (e.g. 2024-01)",
    )
    parser.add_argument(
        "--players-path",
        type=str,
        default="",
        help="Override player list path (default: src/data/players_list.parquet)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="",
        help="Report output path (default: data/validation_reports/YYYY_MM.txt)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also write machine-readable JSON report (same path with .json extension)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Reduce log output",
    )
    args = parser.parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = Path(__file__).resolve().parent.parent
    month_key = f"{args.year}_{args.month:02d}"
    run_name = args.run_name or month_key.replace("_", "-")

    # Paths
    if args.run_type:
        from s3_io import build_local_path_for_run, resolve_latest_players_list_local
        players_path = (
            Path(args.players_path)
            if args.players_path
            else resolve_latest_players_list_local(base / args.local_root)
        )
        if players_path is None:
            players_path = base / "src" / "data" / "players_list.parquet"
        details_path = base / build_local_path_for_run(
            args.local_root, args.run_type, run_name, "data", "tournament_details.parquet"
        )
        reports_path = base / build_local_path_for_run(
            args.local_root, args.run_type, run_name, "data", "tournament_reports_games.parquet"
        )
    else:
        players_path = (
            Path(args.players_path)
            if args.players_path
            else base / "src" / "data" / "players_list.parquet"
        )
        details_path = base / args.data_dir / "tournament_details" / f"{month_key}.parquet"
        reports_path = base / args.data_dir / "tournament_reports" / f"{month_key}_games.parquet"

    logger.info("Validating year=%s month=%s", args.year, args.month)

    has_error = False

    # 1. Player list vs reports
    pl_result = validate_player_list_vs_reports(players_path, reports_path)
    if "error" in pl_result:
        logger.error("Player list vs reports: %s", pl_result["error"])
        has_error = True
    elif pl_result["missing_in_player_list"] > 0:
        has_error = True

    # 2. Details vs reports
    dt_result = validate_details_vs_reports(details_path, reports_path)
    if "error" in dt_result:
        logger.error("Details vs reports: %s", dt_result["error"])
        has_error = True
    elif (
        dt_result["player_count_mismatches"] > 0
        or dt_result["date_issues"] > 0
    ):
        has_error = True

    # Build and write report
    text, report_dict = _format_report(base, args, pl_result, dt_result, has_error)

    output_path = Path(args.output) if args.output else base / args.data_dir / "validation_reports" / f"{month_key}.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    logger.info("Wrote validation report to %s", output_path)

    if args.json:
        json_path = output_path.with_suffix(".json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2, default=str)
        logger.info("Wrote JSON report to %s", json_path)

    if has_error:
        logger.warning("Validation found issues; see report at %s", output_path)
    else:
        logger.info("Validation completed - no issues found")

    return 0


if __name__ == "__main__":
    sys.exit(main())

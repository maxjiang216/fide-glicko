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
import re
import sys
from pathlib import Path

# Ensure scraper modules (s3_io) are importable when using run structure
_SCRAPER_DIR = Path(__file__).resolve().parent.parent / "src" / "scraper"
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_date(s) -> str | None:
    """Parse date string to ISO YYYY-MM-DD or return None."""
    if pd.isna(s) or not str(s).strip():
        return None
    s = str(s).strip()
    # Already ISO?
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Try common formats
    m = re.match(r"(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    m = re.match(r"(\d{1,2})[.\-](\d{1,2})[.\-](\d{4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def validate_player_list_vs_reports(
    players_path: Path,
    reports_path: Path,
    *,
    max_sample: int = 20,
) -> dict:
    """
    Compare player IDs in reports with player list.
    Returns summary dict with errors, missing_count, sample missing IDs.
    """
    if not players_path.exists():
        return {"error": f"Player list not found: {players_path}"}
    if not reports_path.exists():
        return {"error": f"Reports not found: {reports_path}"}

    players_df = pd.read_parquet(players_path)
    reports_df = pd.read_parquet(reports_path)

    if "id" not in players_df.columns:
        return {"error": f"Player list missing 'id' column: {list(players_df.columns)}"}
    white_col = "white_player_id" if "white_player_id" in reports_df.columns else "white_id"
    black_col = "black_player_id" if "black_player_id" in reports_df.columns else "black_id"
    if white_col not in reports_df.columns or black_col not in reports_df.columns:
        return {
            "error": f"Reports missing {white_col}/{black_col}: {list(reports_df.columns)}"
        }

    player_ids = set(players_df["id"].astype(str).dropna())
    white_ids = set(reports_df[white_col].astype(str).dropna())
    black_ids = set(reports_df[black_col].astype(str).dropna())
    report_ids = white_ids | black_ids

    missing = report_ids - player_ids
    return {
        "total_in_player_list": len(player_ids),
        "total_in_reports": len(report_ids),
        "missing_in_player_list": len(missing),
        "sample_missing": sorted(missing)[:max_sample],
    }


def validate_details_vs_reports(
    details_path: Path,
    reports_path: Path,
    *,
    max_sample: int = 20,
) -> dict:
    """
    Compare tournament details with reports.
    Returns summary with: event_code alignment, player count mismatches, date issues.
    """
    if not details_path.exists():
        return {"error": f"Details not found: {details_path}"}
    if not reports_path.exists():
        return {"error": f"Reports not found: {reports_path}"}

    details_df = pd.read_parquet(details_path)
    reports_df = pd.read_parquet(reports_path)

    ec_col = "event_code" if "event_code" in details_df.columns else "id"
    if ec_col not in details_df.columns:
        return {"error": f"Details missing event_code/id: {list(details_df.columns)}"}
    tc_col = "tournament_id" if "tournament_id" in reports_df.columns else "tournament_code"
    if tc_col not in reports_df.columns:
        return {"error": f"Reports missing {tc_col}: {list(reports_df.columns)}"}
    white_col = "white_player_id" if "white_player_id" in reports_df.columns else "white_id"
    black_col = "black_player_id" if "black_player_id" in reports_df.columns else "black_id"
    date_col = "round_date" if "round_date" in reports_df.columns else "date"

    # Successful details only
    success_details = details_df[details_df["success"] == True].copy()
    success_details["event_code_str"] = success_details[ec_col].astype(str)

    report_codes = set(reports_df[tc_col].astype(str).dropna())
    detail_codes = set(
        str(c) for c in success_details[ec_col].dropna() if str(c).strip()
    )

    in_reports_not_details = report_codes - detail_codes
    in_details_not_reports = detail_codes - report_codes

    # Player count: details number_of_players vs actual unique players in reports
    count_mismatches = []
    for tc in report_codes:
        if tc not in detail_codes:
            continue
        sub = reports_df[reports_df[tc_col] == tc]
        report_players = set(sub[white_col].astype(str)) | set(sub[black_col].astype(str))
        report_players.discard("")
        report_players.discard("nan")
        report_count = len(report_players)

        rows = success_details[success_details["event_code_str"] == tc]
        if len(rows) == 0:
            continue
        row = rows.iloc[0]
        detail_count_raw = row.get("n_players") or row.get("number_of_players")
        try:
            detail_count = int(float(detail_count_raw)) if pd.notna(detail_count_raw) and str(detail_count_raw).strip() else None
        except (ValueError, TypeError):
            detail_count = None

        if detail_count is not None and report_count != detail_count:
            count_mismatches.append(
                {
                    "tournament_code": tc,
                    "details_count": detail_count,
                    "reports_count": report_count,
                    "diff": report_count - detail_count,
                }
            )

    # Date consistency: min/max date in reports vs details start/end
    date_issues = []
    for tc in list(report_codes & detail_codes)[:500]:  # limit for speed
        rep_dates = reports_df[reports_df[tc_col] == tc][date_col].dropna()
        if len(rep_dates) == 0:
            continue
        rep_min = str(rep_dates.min())[:10]
        rep_max = str(rep_dates.max())[:10]

        rows = success_details[success_details["event_code_str"] == tc]
        if len(rows) == 0:
            continue
        row = rows.iloc[0]
        start_iso = _parse_date(row.get("start_date"))
        end_iso = _parse_date(row.get("end_date"))

        if start_iso and rep_min < start_iso:
            date_issues.append(
                {"tournament_code": tc, "issue": "report_min < details_start", "report_min": rep_min, "details_start": start_iso}
            )
        if end_iso and rep_max > end_iso:
            date_issues.append(
                {"tournament_code": tc, "issue": "report_max > details_end", "report_max": rep_max, "details_end": end_iso}
            )

    return {
        "details_tournaments": len(detail_codes),
        "reports_tournaments": len(report_codes),
        "in_reports_not_details": len(in_reports_not_details),
        "sample_in_reports_not_details": sorted(in_reports_not_details)[:max_sample],
        "in_details_not_reports": len(in_details_not_reports),
        "sample_in_details_not_reports": sorted(in_details_not_reports)[:max_sample],
        "player_count_mismatches": len(count_mismatches),
        "sample_count_mismatches": count_mismatches[:max_sample],
        "date_issues": len(date_issues),
        "sample_date_issues": date_issues[:max_sample],
    }


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
        from s3_io import build_local_path_for_run
        players_path = (
            Path(args.players_path)
            if args.players_path
            else base / build_local_path_for_run(
                args.local_root, args.run_type, run_name, "data", "players_list.parquet"
            )
        )
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

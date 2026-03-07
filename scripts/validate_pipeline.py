#!/usr/bin/env python3
"""
Validate FIDE pipeline data consistency.

Compares:
1. Player list vs reports: Which player IDs in reports are missing from the player list?
2. Tournament details vs reports: event_code alignment, player counts, date consistency

Can be run standalone (after pipeline has run) or invoked by run_full_pipeline.py.
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


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
    if "white_id" not in reports_df.columns or "black_id" not in reports_df.columns:
        return {
            "error": f"Reports missing white_id/black_id: {list(reports_df.columns)}"
        }

    player_ids = set(players_df["id"].astype(str).dropna())
    white_ids = set(reports_df["white_id"].astype(str).dropna())
    black_ids = set(reports_df["black_id"].astype(str).dropna())
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

    if "event_code" not in details_df.columns:
        return {"error": f"Details missing event_code: {list(details_df.columns)}"}
    if "tournament_code" not in reports_df.columns:
        return {"error": f"Reports missing tournament_code: {list(reports_df.columns)}"}

    # Successful details only
    success_details = details_df[details_df["success"] == True].copy()
    success_details["event_code_str"] = success_details["event_code"].astype(str)

    report_codes = set(reports_df["tournament_code"].astype(str).dropna())
    detail_codes = set(
        str(c) for c in success_details["event_code"].dropna() if str(c).strip()
    )

    in_reports_not_details = report_codes - detail_codes
    in_details_not_reports = detail_codes - report_codes

    # Player count: details number_of_players vs actual unique players in reports
    count_mismatches = []
    for tc in report_codes:
        if tc not in detail_codes:
            continue
        report_players = set(
            reports_df[reports_df["tournament_code"] == tc]["white_id"].astype(str)
        ) | set(
            reports_df[reports_df["tournament_code"] == tc]["black_id"].astype(str)
        )
        report_players.discard("")
        report_players.discard("nan")
        report_count = len(report_players)

        rows = success_details[success_details["event_code_str"] == tc]
        if len(rows) == 0:
            continue
        row = rows.iloc[0]
        detail_count_raw = row["number_of_players"]
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
        rep_dates = reports_df[reports_df["tournament_code"] == tc]["date"].dropna()
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
        "--players-path",
        type=str,
        default="",
        help="Override player list path (default: src/data/players_list.parquet)",
    )
    args = parser.parse_args()

    base = Path(__file__).resolve().parent.parent
    month_key = f"{args.year}_{args.month:02d}"

    # Paths
    players_path = (
        Path(args.players_path)
        if args.players_path
        else base / "src" / "data" / "players_list.parquet"
    )
    details_path = base / args.data_dir / "tournament_details" / f"{month_key}.parquet"
    reports_path = base / args.data_dir / "tournament_reports" / f"{month_key}.parquet"

    print("=" * 80)
    print("FIDE Pipeline Validation")
    print("=" * 80)
    print(f"Year: {args.year}, Month: {args.month}")
    print(f"Details: {details_path}")
    print(f"Reports: {reports_path}")
    print(f"Players: {players_path}")
    print()

    has_error = False

    # 1. Player list vs reports
    print("-" * 80)
    print("1. Player list vs reports")
    print("-" * 80)
    pl_result = validate_player_list_vs_reports(players_path, reports_path)
    if "error" in pl_result:
        print(f"  ERROR: {pl_result['error']}")
        has_error = True
    else:
        print(f"  Players in list: {pl_result['total_in_player_list']}")
        print(f"  Unique players in reports: {pl_result['total_in_reports']}")
        print(f"  Missing from player list: {pl_result['missing_in_player_list']}")
        if pl_result["missing_in_player_list"] > 0:
            print(f"  Sample missing IDs: {pl_result['sample_missing']}")
            has_error = True

    # 2. Details vs reports
    print()
    print("-" * 80)
    print("2. Tournament details vs reports")
    print("-" * 80)
    dt_result = validate_details_vs_reports(details_path, reports_path)
    if "error" in dt_result:
        print(f"  ERROR: {dt_result['error']}")
        has_error = True
    else:
        print(f"  Tournaments in details: {dt_result['details_tournaments']}")
        print(f"  Tournaments in reports: {dt_result['reports_tournaments']}")
        print(f"  In reports, not in details: {dt_result['in_reports_not_details']}")
        if dt_result["in_reports_not_details"] > 0:
            print(f"    Sample: {dt_result['sample_in_reports_not_details']}")
        print(f"  In details, not in reports: {dt_result['in_details_not_reports']}")
        if dt_result["in_details_not_reports"] > 0:
            print(f"    Sample: {dt_result['sample_in_details_not_reports']}")
        print(f"  Player count mismatches: {dt_result['player_count_mismatches']}")
        if dt_result["player_count_mismatches"] > 0:
            for m in dt_result["sample_count_mismatches"]:
                print(f"    {m['tournament_code']}: details={m['details_count']} reports={m['reports_count']} (diff={m['diff']})")
            has_error = True
        print(f"  Date consistency issues: {dt_result['date_issues']}")
        if dt_result["date_issues"] > 0:
            for d in dt_result["sample_date_issues"]:
                print(f"    {d['tournament_code']}: {d['issue']}")
            has_error = True

    print()
    print("=" * 80)
    if has_error:
        print("Validation found issues (see above)")
    else:
        print("Validation completed - no issues found")
    print("=" * 80)

    # Exit 0 - validation is informational; pipeline still succeeds
    return 0


if __name__ == "__main__":
    sys.exit(main())

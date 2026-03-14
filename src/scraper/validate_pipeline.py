"""
Validate FIDE pipeline data consistency.

Compares:
1. Player list vs reports: Which player IDs in reports are missing from the player list?
2. Tournament details vs reports: event_code alignment, player counts, date consistency
"""

import json
import re
from pathlib import Path

import pandas as pd


def _parse_date(s) -> str | None:
    """Parse date string to ISO YYYY-MM-DD or return None."""
    if pd.isna(s) or not str(s).strip():
        return None
    s = str(s).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
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
    players_path: str | Path,
    reports_path: str | Path,
    *,
    max_sample: int = 20,
) -> dict:
    """
    Compare player IDs in reports with player list.
    Returns summary dict with errors, missing_count, sample missing IDs.
    """
    players_path = Path(players_path)
    reports_path = Path(reports_path)
    if not players_path.exists():
        return {"error": f"Player list not found: {players_path}"}
    if not reports_path.exists():
        return {"error": f"Reports not found: {reports_path}"}

    players_df = pd.read_parquet(players_path)
    reports_df = pd.read_parquet(reports_path)

    if "id" not in players_df.columns:
        return {"error": f"Player list missing 'id' column: {list(players_df.columns)}"}
    white_col = (
        "white_player_id" if "white_player_id" in reports_df.columns else "white_id"
    )
    black_col = (
        "black_player_id" if "black_player_id" in reports_df.columns else "black_id"
    )
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
    details_path: str | Path,
    reports_path: str | Path,
    *,
    max_sample: int = 20,
) -> dict:
    """
    Compare tournament details with reports.
    Returns summary with: event_code alignment, player count mismatches, date issues.
    """
    details_path = Path(details_path)
    reports_path = Path(reports_path)
    if not details_path.exists():
        return {"error": f"Details not found: {details_path}"}
    if not reports_path.exists():
        return {"error": f"Reports not found: {reports_path}"}

    details_df = pd.read_parquet(details_path)
    reports_df = pd.read_parquet(reports_path)

    ec_col = "event_code" if "event_code" in details_df.columns else "id"
    if ec_col not in details_df.columns:
        return {"error": f"Details missing event_code/id: {list(details_df.columns)}"}
    tc_col = (
        "tournament_id" if "tournament_id" in reports_df.columns else "tournament_code"
    )
    if tc_col not in reports_df.columns:
        return {"error": f"Reports missing {tc_col}: {list(reports_df.columns)}"}
    white_col = (
        "white_player_id" if "white_player_id" in reports_df.columns else "white_id"
    )
    black_col = (
        "black_player_id" if "black_player_id" in reports_df.columns else "black_id"
    )
    date_col = "round_date" if "round_date" in reports_df.columns else "date"

    success_details = details_df[details_df["success"] == True].copy()
    success_details["event_code_str"] = success_details[ec_col].astype(str)

    report_codes = set(reports_df[tc_col].astype(str).dropna())
    detail_codes = set(
        str(c) for c in success_details[ec_col].dropna() if str(c).strip()
    )

    in_reports_not_details = report_codes - detail_codes
    in_details_not_reports = detail_codes - report_codes

    count_mismatches = []
    for tc in report_codes:
        if tc not in detail_codes:
            continue
        sub = reports_df[reports_df[tc_col] == tc]
        report_players = set(sub[white_col].astype(str)) | set(
            sub[black_col].astype(str)
        )
        report_players.discard("")
        report_players.discard("nan")
        report_count = len(report_players)

        rows = success_details[success_details["event_code_str"] == tc]
        if len(rows) == 0:
            continue
        row = rows.iloc[0]
        detail_count_raw = row.get("n_players") or row.get("number_of_players")
        try:
            detail_count = (
                int(float(detail_count_raw))
                if pd.notna(detail_count_raw) and str(detail_count_raw).strip()
                else None
            )
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

    date_issues = []
    for tc in list(report_codes & detail_codes)[:500]:
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
                {
                    "tournament_code": tc,
                    "issue": "report_min < details_start",
                    "report_min": rep_min,
                    "details_start": start_iso,
                }
            )
        if end_iso and rep_max > end_iso:
            date_issues.append(
                {
                    "tournament_code": tc,
                    "issue": "report_max > details_end",
                    "report_max": rep_max,
                    "details_end": end_iso,
                }
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


def run(
    bucket: str,
    run_type: str,
    run_name: str | None,
    *,
    quiet: bool = False,
) -> dict:
    """
    Validate pipeline data for a run. Downloads from S3, runs validation, uploads report.

    All paths inferred from run_type and run_name:
    - details: {base}/data/tournament_details.parquet
    - reports: {base}/data/tournament_reports_games.parquet
    - players: latest in {bucket}/player_lists/data/

    Args:
        bucket: S3 bucket.
        run_type: prod | custom | test.
        run_name: Required for prod/custom. Ignored for test.
        quiet: Reduce log output.

    Returns:
        Dict with report_uri, has_issues, player_list_vs_reports, details_vs_reports.
    """
    import logging
    import tempfile

    from s3_io import (
        build_run_base,
        build_s3_uri_for_run,
        download_to_file,
        output_exists,
        resolve_latest_players_list_uri,
        write_output,
    )

    logger = logging.getLogger(__name__)
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = build_run_base(run_type, run_name)

    details_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_details.parquet"
    )
    reports_uri = build_s3_uri_for_run(
        bucket, run_type, run_name, "data", "tournament_reports_games.parquet"
    )
    players_uri = resolve_latest_players_list_uri(bucket)

    if not players_uri:
        raise RuntimeError(
            "No player list found in player_lists/data/; run player_list Lambda first"
        )
    if not output_exists(details_uri):
        raise RuntimeError(f"Details not found: {details_uri} (run merge_chunks first)")
    if not output_exists(reports_uri):
        raise RuntimeError(f"Reports not found: {reports_uri} (run merge_chunks first)")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        players_path = download_to_file(players_uri, tmp_path / "players.parquet")
        details_path = download_to_file(details_uri, tmp_path / "details.parquet")
        reports_path = download_to_file(reports_uri, tmp_path / "reports.parquet")

        pl_result = validate_player_list_vs_reports(players_path, reports_path)
        dt_result = validate_details_vs_reports(details_path, reports_path)

    has_issues = False
    if "error" in pl_result:
        has_issues = True
    elif pl_result.get("missing_in_player_list", 0) > 0:
        has_issues = True
    if "error" in dt_result:
        has_issues = True
    elif (
        dt_result.get("player_count_mismatches", 0) > 0
        or dt_result.get("date_issues", 0) > 0
    ):
        has_issues = True

    report_dict = {
        "run_type": run_type,
        "run_name": run_name or "",
        "has_issues": has_issues,
        "player_list_vs_reports": pl_result,
        "details_vs_reports": dt_result,
    }

    report_uri = f"s3://{bucket}/{base}/reports/validation_report.json"
    write_output(json.dumps(report_dict, indent=2, default=str), report_uri)

    logger.info(
        "Validation report written to %s (has_issues=%s)", report_uri, has_issues
    )

    return {
        "report_uri": report_uri,
        "has_issues": has_issues,
        "player_list_vs_reports": pl_result,
        "details_vs_reports": dt_result,
    }

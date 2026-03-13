#!/usr/bin/env python3
"""
FIDE Tournament Details Scraper

Scrapes tournament details from FIDE website for a list of tournament IDs.
Supports rate limiting, retries, checkpoints, and progress tracking.
"""

import argparse
import gzip
import io
import json
import logging
import os
import random
import signal
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# State for graceful shutdown
_shutdown_state = {}


class RateLimiter:
    """Enforces minimum spacing between requests (no bursting)."""

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second
        self.last_request = 0.0

    def wait(self):
        """Wait until enough time has passed since the last request."""
        now = time.perf_counter()
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.perf_counter()

    def get_rate(self) -> float:
        return 1.0 / self.min_interval


# Optional S3 support (used by run() when paths are S3 URIs)
def _is_s3(path: str) -> bool:
    try:
        from s3_io import is_s3_path

        return is_s3_path(path)
    except ImportError:
        return path.strip().lower().startswith("s3://")


def _compress_gzip(data: bytes, level: int = 9) -> bytes:
    """Compress with gzip level 9. Same as players/tournaments raw."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as z:
        z.write(data)
    return buf.getvalue()


def _raw_base_from_output_path(output_path: str) -> Optional[str]:
    """Derive raw/details base from output_path. Returns None if not derivable."""
    if "/data/tournament_details_chunks/" in output_path:
        return output_path.replace(
            "/data/tournament_details_chunks/", "/raw/details/"
        ).rstrip("/")
    return None


def _write_to_path(path: str, content: bytes | str) -> None:
    """Write content to path (local or S3)."""
    if _is_s3(path):
        from s3_io import write_output

        write_output(content, path)
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            p.write_text(content, encoding="utf-8")
        else:
            p.write_bytes(content)


def _read_ids_from_path(path: str) -> List[str]:
    """Read tournament IDs from a file (local or S3)."""
    if _is_s3(path):
        from s3_io import download_to_file

        local_path = Path(tempfile.gettempdir()) / "tournament_ids.txt"
        download_to_file(path, local_path)
        path = str(local_path)
    return read_tournament_ids(path)


def format_duration(seconds: float) -> str:
    """Format duration in a human-readable way."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def read_tournament_ids(file_path: str) -> List[str]:
    """Read tournament IDs from a file."""
    ids = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            tid = line.strip()
            if tid:
                ids.append(tid)
    return ids


def extract_text_from_cell(cell) -> str:
    """Extract text from a table cell, handling links properly."""
    links = cell.find_all("a")
    if not links:
        return cell.get_text(strip=True)

    parts = []
    for link in links:
        text = link.get_text(strip=True)
        if text:
            parts.append(text)

    # Get remaining text after removing links
    cell_copy = cell.__copy__()
    for link in cell_copy.find_all("a"):
        link.decompose()
    remaining = cell_copy.get_text(strip=True)
    if remaining:
        parts.append(remaining)

    if not parts:
        return cell.get_text(strip=True)
    return " ".join(parts)


def extract_links_from_cell(cell) -> List[str]:
    """Extract link texts from a table cell."""
    links = []
    for link in cell.find_all("a"):
        text = link.get_text(strip=True)
        if text:
            links.append(text)
    return links


def extract_link_href(cell) -> str:
    """Extract href from first link in a table cell."""
    link = cell.find("a")
    if link and link.get("href"):
        return link.get("href")
    return ""


def parse_time_control(raw: str) -> tuple[str, bool]:
    """
    Parse time_control to S (standard), R (rapid), or B (blitz).
    First word (case-insensitive): blitz->B, rapid->R, standard->S.
    Otherwise default to S and return (S, True) to indicate it was defaulted.
    Returns (code, was_defaulted).
    """
    if not raw or not str(raw).strip():
        return "S", False
    first = str(raw).strip().split()[0].lower() if str(raw).strip().split() else ""
    if first == "blitz":
        return "B", False
    if first == "rapid":
        return "R", False
    if first == "standard":
        return "S", False
    return "S", True


def parse_n_players(raw: str) -> tuple[Optional[int], bool]:
    """
    Parse n_players to int. Valid if positive and > 2.
    Returns (value_or_none, is_valid).
    """
    if not raw or not str(raw).strip():
        return None, False
    try:
        n = int(str(raw).strip())
        return n, n > 2
    except (ValueError, TypeError):
        return None, False


def parse_date(raw: str) -> Optional[datetime]:
    """Parse date string to datetime. Returns None if unparseable."""
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def parse_nat_championship(raw: str) -> bool:
    """True if non-null/non-empty, False otherwise."""
    return bool(raw and str(raw).strip())


def fetch_tournament_details(
    tournament_id: str,
    session: requests.Session,
    *,
    _attempt_log: Optional[List[Dict]] = None,
    return_raw: bool = False,
) -> Tuple[Optional[Dict], Optional[str], int, Optional[bytes]]:
    """
    Fetch tournament details from FIDE website.

    Returns:
        Tuple of (details_dict, error_string, num_attempts, raw_content).
        If successful, details_dict is not None. raw_content is response bytes when return_raw=True.
    """
    url = f"https://ratings.fide.com/tournament_information.phtml?event={tournament_id}"

    max_retries = 3
    last_error = None
    attempt_times: List[float] = []

    for attempt in range(max_retries):
        if attempt > 0:
            delay = 0.1 * (
                2 ** (attempt - 1)
            )  # Exponential backoff: 100ms, 200ms, 400ms
            time.sleep(delay)

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "max-age=0",
            }

            t0 = time.perf_counter()
            try:
                response = session.get(url, headers=headers, timeout=45)
            finally:
                elapsed = time.perf_counter() - t0
                attempt_times.append(elapsed)

            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_id": tournament_id,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue

            raw_content = response.content if return_raw else None
            soup = BeautifulSoup(response.content, "html.parser")

            details_table = soup.find("table", class_="details_table")
            if not details_table:
                return None, "no data found", len(attempt_times), None

            details = {}

            for row in details_table.find_all("tr"):
                label_cell = row.find("td", class_="info_table_l")
                value_cells = row.find_all("td")

                if not label_cell or len(value_cells) < 2:
                    continue

                value_cell = value_cells[1]
                label = label_cell.get_text(strip=True)
                value = extract_text_from_cell(value_cell)

                # Map labels to JSON field names
                field_map = {
                    "Event code": "id",
                    "Tournament Name": "name",
                    "City": "city",
                    "Country": "fed",
                    "Number of players": "n_players",
                    "System": "system",
                    "Hybrid": "hybrid",
                    "Category": "category",
                    "Start Date": "start_date",
                    "End Date": "end_date",
                    "Date received": "date_received",
                    "Date registered": "date_registered",
                    "Type": "type",
                    "Time Control": "time_control",
                    "Zone": "zone",
                    "Nat. Championship": "nat_championship",
                }

                if label in field_map:
                    details[field_map[label]] = value

            # Remove empty fields
            return (
                {k: v for k, v in details.items() if v},
                None,
                len(attempt_times),
                raw_content,
            )

        except requests.exceptions.Timeout as e:
            last_error = f"timeout: {e}"
            if _attempt_log is not None:
                _attempt_log.append(
                    {
                        "tournament_id": tournament_id,
                        "attempt": attempt + 1,
                        "error": last_error,
                        "duration_s": attempt_times[-1] if attempt_times else 0,
                    }
                )
            continue
        except requests.exceptions.ConnectionError as e:
            error_str = str(e).lower()
            # Check for various connection error patterns that should be retried
            if any(
                pattern in error_str
                for pattern in [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
            ):
                last_error = f"network error: {e}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_id": tournament_id,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue
            last_error = f"connection error: {e}"
            return None, last_error, len(attempt_times), None
        except requests.exceptions.RequestException as e:
            error_str = str(e).lower()
            # Check for various connection error patterns that should be retried
            if any(
                pattern in error_str
                for pattern in [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
            ):
                last_error = f"network error: {e}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_id": tournament_id,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue
            last_error = f"network error: {e}"
            return None, last_error, len(attempt_times), None
        except Exception as e:
            last_error = f"parse error: {e}"
            if _attempt_log is not None:
                _attempt_log.append(
                    {
                        "tournament_id": tournament_id,
                        "attempt": attempt + 1,
                        "error": last_error,
                        "duration_s": attempt_times[-1] if attempt_times else 0,
                    }
                )
            continue

    return None, f"max retries exceeded: {last_error}", len(attempt_times), None


def flatten_result(result: Dict) -> Dict:
    """Flatten a result dictionary for Parquet storage with processed fields."""
    flattened = {
        "tournament_id": result.get("tournament_id", ""),
        "success": result.get("success", False),
        "error": result.get("error", ""),
    }

    details = result.get("details", {})
    if details:
        # Simple string fields
        for field in [
            "id",
            "name",
            "city",
            "fed",
            "system",
            "hybrid",
            "category",
            "type",
            "zone",
        ]:
            flattened[field] = details.get(field, "")

        # n_players: int, None if invalid
        n_players_val, _ = parse_n_players(details.get("n_players", ""))
        flattened["n_players"] = n_players_val

        # time_control: S/R/B from first word
        tc_raw = details.get("time_control", "")
        tc_code, _ = parse_time_control(tc_raw)
        flattened["time_control"] = tc_code

        # Dates: datetime or None
        for field in ["start_date", "end_date", "date_received", "date_registered"]:
            raw = details.get(field, "")
            parsed = parse_date(raw)
            flattened[field] = parsed

        # nat_championship: bool (true if non-null)
        flattened["nat_championship"] = parse_nat_championship(
            details.get("nat_championship", "")
        )

    return flattened


def results_to_dataframe(results: List[Dict]) -> pd.DataFrame:
    """Convert results list to pandas DataFrame."""
    flattened_results = [flatten_result(r) for r in results]
    return pd.DataFrame(flattened_results)


def build_report(results: List[Dict]) -> Dict:
    """
    Build report with tournament count, distributions, nulls, and time_control unique values.
    """
    df = results_to_dataframe(results)
    successful = [r for r in results if r.get("success", False)]
    df_success = results_to_dataframe(successful) if successful else pd.DataFrame()

    report: Dict = {
        "tournaments_total": len(results),
        "tournaments_success": len(successful),
    }

    if df_success.empty:
        report["nulls_by_column"] = {}
        report["distribution"] = {}
        report["time_control_unique_count"] = 0
        return report

    # Nulls per column (empty string or NaN counts as null)
    nulls = {}
    for col in df_success.columns:
        if col in ("tournament_id", "success", "error"):
            continue
        nulls[col] = int(
            (
                df_success[col].isna() | (df_success[col].astype(str).str.strip() == "")
            ).sum()
        )
    report["nulls_by_column"] = nulls

    # Distribution for system, hybrid, category, type, zone
    dist_cols = ["system", "hybrid", "category", "type", "zone"]
    report["distribution"] = {}
    for col in dist_cols:
        if col not in df_success.columns:
            continue
        non_null = df_success[col].dropna()
        non_null = non_null[non_null.astype(str).str.strip() != ""]
        if len(non_null) == 0:
            report["distribution"][col] = {"unique_values": [], "counts": {}}
        else:
            counts = non_null.value_counts().to_dict()
            report["distribution"][col] = {
                "unique_values": sorted(counts.keys(), key=str),
                "counts": {str(k): int(v) for k, v in counts.items()},
            }

    # Time control unique values (for separate file)
    if "time_control" in df_success.columns:
        tc = df_success["time_control"].dropna()
        tc = tc[tc.astype(str).str.strip() != ""]
        report["time_control_unique_count"] = tc.nunique()
    else:
        report["time_control_unique_count"] = 0

    return report


def save_report(results: List[Dict], report_path: str):
    """Save report JSON."""
    try:
        report = build_report(results)
        dirname = os.path.dirname(report_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved report to {report_path}")
    except Exception as e:
        logger.error(f"Report save failed: {e}")


def save_time_control_unique_values(results: List[Dict], output_path: str):
    """Write all unique raw time_control values to a file for parsing analysis."""
    try:
        successful = [r for r in results if r.get("success", False)]
        if not successful:
            return
        raw_values = [
            r.get("details", {}).get("time_control", "")
            for r in successful
            if r.get("details", {}).get("time_control")
        ]
        unique = sorted(
            set(v.strip() for v in raw_values if v and str(v).strip()), key=str
        )
        dirname = os.path.dirname(output_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for v in unique:
                f.write(f"{v}\n")
        logger.info(f"Saved {len(unique)} unique time_control values to {output_path}")
    except Exception as e:
        logger.error(f"Time control unique values save failed: {e}")


def save_results_parquet(results: List[Dict], parquet_path: str) -> None:
    """Save results as Parquet file (local or S3)."""
    try:
        df = results_to_dataframe(results)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        _write_to_path(parquet_path, buf.getvalue())
        logger.info(f"Saved {len(results)} records to {parquet_path}")
    except Exception as e:
        logger.error(f"Parquet save failed: {e}")


def save_results_json_sample(
    results: List[Dict], json_path: str, sample_size: int = 100
) -> None:
    """Save a random sample of flattened results (processed format) as JSON (local or S3)."""
    try:
        successful_results = [r for r in results if r.get("success", False)]
        if not successful_results:
            logger.warning("No successful results to sample for JSON")
            return
        sample = random.sample(
            successful_results, min(sample_size, len(successful_results))
        )
        flattened = [flatten_result(r) for r in sample]
        content = json.dumps(flattened, indent=2, ensure_ascii=False, default=str)
        _write_to_path(json_path, content)
        logger.info(f"Saved random sample of {len(flattened)} records to {json_path}")
    except Exception as e:
        logger.error(f"JSON sample save failed: {e}")


def save_failures_json(results: List[Dict], base_path: str) -> None:
    """Save failed tournament IDs and errors to JSON for investigation (local or S3)."""
    failures = [
        {"tournament_id": r["tournament_id"], "error": r.get("error", "")}
        for r in results
        if not r.get("success", False)
    ]
    if not failures:
        return
    path = base_path.rstrip(".parquet") + "_failures.json"
    content = json.dumps(failures, indent=2, ensure_ascii=False)
    _write_to_path(path, content)
    logger.info(f"Saved {len(failures)} failures to {path}")


def build_and_save_report(
    results: List[Dict],
    parquet_path: str,
    report_base: str | None = None,
) -> None:
    """
    Build report (n_tournaments, distributions, nulls) and write time_control
    unique values to a separate file.
    If report_base is provided, use it for report_path and time_control_path;
    otherwise derive from parquet_path.
    """
    successful = [r for r in results if r.get("success", False)]
    if not successful:
        logger.warning("No successful results for report")
        return

    df = results_to_dataframe(successful)
    detail_cols = [
        "id",
        "name",
        "city",
        "fed",
        "n_players",
        "system",
        "hybrid",
        "category",
        "start_date",
        "end_date",
        "date_received",
        "date_registered",
        "type",
        "time_control",
        "zone",
        "nat_championship",
    ]
    cols = [c for c in detail_cols if c in df.columns]

    nulls_by_column = {}
    for c in cols:
        if c in df.columns:
            nulls_by_column[c] = int(
                (df[c].isna() | (df[c].astype(str).str.strip() == "")).sum()
            )

    dist_cols = ["system", "hybrid", "category", "type", "zone"]
    distributions = {}
    for c in dist_cols:
        if c in df.columns:
            counts = df[c].value_counts()
            distributions[c] = {
                "unique_values": sorted(
                    counts.index.dropna().astype(str).unique().tolist()
                ),
                "distribution": {str(k): int(v) for k, v in counts.items()},
            }

    # time_control_defaulted_to_standard: count and sample when first word wasn't blitz/rapid/standard
    tc_defaulted = [
        (r.get("details", {}).get("time_control", ""), r.get("tournament_id", ""))
        for r in successful
        if r.get("details", {}).get("time_control")
    ]
    tc_defaulted = [
        (raw, tid) for raw, tid in tc_defaulted if raw and parse_time_control(raw)[1]
    ]
    tc_defaulted_sample = [
        {"tournament_id": tid, "raw_value": raw}
        for raw, tid in (tc_defaulted[:10] if len(tc_defaulted) > 10 else tc_defaulted)
    ]

    # n_players_odd: invalid (not int, or <=2)
    n_players_odd = []
    for r in successful:
        raw = r.get("details", {}).get("n_players", "")
        _, valid = parse_n_players(raw)
        if not valid:
            n_players_odd.append((raw, r.get("tournament_id", "")))
    n_players_odd_sample = [
        {"tournament_id": tid, "raw_value": raw}
        for raw, tid in (
            n_players_odd[:10] if len(n_players_odd) > 10 else n_players_odd
        )
    ]

    # nat_championship raw distribution
    nat_raw = [r.get("details", {}).get("nat_championship", "") for r in successful]
    nat_counts = Counter(str(v).strip() if v else "" for v in nat_raw)
    nat_championship_distribution = {
        "unique_values": sorted(nat_counts.keys()),
        "distribution": {str(k): int(v) for k, v in nat_counts.items()},
    }

    report = {
        "tournaments_count": len(successful),
        "nulls_by_column": nulls_by_column,
        "distributions": distributions,
        "time_control_defaulted_to_standard": {
            "count": len(tc_defaulted),
            "sample": tc_defaulted_sample,
        },
        "n_players_odd": {
            "count": len(n_players_odd),
            "sample": n_players_odd_sample,
        },
        "nat_championship_raw_distribution": nat_championship_distribution,
    }

    base = (
        report_base
        if report_base is not None
        else (
            parquet_path.replace(".parquet", "")
            if parquet_path.endswith(".parquet")
            else parquet_path
        )
    )
    report_path = base + "_report.json"
    time_control_path = base + "_time_control_unique_values.txt"

    report_content = json.dumps(report, indent=2, ensure_ascii=False)
    _write_to_path(report_path, report_content)
    logger.info(f"Saved report to {report_path}")

    raw_tc = [
        r.get("details", {}).get("time_control", "")
        for r in successful
        if r.get("details", {}).get("time_control")
    ]
    unique_tc = sorted(set(v.strip() for v in raw_tc if v and str(v).strip()), key=str)
    _write_to_path(time_control_path, "\n".join(unique_tc))
    logger.info(
        f"Saved {len(unique_tc)} unique raw time_control values to {time_control_path}"
    )


def run(
    input_path: str,
    output_path: str,
    rate_limit: float = 0.5,
    max_retries: int = 3,
    checkpoint: int = 0,
    quiet: bool = False,
    limit: int = 0,
    output_sample_path: str | None = None,
    output_reports_base: str | None = None,
    save_raw: bool = False,
) -> int:
    """
    Scrape tournament details for IDs from input_path, write to output_path.

    Args:
        input_path: Path to tournament IDs file (one ID per line). Local or S3 URI.
        output_path: Parquet output path (local or S3).
        rate_limit: Requests per second.
        max_retries: Retry passes for failed fetches.
        checkpoint: Save checkpoint every N successful (0 = disabled).
        quiet: Reduce log output.
        limit: Process only first N IDs (0 = all).
        output_sample_path: Optional path for JSON sample. Default: {output_path}_sample.json.
        output_reports_base: Optional base for report, failures, time_control files.
            Default: output_path base. Used for _report.json, _failures.json,
            _time_control_unique_values.txt.
        save_raw: If True, save raw HTML per tournament to raw/details/{chunk}/{id}.html.gz.

    Returns:
        0 on success, 1 on failure.
    """
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = (
        output_path.replace(".parquet", "")
        if output_path.endswith(".parquet")
        else output_path
    )
    parquet_path = base + ".parquet"
    json_path = (
        output_sample_path if output_sample_path is not None else base + "_sample.json"
    )
    reports_base = output_reports_base if output_reports_base is not None else base

    try:
        tournament_ids = _read_ids_from_path(input_path)
    except Exception as e:
        logger.error("Error reading IDs from %s: %s", input_path, e)
        return 1

    if not tournament_ids:
        logger.error("No tournament IDs found in %s", input_path)
        return 1

    if limit > 0:
        tournament_ids = tournament_ids[:limit]
        logger.info("Limited to first %d tournaments", limit)

    logger.info(
        "Processing %d tournaments from %s -> %s",
        len(tournament_ids),
        input_path,
        parquet_path,
    )

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    rate_limiter = RateLimiter(rate_limit)

    raw_base: Optional[str] = (
        _raw_base_from_output_path(output_path) if save_raw else None
    )
    raw_accumulator: List[Tuple[str, bytes]] = []  # (tournament_id, html)

    all_results: List[Dict] = []
    success_count = 0
    error_count = 0
    total_retries = 0
    current_tournaments = tournament_ids

    pbar = None
    if not quiet:
        pbar = tqdm(
            total=len(tournament_ids),
            desc="Processing",
            unit="tournament",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    start_time = time.time()

    for pass_num in range(max_retries + 1):
        if not current_tournaments:
            break
        if pass_num > 0:
            delay = 3 * (2 ** (pass_num - 1))
            logger.info(
                "Retry pass %d: waiting %s before retrying %d tournaments",
                pass_num,
                format_duration(delay),
                len(current_tournaments),
            )
            time.sleep(delay)
            total_retries += len(current_tournaments)

        pass_failed = []
        for tournament_id in current_tournaments:
            rate_limiter.wait()
            details, error, _, raw_content = fetch_tournament_details(
                tournament_id, session, return_raw=save_raw
            )

            result = {"tournament_id": tournament_id}
            if details is None:
                error_count += 1
                result["success"] = False
                result["error"] = error or "fetch failed"
                error_lower = (error or "").lower()
                network_error_patterns = [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
                is_network_error = any(p in error_lower for p in network_error_patterns)
                if (
                    error
                    and (is_network_error or "timeout" in error_lower)
                    and pass_num < max_retries
                ):
                    pass_failed.append(tournament_id)
            else:
                success_count += 1
                result["success"] = True
                result["details"] = details
                if raw_base and raw_content:
                    raw_accumulator.append((tournament_id, raw_content))
                if checkpoint > 0 and success_count % checkpoint == 0:
                    save_checkpoint(parquet_path, all_results, base + ".checkpoint")

            all_results.append(result)
            if pbar:
                pbar.update(1)
                pbar.set_postfix({"✓": success_count, "✗": error_count})

        current_tournaments = pass_failed

    if pbar:
        pbar.close()

    if raw_base and raw_accumulator:
        from raw_utils import build_concatenated_gzip

        raw_path = raw_base + ".html.gz"
        _write_to_path(raw_path, build_concatenated_gzip(raw_accumulator))
        logger.info(
            "Saved concatenated raw HTML (%d tournaments) to %s",
            len(raw_accumulator),
            raw_path,
        )

    save_results_parquet(all_results, parquet_path)
    save_results_json_sample(all_results, json_path, sample_size=100)
    if success_count > 0:
        build_and_save_report(all_results, parquet_path, report_base=reports_base)
    save_failures_json(all_results, reports_base)

    elapsed = time.time() - start_time
    logger.info(
        "Done: %d success, %d errors in %s",
        success_count,
        error_count,
        format_duration(elapsed),
    )
    return 0


def save_checkpoint(
    output_path: str, results: List[Dict], checkpoint_path: Optional[str] = None
):
    """Save checkpoint file as Parquet."""
    if not output_path or not checkpoint_path:
        return

    try:
        # Convert .json checkpoint path to .parquet
        if checkpoint_path.endswith(".checkpoint"):
            parquet_checkpoint = checkpoint_path.replace(
                ".checkpoint", ".parquet.checkpoint"
            )
        elif checkpoint_path.endswith(".json.checkpoint"):
            parquet_checkpoint = checkpoint_path.replace(
                ".json.checkpoint", ".parquet.checkpoint"
            )
        else:
            parquet_checkpoint = checkpoint_path + ".parquet"

        save_results_parquet(results, parquet_checkpoint)
    except Exception as e:
        logger.error(f"Checkpoint save failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape FIDE tournament details",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", type=str, default="", help="Path to tournament IDs file"
    )
    parser.add_argument("--year", type=int, default=0, help="Year to process")
    parser.add_argument("--month", type=int, default=0, help="Month to process")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Base data directory (default: data)",
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
    parser.add_argument("--output", type=str, default="", help="Output JSON file")
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Initial requests per second (default: 0.5)",
    )
    parser.add_argument(
        "--max-retries", type=int, default=3, help="Max retry passes (default: 3)"
    )
    parser.add_argument(
        "--checkpoint",
        type=int,
        default=100,
        help="Save every N tournaments (default: 100)",
    )
    parser.add_argument(
        "--show-time", action="store_true", help="Show timing info for each tournament"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Use verbose stdout output instead of progress bar (shows detailed error info)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N tournaments (for testing/profiling)",
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Overwrite existing output if it exists",
    )
    parser.add_argument(
        "--verbose-errors",
        action="store_true",
        help="Log failed HTTP attempt details and print retry analysis at end",
    )

    args = parser.parse_args()

    # Determine input path
    if args.input:
        input_path = args.input
    elif args.run_type and (args.run_name or (args.year > 0 and args.month > 0)):
        if args.month < 1 or args.month > 12:
            logger.error("Error: month must be 1-12")
            sys.exit(1)
        run_name = args.run_name or f"{args.year}-{args.month:02d}"
        from s3_io import build_local_path_for_run

        input_path = str(
            build_local_path_for_run(
                args.local_root, args.run_type, run_name, "data", "tournament_ids.txt"
            )
        )
    elif args.year > 0 and args.month > 0:
        if args.month < 1 or args.month > 12:
            logger.error("Error: month must be 1-12")
            sys.exit(1)
        input_path = os.path.join(
            args.data_dir, "tournament_ids", f"{args.year}_{args.month:02d}"
        )
    else:
        logger.error("Error: specify --input or --year and --month (or --run-type)")
        sys.exit(1)

    # Determine output paths
    parquet_path = None
    json_path = None
    report_base = None
    if args.output:
        # If user specifies output, use it as base for parquet, add .json for sample
        if args.output.endswith(".json"):
            parquet_path = args.output.replace(".json", ".parquet")
            json_path = args.output.replace(".json", "_sample.json")
        elif args.output.endswith(".parquet"):
            parquet_path = args.output
            json_path = args.output.replace(".parquet", "_sample.json")
        else:
            parquet_path = args.output + ".parquet"
            json_path = args.output + "_sample.json"
        report_base = None
    elif args.run_type and (args.run_name or (args.year > 0 and args.month > 0)):
        run_name = args.run_name or f"{args.year}-{args.month:02d}"
        from s3_io import build_local_path_for_run

        parquet_path = str(
            build_local_path_for_run(
                args.local_root,
                args.run_type,
                run_name,
                "data",
                "tournament_details.parquet",
            )
        )
        json_path = str(
            build_local_path_for_run(
                args.local_root,
                args.run_type,
                run_name,
                "sample",
                "tournament_details_sample.json",
            )
        )
        report_base = str(
            build_local_path_for_run(
                args.local_root,
                args.run_type,
                run_name,
                "reports",
                "tournament_details",
            )
        )
    elif args.year > 0 and args.month > 0:
        base_path = os.path.join(
            args.data_dir, "tournament_details", f"{args.year}_{args.month:02d}"
        )
        parquet_path = base_path + ".parquet"
        json_path = base_path + "_sample.json"
        report_base = None

    if not args.override and os.path.exists(parquet_path):
        logger.info(
            "Output %s already exists. Use --override to replace.", parquet_path
        )
        sys.exit(0)

    # Read tournament IDs
    try:
        tournament_ids = read_tournament_ids(input_path)
    except Exception as e:
        logger.error(f"Error reading IDs: {e}")
        sys.exit(1)

    if not tournament_ids:
        logger.error("No tournament IDs found")
        sys.exit(1)

    if args.limit > 0:
        tournament_ids = tournament_ids[: args.limit]
        logger.info(f"Limited to first {len(tournament_ids)} tournaments")

    logger.info(f"Processing {len(tournament_ids)} tournaments")
    logger.info(
        f"Settings: {args.rate_limit:.2f} req/s initial rate, checkpoint every {args.checkpoint}"
    )

    start_time = time.time()

    # Create HTTP session with connection reuse disabled
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    rate_limiter = RateLimiter(args.rate_limit)

    all_results = []
    success_count = 0
    error_count = 0

    def _graceful_shutdown(signum, frame):
        logger.warning("\nReceived interrupt, initiating graceful shutdown...")
        if all_results and parquet_path:
            try:
                save_results_parquet(all_results, parquet_path)
                if json_path:
                    save_results_json_sample(all_results, json_path, sample_size=100)
                build_and_save_report(
                    all_results, parquet_path, report_base=report_base
                )
                save_failures_json(
                    all_results,
                    (
                        report_base
                        if report_base
                        else parquet_path.replace(".parquet", "")
                    ),
                )
                logger.info("Saved %d results to %s", len(all_results), parquet_path)
            except Exception as e:
                logger.error("Error saving partial results: %s", e)
        sys.exit(130 if signum == 2 else 0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    total_retries = (
        0  # Total number of tournaments that have been retried at least once
    )
    attempt_log: List[Dict] = (
        [] if args.verbose_errors else []
    )  # Shared across all fetches
    attempt_counts: List[Tuple[str, int]] = (
        [] if args.verbose_errors else []
    )  # (tid, n) in order

    current_tournaments = tournament_ids

    # Progress bar (only if not verbose)
    pbar = None
    if not args.verbose:
        pbar = tqdm(
            total=len(tournament_ids),
            desc="Processing",
            unit="tournament",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    for pass_num in range(args.max_retries + 1):
        if not current_tournaments:
            break

        if pass_num > 0:
            delay = 3 * (2 ** (pass_num - 1))  # Exponential backoff: 3s, 6s, 12s
            logger.info(
                f"Retry pass {pass_num}: waiting {format_duration(delay)} before retrying {len(current_tournaments)} tournaments"
            )
            time.sleep(delay)
            # Count tournaments being retried in this pass
            total_retries += len(current_tournaments)

        pass_failed = []

        for tournament_id in current_tournaments:
            rate_limiter.wait()

            details, error, num_attempts, _ = fetch_tournament_details(
                tournament_id,
                session,
                _attempt_log=attempt_log if args.verbose_errors else None,
            )
            if args.verbose_errors:
                attempt_counts.append((tournament_id, num_attempts))

            result = {"tournament_id": tournament_id}

            if details is None:
                error_count += 1
                result["success"] = False
                result["error"] = error or "fetch failed"

                # Check if it's a rate limit/network error
                error_lower = error.lower() if error else ""
                network_error_patterns = [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
                is_network_error = any(
                    pattern in error_lower for pattern in network_error_patterns
                )

                # Retry on network errors and timeouts
                if error and (is_network_error or "timeout" in error_lower):
                    if pass_num < args.max_retries:
                        pass_failed.append(tournament_id)
            else:
                success_count += 1
                result["success"] = True
                result["details"] = details

                # Checkpoint
                if args.checkpoint > 0 and success_count % args.checkpoint == 0:
                    checkpoint_path = (
                        parquet_path + ".checkpoint" if parquet_path else None
                    )
                    logger.info(f"Saving checkpoint at {success_count} successful...")
                    save_checkpoint(parquet_path, all_results, checkpoint_path)
            all_results.append(result)

            total_processed = success_count + error_count
            elapsed = time.time() - start_time

            if total_processed > 0:
                avg_time = elapsed / total_processed
                remaining = len(tournament_ids) - total_processed
                est_remaining = avg_time * remaining
            else:
                est_remaining = 0

            # Verbose stdout mode
            if args.verbose:
                rate = rate_limiter.get_rate()
                actual_rate = total_processed / elapsed if elapsed > 0 else 0

                if result["success"]:
                    name = result.get("details", {}).get("name", "unknown")
                    retry_info = f" [Retry pass {pass_num + 1}]" if pass_num > 0 else ""
                    http_retries = (
                        f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
                    )
                    print(
                        f"[{total_processed}/{len(tournament_ids)}] ✓ {tournament_id}: {name}{retry_info}{http_retries} | "
                        f"Rate: {rate:.2f}/s (actual: {actual_rate:.2f}/s) | "
                        f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)} | "
                        f"Success: {success_count} | Errors: {error_count} | Retries: {total_retries}"
                    )
                else:
                    error_msg = result.get("error", "unknown")
                    will_retry = tournament_id in pass_failed
                    retry_info = f" [Retry pass {pass_num + 1}]" if pass_num > 0 else ""
                    http_retries = (
                        f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
                    )
                    retry_status = " [WILL RETRY]" if will_retry else " [FINAL FAILURE]"

                    print(
                        f"[{total_processed}/{len(tournament_ids)}] ✗ {tournament_id}: {error_msg}{retry_info}{http_retries}{retry_status} | "
                        f"Rate: {rate:.2f}/s (actual: {actual_rate:.2f}/s) | "
                        f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)} | "
                        f"Success: {success_count} | Errors: {error_count} | Retries: {total_retries}"
                    )
            else:
                # Progress bar mode
                # Build postfix with retry info
                postfix_dict = {
                    "✓": success_count,
                    "✗": error_count,
                    "rate": f"{rate_limiter.get_rate():.2f}/s",
                }

                # Add retry information
                if total_retries > 0 or pass_num > 0:
                    postfix_dict["retries"] = total_retries
                if pass_num > 0:
                    postfix_dict["pass"] = f"{pass_num + 1}/{args.max_retries + 1}"
                if len(pass_failed) > 0:
                    postfix_dict["pending"] = len(pass_failed)

                postfix_dict["est"] = (
                    format_duration(est_remaining) if est_remaining > 0 else "?"
                )

                # Update progress bar
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(postfix_dict)

                if args.show_time:
                    rate = rate_limiter.get_rate()
                    if result["success"]:
                        name = result.get("details", {}).get("name", "unknown")
                        logger.info(
                            f"[{total_processed}/{len(tournament_ids)}] ✓ {tournament_id}: {name} | "
                            f"Rate: {rate:.2f}/s | Est: {format_duration(est_remaining)}"
                        )
                    else:
                        logger.info(
                            f"[{total_processed}/{len(tournament_ids)}] ✗ {tournament_id}: {result.get('error', 'unknown')} | "
                            f"Rate: {rate:.2f}/s"
                        )

            # Periodic progress update (only in non-verbose mode or at milestones)
            if not args.verbose and (
                total_processed % 50 == 0 or total_processed == len(tournament_ids)
            ):
                actual_rate = total_processed / elapsed if elapsed > 0 else 0
                target_rate = rate_limiter.get_rate()
                logger.info(
                    f"Progress: {total_processed}/{len(tournament_ids)} "
                    f"({success_count}✓ {error_count}✗) | "
                    f"Actual: {actual_rate:.2f}/s | Target: {target_rate:.2f}/s | "
                    f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)}"
                )

        current_tournaments = pass_failed

    if pbar:
        pbar.close()

    # Save final results
    if parquet_path:
        # Save all results as Parquet
        save_results_parquet(all_results, parquet_path)

        # Save random sample of 100 successful results as JSON
        if json_path:
            save_results_json_sample(all_results, json_path, sample_size=100)

        # Build and save report (distributions, nulls, time_control unique values)
        build_and_save_report(all_results, parquet_path, report_base=report_base)
        # Save failures for investigation
        save_failures_json(
            all_results,
            report_base if report_base else parquet_path.replace(".parquet", ""),
        )
    else:
        # If no output path specified, dump to stdout as JSON (for backwards compatibility)
        json.dump(all_results, sys.stdout, indent=2, ensure_ascii=False)

    total_time = time.time() - start_time
    final_rate = (success_count + error_count) / total_time if total_time > 0 else 0

    logger.info("\nFinal Summary:")
    logger.info(f"  Total: {len(tournament_ids)}")
    logger.info(
        f"  Success: {success_count} ({100.0 * success_count / len(tournament_ids):.1f}%)"
    )
    logger.info(f"  Errors: {error_count}")
    if total_retries > 0:
        logger.info(f"  Retries: {total_retries}")
    logger.info(f"  Time: {format_duration(total_time)}")
    logger.info(f"  Average rate: {final_rate:.2f} tournaments/sec")
    if parquet_path:
        logger.info(f"  Parquet output: {parquet_path}")
    if json_path:
        logger.info(f"  JSON sample: {json_path}")

    # Verbose error analysis (attempt distribution, retry tournaments, error breakdown)
    if args.verbose_errors and attempt_counts:
        dist = Counter(n for _, n in attempt_counts)
        retried = [(tid, n) for tid, n in attempt_counts if n > 1]
        error_counts = Counter(e.get("error", "unknown") for e in attempt_log)
        logger.info("\nVerbose Error Analysis:")
        logger.info("  Attempt distribution: %s", dict(sorted(dist.items())))
        if retried:
            tids = [tid for tid, _ in retried]
            max_show = 30
            if len(tids) <= max_show:
                logger.info("  Tournaments needing retries (in order): %s", tids)
            else:
                logger.info(
                    "  Tournaments needing retries (first %d): %s ... and %d more",
                    max_show,
                    tids[:max_show],
                    len(tids) - max_show,
                )
        if error_counts:
            logger.info("  Error breakdown: %s", dict(error_counts))


if __name__ == "__main__":
    main()

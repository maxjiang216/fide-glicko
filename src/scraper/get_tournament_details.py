#!/usr/bin/env python3
"""
FIDE Tournament Details Scraper

Scrapes tournament details from FIDE website for a list of tournament IDs.
Supports rate limiting, retries, checkpoints, and progress tracking.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from collections import Counter
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


def fetch_tournament_details(
    tournament_id: str,
    session: requests.Session,
    *,
    _profile: Optional[Dict[str, float]] = None,
    _attempt_log: Optional[List[Dict]] = None,
) -> Tuple[Optional[Dict], Optional[str], int]:
    """
    Fetch tournament details from FIDE website.

    Returns:
        Tuple of (details_dict, error_string). If successful, details_dict is not None.
        If error, error_string contains the error message.
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
                if _profile is not None:
                    _profile.setdefault("_attempt_times", []).append(elapsed)
            fetch_s = time.perf_counter() - t0

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

            t1 = time.perf_counter()
            soup = BeautifulSoup(response.content, "html.parser")

            details_table = soup.find("table", class_="details_table")
            if not details_table:
                if _profile is not None:
                    _profile["fetch_s"] = fetch_s
                    _profile["parse_s"] = time.perf_counter() - t1
                return None, "no data found", len(attempt_times)

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
                    "Event code": "event_code",
                    "Tournament Name": "tournament_name",
                    "City": "city",
                    "Country": "country",
                    "Number of players": "number_of_players",
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
                    "Reported mult. round days": "reported_mult_round_days",
                    "Nat. Championship": "nat_championship",
                    "PGN file": "pgn_file",
                }

                if label in field_map:
                    details[field_map[label]] = value
                elif label == "Chief Arbiter":
                    details["chief_arbiter"] = extract_links_from_cell(value_cell)
                elif label == "Deputy Chief Arbiter":
                    details["deputy_chief_arbiter"] = extract_links_from_cell(
                        value_cell
                    )
                elif label == "Arbiter":
                    details["arbiter"] = extract_links_from_cell(value_cell)
                elif label == "Assistant Arbiter":
                    details["assistant_arbiter"] = extract_links_from_cell(value_cell)
                elif label == "Chief Organizer":
                    details["chief_organizer"] = extract_links_from_cell(value_cell)
                elif label == "Organizer":
                    details["organizer"] = extract_links_from_cell(value_cell)
                elif label == "Orig.Report":
                    details["orig_report"] = extract_link_href(value_cell)
                elif label == "View Report":
                    details["view_report_href"] = extract_link_href(value_cell)
                    details["view_report_text"] = extract_text_from_cell(value_cell)

            # Remove empty fields
            parse_s = time.perf_counter() - t1
            if _profile is not None:
                _profile["fetch_s"] = fetch_s
                _profile["parse_s"] = parse_s
            return {k: v for k, v in details.items() if v}, None, len(attempt_times)

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
            return None, last_error, len(attempt_times)
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
            return None, last_error, len(attempt_times)
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

    return None, f"max retries exceeded: {last_error}", len(attempt_times)


def flatten_result(result: Dict) -> Dict:
    """Flatten a result dictionary for Parquet storage."""
    flattened = {
        "tournament_id": result.get("tournament_id", ""),
        "success": result.get("success", False),
        "error": result.get("error", ""),
    }

    # Flatten details if present
    details = result.get("details", {})
    if details:
        # Simple fields
        for field in [
            "event_code",
            "tournament_name",
            "city",
            "country",
            "number_of_players",
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
            "reported_mult_round_days",
            "nat_championship",
            "pgn_file",
            "orig_report",
            "view_report_href",
            "view_report_text",
        ]:
            flattened[field] = details.get(field, "")

        # List fields - join with semicolon for storage
        for field in [
            "chief_arbiter",
            "deputy_chief_arbiter",
            "arbiter",
            "assistant_arbiter",
            "chief_organizer",
            "organizer",
        ]:
            value = details.get(field, [])
            if isinstance(value, list):
                flattened[field] = ";".join(str(v) for v in value)
            else:
                flattened[field] = ""

    return flattened


def results_to_dataframe(results: List[Dict]) -> pd.DataFrame:
    """Convert results list to pandas DataFrame."""
    flattened_results = [flatten_result(r) for r in results]
    return pd.DataFrame(flattened_results)


def save_results_parquet(results: List[Dict], parquet_path: str):
    """Save results as Parquet file."""
    try:
        df = results_to_dataframe(results)
        dirname = os.path.dirname(parquet_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"Saved {len(results)} records to {parquet_path}")
    except Exception as e:
        logger.error(f"Parquet save failed: {e}")


def save_results_json_sample(
    results: List[Dict], json_path: str, sample_size: int = 100
):
    """Save a random sample of results as JSON file."""
    try:
        # Filter to successful results only for the sample
        successful_results = [r for r in results if r.get("success", False)]

        if len(successful_results) == 0:
            logger.warning("No successful results to sample for JSON")
            return

        # Sample up to sample_size records
        sample = random.sample(
            successful_results, min(sample_size, len(successful_results))
        )

        dirname = os.path.dirname(json_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved random sample of {len(sample)} records to {json_path}")
    except Exception as e:
        logger.error(f"JSON sample save failed: {e}")


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
        "--profile",
        action="store_true",
        help="Print timing breakdown (fetch vs parse) at the end",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N tournaments (for testing/profiling)",
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
    elif args.year > 0 and args.month > 0:
        if args.month < 1 or args.month > 12:
            logger.error("Error: month must be 1-12")
            sys.exit(1)
        input_path = os.path.join(
            args.data_dir, "tournament_ids", f"{args.year}_{args.month:02d}"
        )
    else:
        logger.error("Error: specify --input or --year and --month")
        sys.exit(1)

    # Determine output paths
    parquet_path = None
    json_path = None
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
    elif args.year > 0 and args.month > 0:
        base_path = os.path.join(
            args.data_dir, "tournament_details", f"{args.year}_{args.month:02d}"
        )
        parquet_path = base_path + ".parquet"
        json_path = base_path + "_sample.json"

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
    total_retries = (
        0  # Total number of tournaments that have been retried at least once
    )
    profile_samples: List[Dict[str, float]] = [] if args.profile else []
    attempt_log: List[Dict] = [] if args.verbose_errors else []  # Shared across all fetches
    attempt_counts: List[Tuple[str, int]] = [] if args.verbose_errors else []  # (tid, n) in order

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
        last_cycle_start = None

        for tournament_id in current_tournaments:
            now = time.perf_counter()
            if args.profile and last_cycle_start is not None and profile_samples:
                prev = profile_samples[-1]
                cycle_s = now - last_cycle_start
                prev["cycle_s"] = cycle_s
                prev["other_s"] = cycle_s - prev.get("wait_s", 0) - prev.get("fetch_s", 0) - prev.get("parse_s", 0)
            last_cycle_start = now

            t_wait = time.perf_counter()
            rate_limiter.wait()
            wait_s = time.perf_counter() - t_wait

            t_fetch_start = time.perf_counter() if args.profile else None
            profile = {} if args.profile else None
            details, error, num_attempts = fetch_tournament_details(
                tournament_id,
                session,
                _profile=profile,
                _attempt_log=attempt_log if args.verbose_errors else None,
            )
            if args.verbose_errors:
                attempt_counts.append((tournament_id, num_attempts))
            fetch_total_s = (time.perf_counter() - t_fetch_start) if t_fetch_start else 0
            if args.profile and profile:
                profile["wait_s"] = wait_s
                profile["fetch_total_s"] = fetch_total_s
                profile_samples.append(profile)

            t_post = time.perf_counter() if args.profile else None

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
                t_before_checkpoint = time.perf_counter() if args.profile else None
                if args.checkpoint > 0 and success_count % args.checkpoint == 0:
                    checkpoint_path = (
                        parquet_path + ".checkpoint" if parquet_path else None
                    )
                    logger.info(f"Saving checkpoint at {success_count} successful...")
                    save_checkpoint(parquet_path, all_results, checkpoint_path)
                if args.profile and profile_samples:
                    profile_samples[-1]["checkpoint_s"] = (
                        time.perf_counter() - t_before_checkpoint
                        if t_before_checkpoint
                        else 0
                    )
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
                    name = result.get("details", {}).get("tournament_name", "unknown")
                    retry_info = f" [Retry pass {pass_num + 1}]" if pass_num > 0 else ""
                    http_retries = f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
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
                    http_retries = f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
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
                t_before_pbar = time.perf_counter() if args.profile else None
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(postfix_dict)
                if args.profile and profile_samples and t_before_pbar:
                    profile_samples[-1]["pbar_s"] = time.perf_counter() - t_before_pbar

                if args.show_time:
                    rate = rate_limiter.get_rate()
                    if result["success"]:
                        name = result.get("details", {}).get(
                            "tournament_name", "unknown"
                        )
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
            t_before_periodic = time.perf_counter() if args.profile else None
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
            if args.profile and profile_samples and t_post is not None and t_before_periodic is not None:
                pf = time.perf_counter() - t_post
                pl = time.perf_counter() - t_before_periodic
                profile_samples[-1]["post_fetch_s"] = pf
                profile_samples[-1]["periodic_log_s"] = pl

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
                logger.info("  Tournaments needing retries (first %d): %s ... and %d more", max_show, tids[:max_show], len(tids) - max_show)
        if error_counts:
            logger.info("  Error breakdown: %s", dict(error_counts))

    # Profile timing breakdown
    if profile_samples:
        n = len(profile_samples)
        samples_with_cycle = [p for p in profile_samples if "cycle_s" in p]
        n_cycle = len(samples_with_cycle)
        wait_avg = sum(p["wait_s"] for p in profile_samples) / n
        fetch_avg = sum(p["fetch_s"] for p in profile_samples) / n
        parse_avg = sum(p["parse_s"] for p in profile_samples) / n
        measured_avg = wait_avg + fetch_avg + parse_avg
        logger.info("\nProfile (avg per tournament, n=%d):", n)
        logger.info("  Rate-limit wait: %.3fs", wait_avg)
        logger.info("  HTTP fetch:      %.3fs", fetch_avg)
        logger.info("  HTML parse:      %.3fs", parse_avg)
        fetch_total_avg = sum(p.get("fetch_total_s", 0) for p in profile_samples) / n
        logger.info("  fetch_total (wall): %.3fs", fetch_total_avg)
        n_retries = sum(1 for p in profile_samples if p.get("_attempt_times") and len(p["_attempt_times"]) > 1)
        sum_all_attempts = sum(sum(p.get("_attempt_times", [0])) for p in profile_samples)
        sum_fetch_only = sum(p.get("fetch_s", 0) for p in profile_samples)
        extra_from_retries = sum_all_attempts - sum_fetch_only
        logger.info(
            "  Retries: %d/%d had >1 HTTP attempt (failed attempts + backoff = %.2fs total extra)",
            n_retries, n, extra_from_retries,
        )
        logger.info("  Measured (wait+fetch+parse): %.3fs", measured_avg)
        if n_cycle > 0:
            cycle_avg = sum(p["cycle_s"] for p in samples_with_cycle) / n_cycle
            other_avg = sum(p["other_s"] for p in samples_with_cycle) / n_cycle
            other_min = min(p["other_s"] for p in samples_with_cycle)
            other_max = max(p["other_s"] for p in samples_with_cycle)
            logger.info("  ---")
            logger.info("  Cycle (wall time per item, n=%d): %.3fs", n_cycle, cycle_avg)
            logger.info("  Other (cycle - measured): %.3fs (min=%.3fs max=%.3fs)", other_avg, other_min, other_max)
            logger.info("  Check: measured + other = %.3fs (should ≈ cycle)", measured_avg + other_avg)
            n_with_pf = sum(1 for p in profile_samples if "post_fetch_s" in p)
            if n_with_pf > 0:
                post_vals = [p["post_fetch_s"] for p in profile_samples if "post_fetch_s" in p]
                post_avg = sum(post_vals) / len(post_vals)
                post_min, post_max = min(post_vals), max(post_vals)
                ckpt_avg = sum(p.get("checkpoint_s", 0) for p in profile_samples) / n
                pbar_avg = sum(p.get("pbar_s", 0) for p in profile_samples) / n
                periodic_avg = sum(p.get("periodic_log_s", 0) for p in profile_samples) / n
                logger.info("  --- Breakdown of Other (n_with_post_fetch=%d):", n_with_pf)
                logger.info("    post_fetch (to loop end): %.4fs (min=%.4fs max=%.4fs)", post_avg, post_min, post_max)
                logger.info("    checkpoint: %.4fs | pbar: %.4fs | periodic_log: %.4fs", ckpt_avg, pbar_avg, periodic_avg)


if __name__ == "__main__":
    main()

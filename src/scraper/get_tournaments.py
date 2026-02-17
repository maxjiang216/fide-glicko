#!/usr/bin/env python3
"""
Fast FIDE tournament scraper using direct AJAX calls.

This script fetches tournament lists for all federations using direct HTTP
requests to the JSON endpoints, which is much faster than using a headless browser.
"""

import argparse
import asyncio
import csv
import json
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp

# Base URLs for FIDE tournament endpoints
TOURNAMENTS_URL = "https://ratings.fide.com/a_tournaments.php"
PERIODS_URL = "https://ratings.fide.com/a_tournaments_panel.php"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global state for graceful shutdown
_shutdown_requested = False
_shutdown_state = {}


@dataclass
class Tournament:
    """Represents a tournament with its metadata."""

    tournament_id: str
    name: str
    location: str
    time_control: str  # 's' = standard, 'r' = rapid, 'b' = blitz
    start_date: str
    end_date: str
    federation: str


def format_time(seconds: float) -> str:
    """
    Format time in seconds to a human-readable string.

    Args:
        seconds: Time in seconds.

    Returns:
        Formatted time string (e.g., "1h 23m 45s" or "45.2s").
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def read_federations(federations_path: Path) -> List[Tuple[str, str]]:
    """
    Read federation codes and names from a CSV file.

    Args:
        federations_path: Path to the CSV file with 'code' and 'name' columns.

    Returns:
        List of tuples (code, name) for each federation.

    Raises:
        FileNotFoundError: If the federations file doesn't exist.
        ValueError: If the CSV file is malformed.
    """
    if not federations_path.exists():
        raise FileNotFoundError(f"Federations file not found: {federations_path}")

    federations = []
    with open(federations_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "code" not in reader.fieldnames or "name" not in reader.fieldnames:
            raise ValueError("CSV file must contain 'code' and 'name' columns")
        for row in reader:
            code = row["code"].strip()
            name = row["name"].strip()
            if code:  # Skip empty rows
                federations.append((code, name))

    return federations


def parse_tournament_row(row: List, federation: str) -> Optional[Tournament]:
    """
    Parse a tournament row from the JSON response.

    Row format: [id, name_link, location, time_control, start, end_link, period_text, period, ?]

    Args:
        row: List representing a tournament row from the JSON response.
        federation: Federation code.

    Returns:
        Tournament object if parsing succeeds, None otherwise.
    """
    try:
        tournament_id = str(row[0]) if len(row) > 0 else None
        if not tournament_id:
            return None

        # Extract name from HTML link: "<a href=\/report.phtml?event=399495>4th Annual Forester Open<\/a>"
        name_html = row[1] if len(row) > 1 else ""
        name_start = name_html.find(">") + 1
        name_end = name_html.find("</a>")
        if name_start > 0 and name_end > 0:
            name = name_html[name_start:name_end]
        else:
            # Fallback: try to extract from any HTML
            name = re.sub(r"<[^>]+>", "", name_html).strip() or name_html

        location = row[2] if len(row) > 2 else ""
        time_control = row[3] if len(row) > 3 else "s"
        start_date = row[4] if len(row) > 4 else ""

        # End date is in a link too
        end_html = row[5] if len(row) > 5 else ""
        end_start = end_html.find(">") + 1
        end_end = end_html.find("</a>")
        if end_start > 0 and end_end > 0:
            end_date = end_html[end_start:end_end]
        else:
            end_date = re.sub(r"<[^>]+>", "", end_html).strip() or end_html

        return Tournament(
            tournament_id=tournament_id,
            name=name,
            location=location,
            time_control=time_control,
            start_date=start_date,
            end_date=end_date,
            federation=federation,
        )
    except Exception as e:
        logger.debug(f"Failed to parse tournament row: {e}")
        return None


async def fetch_federation_tournaments(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    code: str,
    name: str,
    year: int,
    month: int,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Tuple[str, str, List[Tournament], Optional[str]]:
    """
    Fetch tournaments for one federation.

    Args:
        session: aiohttp client session.
        semaphore: Semaphore to limit concurrency.
        code: Federation code.
        name: Federation name.
        year: Year to scrape.
        month: Month to scrape.
        max_retries: Maximum number of retry attempts.
        retry_delay: Delay in seconds between retries.

    Returns:
        Tuple of (code, name, tournaments, error_message).
        error_message is None on success.
    """
    period = f"{year}-{month:02d}-01"
    url = f"{TOURNAMENTS_URL}?country={code}&period={period}"

    # Simple headers that work with curl - no need for Referer or Origin
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    for attempt in range(max_retries):
        if _shutdown_requested:
            return (code, name, [], "Shutdown requested")

        async with semaphore:
            try:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        error_msg = f"HTTP {resp.status}"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (2**attempt))
                            continue
                        return (code, name, [], error_msg)

                    # Read response as text first, then parse as JSON
                    # This gives us more control and matches curl's behavior
                    text = await resp.text()

                    # Check if it looks like HTML (starts with <)
                    if text.strip().startswith("<"):
                        error_msg = f"Server returned HTML instead of JSON (got {len(text)} chars)"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (2**attempt))
                            continue
                        return (code, name, [], error_msg)

                    # Try to parse as JSON
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError as e:
                        error_msg = f"Failed to parse JSON response: {e}"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (2**attempt))
                            continue
                        return (code, name, [], error_msg)

                    if "data" not in data:
                        # No tournaments - not an error, might be legitimate
                        return (code, name, [], None)

                    tournaments = []
                    for row in data["data"]:
                        tournament = parse_tournament_row(row, code)
                        if tournament:
                            tournaments.append(tournament)

                    return (code, name, tournaments, None)

            except asyncio.TimeoutError:
                error_msg = "Timeout"
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2**attempt))
                    continue
                return (code, name, [], error_msg)
            except json.JSONDecodeError as e:
                error_msg = f"JSON decode error: {e}"
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2**attempt))
                    continue
                return (code, name, [], error_msg)
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2**attempt))
                    continue
                return (code, name, [], error_msg)

    return (code, name, [], "Max retries exceeded")


async def fetch_available_periods(
    session: aiohttp.ClientSession,
    code: str,
) -> List[dict]:
    """
    Fetch available periods for a federation.

    Args:
        session: aiohttp client session.
        code: Federation code.

    Returns:
        List of period dictionaries with 'num1', 'frl_publish', 'txt2' keys.
    """
    url = f"{PERIODS_URL}?country={code}&periods_tab=1"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (compatible; FIDE-Scraper/1.0)",
    }

    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                return []
            return await resp.json()
    except Exception:
        return []


def graceful_shutdown(signum: int, frame) -> None:
    """
    Handle graceful shutdown on SIGINT (Ctrl+C) or SIGTERM.

    Args:
        signum: Signal number.
        frame: Current stack frame.
    """
    global _shutdown_requested, _shutdown_state

    _shutdown_requested = True

    signal_name = signal.Signals(signum).name
    logger.warning(f"\nReceived {signal_name}, initiating graceful shutdown...")

    # Get state from module-level dict
    all_tournaments = _shutdown_state.get("all_tournaments", [])
    output_path = _shutdown_state.get("output_path")
    log_path = _shutdown_state.get("log_path")
    log_entries = _shutdown_state.get("log_entries", [])
    processed_count = _shutdown_state.get("processed_count", 0)
    total_federations = _shutdown_state.get("total_federations", 0)
    processing_start_time = _shutdown_state.get("processing_start_time", time.time())
    output_format = _shutdown_state.get("output_format", "ids")

    # Remove duplicates
    seen_ids = set()
    unique_tournaments = []
    for t in all_tournaments:
        if t.tournament_id not in seen_ids:
            seen_ids.add(t.tournament_id)
            unique_tournaments.append(t)

    unique_tournaments.sort(key=lambda t: int(t.tournament_id))

    # Save partial results (both formats)
    if output_path and unique_tournaments:
        try:
            # Prepare JSON data
            output_data = [
                {
                    "tournament_id": t.tournament_id,
                    "name": t.name,
                    "location": t.location,
                    "time_control": t.time_control,
                    "start_date": t.start_date,
                    "end_date": t.end_date,
                    "federation": t.federation,
                }
                for t in unique_tournaments
            ]

            # Save IDs file
            ids_path = output_path
            ids_path.parent.mkdir(parents=True, exist_ok=True)
            with open(ids_path, "w", encoding="utf-8") as f:
                for t in unique_tournaments:
                    f.write(f"{t.tournament_id}\n")

            # Save JSON file
            json_path = (
                output_path.parent.parent / "tournament_ids_json" / output_path.name
            )
            if json_path.suffix != ".json":
                json_path = json_path.with_suffix(".json")
            json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            logger.info(
                f"Saved {len(unique_tournaments)} unique tournament IDs to {ids_path} and {json_path}"
            )
        except Exception as e:
            logger.error(f"Error saving partial results: {e}")

    # Save log file
    if log_entries and log_path:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w", encoding="utf-8") as f:
                for entry in log_entries:
                    f.write(f"{entry}\n")
            logger.info(f"Saved {len(log_entries)} log entries to {log_path}")
        except Exception as e:
            logger.warning(f"Error saving log file: {e}")

    # Print summary
    elapsed_time = time.time() - processing_start_time
    print("\n" + "=" * 80)
    print("Graceful Shutdown Summary:")
    print(f"  Federations processed: {processed_count}/{total_federations}")
    print(f"  Tournament IDs collected: {len(all_tournaments)}")
    print(f"  Unique tournament IDs: {len(unique_tournaments)}")
    print(f"  Time elapsed: {format_time(elapsed_time)}")
    if output_path:
        ids_path = output_path
        json_path = output_path.parent.parent / "tournament_ids_json" / output_path.name
        if json_path.suffix != ".json":
            json_path = json_path.with_suffix(".json")
        print(f"  IDs file: {ids_path}")
        print(f"  JSON file: {json_path}")
    if log_entries and log_path:
        print(f"  Log saved to: {log_path}")
    print("=" * 80)

    sys.exit(0)


async def scrape_month(
    year: int,
    month: int,
    federations_path: Path,
    output_path: Path,
    output_format: str = "ids",
    max_concurrency: int = 20,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> List[Tournament]:
    """
    Scrape all federations for a given month.

    Args:
        year: Year to scrape.
        month: Month to scrape (1-12).
        federations_path: Path to federations CSV file.
        output_path: Path to output file.
        output_format: Output format - "ids" for just IDs, "json" for full data.
        max_concurrency: Maximum number of concurrent requests.
        max_retries: Maximum number of retries per federation.
        retry_delay: Base delay in seconds between retries.

    Returns:
        List of unique Tournament objects.
    """
    global _shutdown_state

    # Read federations
    try:
        federations = read_federations(federations_path)
        logger.info(f"Read {len(federations)} federations from {federations_path}")
    except Exception as e:
        logger.error(f"Error reading federations: {e}")
        return []

    logger.info(
        f"Processing {len(federations)} federations for {year}-{month:02d} "
        f"with max concurrency {max_concurrency}"
    )
    start_time = time.time()

    # Set up signal handlers for graceful shutdown
    _shutdown_state = {
        "all_tournaments": [],
        "output_path": output_path,
        "log_path": None,  # Will be set if log_entries exist
        "log_entries": [],
        "processed_count": 0,
        "total_federations": len(federations),
        "processing_start_time": start_time,
        "output_format": output_format,
    }

    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    semaphore = asyncio.Semaphore(max_concurrency)

    # Collect results
    all_tournaments: List[Tournament] = []
    errors = []
    federation_counts = {}
    processed_count = 0

    # Simple session - no cookie jar needed, curl works without it
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_federation_tournaments(
                session, semaphore, code, name, year, month, max_retries, retry_delay
            )
            for code, name in federations
        ]

        # Process results as they complete
        for coro in asyncio.as_completed(tasks):
            if _shutdown_requested:
                logger.warning("Shutdown requested, cancelling remaining tasks...")
                # Cancel remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
                break

            code, name, tournaments, error = await coro
            processed_count += 1

            # Update shutdown state
            _shutdown_state["all_tournaments"] = all_tournaments
            _shutdown_state["processed_count"] = processed_count
            _shutdown_state["log_entries"] = errors

            if error:
                errors.append(f"{code} ({name}): {error}")
                logger.warning(f"{code} ({name}): {error}")
            else:
                federation_counts[code] = len(tournaments)
                if tournaments:
                    logger.info(f"{code} ({name}): {len(tournaments)} tournaments")
                all_tournaments.extend(tournaments)

            # Progress update
            if processed_count % 10 == 0 or processed_count == len(federations):
                elapsed = time.time() - start_time
                progress_pct = (processed_count / len(federations)) * 100
                if processed_count > 0:
                    avg_time = elapsed / processed_count
                    remaining = avg_time * (len(federations) - processed_count)
                    logger.info(
                        f"[{processed_count}/{len(federations)} ({progress_pct:.1f}%)] "
                        f"Elapsed: {format_time(elapsed)}, "
                        f"Est. remaining: {format_time(remaining)}"
                    )

    # Deduplicate by tournament_id
    seen_ids = set()
    unique_tournaments = []
    for t in all_tournaments:
        if t.tournament_id not in seen_ids:
            seen_ids.add(t.tournament_id)
            unique_tournaments.append(t)

    # Sort by ID
    unique_tournaments.sort(key=lambda t: int(t.tournament_id))

    # Prepare JSON data
    output_data = [
        {
            "tournament_id": t.tournament_id,
            "name": t.name,
            "location": t.location,
            "time_control": t.time_control,
            "start_date": t.start_date,
            "end_date": t.end_date,
            "federation": t.federation,
        }
        for t in unique_tournaments
    ]

    # Write both formats to separate subfolders
    # IDs file
    ids_path = output_path
    ids_path.parent.mkdir(parents=True, exist_ok=True)
    with open(ids_path, "w", encoding="utf-8") as f:
        for t in unique_tournaments:
            f.write(f"{t.tournament_id}\n")

    # JSON file (always saved, prettified)
    json_path = output_path.parent.parent / "tournament_ids_json" / output_path.name
    if json_path.suffix != ".json":
        json_path = json_path.with_suffix(".json")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - start_time

    # Time control breakdown
    tc_counts = {}
    for t in unique_tournaments:
        tc_counts[t.time_control] = tc_counts.get(t.time_control, 0) + 1

    # Summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(
        f"  Federations processed: {len(federations) - len(errors)}/{len(federations)}"
    )
    print(f"  Errors: {len(errors)}")
    print(f"  Total tournaments: {len(all_tournaments)}")
    print(f"  Unique tournaments: {len(unique_tournaments)}")
    print(f"  Time taken: {format_time(elapsed)}")
    print(f"  IDs file: {ids_path}")
    print(f"  JSON file: {json_path}")
    if tc_counts:
        print(f"  By time control: {tc_counts}")
    print("=" * 80)

    return unique_tournaments


def main() -> int:
    """
    Main function to orchestrate the tournament ID scraping process.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    parser = argparse.ArgumentParser(
        description="Fast FIDE tournament scraper using direct AJAX calls"
    )
    parser.add_argument(
        "--year", type=int, required=True, help="Year to scrape (e.g., 2025)"
    )
    parser.add_argument(
        "--month", type=int, required=True, help="Month to scrape (1-12)"
    )
    parser.add_argument(
        "--federations",
        "-f",
        type=str,
        default="data/federations.csv",
        help="Path to federations CSV file (default: data/federations.csv from repo root)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file path (default: data/tournament_ids_YYYY_MM from repo root)",
    )
    parser.add_argument(
        "--format",
        choices=["ids", "json"],
        default="ids",
        help="Output format: 'ids' for just IDs, 'json' for full tournament data",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=10,
        help="Maximum number of concurrent requests (default: 10)",
    )
    parser.add_argument(
        "--max-retries",
        "-r",
        type=int,
        default=3,
        help="Maximum number of retries per federation (default: 3)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help="Base delay in seconds between retries (default: 1.0)",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Disable verbose output"
    )

    args = parser.parse_args()

    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Month must be between 1 and 12")
        return 1

    # Set logging level
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # Determine paths relative to repo root
    repo_root = Path(__file__).parent.parent.parent
    federations_path = repo_root / args.federations

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = repo_root / output_path
    else:
        # Always save to tournament_ids subfolder (format flag only affects which is primary)
        output_path = (
            repo_root / "data" / "tournament_ids" / f"{args.year}_{args.month:02d}"
        )

    # Run the scraper
    try:
        asyncio.run(
            scrape_month(
                args.year,
                args.month,
                federations_path,
                output_path,
                args.format,
                args.concurrency,
                args.max_retries,
                args.retry_delay,
            )
        )
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

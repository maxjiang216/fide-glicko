#!/usr/bin/env python3
"""
Scrape FIDE website to get the list of federations.
"""

import argparse
import csv
import logging
import signal
import sys
import time
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

URL = "https://ratings.fide.com/rated_tournaments.phtml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# State for graceful shutdown
_shutdown_state = {"federations": [], "output_file": None, "completed": False}


def is_valid_federation_code(code: str) -> bool:
    """Validate federation code: 3 uppercase letters (A-Z)."""
    if not code or len(code) != 3:
        return False
    return code.isalpha() and code.isupper()


def get_federations_with_retries(
    max_retries: int = 3, retry_delay: float = 1.0
) -> List[Dict[str, str]]:
    """
    Scrape federations from FIDE website with retry logic.

    Args:
        max_retries: Maximum number of retry attempts
        retry_delay: Delay in seconds between retries

    Returns:
        List of dictionaries with 'code' and 'name' keys
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(URL, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            select = soup.find("select", id="select_country")
            if not select:
                logger.warning("Country selector not found; returning empty list")
                return []

            federations = []

            for option in select.find_all("option"):
                value = (option.get("value") or "").strip()
                name = option.text.strip()

                # Skip the placeholder option
                if not value or value.lower() == "all":
                    continue
                # Normalize to uppercase for validation
                code = value.upper() if len(value) == 3 else value
                if not is_valid_federation_code(code):
                    logger.warning(f"Invalid federation code skipped: {value!r} ({name})")
                    continue
                federations.append({"code": code, "name": name})

            if not federations:
                logger.warning("No valid federations found in country selector")

            # CGO (Republic of Congo) not on FIDE country selector; add if missing
            codes = {f["code"] for f in federations}
            if "CGO" not in codes:
                federations.append({"code": "CGO", "name": "Republic of the Congo"})
                federations.sort(key=lambda f: f["code"])

            return federations
        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            else:
                raise


def _graceful_shutdown(signum: int, frame) -> None:
    """Save partial results on SIGINT/SIGTERM."""
    global _shutdown_state
    logger.warning("\nReceived interrupt, attempting graceful shutdown...")
    federations = _shutdown_state.get("federations", [])
    output_file = _shutdown_state.get("output_file")
    if federations and output_file:
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["code", "name"])
                for fed in federations:
                    writer.writerow([fed["code"], fed["name"]])
            logger.info(f"Saved {len(federations)} federations to {output_file}")
        except Exception as e:
            logger.error(f"Error saving partial results: {e}")
    else:
        logger.info("No partial results to save")
    sys.exit(130 if signum == 2 else 0)  # 130 = SIGINT


def main():
    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    parser = argparse.ArgumentParser(
        description="Scrape FIDE website to get the list of federations"
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        default="data",
        help="Directory to output the result (default: 'data' from repo root)",
    )
    parser.add_argument(
        "--filename",
        "-f",
        type=str,
        default="federations.csv",
        help="Output filename (default: federations.csv)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Disable verbose output (default: verbose is enabled)",
    )
    parser.add_argument(
        "--override",
        "-o",
        action="store_true",
        help="Override existing file and scrape again",
    )

    args = parser.parse_args()

    # Log level: DEBUG for verbose, WARNING for quiet
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Determine output path (relative to repo root)
    repo_root = Path(__file__).parent.parent.parent
    output_dir = repo_root / args.directory
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / args.filename

    # Check if file already exists
    if output_file.exists() and not args.override:
        logger.info(
            "File %s already exists. Use --override to scrape and replace.",
            output_file,
        )
        return 0

    start_time = time.time()
    logger.info("Fetching federations list...")

    try:
        federations = get_federations_with_retries()
    except Exception as e:
        logger.error("Error fetching federations: %s", e)
        return 1

    if not federations:
        logger.error("No federations retrieved")
        return 1

    # Store for graceful shutdown
    _shutdown_state["federations"] = federations
    _shutdown_state["output_file"] = output_file

    elapsed_time = time.time() - start_time

    # Write to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for fed in federations:
            writer.writerow([fed["code"], fed["name"]])

    if not args.quiet:
        for fed in federations:
            logger.info("%s: %s", fed["code"], fed["name"])
        logger.info("Found %d federations", len(federations))
        logger.info("Time taken: %.2f seconds", elapsed_time)

    logger.info("Saved %d federations to %s", len(federations), output_file)
    _shutdown_state["completed"] = True
    return 0


if __name__ == "__main__":
    exit(main())

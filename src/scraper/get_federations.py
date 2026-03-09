#!/usr/bin/env python3
"""
Scrape FIDE website to get the list of federations.

Supports flexible output: local path (default) or S3 URI (s3://bucket/key).
Use --output s3://bucket/key for Lambda or remote storage.
"""

import argparse
import csv
import io
import logging
import signal
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

from s3_io import is_s3_path, output_exists, write_output

URL = "https://ratings.fide.com/rated_tournaments.phtml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# State for graceful shutdown
_shutdown_state = {"federations": [], "output_path": None, "completed": False}


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
            response = requests.get(URL, timeout=55)
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
                    logger.warning(
                        f"Invalid federation code skipped: {value!r} ({name})"
                    )
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


def _federations_to_csv(federations: List[Dict[str, str]]) -> str:
    """Convert federations list to CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["code", "name"])
    for fed in federations:
        writer.writerow([fed["code"], fed["name"]])
    return buf.getvalue()


def _graceful_shutdown(signum: int, frame) -> None:
    """Save partial results on SIGINT/SIGTERM."""
    global _shutdown_state
    logger.warning("\nReceived interrupt, attempting graceful shutdown...")
    federations = _shutdown_state.get("federations", [])
    output_path = _shutdown_state.get("output_path")
    if federations and output_path:
        try:
            content = _federations_to_csv(federations)
            write_output(content, output_path)
            logger.info("Saved %d federations to %s", len(federations), output_path)
        except Exception as e:
            logger.error("Error saving partial results: %s", e)
    else:
        logger.info("No partial results to save")
    sys.exit(130 if signum == 2 else 0)  # 130 = SIGINT


def run(
    output_path: str,
    override: bool = False,
    quiet: bool = False,
) -> int:
    """
    Scrape federations and write to output_path.

    Args:
        output_path: Local path or S3 URI (s3://bucket/key).
        override: If True, overwrite existing file. If False, skip when exists.
        quiet: If True, reduce log output to WARNING only.

    Returns:
        0 on success, 1 on failure.
    """
    global _shutdown_state

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if output_exists(output_path) and not override:
        logger.info(
            "Output %s already exists. Use override=True to scrape and replace.",
            output_path,
        )
        return 0

    start_time = time.time()
    logger.info("Fetching federations list from %s...", URL)

    try:
        federations = get_federations_with_retries()
    except Exception as e:
        logger.error("Error fetching federations: %s", e)
        return 1

    if not federations:
        logger.error("No federations retrieved")
        return 1

    _shutdown_state["federations"] = federations
    _shutdown_state["output_path"] = output_path

    elapsed_time = time.time() - start_time
    content = _federations_to_csv(federations)
    write_output(content, output_path)

    if not quiet:
        for fed in federations:
            logger.info("%s: %s", fed["code"], fed["name"])
        logger.info("Found %d federations", len(federations))
        logger.info("Time taken: %.2f seconds", elapsed_time)

    logger.info("Saved %d federations to %s", len(federations), output_path)
    _shutdown_state["completed"] = True
    return 0


def main() -> int:
    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    parser = argparse.ArgumentParser(
        description="Scrape FIDE website to get the list of federations"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path: local file or S3 URI (s3://bucket/key). Overrides -d/-f.",
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        default="data",
        help="Directory to output (default: data). Ignored if --output is set.",
    )
    parser.add_argument(
        "--filename",
        "-f",
        type=str,
        default="federations.csv",
        help="Output filename (default: federations.csv). Ignored if --output is set.",
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

    if args.output is not None:
        output_path = args.output
    else:
        repo_root = Path(__file__).parent.parent.parent
        output_dir = repo_root / args.directory
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / args.filename)

    return run(
        output_path=output_path,
        override=args.override,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    sys.exit(main())

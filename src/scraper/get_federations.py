#!/usr/bin/env python3
"""
Scrape FIDE website to get the list of federations.
"""
import argparse
import csv
import time
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

URL = "https://ratings.fide.com/rated_tournaments.phtml"


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
                raise RuntimeError("Country selector not found")

            federations = []

            for option in select.find_all("option"):
                value = option.get("value")
                name = option.text.strip()

                # Skip the placeholder option
                if value and value.lower() != "all":
                    federations.append({"code": value, "name": name})

            return federations
        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            else:
                raise


def main():
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

    # Verbose is True by default, unless --quiet is specified
    verbose = not args.quiet

    # Determine output path (relative to repo root)
    # From src/scraper/get_federations.py, go up 3 levels to reach repo root
    repo_root = Path(__file__).parent.parent.parent
    output_dir = repo_root / args.directory
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / args.filename

    # Check if file already exists
    if output_file.exists() and not args.override:
        print(
            f"File {output_file} already exists. Use --override to scrape and replace."
        )
        return 0

    start_time = time.time()

    if verbose:
        print("Fetching federations list...")

    try:
        federations = get_federations_with_retries()
    except Exception as e:
        print(f"Error fetching federations: {e}")
        return 1

    elapsed_time = time.time() - start_time

    # Write to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for fed in federations:
            writer.writerow([fed["code"], fed["name"]])

    if verbose:
        # Print all federations
        for fed in federations:
            print(f"{fed['code']}: {fed['name']}")

        # Print count
        print(f"\nFound {len(federations)} federations")

        # Print time taken
        print(f"Time taken: {elapsed_time:.2f} seconds")

    print(f"Saved {len(federations)} federations to {output_file}")

    return 0


if __name__ == "__main__":
    exit(main())

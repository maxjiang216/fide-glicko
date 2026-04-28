#!/usr/bin/env python3
"""
Build the country-months lookup used by the tournaments Lambda to skip federations
that have no data for a requested month, preventing the 900s Lambda timeout on
early months where only a fraction of the ~208 federations had rated tournaments.

Output: s3://fide-glicko/metadata/country_months.json  (or a local path)
Format:
  {
    "generated_at": "2025-01-01T00:00:00Z",
    "country_months": {
      "USA": ["2006-01", "2006-02", ...],
      "FRA": ["2008-03", ...]
    }
  }

Usage:

  # Fast path: convert the existing exploratory CSV (instant)
  uv run scripts/scrape_country_months.py \\
    --from-csv exploratory/data/tournaments_by_country_month.csv \\
    --output s3://fide-glicko/metadata/country_months.json

  # Re-scrape fresh from FIDE via Playwright (~5-10 min for 208 federations)
  uv run scripts/scrape_country_months.py \\
    --output s3://fide-glicko/metadata/country_months.json

  # Save locally for inspection before uploading
  uv run scripts/scrape_country_months.py \\
    --from-csv exploratory/data/tournaments_by_country_month.csv \\
    --output data/country_months.json
"""

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))

from s3_io import write_output

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://ratings.fide.com/rated_tournaments.phtml"
CONCURRENCY = 5


def from_csv(csv_path: Path) -> dict[str, list[str]]:
    """
    Build country_months from the exploratory CSV
    (columns: country, year, month, num_tournaments).
    Only includes rows where num_tournaments > 0.
    """
    country_months: dict[str, set[str]] = defaultdict(set)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count = int(row.get("num_tournaments", 0) or 0)
            if count > 0:
                code = row["country"].strip()
                year = int(row["year"])
                month = int(row["month"])
                country_months[code].add(f"{year}-{month:02d}")
    return {code: sorted(months) for code, months in country_months.items()}


async def scrape_one(semaphore: asyncio.Semaphore, code: str) -> tuple[str, list[str]]:
    """Scrape the #archive dropdown for one federation using Playwright."""
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    async with semaphore:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.goto(
                    f"{BASE_URL}?country={code}",
                    wait_until="networkidle",
                    timeout=30000,
                )
                try:
                    await page.wait_for_selector("#archive", timeout=10000)
                except PWTimeout:
                    return code, []

                html = await page.content()
            finally:
                await browser.close()

    # Parse <select id="archive"> options
    months = []
    for m in re.finditer(r'<option[^>]+value="(\d{4}-\d{2}-\d{2})"', html):
        months.append(m.group(1)[:7])  # YYYY-MM
    return code, sorted(set(months))


async def scrape_all(codes: list[str]) -> dict[str, list[str]]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [scrape_one(semaphore, code) for code in codes]
    results: dict[str, list[str]] = {}
    total = len(tasks)
    done = 0
    for coro in asyncio.as_completed(tasks):
        code, months = await coro
        results[code] = months
        done += 1
        if done % 20 == 0 or done == total:
            logger.info("  %d/%d federations scraped", done, total)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--from-csv",
        metavar="PATH",
        help="Build from existing exploratory CSV instead of scraping (fast path)",
    )
    parser.add_argument(
        "--federations",
        default="data/federations.csv",
        help="Federations CSV for --scrape mode (default: data/federations.csv)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path: local file or s3://bucket/key",
    )
    args = parser.parse_args()

    start = time.time()

    if args.from_csv:
        csv_path = Path(args.from_csv)
        if not csv_path.exists():
            logger.error("CSV not found: %s", csv_path)
            return 1
        logger.info("Building from CSV: %s", csv_path)
        country_months = from_csv(csv_path)
    else:
        fed_path = Path(args.federations)
        if not fed_path.exists():
            logger.error("Federations file not found: %s", fed_path)
            return 1
        codes = []
        with open(fed_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code = (row.get("code") or "").strip()
                if code:
                    codes.append(code)
        logger.info("Scraping %d federations via Playwright...", len(codes))
        country_months = asyncio.run(scrape_all(codes))

    elapsed = time.time() - start
    total_entries = sum(len(v) for v in country_months.values())
    non_empty = sum(1 for v in country_months.values() if v)
    logger.info(
        "Done in %.1fs: %d federations (%d with data), %d total country-month entries",
        elapsed,
        len(country_months),
        non_empty,
        total_entries,
    )

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "country_months": country_months,
    }
    write_output(json.dumps(output, indent=2, ensure_ascii=False), args.output)
    logger.info("Saved to %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())

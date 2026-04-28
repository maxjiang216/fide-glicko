#!/usr/bin/env python3
"""
One-time script: scrape the available tournament months per FIDE federation.

Queries a_tournaments_panel.php?country={code}&periods_tab=1 for each federation
and builds a lookup of which months each country has tournament data for. This is
used by the tournaments Lambda to skip empty federation/month combinations, which
prevents the 900s Lambda timeout on older months where only a fraction of the
~208 federations had rated tournaments.

Output format:
  {
    "generated_at": "2025-01-01T00:00:00Z",
    "country_months": {
      "USA": ["2006-01", "2006-02", ...],
      "FRA": ["2008-03", ...]
    }
  }

Usage:
  # Save to S3 (use before deploying the stack)
  uv run scripts/scrape_country_months.py --output s3://fide-glicko/metadata/country_months.json

  # Save locally for inspection
  uv run scripts/scrape_country_months.py --output data/country_months.json

  # Use a custom federations file
  uv run scripts/scrape_country_months.py --federations data/federations.csv --output ...
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

# Allow running from repo root or scripts/
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))

from get_tournaments import PERIODS_URL, fetch_available_periods, read_federations
from s3_io import is_s3_path, write_output

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONCURRENCY = 5
RATE_LIMIT_DELAY = 0.2  # seconds between requests per worker


async def fetch_periods_for_code(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    code: str,
) -> tuple[str, list[str]]:
    """Return (code, [YYYY-MM, ...]) for one federation."""
    async with semaphore:
        await asyncio.sleep(RATE_LIMIT_DELAY)
        periods = await fetch_available_periods(session, code)
    months = []
    for p in periods:
        raw = p.get("frl_publish") or p.get("num1") or ""
        raw = str(raw).strip()
        if len(raw) >= 7:
            months.append(raw[:7])  # YYYY-MM
    return code, months


async def scrape_all(
    federations: list[tuple[str, str]],
) -> dict[str, list[str]]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_periods_for_code(session, semaphore, code)
            for code, _ in federations
        ]
        results: dict[str, list[str]] = {}
        total = len(tasks)
        done = 0
        for coro in asyncio.as_completed(tasks):
            code, months = await coro
            results[code] = sorted(set(months))
            done += 1
            if done % 20 == 0 or done == total:
                logger.info("  %d/%d federations scraped", done, total)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--federations",
        default="data/federations.csv",
        help="Path to federations CSV (default: data/federations.csv)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path: local file or s3://bucket/key",
    )
    args = parser.parse_args()

    fed_path = Path(args.federations)
    if not fed_path.exists():
        logger.error("Federations file not found: %s", fed_path)
        return 1

    federations = read_federations(fed_path)
    logger.info("Loaded %d federations from %s", len(federations), fed_path)

    start = time.time()
    country_months = asyncio.run(scrape_all(federations))
    elapsed = time.time() - start

    total_entries = sum(len(v) for v in country_months.items())
    logger.info(
        "Done in %.1fs: %d federations, %d total country-month entries",
        elapsed,
        len(country_months),
        total_entries,
    )

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "country_months": country_months,
    }
    content = json.dumps(output, indent=2, ensure_ascii=False)

    write_output(content, args.output)
    logger.info("Saved to %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())

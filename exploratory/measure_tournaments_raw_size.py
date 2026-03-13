#!/usr/bin/env python3
"""
Measure raw JSON response sizes from FIDE tournaments API.

Fetches a sample of federations, reports uncompressed + gzip sizes.
Run from repo root with federations CSV, e.g.:
  python exploratory/measure_tournaments_raw_size.py data/test/data/federations.csv --year 2025 --month 3 --limit 20
"""
import argparse
import asyncio
import csv
import gzip
import io
import sys
from pathlib import Path

import aiohttp

TOURNAMENTS_URL = "https://ratings.fide.com/a_tournaments.php"


def compress_gzip(data: bytes, level: int = 9) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as z:
        z.write(data)
    return buf.getvalue()


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("federations", type=str, help="Path to federations CSV")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=3)
    parser.add_argument("--limit", type=int, default=30, help="Max federations to fetch (0=all)")
    args = parser.parse_args()

    path = Path(args.federations)
    if not path.exists():
        print(f"Not found: {path}")
        return 1

    federations = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            if code:
                federations.append(code)

    if args.limit > 0:
        federations = federations[: args.limit]
    n = len(federations)
    print(f"Fetching {n} federations for {args.year}-{args.month:02d}...")

    async def fetch_with_content(session, code, year, month):
        period = f"{year}-{month:02d}-01"
        url = f"{TOURNAMENTS_URL}?country={code}&period={period}"
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text()
            raw_bytes = text.encode("utf-8")
            gz_bytes = compress_gzip(raw_bytes)
            return code, len(raw_bytes), len(gz_bytes)

    total_raw = 0
    total_gz = 0
    async with aiohttp.ClientSession() as session:
        for i, code in enumerate(federations):
            code, raw_size, gz_size = await fetch_with_content(
                session, code, args.year, args.month
            )
            total_raw += raw_size
            total_gz += gz_size
            if (i + 1) % 10 == 0:
                print(f"  {i + 1}/{n}...")

    print()
    print(f"Sample: {n} federations")
    print(f"  Uncompressed total: {total_raw:,} bytes ({total_raw / 1e6:.2f} MB)")
    print(f"  Gzip-9 total:       {total_gz:,} bytes ({total_gz / 1e6:.2f} MB)")
    print(f"  Ratio: {100 * total_gz / total_raw:.1f}%")
    if n > 0:
        avg_raw = total_raw / n
        avg_gz = total_gz / n
        print(f"  Per federation avg: {avg_raw:,.0f} raw, {avg_gz:,.0f} gzipped")
    # Extrapolate to 208 federations
    full_n = 208
    if n > 0:
        est_raw = total_raw * (full_n / n)
        est_gz = total_gz * (full_n / n)
        print()
        print(f"Extrapolated to {full_n} federations:")
        print(f"  ~{est_raw:,.0f} bytes ({est_raw / 1e6:.1f} MB) uncompressed")
        print(f"  ~{est_gz:,.0f} bytes ({est_gz / 1e6:.1f} MB) gzip-9")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

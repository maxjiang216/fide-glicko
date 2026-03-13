#!/usr/bin/env python3
"""
Measure raw HTML response sizes from FIDE tournament reports API.

Fetches a sample of tournament codes, reports uncompressed + gzip sizes.
Run from repo root, e.g.:
  python exploratory/measure_reports_raw_size.py data/test/data/tournament_id_chunks/chunk_0.txt --limit 20
"""
import argparse
import gzip
import io
import sys
from pathlib import Path

import requests

URL_TEMPLATE = "https://ratings.fide.com/tournament_src_report.phtml?code={code}"


def compress_gzip(data: bytes, level: int = 9) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as z:
        z.write(data)
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("codes_file", type=str, help="Path to tournament codes/IDs file (one per line)")
    parser.add_argument("--limit", type=int, default=20, help="Max codes to fetch (0=all)")
    args = parser.parse_args()

    path = Path(args.codes_file)
    if not path.exists():
        print(f"Not found: {path}")
        return 1

    codes = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    if args.limit > 0:
        codes = codes[: args.limit]
    n = len(codes)
    print(f"Fetching {n} tournament report pages...")

    total_raw = 0
    total_gz = 0
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; FIDE-Scraper/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    for i, code in enumerate(codes):
        try:
            resp = session.get(URL_TEMPLATE.format(code=code), timeout=45)
            if resp.status_code != 200:
                print(f"  {code}: HTTP {resp.status_code}")
                continue
            raw = resp.content
            gz = compress_gzip(raw)
            total_raw += len(raw)
            total_gz += len(gz)
            if (i + 1) % 5 == 0:
                print(f"  {i + 1}/{n}...")
        except Exception as e:
            print(f"  {code}: {e}")

    print()
    print(f"Sample: {n} tournaments")
    print(f"  Uncompressed total: {total_raw:,} bytes ({total_raw / 1e6:.2f} MB)")
    print(f"  Gzip-9 total:       {total_gz:,} bytes ({total_gz / 1e6:.2f} MB)")
    if total_raw > 0:
        print(f"  Ratio: {100 * total_gz / total_raw:.1f}%")
        avg_raw = total_raw / n
        avg_gz = total_gz / n
        print(f"  Per tournament avg: {avg_raw:,.0f} raw, {avg_gz:,.0f} gzipped")

    chunk_size = 225
    if n > 0:
        est_raw_chunk = total_raw * (chunk_size / n)
        est_gz_chunk = total_gz * (chunk_size / n)
        print()
        print(f"Extrapolated per chunk (~{chunk_size} tournaments):")
        print(f"  ~{est_raw_chunk:,.0f} bytes ({est_raw_chunk / 1e6:.1f} MB) uncompressed")
        print(f"  ~{est_gz_chunk:,.0f} bytes ({est_gz_chunk / 1e6:.1f} MB) gzip-9")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Run split IDs locally and report peak memory (RSS).

Usage:
  python exploratory/measure_split_ids_memory.py
  python exploratory/measure_split_ids_memory.py --ids data/tournament_ids/2024_01 --chunk-count 50

Uses resource.getrusage to report max RSS. Split IDs is lightweight; this validates sizing.
"""

import argparse
import resource
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPER_DIR = REPO_ROOT / "src" / "scraper"
sys.path.insert(0, str(SCRAPER_DIR))

from split_tournament_ids import run


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile split IDs memory usage")
    parser.add_argument("--ids", default=None, help="Path to tournament IDs file")
    parser.add_argument("--chunk-count", "-n", type=int, default=50, help="Number of chunks")
    args = parser.parse_args()

    ids_path = args.ids or str(REPO_ROOT / "data" / "tournament_ids" / "2024_01")
    if not Path(ids_path).exists():
        print(f"Error: {ids_path} not found. Run get_tournaments for Jan 2024 first.")
        return 1

    chunks = run(
        ids_path=ids_path,
        chunk_count=args.chunk_count,
        bucket="fide-glicko",
        output_prefix="data",
        year=2024,
        month=1,
        override=False,
        quiet=True,
    )

    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss_kb = usage.ru_maxrss
    if sys.platform == "darwin" and max_rss_kb < 1024 * 1024:
        max_rss_mb = max_rss_kb / (1024 * 1024)
    else:
        max_rss_mb = max_rss_kb / 1024

    print(f"\nPeak memory (RSS): {max_rss_mb:.1f} MB")
    suggested = int(max_rss_mb) + 64
    print(f"Lambda suggestion: >= {suggested} MB")

    return 0 if chunks else 1


if __name__ == "__main__":
    sys.exit(main())

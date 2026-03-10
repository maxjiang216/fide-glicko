#!/usr/bin/env python3
"""
Run tournament details scraper locally and report peak memory (RSS).

Usage:
  python exploratory/measure_tournament_details_memory.py
  python exploratory/measure_tournament_details_memory.py -q           # quiet
  python exploratory/measure_tournament_details_memory.py --limit 50   # scrape 50 tournaments
  python exploratory/measure_tournament_details_memory.py --input path/to/ids.txt

Uses resource.getrusage to report max RSS. Helps size Lambda memory and chunk size.
"""

import argparse
import resource
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPER_DIR = REPO_ROOT / "src" / "scraper"
sys.path.insert(0, str(SCRAPER_DIR))

# Default IDs for a quick profile (from tests/fixtures)
DEFAULT_IDS = [
    "368261",  # Candidates 2024
    "397341",  # World Blitz
    "449502",  # World Cup 25
    "418871",  # Blitz Playoff
    "393912",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile tournament details scraper memory usage",
    )
    parser.add_argument(
        "--input",
        help="Path to tournament IDs file (one per line). If not set, uses default IDs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N tournaments (0 = all).",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Reduce log output during scrape",
    )
    args = parser.parse_args()

    if args.input:
        input_path = args.input
        ids_path = Path(args.input)
        if not ids_path.exists():
            print(f"Error: input file not found: {args.input}")
            return 1
    else:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".txt",
            delete=False,
        ) as f:
            f.write("\n".join(DEFAULT_IDS) + "\n")
            input_path = f.name

    output_path = tempfile.mktemp(prefix="details_profile_", suffix="")

    try:
        from get_tournament_details import run

        limit = args.limit if args.limit > 0 else 0
        exit_code = run(
            input_path=input_path,
            output_path=output_path,
            rate_limit=0.5,
            max_retries=2,
            checkpoint=0,
            quiet=args.quiet,
            limit=limit,
        )
    finally:
        if not args.input and Path(input_path).exists():
            Path(input_path).unlink(missing_ok=True)
        for ext in [".parquet", "_sample.json", "_report.json", "_failures.json", "_time_control_unique_values.txt"]:
            p = Path(output_path + ext)
            if p.exists():
                p.unlink()

    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss_kb = usage.ru_maxrss
    # Linux: ru_maxrss is KB; macOS: bytes (pre-10.13) or KB (10.13+)
    if sys.platform == "darwin" and max_rss_kb < 1024 * 1024:
        max_rss_mb = max_rss_kb / (1024 * 1024)
    else:
        max_rss_mb = max_rss_kb / 1024

    print(f"\nPeak memory (RSS): {max_rss_mb:.1f} MB")
    suggested = int(max_rss_mb) + 256
    print(f"Lambda suggestion: >= {suggested} MB (add headroom for runtime)")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())

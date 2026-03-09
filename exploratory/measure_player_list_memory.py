#!/usr/bin/env python3
"""
Run player list pipeline locally and report peak memory (RSS).

Usage:
  python exploratory/measure_player_list_memory.py
  python exploratory/measure_player_list_memory.py -q   # quiet (less log noise)

Uses resource.getrusage to report max RSS. Helps size Lambda memory.
"""

import resource
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPER_DIR = REPO_ROOT / "src" / "scraper"
sys.path.insert(0, str(SCRAPER_DIR))

# Use local output, override existing; forward -q if present
_quiet = "-q" in sys.argv or "--quiet" in sys.argv
sys.argv = ["get_player_list", "-d", "data", "-o"]
if _quiet:
    sys.argv.append("-q")

from get_player_list import main

if __name__ == "__main__":
    exit_code = main()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss_kb = usage.ru_maxrss
    # Linux: ru_maxrss is KB; macOS: bytes (pre-10.13) or KB (10.13+)
    if sys.platform == "darwin" and max_rss_kb < 1024 * 1024:
        max_rss_mb = max_rss_kb / (1024 * 1024)  # was bytes
    else:
        max_rss_mb = max_rss_kb / 1024
    print(f"\nPeak memory (RSS): {max_rss_mb:.1f} MB")
    suggested = int(max_rss_mb) + 256
    print(f"Lambda suggestion: >= {suggested} MB (add headroom for runtime)")
    sys.exit(exit_code)

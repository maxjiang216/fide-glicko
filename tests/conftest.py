"""Pytest configuration and path setup for scraper tests."""

import sys
from pathlib import Path

# Add src/scraper to path so tests can import scraper modules
_scraper_path = Path(__file__).parent.parent / "src" / "scraper"
if str(_scraper_path) not in sys.path:
    sys.path.insert(0, str(_scraper_path))

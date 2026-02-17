#!/usr/bin/env python3
"""
Exploratory: Download FIDE Combined list in XML format and validate parsing.

The main scraper (get_player_list.py) uses XML. This script downloads the XML
and runs the pipeline to verify counts and sample data.

Run from repo root: python exploratory/explore_player_list_xml.py
"""

import sys
from pathlib import Path

# Add src/scraper for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))


def main():
    print("Downloading and parsing FIDE players list (XML)...")
    from get_player_list import get_player_list

    players = get_player_list()
    print(f"Parsed {len(players)} players")
    if players:
        print(f"Sample: id={players[0]['id']}, name={players[0]['name']!r}, fed={players[0]['fed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Exploratory script to inspect the FIDE players_list XML format.

Downloads the XML zip, parses with get_player_list, and shows value ranges
and distributions.

Run from repo root: python exploratory/explore_player_list_format.py
"""

import sys
from collections import Counter
from pathlib import Path

# Add src/scraper for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))


def main():
    print("Downloading and parsing players_list (XML)...")
    from get_player_list import get_player_list

    players = get_player_list()
    print(f"Parsed {len(players)} players\n")

    if not players:
        print("No players parsed!")
        return 1

    # Value ranges and distributions
    print("\n--- Column analysis ---")
    sample = players[:10000]

    for key in players[0]:
        values = [p[key] for p in sample if p.get(key) is not None]
        if not values:
            print(f"  {key}: all null")
            continue
        if isinstance(values[0], int):
            print(f"  {key}: min={min(values)}, max={max(values)}, sample={values[:3]}")
        else:
            uniq = len(set(values))
            c = Counter(values)
            top = c.most_common(3)
            print(f"  {key}: {uniq} unique, top={top}, sample={list(values[:2])}")

    # Output schema
    print("\n--- Output schema (id, name, byear, sex, fed, title) ---")
    print("  id: int64")
    print("  name: string")
    print("  byear: int32 nullable")
    print("  sex: string (M/F)")
    print("  fed: string (3 chars, uppercase)")
    print("  title: string (GM, IM, FM, etc.)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

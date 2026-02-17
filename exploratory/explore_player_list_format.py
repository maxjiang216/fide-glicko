#!/usr/bin/env python3
"""
Exploratory script to inspect the FIDE players_list.zip format.

Downloads the file, inspects structure, column positions, value ranges,
and helps decide parquet schema and datatypes.

Run from repo root: python exploratory/explore_player_list_format.py
"""

import sys
from collections import Counter
from pathlib import Path

import requests

# Add src/scraper for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))

DOWNLOAD_URL = "https://ratings.fide.com/download/players_list.zip"


def main():
    print("Downloading players_list.zip...")
    resp = requests.get(DOWNLOAD_URL, timeout=120)
    resp.raise_for_status()
    zip_bytes = resp.content
    print(f"Downloaded {len(zip_bytes) / 1e6:.1f} MB\n")

    import zipfile
    from io import BytesIO

    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        print("Files in zip:", names)
        txt_name = next((n for n in names if n.endswith(".txt")), names[0])

        with zf.open(txt_name) as f:
            content = f.read().decode("utf-8", errors="replace")

    lines = content.splitlines()
    print(f"\nTotal lines: {len(lines)}")
    print("\n--- Header ---")
    print(repr(lines[0]))
    print(f"Header length: {len(lines[0])}")
    print("\n--- First 5 data lines (raw) ---")
    for i, line in enumerate(lines[1:6]):
        print(f"{i+1}: {repr(line[:120])}...")

    # Parse with get_player_list and analyze
    from get_player_list import parse_txt_content

    players = parse_txt_content(content)
    print(f"\n--- Parsed {len(players)} players ---")

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

    # Suggested parquet dtypes
    print("\n--- Suggested Parquet schema ---")
    print("  id: int64 (required)")
    print("  name: string")
    print("  fed: string (3 chars)")
    print("  sex: string (M/F)")
    print("  tit, wtit, otit: string")
    print("  foa: int32 nullable")
    print("  srtng, rrtng, brtng: int32 nullable (1000-3000 typical)")
    print("  sgm, rgm, bgm: int32 nullable (game counts)")
    print("  sk, rk, bk: int32 nullable (K factors)")
    print("  byear: int32 nullable (year)")
    print("  flag: string")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

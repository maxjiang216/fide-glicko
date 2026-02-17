#!/usr/bin/env python3
"""
Inspect exact column positions in FIDE players_list TXT format.

Prints character-by-character analysis to refine the fixed-width parser.
"""

import sys
from pathlib import Path

# Add src/scraper for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scraper"))

import requests
import zipfile
from io import BytesIO

DOWNLOAD_URL = "https://ratings.fide.com/download/players_list.zip"


def main():
    print("Downloading...")
    resp = requests.get(DOWNLOAD_URL, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(resp.content), "r") as zf:
        txt_name = next(n for n in zf.namelist() if n.endswith(".txt"))
        with zf.open(txt_name) as f:
            lines = f.read().decode("utf-8", errors="replace").splitlines()

    header = lines[0]
    print("\nHeader with position markers (every 5 chars):")
    for i in range(0, min(165, len(header)), 5):
        seg = header[i : i + 5].replace(" ", "·")
        print(f"  {i:3d}: {repr(seg)}")

    print("\n\nFirst data line with position markers:")
    if len(lines) > 1:
        line = lines[1]
        for i in range(0, min(165, len(line)), 5):
            seg = line[i : i + 5].replace(" ", "·")
            print(f"  {i:3d}: {repr(seg)}")

    # Find a line with ratings
    print("\n\nLine with rating data:")
    for line in lines[2:200]:
        if "1500" in line or "2000" in line:
            idx = lines.index(line)
            print(f"Line {idx}: {repr(line[:140])}")
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Download and process the FIDE Combined Rating List (STD, BLZ, RPD) from
https://ratings.fide.com/download_lists.phtml

Downloads the TXT format zip, parses the fixed-width file, and saves to parquet
with a JSON sample.
"""

import argparse
import json
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# Combined list STD, BLZ, RPD - TXT format
DOWNLOAD_URL = "https://ratings.fide.com/download/players_list.zip"

# Fixed-width column positions (start, end) - 0-based, end exclusive
# Based on header: ID Number Name Fed Sex Tit WTit OTit FOA SRtng SGm SK RRtng RGm Rk BRtng BGm BK B-day Flag
COLUMN_SPEC = [
    ("id", 0, 15),  # ID NUMBER - FIDE identification number
    ("name", 15, 76),  # NAME - player name
    ("fed", 76, 79),  # FED - federation code
    ("sex", 80, 84),  # SEX - M/F
    ("tit", 84, 89),  # TIT/TITL - title (g, wg, m, wm, f, wf, c, wc)
    ("wtit", 89, 94),  # Woman title
    ("otit", 94, 109),  # OTIT - other titles (IA, FA, NA, IO, FT, etc.)
    ("foa", 109, 113),  # FOA - FIDE Online Arena rating
    ("srtng", 113, 119),  # STD/SRTNG - standard rating
    ("sgm", 119, 123),  # SGM - standard rated games
    ("sk", 123, 126),  # SK - standard K factor
    ("rrtng", 126, 132),  # RPD/RRTNG - rapid rating
    ("rgm", 132, 136),  # RGM - rapid rated games
    ("rk", 136, 139),  # RK - rapid K factor
    ("brtng", 139, 145),  # BLZ/BRTNG - blitz rating
    ("bgm", 145, 149),  # BGM - blitz rated games
    ("bk", 149, 152),  # BK - blitz K factor
    ("bday", 152, 156),  # B-day/BORN - year of birth
    ("flag", 156, 162),  # FLAG - I, WI, w (inactivity, woman)
]


def parse_line(line: str) -> dict[str, str] | None:
    """Parse a single fixed-width line into a dict. Returns None for header/invalid."""
    line = line.rstrip("\r\n")
    if len(line) < 100:
        return None
    # Skip header line
    if line.startswith("ID Number") or not line[0].isdigit():
        return None
    return {name: line[a:b].strip() for name, a, b in COLUMN_SPEC}


def _safe_int(value: str | None, allow_zero: bool = True) -> int | None:
    """Convert to int, return None if invalid or empty."""
    if not value or (value == "0" and not allow_zero):
        return None
    try:
        return int(value)
    except ValueError:
        return None


# Output fields only (ID, Name, B-day, SEX, FED, TIT)
OUTPUT_FIELDS = ("id", "name", "byear", "sex", "fed", "title")


def parse_txt_content(content: str) -> list[dict[str, Any]]:
    """Parse the full TXT content into a list of player dicts."""
    rows: list[dict[str, Any]] = []
    for line in content.splitlines():
        parsed = parse_line(line)
        if parsed is None:
            continue
        id_val = _safe_int(parsed.get("id"))
        if id_val is None:
            continue  # Skip rows with invalid id (e.g. org/alternate records)

        row: dict[str, Any] = {
            "id": id_val,
            "name": parsed.get("name") or None,
            "byear": _safe_int(parsed.get("bday"), allow_zero=False),
            "sex": parsed.get("sex") or None,
            "fed": parsed.get("fed") or None,
            "title": parsed.get("tit") or None,
        }
        rows.append(row)
    return rows


def download_player_list(
    max_retries: int = 3,
    retry_delay: float = 2.0,
    session: requests.Session | None = None,
) -> bytes:
    """
    Download the FIDE players_list.zip with retry logic.

    Returns:
        Raw bytes of the zip file.
    """
    sess = session or requests.Session()
    for attempt in range(max_retries):
        try:
            resp = sess.get(DOWNLOAD_URL, timeout=120)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            raise e
    raise RuntimeError("Download failed")  # unreachable


def process_zip(zip_bytes: bytes) -> list[dict[str, Any]]:
    """Extract and parse the TXT from the zip. Returns list of player dicts."""
    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        txt_name = next((n for n in names if n.endswith(".txt")), names[0])
        with zf.open(txt_name) as f:
            content = f.read().decode("utf-8", errors="replace")
    return parse_txt_content(content)


def get_player_list(
    max_retries: int = 3,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """
    Download and process the FIDE player list. Returns list of player dicts.
    """
    zip_bytes = download_player_list(max_retries=max_retries, session=session)
    return process_zip(zip_bytes)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download FIDE Combined Rating List (STD, BLZ, RPD) and save as parquet"
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        default="data",
        help="Directory to output results (default: 'data' from repo root)",
    )
    parser.add_argument(
        "--override",
        "-o",
        action="store_true",
        help="Override existing files",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Reduce output",
    )
    args = parser.parse_args()

    verbose = not args.quiet

    # Output paths: src/data (data folder in src)
    src_dir = Path(__file__).resolve().parent.parent
    output_dir = (
        (src_dir / "data") if args.directory == "data" else Path(args.directory)
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "players_list.parquet"
    json_sample_path = output_dir / "players_list_sample.json"

    if parquet_path.exists() and not args.override:
        if verbose:
            print(f"File {parquet_path} already exists. Use --override to replace.")
        return 0

    if verbose:
        print("Downloading FIDE players list...")
    start = time.time()

    try:
        players = get_player_list()
    except Exception as e:
        print(f"Error: {e}")
        return 1

    elapsed = time.time() - start
    if verbose:
        print(f"Downloaded and parsed {len(players)} players in {elapsed:.1f}s")

    df = pd.DataFrame(players)

    # Write parquet with appropriate dtypes
    df.to_parquet(parquet_path, index=False)

    # JSON sample: first 100 rows
    sample = players[:100]
    with open(json_sample_path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)

    if verbose:
        print(f"Saved parquet: {parquet_path}")
        print(f"Saved JSON sample: {json_sample_path}")
        print(f"Sample row keys: {list(sample[0].keys())}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Download and process the FIDE Combined Rating List (STD, BLZ, RPD) from
https://ratings.fide.com/download_lists.phtml

Downloads the XML format zip, parses it, and saves to parquet with a JSON sample.
XML avoids fixed-width ambiguity (e.g. OTIT overflow in TXT).
"""

import argparse
import datetime
import json
import time
import xml.etree.ElementTree as ET
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# Combined list STD, BLZ, RPD - XML format
DOWNLOAD_URL = "https://ratings.fide.com/download/players_list_xml.zip"

# Title normalization: single-letter -> full code
TITLE_MAP = {
    "g": "GM",
    "wg": "WGM",
    "m": "IM",
    "wm": "WIM",
    "f": "FM",
    "wf": "WFM",
    "c": "CM",
    "wc": "WCM",
}


def _elem_text(elem: ET.Element | None, default: str = "") -> str:
    return (elem.text or "").strip() if elem is not None else default


def _safe_int(value: str | None, allow_zero: bool = True) -> int | None:
    """Convert to int, return None if invalid or empty."""
    if not value or (value == "0" and not allow_zero):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _sanitize_byear(value: int | None) -> int | None:
    """Return byear if in valid range (1900..current_year), else None."""
    if value is None:
        return None
    current_year = datetime.datetime.now().year
    if 1900 <= value < current_year:
        return value
    return None


def parse_xml_content(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse XML into list of player dicts: id, name, byear, sex, fed, title."""
    root = ET.fromstring(xml_bytes)
    current_year = datetime.datetime.now().year
    rows: list[dict[str, Any]] = []

    for player in root.findall("player"):
        fideid = _safe_int(_elem_text(player.find("fideid")))
        if fideid is None:
            continue

        byear_raw = _safe_int(_elem_text(player.find("birthday")), allow_zero=False)
        byear = _sanitize_byear(byear_raw)

        title = _elem_text(player.find("title"))
        if title:
            tit_lo = title.lower()
            title = TITLE_MAP.get(tit_lo, title)

        fed = _elem_text(player.find("country"))
        if fed:
            fed = fed.upper()

        rows.append(
            {
                "id": fideid,
                "name": _elem_text(player.find("name")) or None,
                "byear": byear,
                "sex": _elem_text(player.find("sex")) or None,
                "fed": fed,
                "title": title or None,
            }
        )

    return rows


def download_player_list(
    max_retries: int = 3,
    retry_delay: float = 2.0,
    session: requests.Session | None = None,
) -> bytes:
    """
    Download the FIDE players_list_xml.zip with retry logic.

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
    """Extract and parse the XML from the zip. Returns list of player dicts."""
    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        xml_name = next((n for n in names if n.endswith(".xml")), names[0])
        with zf.open(xml_name) as f:
            xml_content = f.read()
    return parse_xml_content(xml_content)


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

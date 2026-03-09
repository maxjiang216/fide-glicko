#!/usr/bin/env python3
"""
Download and process the FIDE Combined Rating List (STD, BLZ, RPD) from
https://ratings.fide.com/download_lists.phtml

Downloads the XML format zip, parses it, and saves to parquet with a JSON sample.
XML avoids fixed-width ambiguity (e.g. OTIT overflow in TXT).
"""

import argparse
import csv
import logging
import signal
import sys
import tempfile
from collections import Counter
import datetime
import json
import random
import time
import xml.etree.ElementTree as ET
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from s3_io import (
    build_s3_uri,
    download_to_file,
    is_s3_path,
    output_exists,
    write_output,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_shutdown_state = {}

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

# Open titles (GM, IM, FM, CM) - stored in 'title' column
OPEN_TITLES = frozenset({"GM", "IM", "FM", "CM"})
# Women's titles (WGM, WIM, WFM, WCM) - stored in 'w_title' column
WOMEN_TITLES = frozenset({"WGM", "WIM", "WFM", "WCM"})
# All playing titles (for validation)
EXPECTED_TITLES = OPEN_TITLES | WOMEN_TITLES


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


def parse_xml_content(xml_bytes: bytes) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Parse XML into list of player dicts: byear, id, fed, name, sex, title, w_title.

    title = open titles only (GM, IM, FM, CM). w_title = women's titles (WGM, WIM, WFM, WCM).
    If XML has a women's title in 'title' when w_title is empty, it goes in w_title.

    Returns:
        (players, parse_stats) where parse_stats contains:
        - xml_fields_found: sorted list of all element names found in player elements
        - skipped_no_id: count of players skipped for missing/invalid fideid
        - byear_out_of_range: count of players with byear outside 1900..current_year
    """
    root = ET.fromstring(xml_bytes)
    current_year = datetime.datetime.now().year
    rows: list[dict[str, Any]] = []
    xml_fields: set[str] = set()
    skipped_no_id = 0
    byear_out_of_range = 0
    byear_out_of_range_data: list[tuple[int, int | None]] = []
    title_counter: Counter[str] = Counter()
    w_title_counter: Counter[str] = Counter()
    o_title_counter: Counter[str] = Counter()
    title_w_title_pair_counter: Counter[tuple[str, str]] = Counter()
    title_w_title_to_ids: dict[tuple[str, str], list[int]] = {}
    players_with_multiple_titles = 0

    for player in root.findall("player"):
        for child in player:
            xml_fields.add(child.tag)

        fideid = _safe_int(_elem_text(player.find("fideid")))
        if fideid is None:
            skipped_no_id += 1
            continue

        byear_raw = _safe_int(_elem_text(player.find("birthday")), allow_zero=False)
        byear = _sanitize_byear(byear_raw)
        if byear_raw is not None and byear is None:
            byear_out_of_range += 1
            byear_out_of_range_data.append((fideid, byear_raw))

        title_raw = _elem_text(player.find("title"))
        title_normalized = ""
        if title_raw:
            tit_lo = title_raw.lower()
            title_normalized = TITLE_MAP.get(tit_lo, title_raw)
        w_title_raw = _elem_text(player.find("w_title"))
        w_title_normalized = ""
        if w_title_raw:
            wt_lo = w_title_raw.lower()
            w_title_normalized = TITLE_MAP.get(wt_lo, w_title_raw)
        o_title_raw = _elem_text(player.find("o_title"))

        if title_normalized:
            title_counter[title_normalized] += 1
        if w_title_raw:
            w_title_counter[w_title_raw] += 1
        if o_title_raw:
            o_title_counter[o_title_raw] += 1
        pair = (title_normalized or "", w_title_raw or "")
        title_w_title_pair_counter[pair] += 1
        title_w_title_to_ids.setdefault(pair, []).append(fideid)
        non_empty_count = sum(
            1 for v in (title_normalized, w_title_raw, o_title_raw) if v
        )
        if non_empty_count >= 2:
            players_with_multiple_titles += 1

        # Output: title = open titles only (GM, IM, FM, CM); w_title = women's titles
        # If XML has women's title in "title" field, keep title blank (use w_title)
        output_title = title_normalized if title_normalized in OPEN_TITLES else None
        output_w_title = None
        if w_title_normalized in WOMEN_TITLES:
            output_w_title = w_title_normalized
        elif title_normalized in WOMEN_TITLES:
            # Women's title in "title" when w_title empty - put in w_title
            output_w_title = title_normalized

        fed = _elem_text(player.find("country"))
        if fed:
            fed = fed.upper()

        rows.append(
            {
                "byear": byear,
                "id": fideid,
                "fed": fed,
                "name": _elem_text(player.find("name")) or None,
                "sex": _elem_text(player.find("sex")) or None,
                "title": output_title,
                "w_title": output_w_title,
            }
        )

    parse_stats: dict[str, Any] = {
        "xml_fields_found": sorted(xml_fields),
        "skipped_no_id": skipped_no_id,
        "byear_out_of_range": byear_out_of_range,
        "byear_out_of_range_data": byear_out_of_range_data,
        "title_distribution": dict(sorted(title_counter.items())),
        "title_unique_values": sorted(title_counter.keys()),
        "w_title_distribution": dict(sorted(w_title_counter.items())),
        "w_title_unique_values": sorted(w_title_counter.keys()),
        "o_title_distribution": dict(sorted(o_title_counter.items())),
        "o_title_unique_values": sorted(o_title_counter.keys()),
        "players_with_multiple_titles": players_with_multiple_titles,
        "title_consolidation": (
            "Output: title = open titles only (GM, IM, FM, CM); w_title = women's titles "
            "(WGM, WIM, WFM, WCM). If XML has a women's title in 'title' when w_title is "
            "empty, we put it in w_title and leave title blank. o_title (arbiter, etc.) "
            "is parsed for reporting only, not stored."
        ),
        "title_w_title_unique_pairs": [
            {"title": t, "w_title": w}
            for t, w in sorted(title_w_title_pair_counter.keys())
        ],
        "title_w_title_distribution": [
            {
                "title": t,
                "w_title": w,
                "count": c,
                "sample_fide_id": (
                    random.choice(title_w_title_to_ids[(t, w)])
                    if title_w_title_to_ids.get((t, w))
                    else None
                ),
            }
            for (t, w), c in title_w_title_pair_counter.most_common()
        ],
    }
    return rows, parse_stats


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
    players, _, _ = _process_zip_internal(zip_bytes)
    return players


def _process_zip_internal(
    zip_bytes: bytes,
) -> tuple[list[dict[str, Any]], dict[str, Any], bytes]:
    """Extract and parse the XML. Returns (players, parse_stats, xml_content)."""
    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        names = zf.namelist()
        xml_name = next((n for n in names if n.endswith(".xml")), names[0])
        with zf.open(xml_name) as f:
            xml_content = f.read()
    if not xml_content or len(xml_content) < 100:
        raise ValueError(
            "XML content is empty or too small; file may be corrupted or incomplete"
        )
    try:
        players, parse_stats = parse_xml_content(xml_content)
    except ET.ParseError as e:
        raise ValueError(f"XML parse failed (malformed XML): {e}") from e
    return players, parse_stats, xml_content


def get_player_list(
    max_retries: int = 3,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """
    Download and process the FIDE player list. Returns list of player dicts.
    """
    zip_bytes = download_player_list(max_retries=max_retries, session=session)
    return process_zip(zip_bytes)


def load_federations(path: Path) -> frozenset[str]:
    """Load federation codes from CSV (code,name). Returns set of uppercase codes."""
    if not path.exists():
        return frozenset()
    codes: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("code", "").strip().upper()
            if code:
                codes.add(code)
    # CGO is a known exception (not always on FIDE's selector)
    codes.add("CGO")
    return frozenset(codes)


def build_report(
    players: list[dict[str, Any]],
    parse_stats: dict[str, Any],
    federations_path: Path | None = None,
) -> dict[str, Any]:
    """
    Build a report dict with counts, nulls, odd values, and field discovery.

    Returns a dict suitable for JSON serialization.
    """
    current_year = datetime.datetime.now().year
    valid_feds = load_federations(federations_path) if federations_path else frozenset()

    SAMPLE_SIZE = 10

    nulls: dict[str, int] = {}
    odd: dict[str, int] = {
        "id": parse_stats.get("skipped_no_id", 0),
        "name": 0,
        "byear": parse_stats.get("byear_out_of_range", 0),
        "sex": 0,
        "fed": 0,
        "title": 0,
        "w_title": 0,
    }

    odd_name_data: list[tuple[int, Any]] = []  # (fide_id, name)
    odd_sex_data: list[tuple[int, str]] = []  # (fide_id, sex)
    odd_fed_by_code: dict[str, tuple[int, str]] = (
        {}
    )  # fed -> (fide_id, fed), one per fed, exclude FIDE
    odd_title_data: list[tuple[int, str]] = []  # (fide_id, title)
    odd_w_title_data: list[tuple[int, str]] = []  # (fide_id, w_title)
    byear_null_ids: list[int] = []
    non_standard_fed_codes: set[str] = set()
    byear_values: list[int] = []
    byear_min_ids: list[int] = []
    byear_max_ids: list[int] = []
    sex_m = 0
    sex_f = 0
    sex_null = 0

    for p in players:
        fide_id = p.get("id")
        if fide_id is None:
            continue

        for col in ("id", "name", "byear", "sex", "fed", "title", "w_title"):
            val = p.get(col)
            if val is None or val == "":
                nulls[col] = nulls.get(col, 0) + 1

        name = p.get("name")
        if name is None or not any(c.isalpha() for c in (name or "")):
            odd["name"] += 1
            odd_name_data.append((fide_id, name))

        sex = p.get("sex")
        if sex is None or sex == "":
            sex_null += 1
        elif sex.upper() == "M":
            sex_m += 1
        elif sex.upper() == "F":
            sex_f += 1
        else:
            odd["sex"] += 1
            odd_sex_data.append((fide_id, sex))

        fed = p.get("fed")
        if (
            fed
            and fed.upper() != "FIDE"
            and valid_feds
            and fed.upper() not in valid_feds
        ):
            odd["fed"] += 1
            if fed.upper() not in odd_fed_by_code:
                odd_fed_by_code[fed.upper()] = (fide_id, fed)
            non_standard_fed_codes.add(fed.upper())

        title = p.get("title")
        if title and title not in OPEN_TITLES:
            odd["title"] += 1
            odd_title_data.append((fide_id, title))

        w_title = p.get("w_title")
        if w_title and w_title not in WOMEN_TITLES:
            odd["w_title"] += 1
            odd_w_title_data.append((fide_id, w_title))

        byear = p.get("byear")
        if byear is not None:
            byear_values.append(byear)
        else:
            byear_null_ids.append(fide_id)

    byear_min = min(byear_values) if byear_values else None
    byear_max = max(byear_values) if byear_values else None

    if byear_min is not None and byear_max is not None:
        for p in players:
            fid = p.get("id")
            byear = p.get("byear")
            if fid is not None and byear is not None:
                if byear == byear_min:
                    byear_min_ids.append(fid)
                if byear == byear_max:
                    byear_max_ids.append(fid)

    # Ensure all columns appear in nulls (even if 0)
    for col in ("id", "name", "byear", "sex", "fed", "title", "w_title"):
        nulls.setdefault(col, 0)

    def _sample_pairs(data: list[tuple[int, Any]], n: int) -> list[dict[str, Any]]:
        """Sample up to n (fide_id, value) pairs, return as [{"fide_id": x, "value": y}]."""
        if not data:
            return []
        if len(data) <= n:
            sampled = data
        else:
            sampled = random.sample(data, n)
        return [
            {"fide_id": fid, "value": val}
            for fid, val in sorted(sampled, key=lambda x: x[0])
        ]

    def _sample_ids(ids: list[int], n: int) -> list[int]:
        if len(ids) <= n:
            return sorted(ids)
        return sorted(random.sample(ids, n))

    byear_out_of_range_data = parse_stats.get("byear_out_of_range_data", [])

    # Build odd_sample: only include columns with odd values, format as fide_id + value
    odd_sample: dict[str, list[dict[str, Any]]] = {}
    if odd["name"] > 0:
        odd_sample["name"] = _sample_pairs(odd_name_data, SAMPLE_SIZE)
    if odd["byear"] > 0:
        odd_sample["byear"] = [
            {"fide_id": fid, "value": val}
            for fid, val in (
                byear_out_of_range_data
                if len(byear_out_of_range_data) <= SAMPLE_SIZE
                else random.sample(byear_out_of_range_data, SAMPLE_SIZE)
            )
        ]
        odd_sample["byear"].sort(key=lambda x: x["fide_id"])
    if odd["sex"] > 0:
        odd_sample["sex"] = _sample_pairs(odd_sex_data, SAMPLE_SIZE)
    if odd["fed"] > 0:
        fed_list = list(odd_fed_by_code.values())
        odd_sample["fed"] = [
            {"fide_id": fid, "value": fed}
            for fid, fed in (
                fed_list
                if len(fed_list) <= SAMPLE_SIZE
                else random.sample(fed_list, SAMPLE_SIZE)
            )
        ]
        odd_sample["fed"].sort(key=lambda x: x["fide_id"])
    if odd["title"] > 0:
        odd_sample["title"] = _sample_pairs(odd_title_data, SAMPLE_SIZE)
    if odd["w_title"] > 0:
        odd_sample["w_title"] = _sample_pairs(odd_w_title_data, SAMPLE_SIZE)

    # non_standard_federations: exclude FIDE
    non_standard_feds_sorted = (
        sorted(c for c in non_standard_fed_codes if c != "FIDE")
        if non_standard_fed_codes
        else None
    )

    report: dict[str, Any] = {
        "players_found": len(players),
        "xml_fields_found": parse_stats.get("xml_fields_found", []),
        "odd_by_column": odd,
        "odd_sample": odd_sample,
        "byear_null_sample_fide_ids": _sample_ids(byear_null_ids, SAMPLE_SIZE),
        "non_standard_federations": non_standard_feds_sorted,
        "non_standard_federations_sample": (
            [
                {"fide_id": fid, "value": fed}
                for fid, fed in (
                    list(odd_fed_by_code.values())
                    if len(odd_fed_by_code) <= SAMPLE_SIZE
                    else random.sample(list(odd_fed_by_code.values()), SAMPLE_SIZE)
                )
            ]
            if odd_fed_by_code
            else None
        ),
        "byear_min": byear_min,
        "byear_max": byear_max,
        "byear_min_fide_ids": _sample_ids(byear_min_ids, SAMPLE_SIZE),
        "byear_max_fide_ids": _sample_ids(byear_max_ids, SAMPLE_SIZE),
        "sex_counts": {"M": sex_m, "F": sex_f, "null": sex_null},
        "nulls_by_column": nulls,
        "non_standard_federations_count": odd["fed"] if valid_feds else None,
        "federations_file_used": str(federations_path) if federations_path else None,
        "federations_loaded_count": len(valid_feds) if valid_feds else None,
        "title_report": {
            "title": {
                "unique_values": parse_stats.get("title_unique_values", []),
                "distribution": parse_stats.get("title_distribution", {}),
            },
            "w_title": {
                "unique_values": parse_stats.get("w_title_unique_values", []),
                "distribution": parse_stats.get("w_title_distribution", {}),
            },
            "o_title": {
                "unique_values": parse_stats.get("o_title_unique_values", []),
                "distribution": parse_stats.get("o_title_distribution", {}),
            },
            "players_with_multiple_non_empty_titles": parse_stats.get(
                "players_with_multiple_titles", 0
            ),
            "consolidation": parse_stats.get("title_consolidation", ""),
            "title_w_title_unique_pairs": parse_stats.get(
                "title_w_title_unique_pairs", []
            ),
            "title_w_title_distribution": parse_stats.get(
                "title_w_title_distribution", []
            ),
        },
    }
    return report


def _save_results(
    players: list[dict[str, Any]],
    parse_stats: dict[str, Any],
    xml_content: bytes,
    parquet_path: str | Path,
    json_sample_path: str | Path,
    xml_path: str | Path,
    report_path: str | Path,
    federations_path: Path | None,
) -> None:
    """Write all output files (local or S3)."""
    df = pd.DataFrame(players)
    if df.empty:
        logger.warning("No players to save")
        return
    df = df[["byear", "id", "fed", "name", "sex", "title", "w_title"]]
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    write_output(buf.getvalue(), str(parquet_path))
    write_output(xml_content, str(xml_path))
    sample = players[:100]
    write_output(json.dumps(sample, indent=2, default=str), str(json_sample_path))
    report = build_report(players, parse_stats, federations_path)
    write_output(json.dumps(report, indent=2, default=str), str(report_path))
    logger.info("Saved parquet: %s", parquet_path)
    logger.info("Saved XML: %s", xml_path)
    logger.info("Saved JSON sample: %s", json_sample_path)
    logger.info("Saved report: %s", report_path)


def run(
    output_prefix: str,
    bucket: str = "fide-glicko",
    override: bool = False,
    quiet: bool = False,
    federations_s3_uri: str | None = None,
) -> int:
    """
    Download FIDE player list and write to S3.

    Args:
        output_prefix: S3 prefix under bucket (e.g. "data" or "runs/dev-123").
        bucket: S3 bucket name.
        override: If True, overwrite existing files.
        quiet: If True, reduce log output.
        federations_s3_uri: Optional S3 URI for federations.csv (for report's fed check).

    Returns:
        0 on success, 1 on failure.
    """
    global _shutdown_state

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.INFO)

    parquet_uri = build_s3_uri(bucket, output_prefix, "players_list.parquet")
    if output_exists(parquet_uri) and not override:
        logger.info(
            "Output %s already exists. Use override=True to replace.", parquet_uri
        )
        return 0

    federations_path: Path | None = None
    if federations_s3_uri:
        try:
            federations_path = download_to_file(
                federations_s3_uri, Path(tempfile.gettempdir()) / "federations.csv"
            )
            logger.info("Loaded federations from %s", federations_s3_uri)
        except Exception as e:
            logger.warning("Could not load federations from S3: %s", e)

    json_sample_uri = build_s3_uri(bucket, output_prefix, "players_list_sample.json")
    xml_uri = build_s3_uri(bucket, output_prefix, "players_list.xml")
    report_uri = build_s3_uri(bucket, output_prefix, "players_list_report.json")

    def _save(players, parse_stats, xml_content):
        _save_results(
            players,
            parse_stats,
            xml_content,
            parquet_uri,
            json_sample_uri,
            xml_uri,
            report_uri,
            federations_path,
        )

    def _graceful_shutdown(signum, frame):
        logger.warning("\nReceived interrupt, attempting graceful shutdown...")
        state = _shutdown_state.get("state")
        if state:
            players, parse_stats, xml_content = state
            if players:
                _save(players, parse_stats, xml_content)
        sys.exit(130 if signum == 2 else 0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    logger.info("Downloading FIDE players list from %s...", DOWNLOAD_URL)
    start = time.time()

    try:
        zip_bytes = download_player_list()
        players, parse_stats, xml_content = _process_zip_internal(zip_bytes)
    except Exception as e:
        logger.error("Error: %s", e)
        return 1

    if not players:
        logger.error("No players parsed from XML")
        return 1

    _shutdown_state["state"] = (players, parse_stats, xml_content)
    elapsed = time.time() - start
    logger.info("Downloaded and parsed %d players in %.1fs", len(players), elapsed)

    _save(players, parse_stats, xml_content)
    if players:
        logger.info("Sample row keys: %s", list(players[0].keys()))

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download FIDE Combined Rating List (STD, BLZ, RPD) and save as parquet"
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default=None,
        help="S3 output prefix (e.g. data). When set, writes to S3 instead of local.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="fide-glicko",
        help="S3 bucket (only used with --output-prefix)",
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        default="data",
        help="Directory to output results (default: src/data when -d data)",
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
    parser.add_argument(
        "--federations",
        "-f",
        type=str,
        default="",
        help="Path to federations CSV for non-standard fed check (default: data/federations.csv)",
    )
    parser.add_argument(
        "--report",
        "-r",
        type=str,
        default="",
        help="Path for report JSON (default: players_list_report.json in output dir)",
    )
    args = parser.parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if args.output_prefix is not None:
        fed_uri = (
            args.federations
            if args.federations and is_s3_path(args.federations)
            else build_s3_uri(args.bucket, "data", "federations.csv")
        )
        return run(
            output_prefix=args.output_prefix,
            bucket=args.bucket,
            override=args.override,
            quiet=args.quiet,
            federations_s3_uri=fed_uri,
        )

    # Local output (original behavior)
    src_dir = Path(__file__).resolve().parent.parent
    repo_root = src_dir.parent
    output_dir = (
        (src_dir / "data") if args.directory == "data" else Path(args.directory)
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "players_list.parquet"
    json_sample_path = output_dir / "players_list_sample.json"
    xml_path = output_dir / "players_list.xml"
    report_path = (
        Path(args.report) if args.report else output_dir / "players_list_report.json"
    )
    federations_path = (
        Path(args.federations)
        if args.federations
        else repo_root / "data" / "federations.csv"
    )

    if parquet_path.exists() and not args.override:
        logger.info("File %s already exists. Use --override to replace.", parquet_path)
        return 0

    def _graceful_shutdown(signum, frame):
        logger.warning("\nReceived interrupt, attempting graceful shutdown...")
        state = _shutdown_state.get("state")
        if state:
            players, parse_stats, xml_content = state
            if players:
                _save_results(
                    players,
                    parse_stats,
                    xml_content,
                    parquet_path,
                    json_sample_path,
                    xml_path,
                    report_path,
                    federations_path,
                )
        sys.exit(130 if signum == 2 else 0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    logger.info("Downloading FIDE players list...")
    start = time.time()

    try:
        zip_bytes = download_player_list()
        players, parse_stats, xml_content = _process_zip_internal(zip_bytes)
    except Exception as e:
        logger.error("Error: %s", e)
        return 1

    if not players:
        logger.error("No players parsed from XML")
        return 1

    _shutdown_state["state"] = (players, parse_stats, xml_content)
    elapsed = time.time() - start
    logger.info("Downloaded and parsed %d players in %.1fs", len(players), elapsed)

    _save_results(
        players,
        parse_stats,
        xml_content,
        parquet_path,
        json_sample_path,
        xml_path,
        report_path,
        federations_path,
    )
    if players:
        logger.info("Sample row keys: %s", list(players[0].keys()))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
FIDE Tournament Reports Scraper

Scrapes tournament reports (original reports) from FIDE website for a list of tournament codes.
Extracts player data and round-by-round results.
Supports rate limiting, retries, checkpoints, and progress tracking.
"""

import argparse
import copy
import gzip
import io
import json
import logging
import os
import random
import re
import signal
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Enforces minimum spacing between requests."""

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.last_request = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        now = time.perf_counter()
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.perf_counter()


def format_duration(seconds: float) -> str:
    """Format duration in a human-readable way."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


# Optional S3 support (used by run() when paths are S3 URIs)
def _is_s3(path: str) -> bool:
    try:
        from s3_io import is_s3_path

        return is_s3_path(path)
    except ImportError:
        return path.strip().lower().startswith("s3://")


def _compress_gzip(data: bytes, level: int = 9) -> bytes:
    """Compress with gzip level 9."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as z:
        z.write(data)
    return buf.getvalue()


def _raw_base_from_output_path(output_path: str) -> Optional[str]:
    """Derive raw/reports base from output_path. Returns None if not derivable."""
    if "/data/tournament_reports_chunks/" in output_path:
        return output_path.replace(
            "/data/tournament_reports_chunks/", "/raw/reports/"
        ).rstrip("/")
    return None


def _write_to_path(path: str, content: bytes | str) -> None:
    """Write content to path (local or S3)."""
    if _is_s3(path):
        from s3_io import write_output

        write_output(content, path)
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            p.write_text(content, encoding="utf-8")
        else:
            p.write_bytes(content)


def _read_codes_from_path(path: str) -> List[str]:
    """Read tournament codes from a file (local or S3)."""
    if _is_s3(path):
        from s3_io import download_to_file

        local_path = Path(tempfile.gettempdir()) / "tournament_codes.txt"
        download_to_file(path, local_path)
        path = str(local_path)
    return read_tournament_codes(path)


def read_tournament_codes(file_path: str) -> List[str]:
    """Read tournament codes from a file."""
    codes = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            code = line.strip()
            if code:
                codes.append(code)
    return codes


def parse_score(score_text: str) -> Optional[float]:
    """
    Parse score from text.
    Returns float (0.0, 0.5, 1.0) or None if forfeit.
    Forfeit indicators: 'forfeit', '-', '+', or text containing these.
    """
    if not score_text:
        return None

    score_text = score_text.strip().lower()

    # Check for forfeit indicators
    if "forfeit" in score_text or score_text in ["-", "+"]:
        return None  # Will be stored as forfeit indicator separately

    # Try to extract numeric score
    # Look for patterns like "1.0", "0.5", "0", "1"
    match = re.search(r"(\d+\.?\d*)", score_text)
    if match:
        try:
            score = float(match.group(1))
            if score in [0.0, 0.5, 1.0]:
                return score
        except ValueError:
            pass

    return None


def extract_forfeit_indicator(score_text: str) -> str:
    """Extract forfeit indicator from score text."""
    if not score_text:
        return ""

    score_text = score_text.strip()

    if "forfeit" in score_text.lower():
        if "-" in score_text or score_text.endswith("-"):
            return "-"
        elif "+" in score_text or score_text.endswith("+"):
            return "+"
        # Default to "-" if forfeit but no explicit indicator
        return "-"
    elif score_text == "-":
        return "-"
    elif score_text == "+":
        return "+"

    return ""


def _to_year(y: str) -> int:
    """Convert 2- or 4-digit year string to full year."""
    n = int(y)
    if len(y) == 4:
        return n
    return 2000 + n if n < 50 else 1900 + n


def parse_details_date_to_iso(date_val) -> Optional[str]:
    """
    Parse start_date/end_date from tournament details to ISO (YYYY-MM-DD).
    Accepts str, datetime, or pandas Timestamp (from Parquet).
    FIDE uses formats like "2024.12.30", "30.12.2024", "2024-12-30".
    """
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return None
    if hasattr(date_val, "strftime"):
        return date_val.strftime("%Y-%m-%d")
    if not isinstance(date_val, str):
        date_val = str(date_val)
    s = date_val.strip()
    if not s or s.lower() == "nat":
        return None
    # YYYY.MM.DD or YYYY-MM-DD
    m = re.match(r"(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})", s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        except ValueError:
            pass
    # DD.MM.YYYY
    m = re.match(r"(\d{1,2})[.\-](\d{1,2})[.\-](\d{4})", s)
    if m:
        try:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        except ValueError:
            pass
    return None


def _parse_round_date_with_format(date_str: str, fmt: str) -> Optional[str]:
    """Parse round date string (e.g. 24/12/30) using specified format to ISO."""
    if not date_str or not re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", date_str.strip()):
        return None
    parts = date_str.strip().split("/")
    if len(parts) != 3:
        return None
    a, b, c = parts
    try:
        if fmt == "yy/mm/dd":
            m, d = int(b), int(c)
            if 1 <= m <= 12 and 1 <= d <= 31:
                year = _to_year(a)
                return f"{year:04d}-{m:02d}-{d:02d}"
        elif fmt == "dd/mm/yy":
            d, m = int(a), int(b)
            if 1 <= m <= 12 and 1 <= d <= 31:
                year = _to_year(c)
                return f"{year:04d}-{m:02d}-{d:02d}"
    except ValueError:
        pass
    return None


def _is_valid_parsed_year(year: int) -> bool:
    """Rule 1: Years should be in 2002..current_year (FIDE round dates)."""
    current_year = datetime.now().year
    return 2002 <= year <= current_year + 1


def infer_date_format(
    date_strings: List[str],
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    report_start_iso: Optional[str] = None,
) -> str:
    """
    Infer date format (yy/mm/dd vs dd/mm/yy) from round dates.

    Rule 1: Reject parses where year is outside 2002..current_year, or month/day invalid.
    Rule 2: Prefer format that gives tightest date range; use start/end from details
    (and report_start from report page "Start: YYYY-MM-DD") to constrain/penalize.
    """
    candidates = ["yy/mm/dd", "dd/mm/yy"]
    date_strs = [
        s for s in date_strings if s and re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", s.strip())
    ]
    if not date_strs:
        return "yy/mm/dd"  # default

    # Build set of anchor dates from details and report header
    anchor_dates: List[datetime] = []
    for iso in (start_iso, end_iso, report_start_iso):
        if iso:
            try:
                anchor_dates.append(datetime.strptime(iso[:10], "%Y-%m-%d"))
            except ValueError:
                pass
    start_dt = min(anchor_dates) if anchor_dates else None
    end_dt = max(anchor_dates) if anchor_dates else None

    best = "yy/mm/dd"
    best_score = float("inf")

    for fmt in candidates:
        parsed = []
        for s in date_strs:
            iso = _parse_round_date_with_format(s, fmt)
            if not iso:
                continue
            try:
                dt = datetime.strptime(iso, "%Y-%m-%d")
                # Rule 1: discard if year out of valid range
                if not _is_valid_parsed_year(dt.year):
                    continue
                parsed.append(dt)
            except ValueError:
                pass
        if not parsed:
            continue
        min_d, max_d = min(parsed), max(parsed)
        range_days = (max_d - min_d).days

        # Rule 2: penalize dates outside [start, end] from details/report
        out_of_range = 0
        if start_dt and min_d < start_dt:
            out_of_range += (start_dt - min_d).days * 1000
        if end_dt and max_d > end_dt:
            out_of_range += (max_d - end_dt).days * 1000

        score = range_days + out_of_range
        if score < best_score:
            best_score = score
            best = fmt

    return best


def parse_date_to_iso(date_str: str, date_format: Optional[str] = None) -> str:
    """
    Convert FIDE round date string to ISO format (YYYY-MM-DD).
    If date_format is provided ('yy/mm/dd' or 'dd/mm/yy'), use it.
    Otherwise falls back to infer_date_format (caller should prefer passing format).
    """
    if not date_str:
        return ""
    if date_format:
        result = _parse_round_date_with_format(date_str, date_format)
        return result or ""
    # Fallback: try yy/mm/dd first (common for FIDE)
    result = _parse_round_date_with_format(date_str, "yy/mm/dd")
    if result:
        return result
    return _parse_round_date_with_format(date_str, "dd/mm/yy") or ""


def parse_iso_to_datetime(iso_str: str):
    """Convert ISO date string (YYYY-MM-DD) to datetime. Returns None if invalid."""
    if not iso_str or not str(iso_str).strip():
        return None
    try:
        return datetime.strptime(str(iso_str).strip()[:10], "%Y-%m-%d")
    except ValueError:
        return None


def parse_round_date(round_text: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Parse round number and date from text like "1   25/11/22".
    Returns (round_number, date_string).
    """
    if not round_text:
        return None, None

    # Match pattern: number followed by spaces and date
    match = re.match(r"(\d+)\s+(\d{2}/\d{2}/\d{2,4})", round_text.strip())
    if match:
        round_num = int(match.group(1))
        date_str = match.group(2)
        return round_num, date_str

    # Try to extract just round number
    match = re.match(r"(\d+)", round_text.strip())
    if match:
        return int(match.group(1)), None

    return None, None


def extract_href_anchor_from_cell(cell) -> str:
    """
    Extract the href fragment from the first link in a cell.
    E.g. <a href="#65">Name</a> -> "65".
    Used to look up opponent's FIDE ID via anchor map.
    """
    link = cell.find("a", href=True)
    if not link:
        return ""
    href = link.get("href", "")
    if href.startswith("#"):
        return href[1:].strip()
    return ""


def extract_color_from_cell(cell) -> str:
    """Extract color (white/black) from a table cell."""
    white_note = cell.find("span", class_="white_note")
    black_note = cell.find("span", class_="black_note")

    if white_note:
        return "white"
    elif black_note:
        return "black"
    return ""


def extract_text_from_cell(cell) -> str:
    """Extract text from a table cell, handling links properly."""
    links = cell.find_all("a")
    if not links:
        return cell.get_text(strip=True)

    parts = []
    for link in links:
        text = link.get_text(strip=True)
        if text:
            parts.append(text)

    # Get remaining text after removing links
    cell_copy = cell.__copy__()
    for link in cell_copy.find_all("a"):
        link.decompose()
    remaining = cell_copy.get_text(strip=True)
    if remaining:
        parts.append(remaining)

    if not parts:
        return cell.get_text(strip=True)
    return " ".join(parts)


def fetch_tournament_report(
    tournament_code: str,
    session: requests.Session,
    *,
    _attempt_log: Optional[List[Dict]] = None,
    return_raw: bool = False,
) -> Tuple[Optional[Dict], Optional[str], int, Optional[bytes]]:
    """
    Fetch tournament report from FIDE website.

    Returns:
        Tuple of (report_dict, error_string, num_attempts, raw_content).
        If successful, report_dict is not None.
        raw_content is response bytes when return_raw=True, else None.
    """
    url = f"https://ratings.fide.com/tournament_src_report.phtml?code={tournament_code}"

    max_retries = 3
    last_error = None
    attempt_times: List[float] = []

    for attempt in range(max_retries):
        if attempt > 0:
            delay = 0.1 * (
                2 ** (attempt - 1)
            )  # Exponential backoff: 100ms, 200ms, 400ms
            time.sleep(delay)

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "close",
                "Cache-Control": "max-age=0",
            }

            t0 = time.perf_counter()
            try:
                response = session.get(url, headers=headers, timeout=45)
            finally:
                elapsed = time.perf_counter() - t0
                attempt_times.append(elapsed)

            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_code": tournament_code,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue

            soup = BeautifulSoup(response.content, "html.parser")

            # Extract "Start: YYYY-MM-DD" from report header for date format inference
            report_start_iso = None
            calc_body = soup.find("div", id="calc_list") or soup
            start_match = re.search(
                r"Start:\s*<b>(\d{4}-\d{2}-\d{2})</b>",
                str(calc_body),
                re.IGNORECASE,
            )
            if start_match:
                report_start_iso = start_match.group(1)

            # Find the main results table
            raw_content = response.content if return_raw else None
            table = soup.find("table", class_="calc_table")
            if not table:
                return None, "no data found", len(attempt_times), raw_content

            rows = table.find_all("tr")

            # First pass: build anchor -> FIDE ID map
            # Player rows have <a name="X"> in the name cell; X maps to that player's FIDE ID
            anchor_to_id: Dict[str, str] = {}
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    first_text = cells[0].get_text(strip=True)
                    if first_text and first_text.isdigit():
                        # Player summary row - FIDE ID in first cell
                        player_id = first_text
                        for a in cells[1].find_all("a"):
                            anchor = a.get("name") or a.get("id")
                            if anchor:
                                anchor_to_id[str(anchor)] = player_id
                                break

            players = []
            i = 0
            while i < len(rows):
                row = rows[i]
                cells = row.find_all("td")

                # Check if this is a player summary row
                # Format: ID, Name, Country, (empty), (empty), Rating, Total
                if len(cells) >= 7:
                    first_cell_text = cells[0].get_text(strip=True)
                    # Player summary rows start with a numeric ID
                    if first_cell_text and first_cell_text.isdigit():
                        player_id = first_cell_text
                        player_name = extract_text_from_cell(cells[1])
                        player_country = cells[2].get_text(strip=True)
                        player_total = cells[6].get_text(strip=True)

                        # Total score (no longer collecting rating - use profile chart if needed)
                        try:
                            player_total_float = (
                                float(player_total) if player_total else 0.0
                            )
                        except ValueError:
                            player_total_float = 0.0

                        # Rank = 1-based order on page (correlates with tournament rank/tiebreaks)
                        rank = len(players) + 1
                        player = {
                            "id": player_id,
                            "name": player_name,
                            "country": player_country,
                            "total": player_total_float,
                            "rank": rank,
                            "rounds": [],
                        }

                        # Look ahead for round data rows
                        i += 1
                        # Skip the round header row if present
                        if i < len(rows):
                            next_row = rows[i]
                            next_cells = next_row.find_all("td")
                            if (
                                len(next_cells) >= 7
                                and next_cells[0].get_text(strip=True).lower()
                                == "round"
                            ):
                                i += 1  # Skip header row

                        # Collect round data rows
                        while i < len(rows):
                            round_row = rows[i]
                            round_cells = round_row.find_all("td")

                            if len(round_cells) >= 7:
                                round_first_text = round_cells[0].get_text(strip=True)
                                # Check if this is a round data row (starts with digit)
                                if round_first_text and round_first_text[0].isdigit():
                                    round_num, round_date = parse_round_date(
                                        round_first_text
                                    )
                                    opp_name = extract_text_from_cell(round_cells[1])
                                    score_text = round_cells[6].get_text(strip=True)

                                    color = extract_color_from_cell(round_cells[1])
                                    anchor = extract_href_anchor_from_cell(
                                        round_cells[1]
                                    )
                                    opp_id = (
                                        anchor_to_id.get(anchor, "") if anchor else ""
                                    )
                                    score = parse_score(score_text)
                                    forfeit = extract_forfeit_indicator(score_text)
                                    # Forfeit can also appear in Opp. Fed. column (cells[2]) when score cell is empty
                                    if not forfeit and len(round_cells) >= 3:
                                        opp_fed_text = round_cells[2].get_text(
                                            strip=True
                                        )
                                        forfeit = extract_forfeit_indicator(
                                            opp_fed_text
                                        )

                                    has_result = forfeit or (score is not None)
                                    has_opponent = bool(opp_id)

                                    if not has_opponent and has_result:
                                        logger.warning(
                                            "Result without opponent: tournament_id=%s player_id=%s round=%s "
                                            "(forfeit=%s score=%s) - not adding game",
                                            tournament_code,
                                            player_id,
                                            round_num,
                                            forfeit or "",
                                            score,
                                        )
                                    elif has_opponent:
                                        # Add round only when we have an opponent (can form a game)
                                        round_data = {
                                            "round": round_num,
                                            "date": round_date,
                                            "opp_id": opp_id,
                                            "color": color,
                                            "score": score,
                                            "forfeit": forfeit,
                                        }
                                        player["rounds"].append(round_data)
                                    i += 1
                                else:
                                    # Not a round row, break to process next player
                                    break
                            else:
                                # Not enough cells, break
                                break

                        players.append(player)
                        continue

                i += 1

            if not players:
                return None, "no players found", len(attempt_times), raw_content

            report_dict = {
                "tournament_code": tournament_code,
                "players": players,
            }
            if report_start_iso:
                report_dict["report_start"] = report_start_iso
            return (report_dict, None, len(attempt_times), raw_content)

        except requests.exceptions.Timeout as e:
            last_error = f"timeout: {e}"
            if _attempt_log is not None:
                _attempt_log.append(
                    {
                        "tournament_code": tournament_code,
                        "attempt": attempt + 1,
                        "error": last_error,
                        "duration_s": attempt_times[-1] if attempt_times else 0,
                    }
                )
            continue
        except requests.exceptions.ConnectionError as e:
            error_str = str(e).lower()
            # Check for various connection error patterns that should be retried
            if any(
                pattern in error_str
                for pattern in [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
            ):
                last_error = f"network error: {e}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_code": tournament_code,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue
            last_error = f"connection error: {e}"
            return None, last_error, len(attempt_times), None
        except requests.exceptions.RequestException as e:
            error_str = str(e).lower()
            # Check for various connection error patterns that should be retried
            if any(
                pattern in error_str
                for pattern in [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
            ):
                last_error = f"network error: {e}"
                if _attempt_log is not None:
                    _attempt_log.append(
                        {
                            "tournament_code": tournament_code,
                            "attempt": attempt + 1,
                            "error": last_error,
                            "duration_s": attempt_times[-1] if attempt_times else 0,
                        }
                    )
                continue
            last_error = f"network error: {e}"
            return None, last_error, len(attempt_times), None
        except Exception as e:
            last_error = f"parse error: {e}"
            if _attempt_log is not None:
                _attempt_log.append(
                    {
                        "tournament_code": tournament_code,
                        "attempt": attempt + 1,
                        "error": last_error,
                        "duration_s": attempt_times[-1] if attempt_times else 0,
                    }
                )
            continue

    return None, f"max retries exceeded: {last_error}", len(attempt_times), None


def flatten_result(result: Dict) -> List[Dict]:
    """
    Flatten a result to player-round rows (legacy format for tests).
    Each row has: tournament_code, success, player_id, round, round_date, opp_id, color, score, forfeit, etc.
    """
    flattened = []
    tc = result.get("tournament_code", "")
    success = result.get("success", False)
    error = result.get("error", "")

    if not success:
        flattened.append(
            {
                "tournament_code": tc,
                "success": False,
                "error": error,
                "player_id": "",
                "player_name": "",
                "player_country": "",
                "player_total": 0.0,
                "round": None,
                "round_date": "",
                "opp_name": "",
                "opp_id": "",
                "color": "",
                "opp_fed": "",
                "title": "",
                "wtitle": "",
                "score": None,
                "forfeit": "",
            }
        )
        return flattened

    for player in result.get("players", []):
        pid = player.get("id", "")
        pname = player.get("name", "")
        pcountry = player.get("country", "")
        ptotal = player.get("total", 0.0)
        rounds = player.get("rounds", [])
        if not rounds:
            flattened.append(
                {
                    "tournament_code": tc,
                    "success": True,
                    "error": "",
                    "player_id": pid,
                    "player_name": pname,
                    "player_country": pcountry,
                    "player_total": ptotal,
                    "round": None,
                    "round_date": "",
                    "opp_name": "",
                    "opp_id": "",
                    "color": "",
                    "opp_fed": "",
                    "title": "",
                    "wtitle": "",
                    "score": None,
                    "forfeit": "",
                }
            )
        else:
            for rd in rounds:
                flattened.append(
                    {
                        "tournament_code": tc,
                        "success": True,
                        "error": "",
                        "player_id": pid,
                        "player_name": pname,
                        "player_country": pcountry,
                        "player_total": ptotal,
                        "round": rd.get("round"),
                        "round_date": rd.get("date", ""),
                        "opp_name": rd.get("opp_name", ""),
                        "opp_id": rd.get("opp_id", ""),
                        "color": rd.get("color", ""),
                        "opp_fed": rd.get("opp_fed", ""),
                        "title": rd.get("title", ""),
                        "wtitle": rd.get("wtitle", ""),
                        "score": rd.get("score"),
                        "forfeit": rd.get("forfeit", ""),
                    }
                )
    return flattened


def flatten_to_games(
    flattened: List[Dict],
    tournament_code: str = "",
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> List[Dict]:
    """
    Convert player-round rows to games (legacy format for tests).
    Returns list of dicts with white_id, black_id, white_score, forfeit (bool), round, date.
    """
    date_strs = list(
        {
            r.get("round_date", "")
            for r in flattened
            if r.get("round_date") and r.get("success")
        }
    )
    start_iso, end_iso = None, None
    if details_map and tournament_code:
        start_iso, end_iso = details_map.get(tournament_code, (None, None))
    date_format = infer_date_format(date_strs, start_iso=start_iso, end_iso=end_iso)

    games = []
    seen: set = set()
    for row in flattened:
        if (
            not row.get("success")
            or row.get("round") is None
            or not row.get("player_id")
            or not row.get("opp_id")
        ):
            continue
        tc = row.get("tournament_code", tournament_code)
        rnd = row["round"]
        color = (row.get("color") or "").strip().lower()
        forfeit = (row.get("forfeit") or "").strip()

        if color == "white":
            white_id, black_id = row["player_id"], row["opp_id"]
        elif color == "black":
            white_id, black_id = row["opp_id"], row["player_id"]
        else:
            continue

        key = (tc, rnd, white_id, black_id)
        if key in seen:
            continue
        seen.add(key)

        score = row.get("score")
        if forfeit:
            white_score = (
                1.0
                if (color == "white" and forfeit == "+")
                or (color == "black" and forfeit == "-")
                else 0.0
            )
        elif score is not None:
            white_score = float(score) if color == "white" else 1.0 - float(score)
        else:
            continue

        date_iso = (
            parse_date_to_iso(row.get("round_date", ""), date_format=date_format) or ""
        )
        games.append(
            {
                "tournament_code": tc,
                "round": rnd,
                "date": date_iso,
                "white_id": white_id,
                "black_id": black_id,
                "white_score": white_score,
                "forfeit": bool(forfeit),
            }
        )
    return games


def _flatten_rounds_for_games(result: Dict) -> List[Dict]:
    """Flatten player-round rows for games extraction (internal)."""
    rows = []
    tc = result.get("tournament_code", "")
    if not result.get("success"):
        return rows
    for player in result.get("players", []):
        pid = player.get("id", "")
        for rd in player.get("rounds", []):
            if not rd.get("opp_id"):
                continue
            rows.append(
                {
                    "tournament_code": tc,
                    "player_id": pid,
                    "round": rd.get("round"),
                    "round_date": rd.get("date", ""),
                    "opp_id": rd.get("opp_id", ""),
                    "color": (rd.get("color") or "").strip().lower(),
                    "score": rd.get("score"),
                    "forfeit": (rd.get("forfeit") or "").strip(),
                }
            )
    return rows


def validate_against_players_file(result: Dict, players_df: pd.DataFrame) -> None:
    """
    Compare report player name/country to players file. Log mismatches with
    expected (from players file) vs actual (from report), tournament_id, player_id.
    """
    if not result.get("success") or players_df is None or players_df.empty:
        return
    tc = result.get("tournament_code", "")
    id_col = "id" if "id" in players_df.columns else "fide_id"
    if id_col not in players_df.columns:
        return
    name_col = "name" if "name" in players_df.columns else None
    fed_col = "fed" if "fed" in players_df.columns else "country"
    if fed_col not in players_df.columns:
        fed_col = None
    lookup = players_df.set_index(id_col)
    for player in result.get("players", []):
        pid = str(player.get("id", ""))
        if not pid:
            continue
        try:
            row = lookup.loc[pid]
        except (KeyError, TypeError):
            continue
        expected_name = str(row.get(name_col, "")).strip() if name_col else ""
        expected_fed = str(row.get(fed_col, "")).strip() if fed_col else ""
        actual_name = str(player.get("name", "")).strip()
        actual_fed = str(player.get("country", "")).strip()
        if expected_name and actual_name and expected_name != actual_name:
            logger.warning(
                "Player name mismatch: tournament_id=%s player_id=%s "
                "expected=%r actual=%r",
                tc,
                pid,
                expected_name,
                actual_name,
            )
        if expected_fed and actual_fed and expected_fed != actual_fed:
            logger.warning(
                "Player country mismatch: tournament_id=%s player_id=%s "
                "expected=%r actual=%r",
                tc,
                pid,
                expected_fed,
                actual_fed,
            )


def validate_pairings(result: Dict) -> None:
    """
    Validate pairing consistency: each game has mutual pairing, IDs/names match,
    scores sum to 1 for normal games, forfeits are opposite (+ vs -).
    Log any issues.
    """
    if not result.get("success"):
        return
    tc = result.get("tournament_code", "")
    players_by_id = {str(p.get("id", "")): p for p in result.get("players", [])}
    # Build (player_id, round) -> {opp_id, score, forfeit, color}
    rounds_map: Dict[Tuple[str, int], Dict] = {}
    for player in result.get("players", []):
        pid = str(player.get("id", ""))
        for rd in player.get("rounds", []):
            opp_id = rd.get("opp_id")
            if not opp_id:
                continue
            rnd = rd.get("round")
            if rnd is None:
                continue
            key = (pid, rnd)
            rounds_map[key] = {
                "opp_id": str(opp_id),
                "score": rd.get("score"),
                "forfeit": (rd.get("forfeit") or "").strip(),
                "color": (rd.get("color") or "").strip().lower(),
            }
    seen_pairs: set = set()
    for (pid, rnd), data in rounds_map.items():
        opp_id = data["opp_id"]
        pair = tuple(sorted([pid, opp_id])) + (rnd,)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        rev_key = (opp_id, rnd)
        rev = rounds_map.get(rev_key)
        if not rev:
            logger.warning(
                "Pairing not mutual: tournament_id=%s player_id=%s opp_id=%s round=%s "
                "(opponent does not list this player)",
                tc,
                pid,
                opp_id,
                rnd,
            )
            continue
        if rev.get("opp_id") != pid:
            logger.warning(
                "Pairing ID mismatch: tournament_id=%s round=%s "
                "player %s has opp_id=%s but opponent %s has opp_id=%s",
                tc,
                rnd,
                pid,
                opp_id,
                opp_id,
                rev.get("opp_id"),
            )
        opp_in_tournament = opp_id in players_by_id
        if not opp_in_tournament:
            logger.warning(
                "Opponent not in tournament: tournament_id=%s player_id=%s opp_id=%s round=%s",
                tc,
                pid,
                opp_id,
                rnd,
            )
        forfeit_a = data.get("forfeit", "")
        forfeit_b = rev.get("forfeit", "")
        score_a = data.get("score")
        score_b = rev.get("score")
        if forfeit_a and forfeit_b:
            if (forfeit_a, forfeit_b) not in (("+", "-"), ("-", "+")):
                logger.warning(
                    "Forfeit indicators not opposite: tournament_id=%s round=%s "
                    "player %s forfeit=%r opponent %s forfeit=%r",
                    tc,
                    rnd,
                    pid,
                    forfeit_a,
                    opp_id,
                    forfeit_b,
                )
        elif score_a is not None and score_b is not None:
            total = float(score_a) + float(score_b)
            if abs(total - 1.0) > 0.001:
                logger.warning(
                    "Scores do not sum to 1: tournament_id=%s round=%s "
                    "player %s score=%s opponent %s score=%s (sum=%s)",
                    tc,
                    rnd,
                    pid,
                    score_a,
                    opp_id,
                    score_b,
                    total,
                )


def results_to_players_dataframe(results: List[Dict]) -> pd.DataFrame:
    """
    Build players DataFrame. PK: (player_id, tournament_id).
    Columns: player_id, tournament_id, player_name, player_country, player_total, rank.
    """
    rows = []
    for result in results:
        if not result.get("success"):
            continue
        tc = result.get("tournament_code", "")
        for player in result.get("players", []):
            rows.append(
                {
                    "player_id": str(player.get("id", "")),
                    "tournament_id": str(tc),
                    "player_name": player.get("name", ""),
                    "player_country": player.get("country", ""),
                    "player_total": player.get("total", 0.0),
                    "rank": player.get("rank", 0),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "player_id",
                "tournament_id",
                "player_name",
                "player_country",
                "player_total",
                "rank",
            ]
        )
    return pd.DataFrame(rows)


def results_to_games_dataframe(
    results: List[Dict],
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> pd.DataFrame:
    """
    Build games DataFrame. PK: (white_player_id, tournament_id, round_number).
    Columns: white_player_id, black_player_id, tournament_id, round_number, round_date, score, forfeit.
    score = white's score (0, 0.5, 1). forfeit = from white's perspective ("+", "-", or "").
    """
    all_games = []
    for result in results:
        tc = result.get("tournament_code", "")
        flattened = _flatten_rounds_for_games(result)
        if not flattened:
            continue

        date_strs = list(
            {r.get("round_date", "") for r in flattened if r.get("round_date")}
        )
        start_iso, end_iso = None, None
        if details_map and tc:
            start_iso, end_iso = details_map.get(tc, (None, None))
        report_start_iso = result.get("report_start")
        date_format = infer_date_format(
            date_strs,
            start_iso=start_iso,
            end_iso=end_iso,
            report_start_iso=report_start_iso,
        )

        seen: set = set()  # (white_id, tc, round)
        for row in flattened:
            if (
                row.get("round") is None
                or not row.get("player_id")
                or not row.get("opp_id")
            ):
                continue
            color = row.get("color", "")
            if color == "white":
                white_id, black_id = row["player_id"], row["opp_id"]
            elif color == "black":
                white_id, black_id = row["opp_id"], row["player_id"]
            else:
                continue

            key = (white_id, tc, row["round"])
            if key in seen:
                continue
            seen.add(key)

            forfeit = row.get("forfeit", "")
            score_val = row.get("score")
            if forfeit:
                if color == "white":
                    white_score = 1.0 if forfeit == "+" else 0.0
                    white_forfeit = forfeit
                else:
                    white_score = 0.0 if forfeit == "+" else 1.0
                    white_forfeit = (
                        "-" if forfeit == "+" else "+"
                    )  # flip to white's perspective
            elif score_val is not None:
                white_score = (
                    float(score_val) if color == "white" else 1.0 - float(score_val)
                )
                white_forfeit = ""
            else:
                continue

            date_iso = (
                parse_date_to_iso(row.get("round_date", ""), date_format=date_format)
                or ""
            )
            round_dt = parse_iso_to_datetime(date_iso)
            if round_dt is None:
                round_dt = pd.NaT

            all_games.append(
                {
                    "white_player_id": white_id,
                    "black_player_id": black_id,
                    "tournament_id": tc,
                    "round_number": row["round"],
                    "round_date": round_dt,
                    "score": white_score,
                    "forfeit": white_forfeit,
                }
            )

    if not all_games:
        return pd.DataFrame(
            columns=[
                "white_player_id",
                "black_player_id",
                "tournament_id",
                "round_number",
                "round_date",
                "score",
                "forfeit",
            ]
        )
    return pd.DataFrame(all_games)


def _write_parquet_to_path(df: pd.DataFrame, path: str) -> None:
    """Write DataFrame to Parquet (local or S3)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    _write_to_path(path, buf.getvalue())


def save_players_parquet(results: List[Dict], parquet_path: str):
    """Save players Parquet. PK: (player_id, tournament_id)."""
    try:
        df = results_to_players_dataframe(results)
        _write_parquet_to_path(df, parquet_path)
        logger.info(f"Saved {len(df)} player rows to {parquet_path}")
    except Exception as e:
        logger.error(f"Players Parquet save failed: {e}")


def save_games_parquet(
    results: List[Dict],
    parquet_path: str,
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Save games as Parquet file (one row per game, main output format)."""
    try:
        df = results_to_games_dataframe(results, details_map=details_map)
        _write_parquet_to_path(df, parquet_path)
        logger.info(f"Saved {len(results)} tournament(s) to {parquet_path}")
        logger.info(f"  Total games: {len(df)}")
    except Exception as e:
        logger.error(f"Games Parquet save failed: {e}")


def _transform_results_round_dates_to_datetime(
    results: List[Dict],
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> None:
    """Mutate results in place: convert round 'date' from raw string to datetime."""
    for result in results:
        if not result.get("success"):
            continue
        tc = result.get("tournament_code", "")
        date_strs = []
        for player in result.get("players", []):
            for rd in player.get("rounds", []):
                d = rd.get("date", "")
                if d and isinstance(d, str):
                    date_strs.append(d)
        start_iso, end_iso = (None, None)
        if details_map and tc:
            start_iso, end_iso = details_map.get(tc, (None, None))
        report_start_iso = result.get("report_start")
        date_format = infer_date_format(
            date_strs,
            start_iso=start_iso,
            end_iso=end_iso,
            report_start_iso=report_start_iso,
        )
        for player in result.get("players", []):
            for rd in player.get("rounds", []):
                raw = rd.get("date", "")
                if raw and isinstance(raw, str):
                    iso = parse_date_to_iso(raw, date_format=date_format)
                    dt = parse_iso_to_datetime(iso)
                    rd["date"] = dt if dt is not None else raw


def save_verbose_json_sample(
    results: List[Dict],
    json_path: str,
    sample_size: int = 5,
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Save a sample of raw results (tournaments with players/rounds) to JSON. Round dates are datetime (ISO when serialized)."""
    try:
        sample_results = [r for r in results if r.get("success")][:sample_size]
        if not sample_results:
            logger.warning("No successful results, skipping JSON sample")
            return
        # Deep copy to avoid mutating original
        sample_results = copy.deepcopy(sample_results)
        _transform_results_round_dates_to_datetime(
            sample_results, details_map=details_map
        )
        dirname = os.path.dirname(json_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        def _json_default(obj):
            if isinstance(obj, datetime):
                return obj.strftime("%Y-%m-%d")
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable"
            )

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                sample_results, f, indent=2, ensure_ascii=False, default=_json_default
            )
        logger.info(f"Saved sample of {len(sample_results)} tournaments to {json_path}")
    except Exception as e:
        logger.error(f"Verbose JSON sample save failed: {e}")


def save_csv_sample_from_parquet(
    parquet_path: str,
    csv_path: str,
    sample_size: int = 100,
):
    """
    Read parquet, sample rows, save as CSV to confirm the parquet file works.
    """
    try:
        df = pd.read_parquet(parquet_path)
        if df.empty:
            logger.warning("Parquet is empty, skipping CSV sample")
            return
        n = min(sample_size, len(df))
        sample_df = df.sample(n=n, random_state=42) if n < len(df) else df
        csv_dir = os.path.dirname(csv_path)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)
        sample_df.to_csv(csv_path, index=False)
        logger.info(f"Saved sample of {n} games to {csv_path} (confirms parquet)")
    except Exception as e:
        logger.error(f"CSV sample save failed: {e}")


def save_checkpoint(
    games_path: str,
    results: List[Dict],
    checkpoint_path: Optional[str] = None,
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Save checkpoint (games parquet) to checkpoint path."""
    if not checkpoint_path:
        return
    try:
        save_games_parquet(results, checkpoint_path, details_map=details_map)
    except Exception as e:
        logger.error(f"Checkpoint save failed: {e}")


def run(
    input_path: str,
    output_path: str,
    details_path: Optional[str] = None,
    rate_limit: float = 0.5,
    max_retries: int = 3,
    quiet: bool = False,
    limit: int = 0,
    save_raw: bool = False,
) -> int:
    """
    Scrape tournament reports for codes from input_path, write to output_path.

    Args:
        input_path: Path to tournament codes file (one per line). Local or S3 URI.
        output_path: Base path for outputs. Writes {output_path}_players.parquet and
            {output_path}_games.parquet.
        details_path: Optional path to tournament_details parquet for date inference.
        rate_limit: Requests per second (0 = no limit, natural throughput).
        max_retries: Retry passes for failed fetches.
        quiet: Reduce log output.
        limit: Process only first N codes (0 = all).
        save_raw: If True, save concatenated raw HTML to raw/reports/chunk_{i}.html.gz.

    Returns:
        0 on success, 1 on failure.
    """
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    base = output_path.rstrip("/")
    players_path = base + "_players.parquet"
    games_path = base + "_games.parquet"

    try:
        codes = _read_codes_from_path(input_path)
    except Exception as e:
        logger.error("Error reading codes from %s: %s", input_path, e)
        return 1

    if not codes:
        logger.error("No tournament codes found in %s", input_path)
        return 1

    if limit > 0:
        codes = codes[:limit]
        logger.info("Limited to first %d tournaments", limit)

    details_map: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    if details_path:
        try:
            if _is_s3(details_path):
                from s3_io import download_to_file

                local_path = Path(tempfile.gettempdir()) / "details_chunk.parquet"
                download_to_file(details_path, local_path)
                df = pd.read_parquet(local_path)
            else:
                df = pd.read_parquet(details_path)
            ec_col = "event_code" if "event_code" in df.columns else "id"
            for _, row in df.iterrows():
                ec = row.get(ec_col)
                if pd.notna(ec) and str(ec):
                    sd = parse_details_date_to_iso(str(row.get("start_date", "")))
                    ed = parse_details_date_to_iso(str(row.get("end_date", "")))
                    details_map[str(ec)] = (sd, ed)
            logger.info(
                "Loaded date bounds for %d tournaments from %s",
                len(details_map),
                details_path,
            )
        except Exception as e:
            logger.warning("Could not load details for date inference: %s", e)

    logger.info(
        "Processing %d tournaments from %s -> %s",
        len(codes),
        input_path,
        games_path,
    )

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    raw_base: Optional[str] = (
        _raw_base_from_output_path(output_path) if save_raw else None
    )
    raw_accumulator: List[Tuple[str, bytes]] = []  # (code, html)

    all_results: List[Dict] = []
    success_count = 0
    error_count = 0
    current_codes = codes

    pbar = None
    if not quiet:
        pbar = tqdm(
            total=len(codes),
            desc="Processing",
            unit="tournament",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    rate_limiter = RateLimiter(rate_limit) if rate_limit > 0 else None

    start_time = time.time()

    for pass_num in range(max_retries + 1):
        if not current_codes:
            break
        if pass_num > 0:
            delay = 3 * (2 ** (pass_num - 1))
            logger.info(
                "Retry pass %d: waiting %s before retrying %d tournaments",
                pass_num,
                format_duration(delay),
                len(current_codes),
            )
            time.sleep(delay)

        pass_failed = []
        for code in current_codes:
            if rate_limiter:
                rate_limiter.wait()
            report, error, _, raw_content = fetch_tournament_report(
                code, session, return_raw=save_raw
            )

            result = {"tournament_code": code}
            if report is None:
                error_count += 1
                result["success"] = False
                result["error"] = error or "fetch failed"
                error_lower = (error or "").lower()
                network_error_patterns = [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
                is_network_error = any(p in error_lower for p in network_error_patterns)
                if (
                    error
                    and (is_network_error or "timeout" in error_lower)
                    and pass_num < max_retries
                ):
                    pass_failed.append(code)
            else:
                success_count += 1
                result["success"] = True
                result.update(report)
                if raw_base and raw_content:
                    raw_accumulator.append((code, raw_content))

            all_results.append(result)
            if pbar:
                pbar.update(1)
                pbar.set_postfix({"✓": success_count, "✗": error_count})

        current_codes = pass_failed

    if pbar:
        pbar.close()

    if raw_base and raw_accumulator:
        from raw_utils import build_concatenated_gzip

        raw_path = raw_base + ".html.gz"
        _write_to_path(raw_path, build_concatenated_gzip(raw_accumulator))
        logger.info(
            "Saved concatenated raw HTML (%d tournaments) to %s",
            len(raw_accumulator),
            raw_path,
        )

    save_players_parquet(all_results, players_path)
    save_games_parquet(all_results, games_path, details_map=details_map)

    elapsed = time.time() - start_time
    logger.info(
        "Done: %d success, %d errors in %s",
        success_count,
        error_count,
        format_duration(elapsed),
    )
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Scrape FIDE tournament reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", type=str, default="", help="Path to tournament codes file"
    )
    parser.add_argument("--year", type=int, default=0, help="Year to process")
    parser.add_argument("--month", type=int, default=0, help="Month to process")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--local-root",
        type=str,
        default="data",
        help="Local bucket root for run structure",
    )
    parser.add_argument(
        "--run-type",
        type=str,
        choices=("prod", "custom", "test"),
        default=None,
        help="Run type; with --run-name uses run path structure",
    )
    parser.add_argument(
        "--run-name",
        type=str,
        default=None,
        help="Run name (e.g. 2024-01)",
    )
    parser.add_argument("--output", type=str, default="", help="Output file path")
    parser.add_argument(
        "--max-retries", type=int, default=3, help="Max retry passes (default: 3)"
    )
    parser.add_argument(
        "--checkpoint",
        type=int,
        default=50,
        help="Save every N tournaments (default: 50)",
    )
    parser.add_argument(
        "--show-time", action="store_true", help="Show timing info for each tournament"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Use verbose stdout output instead of progress bar (shows detailed error info)",
    )
    parser.add_argument(
        "--details-path",
        type=str,
        default="",
        help="Path to tournament_details parquet (for date inference; optional, improves round date format accuracy)",
    )
    parser.add_argument(
        "--no-samples",
        action="store_true",
        help="Skip JSON and CSV sample outputs (parquet only)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N tournaments (for testing/profiling)",
    )
    parser.add_argument(
        "--verbose-errors",
        action="store_true",
        help="Log failed HTTP attempt details and print retry analysis at end",
    )
    parser.add_argument(
        "--players-file",
        type=str,
        default="",
        help="Path to players_list.parquet for validation. Default: src/data/players_list.parquet (from repo root).",
    )
    parser.add_argument(
        "--no-validation",
        action="store_true",
        help="Skip pairing and player-file validation (faster when not needed).",
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Overwrite existing output if it exists",
    )

    args = parser.parse_args()

    # Repo root for default paths
    _script_dir = Path(__file__).resolve().parent
    repo_root = _script_dir.parent.parent

    # Determine input path and read tournament codes; build details_map for date inference
    tournament_codes = []
    details_map: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    if args.input:
        input_path = args.input
        try:
            tournament_codes = read_tournament_codes(input_path)
        except Exception as e:
            logger.error(f"Error reading codes: {e}")
            sys.exit(1)
        # Optionally load details for date inference
        if args.details_path and os.path.exists(args.details_path):
            try:
                df = pd.read_parquet(args.details_path)
                ec_col = "event_code" if "event_code" in df.columns else "id"
                for _, row in df.iterrows():
                    ec = row.get(ec_col)
                    if pd.notna(ec) and str(ec):
                        sd = parse_details_date_to_iso(str(row.get("start_date", "")))
                        ed = parse_details_date_to_iso(str(row.get("end_date", "")))
                        details_map[str(ec)] = (sd, ed)
                logger.info(
                    f"Loaded date bounds for {len(details_map)} tournaments from {args.details_path}"
                )
            except Exception as e:
                logger.warning(f"Could not load details for date inference: {e}")
    elif args.year > 0 and args.month > 0:
        if args.month < 1 or args.month > 12:
            logger.error("Error: month must be 1-12")
            sys.exit(1)
        run_name = args.run_name or f"{args.year}-{args.month:02d}"
        if args.run_type:
            from s3_io import build_local_path_for_run

            ids_path = str(
                build_local_path_for_run(
                    args.local_root,
                    args.run_type,
                    run_name,
                    "data",
                    "tournament_ids.txt",
                )
            )
            details_path = (
                args.details_path
                if args.details_path
                else str(
                    build_local_path_for_run(
                        args.local_root,
                        args.run_type,
                        run_name,
                        "data",
                        "tournament_details.parquet",
                    )
                )
            )
        else:
            ids_path = os.path.join(
                args.data_dir, "tournament_ids", f"{args.year}_{args.month:02d}"
            )
            details_path = (
                args.details_path
                if args.details_path
                else os.path.join(
                    args.data_dir,
                    "tournament_details",
                    f"{args.year}_{args.month:02d}.parquet",
                )
            )
        if not os.path.exists(ids_path):
            logger.error(f"Error: tournament IDs file not found: {ids_path}")
            logger.error("Run get_tournaments.py first for --year/--month")
            logger.error("Alternatively use --input with a codes file")
            sys.exit(1)
        try:
            tournament_codes = read_tournament_codes(ids_path)
            logger.info(
                f"Loaded {len(tournament_codes)} tournament codes from {ids_path}"
            )
        except Exception as e:
            logger.error(f"Error reading tournament IDs: {e}")
            sys.exit(1)
        # Optionally load tournament_details for date inference (start/end) when available
        if os.path.exists(details_path):
            try:
                df = pd.read_parquet(details_path)
                ec_col = "event_code" if "event_code" in df.columns else "id"
                success_df = df[df["success"] == True]
                for _, row in success_df.iterrows():
                    ec = row.get(ec_col)
                    if pd.notna(ec) and str(ec):
                        sd = parse_details_date_to_iso(str(row.get("start_date", "")))
                        ed = parse_details_date_to_iso(str(row.get("end_date", "")))
                        details_map[str(ec)] = (sd, ed)
                if details_map:
                    logger.info(
                        f"Loaded date bounds for {len(details_map)} tournaments from {details_path} (for date format inference)"
                    )
            except Exception as e:
                logger.warning(f"Could not load details for date inference: {e}")
    else:
        logger.error("Error: specify --input or --year and --month")
        sys.exit(1)

    if not tournament_codes:
        logger.error("No tournament codes found")
        sys.exit(1)

    if args.limit > 0:
        tournament_codes = tournament_codes[: args.limit]
        logger.info(f"Limited to first {len(tournament_codes)} tournaments")

    # Determine output paths: players and games (2 files)
    players_path = None
    games_path = None
    json_path = None
    csv_path = None
    if args.output:
        base = args.output.replace(".json", "").replace(".parquet", "")
        players_path = base + "_players.parquet"
        games_path = base + "_games.parquet"
        json_path = base + "_sample.json"
        csv_path = base + "_sample.csv"
    elif args.year > 0 and args.month > 0:
        if args.run_type:
            from s3_io import build_local_path_for_run

            run_name = args.run_name or f"{args.year}-{args.month:02d}"
            data_base = str(
                build_local_path_for_run(
                    args.local_root,
                    args.run_type,
                    run_name,
                    "data",
                    "tournament_reports",
                )
            )
            sample_base = str(
                build_local_path_for_run(
                    args.local_root,
                    args.run_type,
                    run_name,
                    "sample",
                    "tournament_reports",
                )
            )
            players_path = data_base + "_players.parquet"
            games_path = data_base + "_games.parquet"
            json_path = sample_base + "_verbose_sample.json"
            csv_path = sample_base + "_games_sample.csv"
        else:
            base_path = os.path.join(
                args.data_dir, "tournament_reports", f"{args.year}_{args.month:02d}"
            )
            players_path = base_path + "_players.parquet"
            games_path = base_path + "_games.parquet"
            json_path = base_path + "_sample.json"
            csv_path = base_path + "_sample.csv"

    if games_path and not args.override and os.path.exists(games_path):
        logger.info("Output %s already exists. Use --override to replace.", games_path)
        sys.exit(0)

    # Load players file for validation (name/country, pairing checks)
    players_df: Optional[pd.DataFrame] = None
    if not args.no_validation:
        if args.players_file:
            players_file_path = args.players_file
        elif args.run_type and args.year > 0 and args.month > 0:
            from s3_io import build_local_path_for_run

            run_name = args.run_name or f"{args.year}-{args.month:02d}"
            players_file_path = str(
                build_local_path_for_run(
                    args.local_root,
                    args.run_type,
                    run_name,
                    "data",
                    "players_list.parquet",
                )
            )
        else:
            players_file_path = str(repo_root / "src" / "data" / "players_list.parquet")
        if os.path.exists(players_file_path):
            try:
                players_df = pd.read_parquet(players_file_path)
                logger.info(
                    "Loaded players file for validation: %s (%d rows)",
                    players_file_path,
                    len(players_df),
                )
            except Exception as e:
                logger.warning("Could not load players file for validation: %s", e)
        else:
            logger.info(
                "Players file not found at %s; skipping validation",
                players_file_path,
            )

    logger.info(f"Processing {len(tournament_codes)} tournaments")
    logger.info(
        f"Settings: checkpoint every {args.checkpoint} (no rate limit - natural throughput)"
    )

    start_time = time.time()

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    all_results: List[Dict] = []
    success_count = 0
    error_count = 0
    total_retries = 0
    attempt_log: List[Dict] = [] if args.verbose_errors else []

    def _graceful_shutdown(signum, frame):
        logger.warning("\nReceived interrupt, initiating graceful shutdown...")
        if all_results and games_path:
            try:
                if players_path:
                    save_players_parquet(all_results, players_path)
                save_games_parquet(all_results, games_path, details_map=details_map)
                if not args.no_samples and json_path:
                    save_verbose_json_sample(
                        all_results, json_path, sample_size=100, details_map=details_map
                    )
                if not args.no_samples and csv_path:
                    save_csv_sample_from_parquet(games_path, csv_path, sample_size=100)
                logger.info("Saved %d results to %s", len(all_results), games_path)
            except Exception as e:
                logger.error("Error saving partial results: %s", e)
        sys.exit(130 if signum == 2 else 0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    attempt_counts: List[Tuple[str, int]] = [] if args.verbose_errors else []
    current_tournaments = tournament_codes

    pbar = None
    if not args.verbose:
        pbar = tqdm(
            total=len(tournament_codes),
            desc="Processing",
            unit="tournament",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    for pass_num in range(args.max_retries + 1):
        if not current_tournaments:
            break
        if pass_num > 0:
            delay = 3 * (2 ** (pass_num - 1))  # Exponential backoff: 3s, 6s, 12s
            logger.info(
                f"Retry pass {pass_num}: waiting {format_duration(delay)} before retrying {len(current_tournaments)} tournaments"
            )
            time.sleep(delay)
            total_retries += len(current_tournaments)

        pass_failed = []

        for tournament_code in current_tournaments:
            report, error, num_attempts, _ = fetch_tournament_report(
                tournament_code,
                session,
                _attempt_log=attempt_log if args.verbose_errors else None,
            )
            if args.verbose_errors:
                attempt_counts.append((tournament_code, num_attempts))

            result = {"tournament_code": tournament_code}

            if report is None:
                error_count += 1
                result["success"] = False
                result["error"] = error or "fetch failed"
                error_lower = error.lower() if error else ""
                network_error_patterns = [
                    "eof",
                    "connection reset",
                    "connection aborted",
                    "remotedisconnected",
                    "remote end closed",
                    "broken pipe",
                ]
                is_network_error = any(p in error_lower for p in network_error_patterns)
                if error and (is_network_error or "timeout" in error_lower):
                    if pass_num < args.max_retries:
                        pass_failed.append(tournament_code)
            else:
                success_count += 1
                result["success"] = True
                result.update(report)
                if not args.no_validation:
                    validate_pairings(result)
                    if players_df is not None:
                        validate_against_players_file(result, players_df)
                if args.checkpoint > 0 and success_count % args.checkpoint == 0:
                    checkpoint_path = games_path + ".checkpoint" if games_path else None
                    logger.info(f"Saving checkpoint at {success_count} successful...")
                    save_checkpoint(
                        games_path,
                        all_results,
                        checkpoint_path,
                        details_map=details_map,
                    )

            all_results.append(result)

            total_processed = success_count + error_count
            elapsed = time.time() - start_time

            if total_processed > 0:
                avg_time = elapsed / total_processed
                remaining = len(tournament_codes) - total_processed
                est_remaining = avg_time * remaining
            else:
                est_remaining = 0

            if args.verbose:
                actual_rate = total_processed / elapsed if elapsed > 0 else 0
                if result["success"]:
                    num_players = len(result.get("players", []))
                    retry_info = f" [Retry pass {pass_num + 1}]" if pass_num > 0 else ""
                    http_retries = (
                        f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
                    )
                    print(
                        f"[{total_processed}/{len(tournament_codes)}] ✓ {tournament_code}: {num_players} players{retry_info}{http_retries} | "
                        f"Actual: {actual_rate:.2f}/s | "
                        f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)} | "
                        f"Success: {success_count} | Errors: {error_count} | Retries: {total_retries}"
                    )
                else:
                    error_msg = result.get("error", "unknown")
                    will_retry = tournament_code in pass_failed
                    retry_info = f" [Retry pass {pass_num + 1}]" if pass_num > 0 else ""
                    http_retries = (
                        f" [{num_attempts} HTTP attempts]" if num_attempts > 1 else ""
                    )
                    retry_status = " [WILL RETRY]" if will_retry else " [FINAL FAILURE]"
                    print(
                        f"[{total_processed}/{len(tournament_codes)}] ✗ {tournament_code}: {error_msg}{retry_info}{http_retries}{retry_status} | "
                        f"Actual: {actual_rate:.2f}/s | "
                        f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)} | "
                        f"Success: {success_count} | Errors: {error_count} | Retries: {total_retries}"
                    )
            else:
                postfix_dict = {
                    "✓": success_count,
                    "✗": error_count,
                    "rate": f"{total_processed / elapsed if elapsed > 0 else 0:.2f}/s",
                }
                if total_retries > 0 or pass_num > 0:
                    postfix_dict["retries"] = total_retries
                if pass_num > 0:
                    postfix_dict["pass"] = f"{pass_num + 1}/{args.max_retries + 1}"
                if len(pass_failed) > 0:
                    postfix_dict["pending"] = len(pass_failed)
                postfix_dict["est"] = (
                    format_duration(est_remaining) if est_remaining > 0 else "?"
                )
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix(postfix_dict)

                if args.show_time:
                    actual_rate = total_processed / elapsed if elapsed > 0 else 0
                    if result["success"]:
                        num_players = len(result.get("players", []))
                        logger.info(
                            f"[{total_processed}/{len(tournament_codes)}] ✓ {tournament_code}: {num_players} players | "
                            f"Rate: {actual_rate:.2f}/s | Est: {format_duration(est_remaining)}"
                        )
                    else:
                        logger.info(
                            f"[{total_processed}/{len(tournament_codes)}] ✗ {tournament_code}: {result.get('error', 'unknown')} | "
                            f"Rate: {actual_rate:.2f}/s"
                        )

            if not args.verbose and (
                total_processed % 50 == 0 or total_processed == len(tournament_codes)
            ):
                actual_rate = total_processed / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: {total_processed}/{len(tournament_codes)} "
                    f"({success_count}✓ {error_count}✗) | "
                    f"Actual: {actual_rate:.2f}/s | "
                    f"Elapsed: {format_duration(elapsed)} | Est: {format_duration(est_remaining)}"
                )

        current_tournaments = pass_failed

    if pbar:
        pbar.close()

    # Save final results
    if players_path and games_path:
        save_players_parquet(all_results, players_path)
        save_games_parquet(all_results, games_path, details_map=details_map)

        if not args.no_samples:
            if csv_path:
                save_csv_sample_from_parquet(games_path, csv_path, sample_size=100)
            if json_path:
                save_verbose_json_sample(
                    all_results, json_path, sample_size=100, details_map=details_map
                )
    else:
        # If no output path specified, dump to stdout as JSON (for backwards compatibility)
        json.dump(all_results, sys.stdout, indent=2, ensure_ascii=False)

    total_time = time.time() - start_time
    final_rate = (success_count + error_count) / total_time if total_time > 0 else 0

    logger.info("\nFinal Summary:")
    logger.info(f"  Total: {len(tournament_codes)}")
    logger.info(
        f"  Success: {success_count} ({100.0 * success_count / len(tournament_codes):.1f}%)"
    )
    logger.info(f"  Errors: {error_count}")
    if total_retries > 0:
        logger.info(f"  Retries: {total_retries}")
    logger.info(f"  Time: {format_duration(total_time)}")
    logger.info(f"  Average rate: {final_rate:.2f} tournaments/sec")

    # Report tournaments with no successful fetch (orphaned / failed)
    failed_codes = [
        r.get("tournament_code", "") for r in all_results if not r.get("success", False)
    ]
    if failed_codes:
        max_show = 20
        sample = failed_codes[:max_show]
        logger.info(
            "  Tournaments with no successful report: %d (%s%s)",
            len(failed_codes),
            ", ".join(sample),
            " ..." if len(failed_codes) > max_show else "",
        )
    if players_path:
        logger.info(f"  Players Parquet: {players_path}")
    if games_path:
        logger.info(f"  Games Parquet: {games_path}")
    if not args.no_samples and json_path:
        logger.info(f"  JSON sample: {json_path}")
    if not args.no_samples and csv_path:
        logger.info(f"  CSV sample: {csv_path}")

    # Verbose error analysis
    if args.verbose_errors and attempt_counts:
        dist = Counter(n for _, n in attempt_counts)
        retried = [(code, n) for code, n in attempt_counts if n > 1]
        error_counts = Counter(e.get("error", "unknown") for e in attempt_log)
        logger.info("\nVerbose Error Analysis:")
        logger.info("  Attempt distribution: %s", dict(sorted(dist.items())))
        if retried:
            codes = [c for c, _ in retried]
            max_show = 30
            if len(codes) <= max_show:
                logger.info("  Tournaments needing retries (in order): %s", codes)
            else:
                logger.info(
                    "  Tournaments needing retries (first %d): %s ... and %d more",
                    max_show,
                    codes[:max_show],
                    len(codes) - max_show,
                )
        if error_counts:
            logger.info("  Error breakdown: %s", dict(error_counts))


if __name__ == "__main__":
    main()

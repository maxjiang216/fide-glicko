#!/usr/bin/env python3
"""
FIDE Tournament Reports Scraper

Scrapes tournament reports (original reports) from FIDE website for a list of tournament codes.
Extracts player data and round-by-round results.
Supports rate limiting, retries, checkpoints, and progress tracking.
"""

import argparse
import json
import logging
import os
import random
import re
import sys
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


def parse_details_date_to_iso(date_str: str) -> Optional[str]:
    """
    Parse start_date/end_date from tournament details to ISO (YYYY-MM-DD).
    FIDE uses formats like "2024.12.30", "30.12.2024", "2024-12-30".
    """
    if not date_str or not isinstance(date_str, str):
        return None
    s = date_str.strip()
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


def infer_date_format(
    date_strings: List[str],
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
) -> str:
    """
    Infer date format (yy/mm/dd vs dd/mm/yy) from round dates.
    Picks format that minimizes date range; if start/end from tournament
    details are provided, prefers format where all dates fall within [start, end].
    """
    candidates = ["yy/mm/dd", "dd/mm/yy"]
    date_strs = [
        s for s in date_strings if s and re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", s.strip())
    ]
    if not date_strs:
        return "yy/mm/dd"  # default

    start_dt = None
    end_dt = None
    if start_iso:
        try:
            start_dt = datetime.strptime(start_iso, "%Y-%m-%d")
        except ValueError:
            pass
    if end_iso:
        try:
            end_dt = datetime.strptime(end_iso, "%Y-%m-%d")
        except ValueError:
            pass

    best = "yy/mm/dd"
    best_score = float("inf")

    for fmt in candidates:
        parsed = []
        for s in date_strs:
            iso = _parse_round_date_with_format(s, fmt)
            if iso:
                try:
                    parsed.append(datetime.strptime(iso, "%Y-%m-%d"))
                except ValueError:
                    pass
        if not parsed:
            continue
        min_d, max_d = min(parsed), max(parsed)
        range_days = (max_d - min_d).days

        # Penalty if dates fall outside [start, end]
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
    _profile: Optional[Dict[str, float]] = None,
    _attempt_log: Optional[List[Dict]] = None,
) -> Tuple[Optional[Dict], Optional[str], int]:
    """
    Fetch tournament report from FIDE website.

    Returns:
        Tuple of (report_dict, error_string). If successful, report_dict is not None.
        If error, error_string contains the error message.
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
                if _profile is not None:
                    _profile.setdefault("_attempt_times", []).append(elapsed)
            fetch_s = time.perf_counter() - t0

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

            t1 = time.perf_counter()
            soup = BeautifulSoup(response.content, "html.parser")

            # Find the main results table
            table = soup.find("table", class_="calc_table")
            if not table:
                if _profile is not None:
                    _profile["fetch_s"] = fetch_s
                    _profile["parse_s"] = time.perf_counter() - t1
                return None, "no data found", len(attempt_times)

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
                        player_rating = cells[5].get_text(strip=True)
                        player_total = cells[6].get_text(strip=True)

                        # Try to parse rating and total as numbers
                        try:
                            player_rating_int = (
                                int(player_rating) if player_rating else 0
                            )
                        except ValueError:
                            player_rating_int = 0

                        try:
                            player_total_float = (
                                float(player_total) if player_total else 0.0
                            )
                        except ValueError:
                            player_total_float = 0.0

                        player = {
                            "id": player_id,
                            "name": player_name,
                            "country": player_country,
                            "rating": player_rating_int,
                            "total": player_total_float,
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
                                    opp_fed = round_cells[2].get_text(strip=True)
                                    title = round_cells[3].get_text(strip=True)
                                    wtitle = round_cells[4].get_text(strip=True)
                                    opp_rating_text = round_cells[5].get_text(
                                        strip=True
                                    )
                                    score_text = round_cells[6].get_text(strip=True)

                                    # Extract color from opponent name cell
                                    color = extract_color_from_cell(round_cells[1])

                                    # Extract opponent FIDE ID from href (e.g. #65 -> lookup anchor 65)
                                    anchor = extract_href_anchor_from_cell(
                                        round_cells[1]
                                    )
                                    opp_id = (
                                        anchor_to_id.get(anchor, "") if anchor else ""
                                    )

                                    # Parse opponent rating
                                    try:
                                        opp_rating = (
                                            int(opp_rating_text)
                                            if opp_rating_text
                                            else 0
                                        )
                                    except ValueError:
                                        opp_rating = 0

                                    # Parse score
                                    score = parse_score(score_text)
                                    forfeit = extract_forfeit_indicator(score_text)

                                    # Skip byes: when opp_name is empty AND not a forfeit, the game didn't happen
                                    # Forfeits have empty opp_name but forfeit indicator in score
                                    is_forfeit = bool(forfeit)
                                    if opp_name or is_forfeit:
                                        round_data = {
                                            "round": round_num,
                                            "date": round_date,
                                            "opp_name": opp_name,
                                            "opp_id": opp_id,
                                            "color": color,
                                            "opp_fed": opp_fed,
                                            "title": title,
                                            "wtitle": wtitle,
                                            "opp_rating": opp_rating,
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
                if _profile is not None:
                    _profile["fetch_s"] = fetch_s
                    _profile["parse_s"] = time.perf_counter() - t1
                return None, "no players found", len(attempt_times)

            parse_s = time.perf_counter() - t1
            if _profile is not None:
                _profile["fetch_s"] = fetch_s
                _profile["parse_s"] = parse_s
            return (
                {"tournament_code": tournament_code, "players": players},
                None,
                len(attempt_times),
            )

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
            return None, last_error, len(attempt_times)
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
            return None, last_error, len(attempt_times)
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

    return None, f"max retries exceeded: {last_error}", len(attempt_times)


def flatten_result(result: Dict) -> List[Dict]:
    """
    Flatten a result dictionary for Parquet storage.
    Creates one row per player-round combination.
    """
    flattened = []
    tournament_code = result.get("tournament_code", "")
    success = result.get("success", False)
    error = result.get("error", "")

    if not success:
        # For failed results, create a single row with error
        flattened.append(
            {
                "tournament_code": tournament_code,
                "success": False,
                "error": error,
                "player_id": "",
                "player_name": "",
                "player_country": "",
                "player_rating": 0,
                "player_total": 0.0,
                "round": None,
                "round_date": "",
                "opp_name": "",
                "opp_id": "",
                "color": "",
                "opp_fed": "",
                "title": "",
                "wtitle": "",
                "opp_rating": 0,
                "score": None,
                "forfeit": "",
            }
        )
        return flattened

    players = result.get("players", [])
    for player in players:
        player_id = player.get("id", "")
        player_name = player.get("name", "")
        player_country = player.get("country", "")
        player_rating = player.get("rating", 0)
        player_total = player.get("total", 0.0)

        rounds = player.get("rounds", [])
        if not rounds:
            # If player has no rounds, create one row with player info only
            flattened.append(
                {
                    "tournament_code": tournament_code,
                    "success": True,
                    "error": "",
                    "player_id": player_id,
                    "player_name": player_name,
                    "player_country": player_country,
                    "player_rating": player_rating,
                    "player_total": player_total,
                    "round": None,
                    "round_date": "",
                    "opp_name": "",
                    "opp_id": "",
                    "color": "",
                    "opp_fed": "",
                    "title": "",
                    "wtitle": "",
                    "opp_rating": 0,
                    "score": None,
                    "forfeit": "",
                }
            )
        else:
            for round_data in rounds:
                flattened.append(
                    {
                        "tournament_code": tournament_code,
                        "success": True,
                        "error": "",
                        "player_id": player_id,
                        "player_name": player_name,
                        "player_country": player_country,
                        "player_rating": player_rating,
                        "player_total": player_total,
                        "round": round_data.get("round"),
                        "round_date": round_data.get("date", ""),
                        "opp_name": round_data.get("opp_name", ""),
                        "opp_id": round_data.get("opp_id", ""),
                        "color": round_data.get("color", ""),
                        "opp_fed": round_data.get("opp_fed", ""),
                        "title": round_data.get("title", ""),
                        "wtitle": round_data.get("wtitle", ""),
                        "opp_rating": round_data.get("opp_rating", 0),
                        "score": round_data.get("score"),
                        "forfeit": round_data.get("forfeit", ""),
                    }
                )

    return flattened


def flatten_to_games(
    flattened: List[Dict],
    tournament_code: str = "",
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> List[Dict]:
    """
    Convert player-round records to game-centric rows (one per game, deduplicated).
    Uses the white player's record as canonical; each game appears once.
    details_map: event_code -> (start_date_iso, end_date_iso) from tournament details.
    """
    # Step 1: collect round date strings and infer format
    date_strs = list(
        {
            row.get("round_date", "")
            for row in flattened
            if row.get("round_date") and row.get("success")
        }
    )
    start_iso, end_iso = None, None
    if details_map and tournament_code:
        start_iso, end_iso = details_map.get(tournament_code, (None, None))
    date_format = infer_date_format(date_strs, start_iso=start_iso, end_iso=end_iso)

    games = []
    seen: set = set()  # (tournament_code, round, white_id, black_id)

    for row in flattened:
        if not row.get("success") or row.get("round") is None:
            continue
        if not row.get("player_id") or not row.get("opp_id"):
            continue  # Skip byes (no opponent)
        tc = row.get("tournament_code", tournament_code)
        rnd = row["round"]
        color = (row.get("color") or "").strip().lower()
        forfeit = (row.get("forfeit") or "").strip()

        if color == "white":
            white_id = row["player_id"]
            black_id = row["opp_id"]
        elif color == "black":
            white_id = row["opp_id"]
            black_id = row["player_id"]
        else:
            continue  # Skip if color unknown

        key = (tc, rnd, white_id, black_id)
        if key in seen:
            continue
        seen.add(key)

        # White player's score: 1 win, 0.5 draw, 0 loss. For forfeit: + = that player won
        score = row.get("score")
        if forfeit:
            if color == "white":
                white_score = 1.0 if forfeit == "+" else 0.0
            else:
                white_score = (
                    0.0 if forfeit == "+" else 1.0
                )  # Black's forfeit- = white won
        elif score is not None:
            white_score = float(score) if color == "white" else 1.0 - float(score)
        else:
            continue

        date_str = row.get("round_date", "")
        date_iso = (
            parse_date_to_iso(date_str, date_format=date_format) if date_str else ""
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


def results_to_dataframe(results: List[Dict]) -> pd.DataFrame:
    """Convert results list to pandas DataFrame (player-round rows)."""
    all_flattened = []
    for result in results:
        all_flattened.extend(flatten_result(result))
    return pd.DataFrame(all_flattened)


def results_to_games_dataframe(
    results: List[Dict],
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> pd.DataFrame:
    """Convert results list to games DataFrame (one row per game, deduplicated)."""
    all_games = []
    for result in results:
        tc = result.get("tournament_code", "")
        flattened = flatten_result(result)
        games = flatten_to_games(flattened, tournament_code=tc, details_map=details_map)
        all_games.extend(games)
    return pd.DataFrame(all_games)


def save_results_parquet(results: List[Dict], parquet_path: str):
    """Save results as Parquet file (player-round rows, legacy format)."""
    try:
        df = results_to_dataframe(results)
        dirname = os.path.dirname(parquet_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"Saved {len(results)} tournament records to {parquet_path}")
        logger.info(f"  Total rows (player-rounds): {len(df)}")
    except Exception as e:
        logger.error(f"Parquet save failed: {e}")


def save_games_parquet(
    results: List[Dict],
    parquet_path: str,
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Save games as Parquet file (one row per game, main output format)."""
    try:
        df = results_to_games_dataframe(results, details_map=details_map)
        dirname = os.path.dirname(parquet_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"Saved {len(results)} tournament(s) to {parquet_path}")
        logger.info(f"  Total games: {len(df)}")
    except Exception as e:
        logger.error(f"Games Parquet save failed: {e}")


def save_verbose_json_sample(
    results: List[Dict],
    json_path: str,
    sample_size: int = 100,
):
    """
    Save a sample of the verbose flattened player-round format to JSON.
    Validates that scraping captures all fields (player_id, opp_name, title, wtitle, etc.).
    """
    try:
        all_flattened = []
        for result in results:
            all_flattened.extend(flatten_result(result))
        if not all_flattened:
            logger.warning("No flattened rows, skipping verbose JSON sample")
            return
        n = min(sample_size, len(all_flattened))
        rng = random.Random(42)
        sample = (
            rng.sample(all_flattened, n) if n < len(all_flattened) else all_flattened
        )
        dirname = os.path.dirname(json_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved verbose sample of {n} player-round rows to {json_path}")
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
    output_path: str,
    results: List[Dict],
    checkpoint_path: Optional[str] = None,
    details_map: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Save checkpoint file as Parquet."""
    if not output_path or not checkpoint_path:
        return

    try:
        # Convert .json checkpoint path to .parquet
        if checkpoint_path.endswith(".checkpoint"):
            parquet_checkpoint = checkpoint_path.replace(
                ".checkpoint", ".parquet.checkpoint"
            )
        elif checkpoint_path.endswith(".json.checkpoint"):
            parquet_checkpoint = checkpoint_path.replace(
                ".json.checkpoint", ".parquet.checkpoint"
            )
        else:
            parquet_checkpoint = checkpoint_path + ".parquet"

        save_games_parquet(results, parquet_checkpoint, details_map=details_map)
    except Exception as e:
        logger.error(f"Checkpoint save failed: {e}")


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
        help="Path to tournament_details parquet (for date inference when using --input)",
    )
    parser.add_argument(
        "--no-samples",
        action="store_true",
        help="Skip JSON and CSV sample outputs (parquet only)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Print timing breakdown (fetch vs parse) at the end",
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

    args = parser.parse_args()

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
                for _, row in df.iterrows():
                    ec = row.get("event_code")
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
        # Read from tournament_details (preferred) or fall back to tournament_ids
        details_path = os.path.join(
            args.data_dir, "tournament_details", f"{args.year}_{args.month:02d}.parquet"
        )
        ids_path = os.path.join(
            args.data_dir, "tournament_ids", f"{args.year}_{args.month:02d}"
        )
        if os.path.exists(details_path):
            # Extract event codes and build details_map for date inference
            try:
                df = pd.read_parquet(details_path)
                if "event_code" in df.columns:
                    success_df = df[df["success"] == True]
                    tournament_codes = (
                        success_df["event_code"].dropna().astype(str).tolist()
                    )
                    tournament_codes = [code for code in tournament_codes if code]
                    for _, row in success_df.iterrows():
                        ec = row.get("event_code")
                        if pd.notna(ec) and str(ec):
                            sd = parse_details_date_to_iso(
                                str(row.get("start_date", ""))
                            )
                            ed = parse_details_date_to_iso(str(row.get("end_date", "")))
                            details_map[str(ec)] = (sd, ed)
                    if details_map:
                        logger.info(
                            f"Loaded date bounds for {len(details_map)} tournaments from details"
                        )
                else:
                    logger.error(
                        f"Error: event_code column not found in {details_path}"
                    )
                    sys.exit(1)
            except Exception as e:
                logger.error(f"Error reading tournament details: {e}")
                sys.exit(1)
        elif os.path.exists(ids_path):
            # Fallback: use tournament_ids when details not yet run (for quick testing)
            try:
                tournament_codes = read_tournament_codes(ids_path)
                logger.info(
                    f"Using tournament_ids from {ids_path} (details not run - date inference from round dates only)"
                )
            except Exception as e:
                logger.error(f"Error reading tournament IDs: {e}")
                sys.exit(1)
        else:
            logger.error(f"Error: tournament details file not found: {details_path}")
            logger.error(
                "Run get_tournament_details.py first, or get_tournaments.py (faster) for --year/--month testing"
            )
            logger.error("Alternatively use --input with a codes file")
            sys.exit(1)
    else:
        logger.error("Error: specify --input or --year and --month")
        sys.exit(1)

    if not tournament_codes:
        logger.error("No tournament codes found")
        sys.exit(1)

    if args.limit > 0:
        tournament_codes = tournament_codes[: args.limit]
        logger.info(f"Limited to first {len(tournament_codes)} tournaments")

    # Determine output paths
    parquet_path = None
    json_path = None
    csv_path = None
    if args.output:
        # If user specifies output, use it as base for parquet and samples
        if args.output.endswith(".json"):
            parquet_path = args.output.replace(".json", ".parquet")
            json_path = args.output.replace(".json", "_sample.json")
            csv_path = args.output.replace(".json", "_sample.csv")
        elif args.output.endswith(".parquet"):
            parquet_path = args.output
            json_path = args.output.replace(".parquet", "_sample.json")
            csv_path = args.output.replace(".parquet", "_sample.csv")
        else:
            parquet_path = args.output + ".parquet"
            json_path = args.output + "_sample.json"
            csv_path = args.output + "_sample.csv"
    elif args.year > 0 and args.month > 0:
        base_path = os.path.join(
            args.data_dir, "tournament_reports", f"{args.year}_{args.month:02d}"
        )
        parquet_path = base_path + ".parquet"
        json_path = base_path + "_sample.json"
        csv_path = base_path + "_sample.csv"

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
    profile_samples: List[Dict[str, float]] = [] if args.profile else []
    attempt_log: List[Dict] = [] if args.verbose_errors else []
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
        last_cycle_start = None

        for tournament_code in current_tournaments:
            now = time.perf_counter()
            if args.profile and last_cycle_start is not None and profile_samples:
                prev = profile_samples[-1]
                cycle_s = now - last_cycle_start
                prev["cycle_s"] = cycle_s
                prev["other_s"] = (
                    cycle_s
                    - prev.get("wait_s", 0)
                    - prev.get("fetch_s", 0)
                    - prev.get("parse_s", 0)
                )
            last_cycle_start = now

            profile = {} if args.profile else None
            report, error, num_attempts = fetch_tournament_report(
                tournament_code,
                session,
                _profile=profile,
                _attempt_log=attempt_log if args.verbose_errors else None,
            )
            if args.verbose_errors:
                attempt_counts.append((tournament_code, num_attempts))
            if args.profile and profile:
                profile.setdefault("wait_s", 0)
                profile_samples.append(profile)

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
                if args.checkpoint > 0 and success_count % args.checkpoint == 0:
                    checkpoint_path = (
                        parquet_path + ".checkpoint" if parquet_path else None
                    )
                    logger.info(f"Saving checkpoint at {success_count} successful...")
                    save_checkpoint(
                        parquet_path,
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
    if parquet_path:
        # Save games as Parquet (one row per game, main output)
        save_games_parquet(all_results, parquet_path, details_map=details_map)

        # Save verbose JSON sample (from raw results) and CSV sample (from parquet)
        if not args.no_samples:
            if json_path:
                save_verbose_json_sample(all_results, json_path, sample_size=100)
            if csv_path:
                save_csv_sample_from_parquet(parquet_path, csv_path, sample_size=100)
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
    if parquet_path:
        logger.info(f"  Parquet output: {parquet_path}")
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

    # Profile timing breakdown
    if profile_samples:
        n = len(profile_samples)
        samples_with_cycle = [p for p in profile_samples if "cycle_s" in p]
        n_cycle = len(samples_with_cycle)
        wait_avg = sum(p.get("wait_s", 0) for p in profile_samples) / n
        fetch_avg = sum(p.get("fetch_s", 0) for p in profile_samples) / n
        parse_avg = sum(p.get("parse_s", 0) for p in profile_samples) / n
        measured_avg = wait_avg + fetch_avg + parse_avg
        logger.info("\nProfile (avg per tournament, n=%d):", n)
        logger.info("  Rate-limit wait: %.3fs", wait_avg)
        logger.info("  HTTP fetch:      %.3fs", fetch_avg)
        logger.info("  HTML parse:      %.3fs", parse_avg)
        n_retries = sum(
            1
            for p in profile_samples
            if p.get("_attempt_times") and len(p["_attempt_times"]) > 1
        )
        if n_retries > 0:
            sum_all = sum(sum(p.get("_attempt_times", [0])) for p in profile_samples)
            sum_fetch = sum(p.get("fetch_s", 0) for p in profile_samples)
            logger.info(
                "  Retries: %d/%d had >1 HTTP attempt (extra: %.2fs)",
                n_retries,
                n,
                sum_all - sum_fetch,
            )
        logger.info("  Measured (wait+fetch+parse): %.3fs", measured_avg)
        if n_cycle > 0:
            cycle_avg = sum(p["cycle_s"] for p in samples_with_cycle) / n_cycle
            other_avg = sum(p["other_s"] for p in samples_with_cycle) / n_cycle
            other_min = min(p["other_s"] for p in samples_with_cycle)
            other_max = max(p["other_s"] for p in samples_with_cycle)
            logger.info("  ---")
            logger.info("  Cycle (wall time per item, n=%d): %.3fs", n_cycle, cycle_avg)
            logger.info(
                "  Other (cycle - measured): %.3fs (min=%.3fs max=%.3fs)",
                other_avg,
                other_min,
                other_max,
            )
            logger.info(
                "  Check: measured + other = %.3fs (should ≈ cycle)",
                measured_avg + other_avg,
            )


if __name__ == "__main__":
    main()

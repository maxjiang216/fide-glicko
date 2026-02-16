#!/usr/bin/env python3
"""
Exploratory script to examine the structure of FIDE tournament report pages.
"""

import argparse
import sys
import time

import requests
from bs4 import BeautifulSoup


def fetch_tournament_report(tournament_code: str) -> tuple[BeautifulSoup | None, str | None]:
    """Fetch tournament report page and return parsed HTML."""
    url = f"https://ratings.fide.com/tournament_src_report.phtml?code={tournament_code}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Cache-Control": "max-age=0",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        
        soup = BeautifulSoup(response.content, "html.parser")
        return soup, None
    except Exception as e:
        return None, str(e)


def analyze_structure(soup: BeautifulSoup, tournament_code: str = ""):
    """Analyze the HTML structure of the tournament report."""
    print("=" * 80)
    print("TOURNAMENT REPORT STRUCTURE ANALYSIS")
    print("=" * 80)
    
    # Find all tables
    tables = soup.find_all("table")
    print(f"\nFound {len(tables)} table(s)")
    
    for i, table in enumerate(tables):
        print(f"\n--- Table {i+1} ---")
        print(f"Classes: {table.get('class', [])}")
        print(f"ID: {table.get('id', 'None')}")
        
        # Check for headers
        headers = table.find_all("th")
        if headers:
            print(f"Headers ({len(headers)}):")
            for j, th in enumerate(headers):
                print(f"  {j+1}. {th.get_text(strip=True)}")
        
        # Check first few rows
        rows = table.find_all("tr")
        print(f"Rows: {len(rows)}")
        
        if rows:
            print("\nFirst 3 rows:")
            for row_idx, row in enumerate(rows[:3]):
                cells = row.find_all(["td", "th"])
                print(f"  Row {row_idx + 1} ({len(cells)} cells):")
                for cell_idx, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    # Check for color indicators
                    white_note = cell.find("span", class_="white_note")
                    black_note = cell.find("span", class_="black_note")
                    color = ""
                    if white_note:
                        color = " [WHITE]"
                    elif black_note:
                        color = " [BLACK]"
                    
                    # Check for links
                    links = cell.find_all("a")
                    link_info = ""
                    if links:
                        link_info = f" [Links: {len(links)}]"
                    
                    print(f"    Cell {cell_idx + 1}: {text[:50]}{color}{link_info}")
    
    # Look for specific patterns
    print("\n" + "=" * 80)
    print("SPECIFIC PATTERN SEARCH")
    print("=" * 80)
    
    # Look for white_note and black_note spans
    white_notes = soup.find_all("span", class_="white_note")
    black_notes = soup.find_all("span", class_="black_note")
    print(f"\nWhite notes: {len(white_notes)}")
    print(f"Black notes: {len(black_notes)}")
    
    # Look for player links (usually in format /profile/XXXXX)
    player_links = soup.find_all("a", href=lambda x: x and "/profile/" in x)
    print(f"\nPlayer profile links: {len(player_links)}")
    if player_links:
        print("Sample player links:")
        for link in player_links[:5]:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            print(f"  {text} -> {href}")
    
    # Look for round information
    print("\n" + "=" * 80)
    print("ROUND INFORMATION")
    print("=" * 80)
    
    # Try to find round headers or round numbers
    all_text = soup.get_text()
    if "Round" in all_text or "round" in all_text:
        print("Found 'Round' text in page")
    
    # Save HTML to file for manual inspection
    print("\n" + "=" * 80)
    print("SAVING HTML FOR MANUAL INSPECTION")
    print("=" * 80)
    output_file = f"tournament_report_{tournament_code}.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(soup.prettify())
    print(f"Saved HTML to: {output_file}")


def extract_sample_data(soup: BeautifulSoup, tournament_code: str):
    """Try to extract sample player data from the report."""
    print("\n" + "=" * 80)
    print("SAMPLE DATA EXTRACTION")
    print("=" * 80)
    
    # Find the main results table
    tables = soup.find_all("table")
    
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        
        # Check if this looks like a results table (has multiple columns)
        first_row = rows[0]
        cells = first_row.find_all(["td", "th"])
        if len(cells) < 5:
            continue
        
        print(f"\nFound potential results table with {len(rows)} rows and {len(cells)} columns")
        
        # Try to extract first few players
        print("\nFirst 3 players (if table structure matches):")
        for row_idx, row in enumerate(rows[1:4]):  # Skip header, get first 3 data rows
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            
            player_data = {}
            
            # Try to extract player ID from link
            player_link = row.find("a", href=lambda x: x and "/profile/" in x)
            if player_link:
                href = player_link.get("href", "")
                # Extract ID from href like /profile/123456
                if "/profile/" in href:
                    player_id = href.split("/profile/")[1].split("?")[0].split("/")[0]
                    player_data["id"] = player_id
                player_data["name"] = player_link.get_text(strip=True)
            
            # Extract all cell texts
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            player_data["cells"] = cell_texts
            
            # Check for color indicators in cells
            for cell_idx, cell in enumerate(cells):
                white_note = cell.find("span", class_="white_note")
                black_note = cell.find("span", class_="black_note")
                if white_note:
                    player_data[f"color_cell_{cell_idx}"] = "white"
                elif black_note:
                    player_data[f"color_cell_{cell_idx}"] = "black"
            
            print(f"  Player {row_idx + 1}: {player_data}")


def main():
    parser = argparse.ArgumentParser(
        description="Explore FIDE tournament report structure"
    )
    parser.add_argument(
        "tournament_code",
        type=str,
        help="Tournament code (event code) to examine"
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Try to extract sample data"
    )
    
    args = parser.parse_args()
    
    print(f"Fetching tournament report for code: {args.tournament_code}")
    soup, error = fetch_tournament_report(args.tournament_code)
    
    if error:
        print(f"Error: {error}")
        sys.exit(1)
    
    if not soup:
        print("Error: Failed to parse HTML")
        sys.exit(1)
    
    analyze_structure(soup, args.tournament_code)
    
    if args.extract:
        extract_sample_data(soup, args.tournament_code)


if __name__ == "__main__":
    main()


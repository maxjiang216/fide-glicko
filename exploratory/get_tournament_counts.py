import csv
import time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from collections import defaultdict
import re

BASE_URL = "https://ratings.fide.com/rated_tournaments.phtml"


def get_tournament_counts_for_federation(country_code: str):
    """
    Get tournament counts for each month from the dropdown menu.
    Uses Playwright to ensure the page is fully loaded.
    
    Args:
        country_code: 3-letter country code (e.g., 'FRA')
    
    Returns:
        List of dicts with 'period' (YYYY-MM-DD), 'year', 'month', and 'count'
    """
    url = f"{BASE_URL}?country={country_code}"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            
            # Wait for the archive dropdown to appear
            try:
                page.wait_for_selector("#archive", timeout=10000)
            except PlaywrightTimeoutError:
                browser.close()
                return []
            
            # Get the page HTML after it's fully loaded
            html = page.content()
            browser.close()
        except Exception as e:
            browser.close()
            return []
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find the archive dropdown
    archive_select = soup.find("select", id="archive")
    if not archive_select:
        return []
    
    months = []
    for option in archive_select.find_all("option"):
        value = option.get("value")
        text = option.text.strip()
        
        # Skip "current" option
        if value == "current" or not value:
            continue
        
        # Parse the text to extract count: "August 2025 (113)"
        match = re.search(r'\((\d+)\)', text)
        count = int(match.group(1)) if match else 0
        
        # Parse the period date (YYYY-MM-DD)
        try:
            year, month, day = map(int, value.split('-'))
            months.append({
                'period': value,
                'year': year,
                'month': month,
                'count': count
            })
        except ValueError:
            continue
    
    return months


def format_time(seconds):
    """Format seconds into a readable time string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def main():
    # Read federations from CSV
    data_dir = Path(__file__).parent / "data"
    federations_file = data_dir / "federations.csv"
    
    if not federations_file.exists():
        raise FileNotFoundError(f"Federations file not found: {federations_file}. Run get_federations.py first.")
    
    print(f"Reading federations from {federations_file}...")
    federations = []
    with open(federations_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            federations.append({
                'code': row['code'],
                'name': row['name']
            })
    
    print(f"Found {len(federations)} federations\n")
    
    # Aggregate data structures
    country_month_data = []  # List of (country, year, month, count)
    tournaments_by_month = defaultdict(int)  # period -> total count
    tournaments_by_year = defaultdict(int)   # year -> total count
    months_by_year = defaultdict(set)       # year -> set of months
    
    print("Processing federations...")
    print("=" * 80)
    
    start_time = time.time()
    processed = 0
    failed = 0
    
    for idx, fed in enumerate(federations, 1):
        country_code = fed['code']
        country_name = fed['name']
        
        # Calculate progress
        elapsed = time.time() - start_time
        if processed > 0:
            avg_time_per_country = elapsed / processed
            remaining = len(federations) - processed
            estimated_remaining = avg_time_per_country * remaining
        else:
            estimated_remaining = 0
        
        print(f"[{idx}/{len(federations)}] {country_code} ({country_name}) | "
              f"Completed: {processed} | "
              f"Time left: ~{format_time(estimated_remaining)}")
        
        try:
            months = get_tournament_counts_for_federation(country_code)
            
            if months:
                for month_info in months:
                    period = month_info['period']
                    year = month_info['year']
                    month = month_info['month']
                    count = month_info['count']
                    
                    # Store country-specific data
                    country_month_data.append({
                        'country': country_code,
                        'year': year,
                        'month': month,
                        'num_tournaments': count
                    })
                    
                    # Aggregate global data
                    tournaments_by_month[period] += count
                    tournaments_by_year[year] += count
                    months_by_year[year].add(month)
                
                processed += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ Error: {e}")
    
    elapsed_total = time.time() - start_time
    print(f"\n{'=' * 80}")
    print(f"Completed: {processed} successful, {failed} failed")
    print(f"Total time: {format_time(elapsed_total)}")
    print(f"{'=' * 80}\n")
    
    # Save CSVs
    print("Saving results to CSV files...")
    
    # CSV 1: country, year, month, num_tournaments
    country_month_file = data_dir / "tournaments_by_country_month.csv"
    with open(country_month_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['country', 'year', 'month', 'num_tournaments'])
        for row in sorted(country_month_data, key=lambda x: (x['country'], x['year'], x['month'])):
            writer.writerow([row['country'], row['year'], row['month'], row['num_tournaments']])
    print(f"  Saved: {country_month_file}")
    
    # CSV 2: year, month, total_tournaments (globally)
    global_month_file = data_dir / "tournaments_by_month_global.csv"
    sorted_periods = sorted(tournaments_by_month.keys())
    with open(global_month_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['year', 'month', 'total_tournaments'])
        for period in sorted_periods:
            year, month, day = period.split('-')
            writer.writerow([year, month, tournaments_by_month[period]])
    print(f"  Saved: {global_month_file}")
    
    # CSV 3: year, total_tournaments, avg_monthly_tournaments
    global_year_file = data_dir / "tournaments_by_year_global.csv"
    sorted_years = sorted(tournaments_by_year.keys())
    with open(global_year_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['year', 'total_tournaments', 'avg_monthly_tournaments'])
        for year in sorted_years:
            total = tournaments_by_year[year]
            num_months = len(months_by_year[year])
            avg = total / num_months if num_months > 0 else 0
            writer.writerow([year, total, f"{avg:.2f}"])
    print(f"  Saved: {global_year_file}")
    
    print("\nDone!")


if __name__ == "__main__":
    main()


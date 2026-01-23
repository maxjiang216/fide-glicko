import csv
import time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from collections import defaultdict
import re
import asyncio

BASE_URL = "https://ratings.fide.com/rated_tournaments.phtml"


async def get_tournament_counts_for_federation_async(page, country_code: str):
    """
    Get tournament counts for each month from the dropdown menu.
    Uses an existing Playwright page (reused across requests).
    
    Args:
        page: Playwright page object (reused)
        country_code: 3-letter country code (e.g., 'FRA')
    
    Returns:
        List of dicts with 'period' (YYYY-MM-DD), 'year', 'month', and 'count'
    """
    url = f"{BASE_URL}?country={country_code}"
    
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        
        # Wait for the archive dropdown to appear
        try:
            await page.wait_for_selector("#archive", timeout=10000)
        except PlaywrightTimeoutError:
            return []
        
        # Get the page HTML after it's fully loaded
        html = await page.content()
    except Exception as e:
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


async def process_federations_async(federations, max_concurrent=10):
    """
    Process federations concurrently using async Playwright.
    
    Args:
        federations: List of federation dicts with 'code' and 'name'
        max_concurrent: Maximum number of concurrent browser contexts
    
    Returns:
        Tuple of (country_month_data, tournaments_by_month, tournaments_by_year, months_by_year)
    """
    country_month_data = []
    tournaments_by_month = defaultdict(int)
    tournaments_by_year = defaultdict(int)
    months_by_year = defaultdict(set)
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_one(fed, idx, total):
        async with semaphore:
            country_code = fed['code']
            country_name = fed['name']
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()
                
                try:
                    months = await get_tournament_counts_for_federation_async(page, country_code)
                    
                    if months:
                        for month_info in months:
                            period = month_info['period']
                            year = month_info['year']
                            month = month_info['month']
                            count = month_info['count']
                            
                            country_month_data.append({
                                'country': country_code,
                                'year': year,
                                'month': month,
                                'num_tournaments': count
                            })
                            
                            tournaments_by_month[period] += count
                            tournaments_by_year[year] += count
                            months_by_year[year].add(month)
                        
                        return (True, idx, total, country_code, country_name, len(months))
                    else:
                        return (False, idx, total, country_code, country_name, 0)
                except Exception as e:
                    return (False, idx, total, country_code, country_name, f"Error: {e}")
                finally:
                    await browser.close()
    
    # Create tasks for all federations
    tasks = [process_one(fed, idx, len(federations)) for idx, fed in enumerate(federations, 1)]
    
    # Process with progress tracking
    processed = 0
    failed = 0
    start_time = time.time()
    
    for coro in asyncio.as_completed(tasks):
        success, idx, total, code, name, result = await coro
        processed += 1 if success else 0
        failed += 0 if success else 1
        
        elapsed = time.time() - start_time
        if processed > 0:
            avg_time = elapsed / processed
            remaining = len(federations) - processed
            estimated = avg_time * remaining
        else:
            estimated = 0
        
        status = "✓" if success else "✗"
        info = f"{result} months" if isinstance(result, int) else result
        print(f"[{idx}/{total}] {status} {code} ({name}) | "
              f"Completed: {processed} | "
              f"Time left: ~{format_time(estimated)} | {info}")
    
    return country_month_data, tournaments_by_month, tournaments_by_year, months_by_year


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
    print("Processing federations concurrently (async)...")
    print("=" * 80)
    
    start_time = time.time()
    
    # Process with async (10 concurrent by default, adjust based on your system)
    country_month_data, tournaments_by_month, tournaments_by_year, months_by_year = asyncio.run(
        process_federations_async(federations, max_concurrent=10)
    )
    
    elapsed_total = time.time() - start_time
    processed = len([d for d in country_month_data if d])
    failed = len(federations) - processed
    
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


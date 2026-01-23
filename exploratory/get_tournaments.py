import requests
from pathlib import Path
import json
import re
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from get_federations import get_federations

BASE_URL = "https://ratings.fide.com/rated_tournaments.phtml"
AJAX_URL = "https://ratings.fide.com/a_tournaments.php"


def fetch_tournament_page(country_code: str, year: int, month: int):
    """
    Fetch the tournament page for a given country and month.
    
    Args:
        country_code: 3-letter country code (e.g., 'FRA')
        year: Year (e.g., 2025)
        month: Month (1-12)
    
    Returns:
        Response text (HTML content)
    """
    period = f"{year}-{month:02d}-01"
    url = f"{BASE_URL}?country={country_code}&period={period}"
    
    print(f"Fetching page: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    return response.text


def get_available_months_for_federation(country_code: str):
    """
    Get available months and expected tournament counts for a federation.
    Uses Playwright to ensure the page is fully loaded.
    
    Args:
        country_code: 3-letter country code (e.g., 'FRA')
    
    Returns:
        List of dicts with 'period' (YYYY-MM-DD), 'year', 'month', and 'expected_count'
    """
    url = f"{BASE_URL}?country={country_code}"
    
    print(f"  Fetching available months for {country_code}...")
    
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
                print(f"    Warning: Archive dropdown not found for {country_code}")
                browser.close()
                return []
            
            # Get the page HTML after it's fully loaded
            html = page.content()
            browser.close()
        except Exception as e:
            print(f"    Error loading page: {e}")
            browser.close()
            return []
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find the archive dropdown
    archive_select = soup.find("select", id="archive")
    if not archive_select:
        print(f"    Warning: Archive dropdown not found in HTML for {country_code}")
        return []
    
    months = []
    for option in archive_select.find_all("option"):
        value = option.get("value")
        text = option.text.strip()
        
        # Skip "current" option
        if value == "current" or not value:
            continue
        
        # Parse the text to extract expected count: "August 2025 (113)"
        match = re.search(r'\((\d+)\)', text)
        expected_count = int(match.group(1)) if match else None
        
        # Parse the period date (YYYY-MM-DD)
        try:
            year, month, day = map(int, value.split('-'))
            months.append({
                'period': value,
                'year': year,
                'month': month,
                'expected_count': expected_count
            })
        except ValueError:
            continue
    
    return months


def fetch_tournament_data_with_browser(country_code: str, year: int, month: int):
    """
    Fetch the tournament data using a headless browser to handle AJAX.
    
    Args:
        country_code: 3-letter country code (e.g., 'FRA')
        year: Year (e.g., 2025)
        month: Month (1-12)
    
    Returns:
        Tuple of (response text, response info dict, page HTML)
    """
    period = f"{year}-{month:02d}-01"
    url = f"{BASE_URL}?country={country_code}&period={period}"
    
    print(f"Loading page with browser: {url}")
    
    ajax_response_data = None
    ajax_response_info = None
    
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        # Set up response interception for the AJAX endpoint
        def handle_response(response):
            if AJAX_URL in response.url:
                nonlocal ajax_response_data, ajax_response_info
                try:
                    ajax_response_data = response.text()
                    ajax_response_info = {
                        'status': response.status,
                        'url': response.url,
                        'headers': dict(response.headers),
                        'content_type': response.headers.get('content-type', 'unknown')
                    }
                    print(f"  Captured AJAX response: {len(ajax_response_data)} bytes")
                except Exception as e:
                    print(f"  Error reading AJAX response: {e}")
        
        page.on("response", handle_response)
        
        # Navigate to the page and wait for network to be idle
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            
            # Wait a bit more for DataTables to load the data
            # Look for the table to be populated or wait for a specific timeout
            try:
                # Wait for the table to appear (even if empty)
                page.wait_for_selector("#main_table", timeout=10000)
                # Give DataTables time to make the AJAX call
                page.wait_for_timeout(3000)
            except PlaywrightTimeoutError:
                print("  Warning: Table selector not found, but continuing...")
            
            # Get the final page HTML
            page_html = page.content()
            
        except Exception as e:
            print(f"  Error loading page: {e}")
            page_html = page.content() if page else ""
        
        browser.close()
    
    info = ajax_response_info or {
        'status': 'no_response',
        'url': 'not_captured',
        'content_type': 'unknown'
    }
    
    return ajax_response_data or "", info, page_html


def main():
    # Get all federations
    print("Fetching federations list...")
    federations = get_federations()
    print(f"Found {len(federations)} federations")
    
    # Create output directory for saved data
    output_dir = Path("exploratory/tournament_html_samples")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # For testing, use a subset. Remove this to process all federations
    test_codes = ['FRA', 'ESP', 'ITA']  # France, Spain, Italy
    test_federations = [f for f in federations if f['code'] in test_codes]
    
    print(f"\nProcessing {len(test_federations)} federations")
    print("=" * 60)
    
    total_tournaments = 0
    total_verified = 0
    total_mismatches = 0
    
    for fed in test_federations:
        country_code = fed['code']
        country_name = fed['name']
        
        print(f"\n--- {country_code} ({country_name}) ---")
        
        try:
            # Get available months for this federation
            available_months = get_available_months_for_federation(country_code)
            print(f"  Found {len(available_months)} available months")
            
            if not available_months:
                print(f"  ⚠ No months found, skipping...")
                continue
            
            # Process each month
            for month_info in available_months:
                year = month_info['year']
                month = month_info['month']
                period = month_info['period']
                expected_count = month_info['expected_count']
                
                try:
                    # Fetch using headless browser
                    data, info, page_html = fetch_tournament_data_with_browser(country_code, year, month)
                    
                    # Save the AJAX data
                    data_filename = output_dir / f"{country_code}_{year}-{month:02d}_data.txt"
                    data_filename.write_text(data, encoding='utf-8')
                    
                    # Parse and verify tournament data
                    actual_count = 0
                    if len(data) > 0:
                        try:
                            tournament_json = json.loads(data)
                            tournaments = tournament_json.get('data', [])
                            actual_count = len(tournaments)
                            total_tournaments += actual_count
                        except json.JSONDecodeError:
                            pass
                    
                    # Verify count matches expected
                    status = "✓"
                    if expected_count is not None:
                        if actual_count == expected_count:
                            total_verified += 1
                            status = "✓"
                        else:
                            total_mismatches += 1
                            status = "✗"
                            print(f"  {status} {period}: Expected {expected_count}, got {actual_count}")
                        print(f"    {period}: {actual_count} tournaments (expected: {expected_count}) {status}")
                    else:
                        print(f"    {period}: {actual_count} tournaments (no expected count)")
                    
                except Exception as e:
                    print(f"  ✗ Error fetching {period}: {e}")
        
        except Exception as e:
            print(f"✗ {country_code} ({country_name}): Error - {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print(f"Summary:")
    print(f"  Total tournaments collected: {total_tournaments}")
    print(f"  Verified matches: {total_verified}")
    print(f"  Count mismatches: {total_mismatches}")
    print(f"  Data saved to: {output_dir.absolute()}")


if __name__ == "__main__":
    main()


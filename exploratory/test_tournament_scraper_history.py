#!/usr/bin/env python3
"""
Test tournament scraper for historical months to find when data becomes unavailable.

This script calls get_tournaments.py for all months from April 2002 to December 2025
to determine when the FIDE tournament data becomes unavailable or the API stops working.
"""
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Path to the scraper script (relative to repo root)
SCRAPER_SCRIPT = Path(__file__).parent.parent / "src" / "scraper" / "get_tournaments.py"


def test_month(year: int, month: int, quiet: bool = True) -> tuple[bool, str, int]:
    """
    Test scraping for a specific month.

    Args:
        year: Year to test.
        month: Month to test (1-12).
        quiet: Whether to suppress scraper output.

    Returns:
        Tuple of (success, message, tournament_count).
        success: True if scraping succeeded.
        message: Error message or success message.
        tournament_count: Number of tournaments found (0 if failed).
    """
    cmd = [
        sys.executable,
        str(SCRAPER_SCRIPT),
        "--year", str(year),
        "--month", str(month),
    ]
    
    if quiet:
        cmd.append("--quiet")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout per month
        )
        
        if result.returncode == 0:
            # Try to extract tournament count from output file
            tournament_count = 0
            output_file = Path(__file__).parent.parent / f"data/tournament_ids_{year}_{month:02d}"
            if output_file.exists():
                try:
                    content = output_file.read_text().strip()
                    if content:
                        tournament_count = len([line for line in content.split('\n') if line.strip()])
                except Exception:
                    pass
            
            # Also try to parse from stdout if file doesn't exist
            if tournament_count == 0:
                for line in result.stdout.split('\n'):
                    if "Unique tournaments:" in line:
                        try:
                            tournament_count = int(line.split("Unique tournaments:")[1].strip().split()[0])
                        except (ValueError, IndexError):
                            pass
            
            return (True, f"Success: {tournament_count} tournaments", tournament_count)
        else:
            error_msg = result.stderr.strip() or result.stdout.strip()
            if not error_msg:
                error_msg = f"Exit code {result.returncode}"
            return (False, error_msg, 0)
            
    except subprocess.TimeoutExpired:
        return (False, "Timeout after 2 minutes", 0)
    except Exception as e:
        return (False, f"Exception: {str(e)}", 0)


def main():
    """Test all months from April 2002 to December 2025."""
    start_year = 2002
    start_month = 4  # April 2002
    end_year = 2025
    end_month = 12  # December 2025
    
    print("=" * 80)
    print("Testing Tournament Scraper Historical Coverage")
    print(f"Testing months from {start_month:02d}/{start_year} to {end_month:02d}/{end_year}")
    print("=" * 80)
    print()
    
    results = []
    current_year = start_year
    current_month = start_month
    total_months = 0
    
    # Calculate total months
    y = start_year
    m = start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        total_months += 1
        m += 1
        if m > 12:
            m = 1
            y += 1
    
    print(f"Total months to test: {total_months}")
    print()
    
    start_time = time.time()
    last_success_year = None
    last_success_month = None
    first_failure_year = None
    first_failure_month = None
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        month_str = f"{current_year}-{current_month:02d}"
        print(f"Testing {month_str}... ", end="", flush=True)
        
        success, message, count = test_month(current_year, current_month, quiet=True)
        
        if success:
            results.append((current_year, current_month, True, message, count))
            last_success_year = current_year
            last_success_month = current_month
            print(f"✓ {count} tournaments")
        else:
            results.append((current_year, current_month, False, message, 0))
            if first_failure_year is None:
                first_failure_year = current_year
                first_failure_month = current_month
            print(f"✗ {message[:60]}")
        
        # Move to next month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
        
        # Small delay to avoid overwhelming the server
        time.sleep(0.5)
    
    elapsed = time.time() - start_time
    
    # Print summary
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    
    successful = sum(1 for r in results if r[2])
    failed = len(results) - successful
    total_tournaments = sum(r[4] for r in results if r[2])
    
    print(f"Total months tested: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total tournaments found: {total_tournaments:,}")
    print(f"Time taken: {elapsed/60:.1f} minutes")
    print()
    
    if last_success_year:
        print(f"Last successful month: {last_success_month:02d}/{last_success_year}")
    if first_failure_year:
        print(f"First failure month: {first_failure_month:02d}/{first_failure_year}")
    print()
    
    # Show failure details
    failures = [r for r in results if not r[2]]
    if failures:
        print("Failed months:")
        for year, month, _, msg, _ in failures[:20]:  # Show first 20 failures
            print(f"  {year}-{month:02d}: {msg[:70]}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more")
        print()
    
    # Show months with 0 tournaments (might indicate data unavailability)
    zero_tournaments = [r for r in results if r[2] and r[4] == 0]
    if zero_tournaments:
        print(f"Months with 0 tournaments (but API worked): {len(zero_tournaments)}")
        if len(zero_tournaments) <= 20:
            for year, month, _, _, _ in zero_tournaments:
                print(f"  {year}-{month:02d}")
        else:
            for year, month, _, _, _ in zero_tournaments[:10]:
                print(f"  {year}-{month:02d}")
            print(f"  ... and {len(zero_tournaments) - 10} more")
        print()
    
    # Write detailed results to file
    output_file = Path(__file__).parent / "data" / "tournament_scraper_history_test_results.txt"
    with open(output_file, 'w') as f:
        f.write("Tournament Scraper Historical Test Results\n")
        f.write("=" * 80 + "\n")
        f.write(f"Test date: {datetime.now().isoformat()}\n")
        f.write(f"Months tested: {start_month:02d}/{start_year} to {end_month:02d}/{end_year}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Summary:\n")
        f.write(f"  Total months: {len(results)}\n")
        f.write(f"  Successful: {successful}\n")
        f.write(f"  Failed: {failed}\n")
        f.write(f"  Total tournaments: {total_tournaments:,}\n")
        f.write(f"  Time taken: {elapsed/60:.1f} minutes\n\n")
        
        if last_success_year:
            f.write(f"Last successful: {last_success_month:02d}/{last_success_year}\n")
        if first_failure_year:
            f.write(f"First failure: {first_failure_month:02d}/{first_failure_year}\n")
        f.write("\n")
        
        f.write("Detailed Results:\n")
        f.write("-" * 80 + "\n")
        for year, month, success, msg, count in results:
            status = "✓" if success else "✗"
            f.write(f"{status} {year}-{month:02d}: {msg}")
            if success:
                f.write(f" ({count} tournaments)")
            f.write("\n")
    
    print(f"Detailed results saved to: {output_file}")
    print("=" * 80)
    
    return 0 if successful > 0 else 1


if __name__ == "__main__":
    sys.exit(main())


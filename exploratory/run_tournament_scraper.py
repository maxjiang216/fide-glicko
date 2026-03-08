#!/usr/bin/env python3
"""
Script to run the tournament scraping pipeline:
1. Fetch federations and save to CSV
2. Get tournament counts for each federation and save to CSVs
"""

import sys
import subprocess
from pathlib import Path

def main():
    print("=" * 80)
    print("FIDE Tournament Scraper")
    print("=" * 80)
    print()
    
    # Get paths
    base_dir = Path(__file__).parent.parent
    exploratory_dir = base_dir / "exploratory"
    
    # Step 1: Get federations
    print("STEP 1: Fetching federations...")
    print("-" * 80)
    result = subprocess.run(
        [sys.executable, str(exploratory_dir / "get_federations.py")],
        cwd=str(base_dir),
        capture_output=False
    )
    
    if result.returncode != 0:
        print("ERROR: Failed to fetch federations")
        sys.exit(1)
    
    print()
    
    # Step 2: Get tournament counts
    print("STEP 2: Getting tournament counts...")
    print("-" * 80)
    result = subprocess.run(
        [sys.executable, str(exploratory_dir / "get_tournament_counts.py")],
        cwd=str(base_dir),
        capture_output=False
    )
    
    if result.returncode != 0:
        print("ERROR: Failed to get tournament counts")
        sys.exit(1)
    
    print()
    print("=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()


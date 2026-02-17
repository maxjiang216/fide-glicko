#!/usr/bin/env python3
"""Quick test of tournament report scraper."""

import sys
import requests
from bs4 import BeautifulSoup

# Import the fetch function
sys.path.insert(0, 'src/scraper')
from get_tournament_reports import fetch_tournament_report

# Test with a known tournament code
tournament_code = "393912"
print(f"Testing tournament code: {tournament_code}")

session = requests.Session()
report, error, _ = fetch_tournament_report(tournament_code, session)

if error:
    print(f"Error: {error}")
    sys.exit(1)

print(f"\nSuccess! Found {len(report['players'])} players")
print(f"\nFirst player:")
player = report['players'][0]
print(f"  ID: {player['id']}")
print(f"  Name: {player['name']}")
print(f"  Country: {player['country']}")
print(f"  Rating: {player['rating']}")
print(f"  Total: {player['total']}")
print(f"  Rounds: {len(player['rounds'])}")

if player['rounds']:
    print(f"\nFirst round:")
    round_data = player['rounds'][0]
    for key, value in round_data.items():
        print(f"  {key}: {value}")

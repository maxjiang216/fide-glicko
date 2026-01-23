import requests
from bs4 import BeautifulSoup
from pathlib import Path
import csv

URL = "https://ratings.fide.com/rated_tournaments.phtml"

def get_federations():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    select = soup.find("select", id="select_country")
    if not select:
        raise RuntimeError("Country selector not found")

    federations = []

    for option in select.find_all("option"):
        value = option.get("value")
        name = option.text.strip()

        # Skip the placeholder option
        if value and value.lower() != "all":
            federations.append({
                "code": value,
                "name": name
            })

    return federations


if __name__ == "__main__":
    print("Fetching federations list...")
    federations = get_federations()
    print(f"Found {len(federations)} federations")
    
    # Save to CSV
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    output_file = data_dir / "federations.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name'])
        for fed in federations:
            writer.writerow([fed['code'], fed['name']])
    
    print(f"Saved {len(federations)} federations to {output_file}")

# FIDE Glicko Rating System

A Python implementation of the FIDE Glicko rating system for chess players.

## Overview

This project implements the Glicko rating system as used by FIDE (Fédération Internationale des Échecs) for calculating chess player ratings. The Glicko system is an improvement over the Elo rating system, incorporating rating deviation (RD) to better represent the uncertainty in a player's rating.

**Architecture:**
- **Python**: Exploratory work, prototyping, and web scraping
- **Rust**: Core rating computations (planned)

## Project Structure

- **`src/`**: Production-ready source code
  - **`src/scraper/`**: FIDE website scraping scripts (see [src/scraper/README.md](src/scraper/README.md) for detailed documentation)
- **`exploratory/`**: Experimental code, prototypes, and one-off analysis scripts
- **`data/`**: Scraped data and intermediate files
- **`scripts/`**: Utility scripts and automation tools

## Installation

We use `uv` for fast Python dependency management:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Or use uv to run scripts directly
uv run src/scraper/get_federations.py
```

Alternatively, use standard pip:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

## Usage

### Web Scraping

The project includes Python scripts for scraping tournament data from the FIDE website. For detailed documentation on all scraping scripts, command-line options, and usage examples, see:

**[src/scraper/README.md](src/scraper/README.md)**

**Quick Start:**

1. **Get federations list:**
   ```bash
   uv run src/scraper/get_federations.py
   ```

2. **Get player list** (FIDE Combined Rating List: id, name, fed, title, etc.):
   ```bash
   uv run src/scraper/get_player_list.py
   ```

3. **Get tournament IDs for a month:**
   ```bash
   uv run src/scraper/get_tournaments.py --year 2025 --month 12
   ```

4. **Get detailed tournament information:**
   ```bash
   uv run src/scraper/get_tournament_details.py --year 2025 --month 12
   ```

5. **Get tournament reports (games) — run after details:**
   ```bash
   uv run src/scraper/get_tournament_reports.py --year 2025 --month 12
   ```
   The reports scraper reads tournament codes from the details output and extracts game results.

The scraper outputs data in efficient Parquet format with JSON samples for quick inspection. See the [scraper README](src/scraper/README.md) for complete documentation.

## Testing

Run unit tests (offline, no network):

```bash
uv run pytest -m "not online"
```

Run all tests including online endpoint checks:

```bash
uv run pytest -m online
```

See [tests/README.md](tests/README.md) for detailed documentation on running tests and what they cover.

## License

MIT License

## Contributing

Contributions are welcome! Feel free to contribute however you like—see [CONTRIBUTING.md](CONTRIBUTING.md) for more details. Reach out to me via [email](mailto:maxjiang216@gmail.com) if you have any questions.

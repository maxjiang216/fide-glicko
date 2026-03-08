# Scripts

Production automation for the FIDE data pipeline.

## run_full_pipeline.py

The main pipeline script. Fetches all FIDE data needed for a given month and optionally validates consistency.

### What it does

Runs six steps in order:

1. **Fetch federations** → `data/federations.csv`  
   Scrapes the country selector from FIDE and saves federation codes (e.g. USA, RUS).

2. **Get tournaments** → `data/tournament_ids/YYYY_MM`  
   For each federation, queries FIDE for tournaments in the given year/month. Writes tournament IDs to a text file (one per line).

3. **Get tournament details** → `data/tournament_details/YYYY_MM.parquet`  
   Fetches start/end dates, event codes, player counts, etc. for each tournament ID.

4. **Get player list** → `src/data/players_list.parquet`  
   Downloads the FIDE Combined Rating List (id, name, federation, title). Run before reports so validation can check that players in reports exist in the list.

5. **Get tournament reports** → `data/tournament_reports/YYYY_MM_players.parquet`, `_games.parquet`  
   For each tournament with details, scrapes cross tables and round-by-round results. Extracts games (white/black, score, round date) and player summaries.

6. **Validate** (optional)  
   Compares player list vs reports (missing IDs) and details vs reports (event codes, player counts, date consistency). Writes `data/validation_reports/YYYY_MM.txt`.

### Usage

```bash
uv run scripts/run_full_pipeline.py --year 2025 --month 12
```

### Options

| Option | Description |
|--------|-------------|
| `--year`, `--month` | Required. Target month (e.g. `--year 2024 --month 1`). |
| `--data-dir` | Base data directory (default: `data`). |
| `--test` | Quick smoke run: limit to 5 tournaments, 5 details, 5 reports; skip JSON/CSV samples. |
| `--limit N` | Limit tournaments/details/reports to N each (overrides `--test` defaults when set). |
| `--skip-federations` | Use existing `data/federations.csv` instead of fetching. |
| `--skip-player-list` | Use existing `src/data/players_list.parquet` instead of downloading. |
| `--skip-validation` | Skip step 6 (validation). |
| `--no-validation` | Pass through to reports scraper: skip pairing/player checks (faster, less strict). |
| `--quiet` | Reduce log output. |
| `--override`, `-o` | Overwrite federations and player list instead of skipping when files exist. |

### Example

```bash
# Full run for January 2024
uv run scripts/run_full_pipeline.py --year 2024 --month 1

# Quick test (5 tournaments per step, no samples)
uv run scripts/run_full_pipeline.py --year 2024 --month 1 --test

# Reuse existing federations and player list
uv run scripts/run_full_pipeline.py --year 2024 --month 1 --skip-federations --skip-player-list

# Run without validation (e.g. for debugging)
uv run scripts/run_full_pipeline.py --year 2024 --month 1 --skip-validation
```

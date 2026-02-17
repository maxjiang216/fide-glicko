# Tests

This directory contains tests for the FIDE scraper scripts. Tests are organized by script and include both unit tests (pure functions, fixture-based parsing) and online endpoint smoke tests.

## Running Tests

### Quick reference

```bash
# All tests except online (recommended for CI, offline)
uv run pytest -m "not online"

# Only online endpoint tests (requires network)
uv run pytest -m online

# All tests
uv run pytest

# Verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_get_federations.py -v

# Run a specific test class or test
uv run pytest tests/test_get_tournament_reports.py::TestParseScore::test_valid_scores -v
```

### Using standard Python

If you use a regular venv instead of `uv`:

```bash
pytest -m "not online"
pytest -m online
```

## Test Markers

- **`online`** — Tests that hit real FIDE endpoints over the network. These verify that the scrapers work against the live site and return non-empty data in the expected format. Marked in `pyproject.toml`; exclude them in CI or when offline with `-m "not online"`.

## What Gets Tested

### `test_get_federations.py`
- **Unit / fixture**: None (federations list comes from live HTML)
- **Live**: Federation list returns ~207 items with `code` and `name`; endpoint returns non-empty data with expected structure

### `test_get_tournaments.py`
- **Live**: USA Dec 2025 returns a known count of tournaments; endpoint returns non-empty list with `Tournament` objects (id, name, location, time_control, dates, federation)
- Requires `aiohttp`; tests are skipped if not installed

### `test_get_tournament_details.py`
- **Fixture**: Parses `candidates_24_details.html` (Candidates 2024), asserts event_code, tournament_name, city, country, dates, etc.
- **Live**: Fetch event 368261 from FIDE; compare to fixture; verify endpoint returns non-empty details with expected keys

### `test_get_tournament_reports.py`
- **Unit**: `parse_score`, `extract_forfeit_indicator`, date parsing (`parse_date_to_iso`, `parse_round_date`, etc.), `flatten_result`, `flatten_to_games`
- **Fixture**: Parses `world_cup_25_report.html` (World Cup 2025), asserts tournament_code, players, rounds, bye handling, forfeits
- **Live**: Fetch report 449502 from FIDE; compare to fixture; verify endpoint returns non-empty report with players and expected structure

## Test Setup

`conftest.py` adds `src/scraper` to `sys.path` so tests can import scraper modules directly (e.g. `from get_federations import get_federations_with_retries`).

## Fixtures

HTML fixtures live in `tests/fixtures/`:

- `candidates_24_details.html` — Tournament details for FIDE Candidates 2024 (event 368261)
- `world_cup_25_report.html` — Original report for FIDE World Cup 2025 (code 449502)

These are used for offline parsing tests and for comparing live fetches when running live tests.

## CI Recommendation

In continuous integration (no network or rate limits):

```bash
uv run pytest -m "not online" -v
```

To also validate online endpoints:

```bash
uv run pytest -m online -v
```

Note: Live tests depend on FIDE’s site being up and may be sensitive to layout changes.

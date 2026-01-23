# FIDE Federations Scraper

This script scrapes the FIDE website to retrieve a list of all chess federations and saves them to a CSV file.

## Overview

The `get_federations.py` script fetches the list of federations from the FIDE ratings website (`https://ratings.fide.com/rated_tournaments.phtml`) and saves them to a CSV file with two columns: the 3-letter federation code and the full federation name.

## Prerequisites

- Python 3.13 or higher
- Required dependencies (install via `uv` or `pip`):
  - `beautifulsoup4>=4.14.3`
  - `requests>=2.32.5`

## Usage

### Basic Usage

Run the script with default settings:

```bash
python src/scraper/get_federations.py
```

This will:
- Scrape the FIDE website
- Save the results to `data/federations.csv` (relative to the repo root)
- Print verbose output (all federations, count, and execution time)

### Command Line Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--directory` | `-d` | `data` | Directory to output the result (relative to repo root) |
| `--filename` | `-f` | `federations.csv` | Output filename |
| `--quiet` | `-q` | `False` | Disable verbose output |
| `--override` | `-o` | `False` | Force re-scraping even if output file exists |

### Examples

**Custom output directory and filename:**
```bash
python src/scraper/get_federations.py --directory custom_data --filename my_federations.csv
```

**Quiet mode (minimal output):**
```bash
python src/scraper/get_federations.py --quiet
```

**Force re-scrape even if file exists:**
```bash
python src/scraper/get_federations.py --override
```

**Combine options:**
```bash
python src/scraper/get_federations.py --directory data --filename federations.csv --override --quiet
```

## Behavior

### File Existence Check

By default, if the output file already exists, the script will:
- Print a message indicating the file exists
- Exit without scraping
- Return exit code 0

To force re-scraping and overwrite the existing file, use the `--override` flag.

### Retry Logic

The script includes automatic retry logic with exponential backoff:
- Up to 3 retry attempts on failure
- Exponential backoff between retries (1s, 2s, 3s)
- Handles network errors and parsing errors

### Verbose Output

When verbose mode is enabled (default), the script prints:
1. All federations in the format: `CODE: Full Name`
2. The total count of federations
3. The execution time in seconds

Example verbose output:
```
Fetching federations list...
AFG: Afghanistan
ALB: Albania
...
ZIM: Zimbabwe

Found 208 federations
Time taken: 2.45 seconds
Saved 208 federations to /path/to/data/federations.csv
```

## Output Format

The script generates a CSV file with the following structure:

```csv
code,name
AFG,Afghanistan
ALB,Albania
...
ZIM,Zimbabwe
```

- **code**: 3-letter federation abbreviation
- **name**: Full federation name

## Error Handling

The script handles various error conditions:
- Network timeouts and connection errors (with retries)
- Missing HTML elements (raises RuntimeError)
- File I/O errors

On error, the script will:
- Print an error message
- Return exit code 1


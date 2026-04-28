# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run offline tests (use for CI and local dev)
uv run pytest -m "not online"

# Run a single test file
uv run pytest tests/test_get_tournament_reports.py -v

# Run a single test
uv run pytest tests/test_get_tournament_reports.py::TestParseScore::test_valid_scores -v

# Run online tests (hits live FIDE endpoints)
uv run pytest -m online

# Format code (excludes exploratory/)
uv run black src/ handlers/ tests/ scripts/

# Local full pipeline (one month)
uv run scripts/run_full_pipeline.py --year 2025 --month 12

# Prepare ZIP function dirs before sam build
bash scripts/prepare_functions.sh

# Build SAM stack
sam build --cached

# Deploy (local, requires AWS CLI configured)
sam deploy
```

## Architecture

The project scrapes FIDE chess data (federations, tournaments, player lists, game reports) and stores it in S3 as Parquet files. It runs both locally and as an AWS Step Functions pipeline.

### Code layout

- **`src/scraper/`** — Core scraping logic. Runs locally and is also imported by Lambda handlers. Five main scripts: `get_federations.py`, `get_player_list.py`, `get_tournaments.py`, `get_tournament_details.py`, `get_tournament_reports.py`. Shared utilities: `s3_io.py` (S3 read/write), `schema.py` (Parquet schemas), `raw_utils.py`, `split_tournament_ids.py`, `validate_pipeline.py`, `merge_chunks.py`.
- **`handlers/`** — Lambda entry points (thin wrappers). Parse events, build S3 paths, call scraper functions, return responses. One file per Lambda function.
- **`infra/step-function/pipeline.asl.json`** — Step Functions ASL definition for the full pipeline.
- **`template.yaml`** — SAM template defining all Lambdas and the state machine.
- **`scripts/`** — Automation: `run_full_pipeline.py` (local end-to-end), `run_prod_backfill.py` (AWS Step Functions backfill), `prepare_functions.sh` (build prep).
- **`tests/`** — Pytest tests. HTML fixtures in `tests/fixtures/`. `conftest.py` adds `src/scraper` to sys.path.
- **`exploratory/`** — Prototyping and one-off scripts; excluded from Black formatting.

### Lambda packaging

- **ZIP Lambdas** (federations, tournaments, split_ids, ensure_run_name): `scripts/prepare_functions.sh` copies `handlers/` and all `src/scraper/*.py` into `.functions/<name>/` at the repo root. Each function's `CodeUri` points to `.functions/<name>`.
- **Docker Lambdas** (player_list, details_chunk, reports_chunk, merge_chunks, validate): All share one Docker image (`docker/Dockerfile`). The image copies `handlers/`, `src/`, and all `src/scraper/*.py` to `/var/task/`. SAM tracks these under the logical ID `LambdaImage` (first alphabetically).

### Step Functions pipeline flow

```
EnsureRunName → Federations → Tournaments → parallel(SplitIds, PlayerList)
  → Map(DetailsChunk → ReportsChunk, per chunk)
  → MergeChunks → Validate
```

- **Run types**: `prod` (S3 prefix `prod/YYYY-MM/`), `custom` (prefix `custom/<run_name>/`), `test` (prefix `test/`).
- **Tunable defaults** without redeploy: SSM Parameter Store at `/fide-glicko/pipeline/config` (JSON). Precedence: execution input > SSM > code defaults.

### S3 data layout

```
s3://fide-glicko/
├── federations/data/federations_{timestamp}.csv        # shared
├── player_lists/data/player_list_{timestamp}.parquet   # shared
├── prod/{YYYY-MM}/data/                                # per-run
│   ├── tournament_ids.txt
│   ├── tournament_id_chunks/ids_chunk_N_of_total.txt
│   ├── tournament_details_chunks/details_chunk_N_of_total.parquet
│   ├── tournament_reports_chunks/reports_chunk_N_of_total_{players,games}.parquet
│   ├── tournament_details.parquet                      # merged
│   ├── tournament_reports_players.parquet
│   └── tournament_reports_games.parquet
└── prod/{YYYY-MM}/reports/validation_report.json
```

### Key behaviors and constraints

- **CGO fallback**: Republic of Congo is hard-coded into `get_federations.py` because FIDE's country selector omits it. The `test_cgo_in_federations` online test verifies it's present.
- **Rate limits**: Details endpoint throttles above ~0.6 req/s (causes `RemoteDisconnected`). Default is 0.33 req/s. Reports endpoint tolerates higher rates. Tournaments Lambda has a 900s hard limit — raise `tournaments_max_concurrency` if it times out.
- **Parquet list fields**: Arbiter and organizer columns are stored as semicolon-separated strings in Parquet (not arrays) for compatibility. Split with `df['col'].str.split(';')`.
- **Lambda imports**: Scraper modules are placed at the Lambda function root so `from get_federations import ...` resolves without package paths.
- **CI**: Offline tests run on every push/PR. Deploy triggers on push to main when relevant files change. PRs generate a SAM changeset preview (no deploy).

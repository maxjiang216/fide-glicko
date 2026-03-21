# Lambda Handlers

All Lambdas accept **run_type**, **run_name**, **bucket**, **override** where applicable. Paths are inferred from these; explicit URIs are optional overrides.

**Prod runs:** `run_name` is derived as `YYYY-MM` from year and month by the pipeline; Lambdas receive it from state. For custom runs, `run_name` is required.

## Event Shapes (minimal = run params only)

### ensure_run_name
```json
{
  "year": 2025,
  "month": 3,
  "run_type": "prod",
  "bucket": "fide-glicko",
  "override": false
}
```
- Normalizes `run_name` before pipeline: prod = `{year}-{month:02d}`, custom = required, test = `"test"`
- Returns full passthrough with `run_name` set. Called first by the Step Function.

### federations
```json
{
  "bucket": "fide-glicko",
  "override": false
}
```
- **bucket**: default fide-glicko
- **override**: If true, always fetch and write. Else skip if latest < 2 weeks old; only write if content changed (order-independent compare).
- Output: `{bucket}/federations/data/federations_{timestamp}.csv` (shared across all run types)
- Returns: `federations_uri`

### tournaments
```json
{
  "year": 2025,
  "month": 3,
  "run_type": "prod",
  "run_name": "2025-03",
  "bucket": "fide-glicko",
  "override": false,
  "tournaments_max_concurrency": null
}
```
- **year**, **month**: Required
- **run_type**, **run_name**: run_name comes from EnsureRunName (prod: YYYY-MM; custom: user-provided)
- **federations_s3_uri**: Optional. Defaults to latest in `{bucket}/federations/data/`
- **tournaments_max_concurrency**: Optional. Parallel federation HTTP requests (default `1` when omitted/null). Increase (e.g. `3`–`5`) if the step fails with **Sandbox.Timedout** at **900s** (Lambda hard limit). Lower values reduce burst load on FIDE from AWS.
- Outputs: `{base}/data/tournament_ids.txt`, `{base}/sample/tournament_ids_sample.json`, `{base}/raw/tournaments.json.gz` (raw API JSON, all federations concatenated, gzip-9)

**Tournaments step & 900s timeout:** One Lambda invocation must finish all federations (~200). Standard Lambdas cannot run longer than **900 seconds**. CloudWatch logs include `projected_total_s`, `lambda_remaining_ms`, and warnings when the scrape is on track to exceed the limit. Mitigations: raise **tournaments_max_concurrency** (faster wall clock, more FIDE load), or split work across multiple runs (e.g. custom federation subsets — not built in). For very slow FIDE responses, **merge** / **ECS** would be a larger architectural change.

### split_ids
```json
{
  "run_type": "custom",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "chunk_size": 300,
  "override": false
}
```
- **run_type**, **run_name**: Used to locate `{base}/data/tournament_ids.txt`. No year/month
  required — paths derive from run folder.
- **ids_uri**: Optional. Defaults to `{base}/data/tournament_ids.txt`
- **chunk_size**: default 300
- **chunk_count**: Optional override
- Returns: `chunks: [{ input_path, output_path, tournament_count, chunk_index }, ...]`

### details_chunk
```json
{
  "run_type": "prod",
  "run_name": "2024-01",
  "chunk_index": 0,
  "bucket": "fide-glicko",
  "override": false
}
```
- **chunk_index**: Required (0-based). **chunk_count**: Required. Paths: `{base}/data/tournament_id_chunks/ids_chunk_{i}_of_{n}.txt` → `{base}/data/tournament_details_chunks/details_chunk_{i}_of_{n}.parquet`
- **override**: If true, overwrite existing output (default: false)
- **save_raw**: If true, save raw HTML to `{base}/raw/details/details_chunk_{i}_of_{n}.html.gz` (default: false)
- Orchestrator: use `chunk_index` from each split_ids chunk, pass run_type/run_name from state

### reports_chunk
```json
{
  "run_type": "prod",
  "run_name": "2024-01",
  "chunk_index": 0,
  "bucket": "fide-glicko",
  "override": false,
  "save_raw": false
}
```
- **chunk_index**: Required (0-based). **chunk_count**: Required. Paths: `{base}/data/tournament_id_chunks/ids_chunk_{i}_of_{n}.txt` → `{base}/data/tournament_reports_chunks/reports_chunk_{i}_of_{n}_*.parquet`
- **override**: If true, overwrite existing output (default: false)
- **save_raw**: If true, save raw HTML to `{base}/raw/reports/reports_chunk_{i}.html.gz` (default: false)
- **details_path**: Optional. Defaults to `{base}/data/tournament_details_chunks/details_chunk_{i}_of_{n}.parquet`
- **Rate limit**: Fixed **0.33 requests/s** to FIDE per chunk (reduces load when Map runs multiple chunks in parallel).
- Outputs: `reports_chunk_{i}_of_{n}_players.parquet`, `reports_chunk_{i}_of_{n}_games.parquet`; `reports_chunk_{i}_of_{n}_verbose_sample.json`, `reports_chunk_{i}_of_{n}_games_sample.csv`; `{base}/reports/reports_chunk_{i}_of_{n}_skipped.json` when any tournaments have no original report (updated/replaced)
- Orchestrator: use `chunk_index` from each split_ids chunk, pass run_type/run_name from state

### merge_chunks
```json
{
  "run_type": "prod",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "override": false
}
```
- **run_type**, **run_name**: Required (as above). Locates chunk prefixes.
- **bucket**: default fide-glicko
- **override**: If true, overwrite existing merged files (default: false)
- Inputs: `{base}/data/tournament_details_chunks/details_chunk_*_of_{n}.parquet`, `{base}/data/tournament_reports_chunks/reports_chunk_*_of_{n}_players.parquet`, `reports_chunk_*_of_{n}_games.parquet` (n=chunk_count from run_metadata)
- Outputs: `{base}/data/tournament_details.parquet`, `{base}/data/tournament_reports_players.parquet`, `{base}/data/tournament_reports_games.parquet`
- Returns: `details_uri`, `reports_players_uri`, `reports_games_uri`, `details_chunks`, `reports_chunks`

### validate
```json
{
  "run_type": "prod",
  "run_name": "2024-01",
  "bucket": "fide-glicko"
}
```
- **run_type**, **run_name**: Required (as above).
- **bucket**: default fide-glicko
- All paths inferred: details `{base}/data/tournament_details.parquet`, reports `{base}/data/tournament_reports_games.parquet`, players latest in `{bucket}/player_lists/data/`.
- Inputs: Merged details, reports_games, and latest player list (run merge_chunks first).
- Output: `{base}/reports/validation_report.json`
- Returns: `report_uri`, `has_issues`, `player_list_vs_reports`, `details_vs_reports`

### player_list
```json
{
  "bucket": "fide-glicko",
  "override": false,
  "federations_uri": null
}
```
- **bucket**: default fide-glicko
- **override**: If true, always fetch and write. Else skip if latest < 2 weeks old.
- **federations_uri**: Optional. For report. Defaults to latest in `{bucket}/federations/data/`.
- Outputs: `{bucket}/player_lists/data/player_list_{timestamp}.parquet`, `{bucket}/player_lists/raw/player_list_{timestamp}.xml.gz`, etc. (shared across all run types)
- Returns: `players_list_uri`

## Path formula

- **base** = `prod/{YYYY-MM}` for prod, `custom/{run_name}` for custom, or `test` for run_type=test
- **Shared** (federations, player list): `{bucket}/federations/data/federations_{timestamp}.csv`, `{bucket}/player_lists/data/player_list_{timestamp}.parquet`, `{bucket}/player_lists/raw/player_list_{timestamp}.xml.gz` — all run types share these; 2-week staleness check.
- **Per-run data**: `{bucket}/{base}/data/...`
- **raw**: `{bucket}/{base}/raw/...` (compressed downloads)
- **sample**: `{bucket}/{base}/sample/...`
- **reports**: `{bucket}/{base}/reports/...`

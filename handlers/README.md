# Lambda Handlers

All Lambdas accept **run_type**, **run_name**, **bucket**, **override** where applicable. Paths are inferred from these; explicit URIs are optional overrides.

## Event Shapes (minimal = run params only)

### federations
```json
{
  "run_type": "prod",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "override": false
}
```
- **run_type**: prod | custom | test (default: custom)
- **run_name**: Required for prod/custom. Ignored for test.
- **bucket**: default fide-glicko
- Output: `{base}/data/federations.csv`

### tournaments
```json
{
  "year": 2025,
  "month": 3,
  "run_type": "prod",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "override": false
}
```
- **year**, **month**: Required
- **run_type**, **run_name**: As above
- **federations_s3_uri**: Optional. Defaults to `{base}/data/federations.csv`
- Outputs: `{base}/data/tournament_ids.txt`, `{base}/sample/tournament_ids_sample.json`, `{base}/raw/tournaments.json.gz` (raw API JSON, all federations concatenated, gzip-9)

### split_ids
```json
{
  "run_type": "custom",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "chunk_size": 225,
  "override": false
}
```
- **run_type**, **run_name**: Used to locate `{base}/data/tournament_ids.txt`. No year/month
  required — paths derive from run folder.
- **ids_uri**: Optional. Defaults to `{base}/data/tournament_ids.txt`
- **chunk_size**: default 225
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
- **chunk_index**: Required (0-based). Paths inferred: `{base}/data/tournament_id_chunks/chunk_{i}.txt` → `{base}/data/tournament_details_chunks/chunk_{i}`
- **override**: If true, overwrite existing output (default: false)
- **save_raw**: If true, save concatenated raw HTML to `{base}/raw/details/chunk_{i}.html.gz` (default: false, ~2 MB gzipped per chunk)
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
- **chunk_index**: Required (0-based). Paths inferred: `{base}/data/tournament_id_chunks/chunk_{i}.txt` → `{base}/data/tournament_reports_chunks/chunk_{i}`
- **override**: If true, overwrite existing output (default: false)
- **save_raw**: If true, save concatenated raw HTML to `{base}/raw/reports/chunk_{i}.html.gz` (default: false, ~3.4 MB gzipped per chunk)
- **details_path**: Optional. Defaults to `{base}/data/tournament_details_chunks/chunk_{i}.parquet` for date inference
- Outputs: `{base}/data/tournament_reports_chunks/chunk_{i}_players.parquet`, `chunk_{i}_games.parquet`
- Orchestrator: use `chunk_index` from each split_ids chunk, pass run_type/run_name from state

### player_list
```json
{
  "run_type": "custom",
  "run_name": "2024-01",
  "bucket": "fide-glicko",
  "override": false
}
```
- **federations_s3_uri**: Optional. Defaults to `{base}/data/federations.csv`
- Outputs: `{base}/data/players_list.parquet`, `{base}/raw/players_list.xml.gz`, `{base}/sample/players_list_sample.json`, `{base}/reports/players_list_report.json`

## Path formula

- **base** = `{run_type}/{run_name}` for prod/custom, or `test` for run_type=test
- **data**: `{bucket}/{base}/data/...`
- **raw**: `{bucket}/{base}/raw/...` (compressed downloads, e.g. players_list.xml.gz)
- **sample**: `{bucket}/{base}/sample/...`
- **reports**: `{bucket}/{base}/reports/...`

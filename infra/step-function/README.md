# FIDE Scraping Step Functions Pipeline

Orchestrates the full scraping flow: federations → tournaments + player_list (parallel) → split_ids → Map(details + reports per chunk) → merge → validate.

## Flow

1. **Federations** – fetch federation list (shared across runs)
2. **Parallel** – tournaments and player_list run in parallel
3. **SplitIds** – chunk tournament IDs (~225 per chunk, ~75 chunks)
4. **Map** – each chunk runs details_chunk then reports_chunk (sequential per chunk; MaxConcurrency 40)
5. **MergeChunks** – combine parquet outputs
6. **Validate** – run validation report

## Deploy

The pipeline is deployed as part of the SAM stack. See [../README.md](../README.md).

```bash
sam build && sam deploy
```

## Run

Start an execution with the required input:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:fide-glicko-pipeline \
  --name "run-$(date +%Y%m%d-%H%M%S)" \
  --input '{
    "year": 2025,
    "month": 3,
    "run_type": "prod",
    "run_name": "2024-01",
    "bucket": "fide-glicko",
    "override": false
  }'
```

**Input fields:**

- **year**, **month** (required) – for tournaments step
- **run_type** – `prod`, `custom`, or `test`
- **run_name** – e.g. `2024-01` (required for prod/custom)
- **bucket** – S3 bucket (default: fide-glicko)
- **override** – if true, refetch/overwrite even when cached (default: false)
- **chunk_size** – optional; Lambda default is 225

## Check status

```bash
aws stepfunctions describe-execution --execution-arn EXECUTION_ARN
aws stepfunctions get-execution-history --execution-arn EXECUTION_ARN
```

## Estimated duration

- Federations: ~1 min
- Tournaments + Player list (parallel): ~5–10 min
- SplitIds: ~30 s
- Map (details ~7.5 min + reports ~3.75 min per chunk, 40 concurrent): ~12–15 min
- Merge + Validate: ~2 min

**Total: ~25–35 min**

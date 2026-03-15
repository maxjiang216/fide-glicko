# FIDE Scraping Step Functions Pipeline

Orchestrates the full scraping flow: federations → tournaments → parallel(split_ids, player_list) → Map(details + reports per chunk) → merge → validate.

## Flow

1. **Federations** – fetch federation list (shared across runs)
2. **Tournaments** – fetch tournament IDs per federation
3. **Parallel** – split_ids and player_list run in parallel (neither depends on the other)
4. **Map** – each chunk runs details_chunk then reports_chunk (sequential per chunk; MaxConcurrency configurable via input, default 5)
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
- **max_concurrency** – Map state parallelism for chunk processing (default: 5)
- **chunk_size** – optional; Lambda default is 225

## Check status

```bash
aws stepfunctions describe-execution --execution-arn EXECUTION_ARN
aws stepfunctions get-execution-history --execution-arn EXECUTION_ARN
```

## Estimated duration

- Federations: ~1 min
- Tournaments: ~5–10 min
- SplitIds + Player list (parallel): ~2 min
- Map (details ~7.5 min + reports ~3.75 min per chunk, 5 concurrent): ~45–60 min
- Merge + Validate: ~2 min

**Total: ~25–35 min**

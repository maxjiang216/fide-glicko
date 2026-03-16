# FIDE Scraping Step Functions Pipeline

Orchestrates the full scraping flow: ensure_run_name → federations → tournaments → parallel(split_ids, player_list) → Map(details + reports per chunk) → merge → validate.

## Flow

0. **EnsureRunName** – normalize `run_name`: prod = `YYYY-MM` from year/month; custom = required; test = default `"test"`
1. **Federations** – fetch federation list (shared across runs)
2. **Tournaments** – fetch tournament IDs per federation
3. **Parallel** – split_ids and player_list run in parallel (neither depends on the other)
4. **Map** – each chunk runs details_chunk then reports_chunk (sequential per chunk; MaxConcurrency configurable via input, default 10)
5. **MergeChunks** – combine parquet outputs
6. **Validate** – run validation report

## Deploy

The pipeline is deployed as part of the SAM stack. See [../README.md](../README.md).

```bash
sam build && sam deploy
```

## Run

Start an execution with the required input. For **prod**, omit `run_name` — it is always derived as `YYYY-MM` from year and month to avoid repeat runs.

**Production (monthly run):**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:fide-glicko-pipeline \
  --name "run-$(date +%Y%m%d-%H%M%S)" \
  --input '{
    "year": 2025,
    "month": 3,
    "run_type": "prod",
    "bucket": "fide-glicko",
    "override": false
  }'
```

**Custom (backfill, dev):**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:fide-glicko-pipeline \
  --name "backfill-2024-06" \
  --input '{
    "year": 2024,
    "month": 6,
    "run_type": "custom",
    "run_name": "backfill-2024-06",
    "bucket": "fide-glicko"
  }'
```

**Test:**
```bash
# run_name defaults to "test" when omitted
--input '{"year": 2025, "month": 1, "run_type": "test"}'
```

**Input fields:**

- **year**, **month** (required) – for tournaments step
- **run_type** – `prod`, `custom`, or `test`
- **run_name** – For prod: omit (derived as `YYYY-MM`). For custom: required (e.g. `"backfill-2024-06"`). For test: optional (default `"test"`)
- **bucket** – S3 bucket (default: fide-glicko)
- **override** – if true, refetch/overwrite even when cached (default: false)
- **max_concurrency** – Map state parallelism for chunk processing (default: 5)
- **chunk_size** – optional; default 300

## Check status

```bash
aws stepfunctions describe-execution --execution-arn EXECUTION_ARN
aws stepfunctions get-execution-history --execution-arn EXECUTION_ARN
```

## Estimated duration

- Federations: ~1 min
- Tournaments: ~5–10 min
- SplitIds + Player list (parallel): ~2 min
- Map (details ~7.5 min + reports ~3.75 min per chunk, 10 concurrent): ~25–35 min
- Merge + Validate: ~2 min

**Total: ~25–35 min**

## Troubleshooting

**ReportsChunk Lambda timeouts** – Each invocation has a 15 min max (Lambda limit). If a chunk times out (or Lambda.SdkClientException/Lambda.Unknown), the Map iterator retries it once (transient issues). Check CloudWatch Logs for `Slow report fetch`, `timeout`, `connection error` to diagnose anomalous chunks. For persistently slow months, re-run with smaller `chunk_size` (e.g. `150`).

**Connect timeout (all-or-nothing per chunk)** – When a Lambda can't connect to ratings.fide.com, it typically fails for every tournament in that chunk. The scraper fails fast (15s connect timeout, abort after 2 consecutive) so the Step Function can retry with fresh Lambdas. DetailsChunk errors go to ChunkFailed (Pass state); ReportsChunk has no Catch so its failures surface as MapIterationFailed. The Map retries 4× to handle transient connectivity.

# AWS Infrastructure

Infrastructure is defined in `template.yaml` (AWS SAM) and deployed via GitHub Actions or `sam deploy`.

**Packaging:** 3 functions (federations, tournaments, split_ids) use ZIP with minimal deps; 5 data-heavy functions share one Docker image (pandas, pyarrow). This keeps builds fast and avoids Lambda's 250 MB zip limit.

## Handler separation

Lambda entry points live in `handlers/` — thin wrappers around core scrapers in `src/scraper/`:

- **handlers/*.py**: Event parsing, S3 paths, response format
- **src/scraper/*.py**: Scraping logic (shared with local execution)

---

## Deploy

### From GitHub (recommended)

The workflow `.github/workflows/deploy-sam.yml` deploys on push to main when `template.yaml`, handlers, or `src/scraper` change.

**OIDC setup:**

1. **IAM → Identity providers → Add provider**
   - Provider URL: `https://token.actions.githubusercontent.com`
   - Audience: `sts.amazonaws.com`

2. **Create role** for GitHub Actions with trust policy for `repo:YOUR_ORG/fide-glicko:*`. For the inline policy, use `github-deploy-policy.json` but replace `__DEPLOY_ROLE_ARN__` with the role's ARN. The workflow syncs this file to the role on every deploy, so the repo remains the source of truth.

3. **GitHub → Settings → Secrets**: Add `AWS_ROLE_ARN` = the role ARN.

4. **GitHub → Settings → Variables** (optional): `AWS_REGION` = `us-east-1` (or your region).

**Manual run:** Actions → Deploy SAM → Run workflow → set "Deploy to AWS" to true.

### Manual (local)

```bash
# Prerequisites: AWS CLI configured, SAM CLI installed
pip install aws-sam-cli  # or: brew install aws-sam-cli

bash scripts/prepare_functions.sh   # ZIP functions only (data functions use Docker image)
sam build --cached                  # --cached skips unchanged functions
sam deploy
# Or with prompts: sam deploy --guided  # first time only
```

### Migration from bash deploy scripts

If you previously deployed with the old shell scripts, the Lambdas and Step Function were created outside CloudFormation. To adopt SAM:

1. Delete the existing Step Function state machine (or it will conflict).
2. Delete the existing Lambdas (they use the same names the stack will create).
3. Ensure the S3 bucket `fide-glicko` exists.
4. Run `sam deploy`.

---

## Stack contents

- **8 Lambda functions**: federations, tournaments, player_list, split_ids, details_chunk, reports_chunk, merge_chunks, validate
- **1 Step Functions state machine**: fide-glicko-pipeline

All Lambdas share the same code package (handlers + `src/scraper`). The Step Function orchestrates the full scraping flow.

**Run the pipeline:**
```bash
# Get state machine ARN from deploy output (or: aws stepfunctions list-state-machines)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:fide-glicko-pipeline \
  --name "run-$(date +%Y%m%d-%H%M%S)" \
  --input '{"year": 2025, "month": 3, "run_type": "prod", "bucket": "fide-glicko"}'
```
For prod, omit `run_name`; it is derived as `YYYY-MM`. See [step-function/README.md](step-function/README.md) for full input options.

---

## S3 Bucket Structure

The `fide-glicko` bucket stores scraped data. Layout (mirrors `build_run_base` + `build_s3_uri_for_run`):

```
s3://fide-glicko/
├── federations/                    # Shared across all run types
│   └── data/
│       └── federations_{timestamp}.csv
│
├── player_lists/                  # Shared across all run types
│   ├── data/
│   │   └── player_list_{timestamp}.parquet
│   ├── raw/
│   │   └── player_list_{timestamp}.xml.gz
│   ├── sample/
│   │   └── player_list_sample_{timestamp}.json
│   └── reports/
│       └── player_list_report_{timestamp}.json
│
├── prod/                          # Production runs (one per month)
│   └── {YYYY-MM}/                 # e.g. 2024-01
│       ├── data/
│       │   ├── tournament_ids.txt
│       │   ├── tournament_id_chunks/
│       │   │   └── ids_chunk_{N}.txt
│       │   ├── tournament_details_chunks/
│       │   │   └── details_chunk_{N}.parquet
│       │   ├── tournament_reports_chunks/
│       │   │   ├── reports_chunk_{N}_players.parquet
│       │   │   └── reports_chunk_{N}_games.parquet
│       │   ├── tournament_details.parquet
│       │   ├── tournament_reports_players.parquet
│       │   └── tournament_reports_games.parquet
│       ├── sample/
│       │   └── tournament_ids_sample.json
│       ├── raw/
│       │   └── tournaments.json.gz
│       ├── reports/
│       │   └── validation_report.json
│       └── run_metadata.json
│
├── custom/                        # Custom/backfill runs (user-named)
│   └── {run_name}/
│       └── ...                    # Same structure as prod/{YYYY-MM}
│
└── test/                          # Test runs (no run_name subfolder)
    ├── data/
    │   └── ...
    └── ...
```

- **`prod/{YYYY-MM}/`** – Scheduled monthly runs. Pass `run_type: "prod"` with `year` and `month`; `run_name` is derived.
- **`custom/{run_name}/`** – Dev, backfills. Pass `run_type: "custom"` and `run_name`.
- **`test/`** – Test runs. Pass `run_type: "test"`; `run_name` defaults to `"test"`.

## Logs

Lambda logs go to **CloudWatch Logs** under `/aws/lambda/<function-name>`.

**CloudWatch Logs cost** (typical for this project): ~pennies per month for monthly pipeline runs.

### Raw storage cost (when save_raw enabled)

| Source | Size per run (gzip) | Est. monthly (S3 Standard) |
|--------|---------------------|----------------------------|
| players_list.xml | ~42 MB | ~$0.001 |
| tournaments (208 federations) | ~124 KB | negligible |
| details (75 chunks × 225) | ~150 MB | ~$0.003 |
| **Total raw** | ~192 MB | **~$0.004** |

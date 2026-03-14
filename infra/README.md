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

2. **Create role** for GitHub Actions with trust policy for `repo:YOUR_ORG/fide-glicko:*`, then attach permissions for:
   - CloudFormation (create/update stack)
   - S3 (deploy artifacts, must include fide-glicko or your deploy bucket)
   - Lambda (create/update functions)
   - IAM (create roles for Lambdas and Step Function)
   - Step Functions (create/update state machine)

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

All Lambdas share the same code package (handlers + `src/scraper`). The Step Function orchestrates the full scraping flow. See [step-function/README.md](step-function/README.md) for pipeline details and run instructions.

---

## S3 Bucket Structure

The `fide-glicko` bucket stores scraped data. Layout:

```
s3://fide-glicko/
├── data/                           # Production data (monthly runs, canonical)
│   ├── federations.csv             # Shared across all months
│   ├── players_list.parquet        # Shared, updated periodically
│   ├── tournament_ids/
│   │   └── {YYYY_MM}
│   ├── tournament_details/
│   │   └── {YYYY_MM}.parquet
│   ├── tournament_reports/
│   │   ├── {YYYY_MM}_players.parquet
│   │   └── {YYYY_MM}_games.parquet
│   └── validation_reports/
│       └── {YYYY_MM}.txt
│
└── runs/                           # Dev/test runs (isolated by run_id)
    └── {run_id}/
        ├── federations.csv
        ├── tournament_ids/
        ├── tournament_details/
        └── ...
```

- **`data/`** – Scheduled production runs. Use `run_type: prod`, `run_name: "2024-01"`, etc.
- **`runs/{run_id}/`** – Dev, test, backfills. Use `run_type: custom` or `test`.

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

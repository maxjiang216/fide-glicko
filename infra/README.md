# AWS Infrastructure

## Handler separation

The Lambda entry point is in `handlers/federations.py` — a thin wrapper around the core scraper. This keeps:

- **handlers/federations.py**: Lambda concerns (event parsing, S3 path building, response format)
- **src/scraper/get_federations.py**: Scraping logic (unchanged when run locally)

The handler imports and calls `run()`; it does not shell out to the script. Same pattern for future Lambdas (tournaments, details, etc.).

---

## Federations Lambda

### Deploy from GitHub (recommended)

The workflow `.github/workflows/deploy-federations-lambda.yml` deploys on push to main when federations-related files change.

**OIDC setup (no stored credentials):**

1. **IAM → Identity providers → Add provider**
   - Provider URL: `https://token.actions.githubusercontent.com`
   - Audience: `sts.amazonaws.com`

2. **Create role** for GitHub Actions with trust policy:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": {
         "Federated": "arn:aws:iam::YOUR_ACCOUNT:oidc-provider/token.actions.githubusercontent.com"
       },
       "Action": "sts:AssumeRoleWithWebIdentity",
       "Condition": {
         "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
         "StringLike": { "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/fide-glicko:*" }
       }
     }]
   }
   ```
   Attach policies: `AWSLambda_FullAccess` (or scoped: `lambda:UpdateFunctionCode`, `lambda:GetFunction`, `lambda:CreateFunction`), `iam:PassRole`.

3. **GitHub repo → Settings → Secrets → Actions**: Add `AWS_ROLE_ARN` = the role ARN.

4. **For first-time Lambda create**: Add `LAMBDA_ROLE_ARN` (execution role for the Lambda; needs S3 + CloudWatch).

**Access keys (simpler, less secure):** Add secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Edit the workflow: change the Configure step to use `aws-access-key-id` and `aws-secret-access-key` instead of `role-to-assume`. See [configure-aws-credentials](https://github.com/aws-actions/configure-aws-credentials).

---

### Manual deploy
```bash
# 1. Create IAM role (one-time)
# In AWS Console: IAM → Roles → Create role
#   - Trusted entity: AWS service → Lambda
#   - Attach policies: AWSLambdaBasicExecutionRole, AmazonS3FullAccess
#     (or create custom policy scoped to fide-glicko bucket)
#   - Name: fide-glicko-lambda-role
# Copy the role ARN (e.g. arn:aws:iam::123456789012:role/fide-glicko-lambda-role)

# 2. Deploy
export LAMBDA_ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT:role/fide-glicko-lambda-role"
./infra/deploy_federations_lambda.sh

# 3. Invoke (production - writes to data/)
aws lambda invoke --function-name fide-glicko-federations \
  --payload '{"bucket":"fide-glicko","output_prefix":"data"}' \
  out.json && cat out.json

# 4. Dev run (isolated prefix)
aws lambda invoke --function-name fide-glicko-federations \
  --payload '{"bucket":"fide-glicko","output_prefix":"runs/dev-20250308","override":true}' \
  out.json && cat out.json
```

**Logs:** CloudWatch Logs → Log groups → `/aws/lambda/fide-glicko-federations`

---

## S3 Bucket Structure

The `fide-glicko` bucket stores scraped data and run artifacts.

## Layout

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

## When to use each

- **`data/`** – Scheduled monthly production runs. Single source of truth. Use `output_prefix: "data"` in Lambda events.
- **`runs/{run_id}/`** – Development, testing, historical backfills, new versions. Use `output_prefix: "runs/dev-1736..."` or similar. Run ID can be a timestamp (`dev-20250308-143022`) or execution ID from Step Functions.

## Logs

Lambda logs (stdout/stderr) go to **CloudWatch Logs** under `/aws/lambda/<function-name>`. No extra configuration needed. Use INFO level for verbose output during runs.

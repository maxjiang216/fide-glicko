#!/bin/bash
# Build and deploy tournament reports chunk Lambda to AWS.
#
# Scrapes tournament reports for a list of codes from an input path, outputs
# _players.parquet and _games.parquet to S3/local. Saves raw HTML to raw/reports
# when save_raw=true.
# Requires: pandas, pyarrow, requests, beautifulsoup4, tqdm (boto3 in runtime).
#
# Usage:
#   ./infra/deploy_reports_chunk_lambda.sh              # Create/update Lambda
#   ./infra/deploy_reports_chunk_lambda.sh --zip-only   # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_reports_chunk"
ZIP_PATH="$REPO_ROOT/build/reports_chunk_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-reports-chunk}"
RUNTIME="python3.12"
HANDLER="handlers.reports_chunk.lambda_handler"
TIMEOUT=900
# Reports can process many tournaments; increase if OOM
MEMORY=1024

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and scraper code
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/get_tournament_reports.py" .
cp "$REPO_ROOT/src/scraper/raw_utils.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Install dependencies (pandas+pyarrow are large; use platform for Lambda compatibility)
if pip install --target . --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all: pandas pyarrow requests beautifulsoup4 tqdm --quiet 2>/dev/null; then
  :  # Success
else
  pip install --target . pandas pyarrow requests beautifulsoup4 tqdm --quiet
fi

# Zip
zip -r "$ZIP_PATH" . -x "*.pyc" -x "__pycache__/*" -x "*__pycache__*"

ZIP_SIZE_MB=$(stat -c%s "$ZIP_PATH" 2>/dev/null | awk '{printf "%.1f", $1/1024/1024}' || echo "?")
echo "Built $ZIP_PATH (${ZIP_SIZE_MB}MB)"

if [[ "${1:-}" == "--zip-only" ]]; then
  echo "Zip only. Skipping deploy."
  exit 0
fi

# Lambda direct upload limit is 50MB; use S3 for larger packages
DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-fide-glicko}"
DEPLOY_KEY="lambda-packages/reports_chunk_$(date +%Y%m%d%H%M%S).zip"
ZIP_SIZE_BYTES=$(stat -c%s "$ZIP_PATH")

# Create or update Lambda
if aws lambda get-function --function-name "$FUNCTION_NAME" 2>/dev/null; then
  echo "Updating existing Lambda $FUNCTION_NAME..."
  if [[ "$ZIP_SIZE_BYTES" -gt 52428800 ]]; then
    echo "Zip exceeds 50MB, uploading to s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws s3 cp "$ZIP_PATH" "s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --s3-bucket "$DEPLOY_BUCKET" \
      --s3-key "$DEPLOY_KEY"
  else
    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --zip-file "fileb://$ZIP_PATH"
  fi
  echo "Waiting for code update to complete..."
  aws lambda wait function-updated --function-name "$FUNCTION_NAME"
  aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --timeout "$TIMEOUT" \
    --memory-size "$MEMORY"
else
  echo "Creating Lambda $FUNCTION_NAME..."
  if [[ -z "${LAMBDA_ROLE_ARN:-}" ]]; then
    echo "Error: Set LAMBDA_ROLE_ARN (execution role with S3 + CloudWatch permissions)."
    exit 1
  fi
  if [[ "$ZIP_SIZE_BYTES" -gt 52428800 ]]; then
    echo "Zip exceeds 50MB, uploading to s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws s3 cp "$ZIP_PATH" "s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --runtime "$RUNTIME" \
      --handler "$HANDLER" \
      --role "$LAMBDA_ROLE_ARN" \
      --timeout "$TIMEOUT" \
      --memory-size "$MEMORY" \
      --code "S3Bucket=$DEPLOY_BUCKET,S3Key=$DEPLOY_KEY"
  else
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --runtime "$RUNTIME" \
      --handler "$HANDLER" \
      --role "$LAMBDA_ROLE_ARN" \
      --timeout "$TIMEOUT" \
      --memory-size "$MEMORY" \
      --zip-file "fileb://$ZIP_PATH"
  fi
fi

echo "Done. Invoke with:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"run_type\":\"test\",\"chunk_index\":0}' out.json && cat out.json"

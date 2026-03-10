#!/bin/bash
# Build and deploy split IDs Lambda to AWS.
#
# Splits tournament IDs into even chunks for details Lambda fan-out.
# Requires: boto3 (in Lambda runtime). Optionally invokes tournaments Lambda.
#
# Usage:
#   ./infra/deploy_split_ids_lambda.sh              # Create/update Lambda
#   ./infra/deploy_split_ids_lambda.sh --zip-only   # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_split_ids"
ZIP_PATH="$REPO_ROOT/build/split_ids_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-split-ids}"
RUNTIME="python3.12"
HANDLER="handlers.split_ids.lambda_handler"
TIMEOUT=120
MEMORY=256

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and split logic (boto3 in Lambda runtime)
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/split_tournament_ids.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Zip (no extra deps beyond boto3 in runtime)
zip -r "$ZIP_PATH" . -x "*.pyc" -x "__pycache__/*" -x "*__pycache__*"

echo "Built $ZIP_PATH ($(du -h "$ZIP_PATH" | cut -f1))"

if [[ "${1:-}" == "--zip-only" ]]; then
  echo "Zip only. Skipping deploy."
  exit 0
fi

# Create or update Lambda
if aws lambda get-function --function-name "$FUNCTION_NAME" 2>/dev/null; then
  echo "Updating existing Lambda $FUNCTION_NAME..."
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH"
  echo "Waiting for code update to complete..."
  aws lambda wait function-updated --function-name "$FUNCTION_NAME"
  aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --timeout "$TIMEOUT" \
    --memory-size "$MEMORY"
else
  echo "Creating Lambda $FUNCTION_NAME..."
  ROLE_ARN="${LAMBDA_ROLE_ARN:-}"
  if [[ -z "$ROLE_ARN" ]]; then
    echo "Error: Set LAMBDA_ROLE_ARN (execution role with S3 + CloudWatch + lambda:InvokeFunction)."
    exit 1
  fi
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "$RUNTIME" \
    --handler "$HANDLER" \
    --role "$ROLE_ARN" \
    --zip-file "fileb://$ZIP_PATH" \
    --timeout "$TIMEOUT" \
    --memory-size "$MEMORY"
fi

echo "Done. Invoke with:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"year\":2024,\"month\":1,\"invoke_tournaments\":true,\"chunk_count\":50}' out.json && cat out.json"

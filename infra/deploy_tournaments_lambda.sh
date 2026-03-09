#!/bin/bash
# Build and deploy tournaments Lambda to AWS.
#
# Prerequisites: aws CLI configured
# The tournaments scraper uses aiohttp (async), fetches from FIDE per federation.
#
# Usage:
#   ./infra/deploy_tournaments_lambda.sh              # Create/update Lambda
#   ./infra/deploy_tournaments_lambda.sh --zip-only  # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_tournaments"
ZIP_PATH="$REPO_ROOT/build/tournaments_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-tournaments}"
RUNTIME="python3.12"
HANDLER="handlers.tournaments.lambda_handler"
TIMEOUT=300
MEMORY=512

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and scraper code
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/get_tournaments.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Install aiohttp for Lambda Python 3.12 (boto3 in runtime)
if pip install --target . --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all: aiohttp --quiet 2>/dev/null; then
  :  # Success
else
  pip install --target . aiohttp --quiet
fi

# Zip
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
    echo "Error: Set LAMBDA_ROLE_ARN (execution role with S3 + CloudWatch permissions)."
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
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"year\":2025,\"month\":3,\"bucket\":\"fide-glicko\",\"output_prefix\":\"data\"}' out.json && cat out.json"

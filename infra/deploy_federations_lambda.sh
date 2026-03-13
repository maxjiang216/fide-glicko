#!/bin/bash
# Build and deploy federations Lambda to AWS.
#
# Prerequisites: aws CLI configured, boto3/requests/beautifulsoup4 for the Lambda
#
# Usage:
#   ./infra/deploy_federations_lambda.sh              # Create/update Lambda
#   ./infra/deploy_federations_lambda.sh --zip-only   # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_federations"
ZIP_PATH="$REPO_ROOT/build/federations_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-federations}"
RUNTIME="python3.12"
HANDLER="handlers.federations.lambda_handler"
TIMEOUT=60
MEMORY=256

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and scraper code
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/get_federations.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Install dependencies for Lambda Python 3.12 (boto3 is in runtime)
# Target Lambda's Linux x86_64 environment when building from other platforms
if pip install --target . --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all: requests beautifulsoup4 --quiet 2>/dev/null; then
  :  # Success
else
  pip install --target . requests beautifulsoup4 --quiet
fi

# Zip (must include current dir contents at zip root)
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
  # Create execution role if needed (or use existing)
  ROLE_ARN="${LAMBDA_ROLE_ARN:-}"
  if [[ -z "$ROLE_ARN" ]]; then
    echo "Error: Set LAMBDA_ROLE_ARN or create a role with lambda.amazonaws.com trust and S3 + CloudWatch permissions."
    echo "Quick start: create a role 'fide-glicko-lambda-role' with policies: AWSLambdaBasicExecutionRole, AmazonS3FullAccess (or scoped to fide-glicko bucket)"
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
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"bucket\":\"fide-glicko\",\"override\":false}' out.json && cat out.json"

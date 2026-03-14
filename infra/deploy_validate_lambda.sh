#!/bin/bash
# Build and deploy validate Lambda to AWS.
#
# Validates pipeline data: player list vs reports, details vs reports.
# Requires: pandas, pyarrow (boto3 in runtime).
#
# Usage:
#   ./infra/deploy_validate_lambda.sh              # Create/update Lambda
#   ./infra/deploy_validate_lambda.sh --zip-only   # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_validate"
ZIP_PATH="$REPO_ROOT/build/validate_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-validate}"
RUNTIME="python3.12"
HANDLER="handlers.validate.lambda_handler"
TIMEOUT=300
MEMORY=1024

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and scraper code
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/validate_pipeline.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Install dependencies
if pip install --target . --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all: pandas pyarrow --quiet 2>/dev/null; then
  :  # Success
else
  pip install --target . pandas pyarrow --quiet
fi

# Zip
zip -r "$ZIP_PATH" . -x "*.pyc" -x "__pycache__/*" -x "*__pycache__*"

ZIP_SIZE_MB=$(stat -c%s "$ZIP_PATH" 2>/dev/null | awk '{printf "%.1f", $1/1024/1024}' || echo "?")
echo "Built $ZIP_PATH (${ZIP_SIZE_MB}MB)"

if [[ "${1:-}" == "--zip-only" ]]; then
  echo "Zip only. Skipping deploy."
  exit 0
fi

DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-fide-glicko}"
DEPLOY_KEY="lambda-packages/validate_$(date +%Y%m%d%H%M%S).zip"
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
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"run_type\":\"test\",\"bucket\":\"fide-glicko\"}' out.json && cat out.json"

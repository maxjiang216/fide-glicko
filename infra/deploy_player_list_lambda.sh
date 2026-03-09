#!/bin/bash
# Build and deploy player list Lambda to AWS.
#
# The player list downloads ~45MB from FIDE, parses XML, writes parquet.
# Requires: pandas, pyarrow, requests (boto3 in runtime).
#
# Usage:
#   ./infra/deploy_player_list_lambda.sh              # Create/update Lambda
#   ./infra/deploy_player_list_lambda.sh --zip-only   # Just build the zip

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build/lambda_player_list"
ZIP_PATH="$REPO_ROOT/build/player_list_lambda.zip"

FUNCTION_NAME="${FUNCTION_NAME:-fide-glicko-player-list}"
RUNTIME="python3.12"
HANDLER="handlers.player_list.lambda_handler"
TIMEOUT=300
MEMORY=1024

mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/*
cd "$BUILD_DIR"

# Copy handler and scraper code
cp -r "$REPO_ROOT/handlers" .
cp "$REPO_ROOT/src/scraper/get_player_list.py" .
cp "$REPO_ROOT/src/scraper/s3_io.py" .

# Install dependencies (pandas+pyarrow are large; use platform for Lambda compatibility)
if pip install --target . --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all: pandas pyarrow requests --quiet 2>/dev/null; then
  :  # Success
else
  pip install --target . pandas pyarrow requests --quiet
fi

# Zip
zip -r "$ZIP_PATH" . -x "*.pyc" -x "__pycache__/*" -x "*__pycache__*"

ZIP_SIZE_MB=$(stat -c%s "$ZIP_PATH" | awk '{printf "%.1f", $1/1024/1024}')
echo "Built $ZIP_PATH (${ZIP_SIZE_MB}MB)"

if [[ "${1:-}" == "--zip-only" ]]; then
  echo "Zip only. Skipping deploy."
  exit 0
fi

# Lambda direct upload limit is 50MB; use S3 for larger packages
DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-fide-glicko}"
DEPLOY_KEY="lambda-packages/player_list_$(date +%Y%m%d%H%M%S).zip"
ZIP_SIZE_BYTES=$(stat -c%s "$ZIP_PATH")

update_code() {
  if [[ "$ZIP_SIZE_BYTES" -gt 52428800 ]]; then
    echo "Zip exceeds 50MB, uploading to s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws s3 cp "$ZIP_PATH" "s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws lambda update-function-code --function-name "$1" --s3-bucket "$DEPLOY_BUCKET" --s3-key "$DEPLOY_KEY"
  else
    aws lambda update-function-code --function-name "$1" --zip-file "fileb://$ZIP_PATH"
  fi
}

create_func() {
  if [[ "$ZIP_SIZE_BYTES" -gt 52428800 ]]; then
    echo "Zip exceeds 50MB, uploading to s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws s3 cp "$ZIP_PATH" "s3://$DEPLOY_BUCKET/$DEPLOY_KEY"
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --runtime "$RUNTIME" --handler "$HANDLER" --role "$LAMBDA_ROLE_ARN" \
      --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --s3-bucket "$DEPLOY_BUCKET" --s3-key "$DEPLOY_KEY"
  else
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --runtime "$RUNTIME" --handler "$HANDLER" --role "$LAMBDA_ROLE_ARN" \
      --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --zip-file "fileb://$ZIP_PATH"
  fi
}

# Create or update Lambda
if aws lambda get-function --function-name "$FUNCTION_NAME" 2>/dev/null; then
  echo "Updating existing Lambda $FUNCTION_NAME..."
  update_code "$FUNCTION_NAME"
else
  echo "Creating Lambda $FUNCTION_NAME..."
  if [[ -z "${LAMBDA_ROLE_ARN:-}" ]]; then
    echo "Error: Set LAMBDA_ROLE_ARN (execution role with S3 + CloudWatch permissions)."
    exit 1
  fi
  create_func
fi

echo "Done. Invoke with:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"bucket\":\"fide-glicko\",\"output_prefix\":\"data\"}' out.json && cat out.json"

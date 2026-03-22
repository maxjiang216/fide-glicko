#!/usr/bin/env bash
# Create the shared ECR repo for all SAM container Lambdas (name must match samconfig.toml image_repositories).
# Apply lifecycle policy from infra/ecr-lifecycle-policy.json.
# Apply repository policy so lambda.amazonaws.com can pull images (required for container Lambdas).
# Policy: only when 2+ images exist, expire oldest until one remains. One image per repo is kept
# forever (no time limit), so Lambdas still work after you stop deploying.
# Run once per account/region before the first deploy that uses these URIs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REGION="${AWS_REGION:-us-east-1}"
POLICY_FILE="$REPO_ROOT/infra/ecr-lifecycle-policy.json"
PULL_POLICY_TEMPLATE="$REPO_ROOT/infra/ecr-lambda-pull-policy.json"
# Match samconfig / image_repositories account unless overridden (no sts: call — deploy role may lack it)
ACCOUNT_ID="${AWS_ACCOUNT_ID:-710271917035}"
TMP_PULL_POLICY="$(mktemp)"
trap 'rm -f "$TMP_PULL_POLICY"' EXIT

sed -e "s/__REGION__/${REGION}/g" -e "s/__ACCOUNT__/${ACCOUNT_ID}/g" \
  "$PULL_POLICY_TEMPLATE" >"$TMP_PULL_POLICY"

REPOS=(
  "fideglicko/lambda-data"
)

for name in "${REPOS[@]}"; do
  if aws ecr describe-repositories --repository-names "$name" --region "$REGION" &>/dev/null; then
    echo "exists: $name"
  else
    echo "creating: $name"
    aws ecr create-repository --repository-name "$name" --region "$REGION" >/dev/null
  fi
  aws ecr put-lifecycle-policy \
    --repository-name "$name" \
    --region "$REGION" \
    --lifecycle-policy-text "file://${POLICY_FILE}" >/dev/null
  aws ecr set-repository-policy \
    --repository-name "$name" \
    --region "$REGION" \
    --policy-text "file://${TMP_PULL_POLICY}" >/dev/null
  echo "lifecycle + Lambda pull policy applied: $name"
done

echo "Done. Repos are ready for sam deploy."

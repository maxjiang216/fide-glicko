#!/usr/bin/env bash
# Create ECR repositories for SAM image-based Lambdas (names must match samconfig.toml).
# Apply lifecycle policy from infra/ecr-lifecycle-policy.json.
# Policy: only when 2+ images exist, expire oldest until one remains. One image per repo is kept
# forever (no time limit), so Lambdas still work after you stop deploying.
# Run once per account/region before the first deploy that uses these URIs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REGION="${AWS_REGION:-us-east-1}"
POLICY_FILE="$REPO_ROOT/infra/ecr-lifecycle-policy.json"

REPOS=(
  "fideglicko/player-list"
  "fideglicko/details-chunk"
  "fideglicko/reports-chunk"
  "fideglicko/merge-chunks"
  "fideglicko/validate"
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
  echo "lifecycle policy applied: $name"
done

echo "Done. Repos are ready for sam deploy."

#!/usr/bin/env bash
# One-time bootstrap: attach infra/github-deploy-policy.json as inline policy "GithubDeploy"
# on the GitHub OIDC deploy role, when the role cannot yet update itself via CI.
#
# After this, every "Sync deploy policy from repo" step in .github/workflows/deploy-sam.yml
# keeps the same document in sync from the repo (no manual runs needed for routine changes).
#
# Requires: AWS credentials allowed to call iam:PutRolePolicy on the target role (e.g. console admin).
#
# Usage:
#   AWS_PROFILE=your-admin-profile bash scripts/bootstrap_github_deploy_policy.sh
#   GITHUB_DEPLOY_ROLE_NAME=my-role-name bash scripts/bootstrap_github_deploy_policy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ROLE_NAME="${GITHUB_DEPLOY_ROLE_NAME:-github-fide-glicko-deploy}"
POLICY_DOC="$REPO_ROOT/infra/github-deploy-policy.json"

if [[ ! -f "$POLICY_DOC" ]]; then
  echo "Missing $POLICY_DOC" >&2
  exit 1
fi

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name GithubDeploy \
  --policy-document "file://${POLICY_DOC}"

echo "OK: inline policy GithubDeploy applied to role $ROLE_NAME"

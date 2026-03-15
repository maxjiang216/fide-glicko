"""Validate GitHub deploy policy includes permissions required for SAM/CloudFormation.

Catches deploy failures from missing IAM permissions before they hit CI:
- lambda:TagResource / lambda:UntagResource (CloudFormation tags Lambda functions)
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
POLICY_PATH = REPO_ROOT / "infra" / "github-deploy-policy.json"

# Actions CloudFormation/SAM needs for Lambda create/update.
# Add here when a deploy fails with AccessDenied on a new action.
REQUIRED_LAMBDA_ACTIONS = {
    "lambda:CreateFunction",
    "lambda:DeleteFunction",
    "lambda:GetFunction",
    "lambda:GetFunctionConfiguration",
    "lambda:UpdateFunctionCode",
    "lambda:UpdateFunctionConfiguration",
    "lambda:TagResource",
    "lambda:UntagResource",
}


def _load_policy() -> dict:
    with open(POLICY_PATH) as f:
        return json.load(f)


def _get_lambda_statement_actions(policy: dict) -> set[str]:
    for stmt in policy.get("Statement", []):
        if stmt.get("Sid") == "Lambda":
            actions = stmt.get("Action", [])
            return set(actions) if isinstance(actions, list) else {actions}
    return set()


def test_github_deploy_policy_includes_required_lambda_actions() -> None:
    """Lambda statement must include all actions needed for SAM deploy."""
    policy = _load_policy()
    allowed = _get_lambda_statement_actions(policy)
    missing = REQUIRED_LAMBDA_ACTIONS - allowed
    assert not missing, (
        f"Missing Lambda actions in infra/github-deploy-policy.json: {sorted(missing)}. "
        "These cause 403 AccessDenied during sam deploy. Add them to the Lambda statement."
    )

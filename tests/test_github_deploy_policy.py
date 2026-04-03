"""Validate GitHub deploy policy includes permissions required for SAM/CloudFormation.

Catches deploy failures from missing IAM permissions before they hit CI:
- lambda:TagResource / lambda:UntagResource (CloudFormation tags Lambda functions)
- states:TagResource / states:UntagResource (CloudFormation tags Step Functions state machines)
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

# Same for Step Functions (SAM applies stack tags to the state machine resource).
REQUIRED_STEP_FUNCTIONS_ACTIONS = {
    "states:CreateStateMachine",
    "states:UpdateStateMachine",
    "states:DeleteStateMachine",
    "states:DescribeStateMachine",
    "states:TagResource",
    "states:UntagResource",
}

STEP_FUNCTIONS_RESOURCE = (
    "arn:aws:states:us-east-1:710271917035:stateMachine:fide-glicko-pipeline"
)


def _load_policy() -> dict:
    with open(POLICY_PATH) as f:
        return json.load(f)


def _get_statement_actions(policy: dict, sid: str) -> set[str]:
    for stmt in policy.get("Statement", []):
        if stmt.get("Sid") == sid:
            actions = stmt.get("Action", [])
            return set(actions) if isinstance(actions, list) else {actions}
    return set()


def _get_statement(policy: dict, sid: str) -> dict:
    for stmt in policy.get("Statement", []):
        if stmt.get("Sid") == sid:
            return stmt
    return {}


def test_github_deploy_policy_includes_required_lambda_actions() -> None:
    """Lambda statement must include all actions needed for SAM deploy."""
    policy = _load_policy()
    allowed = _get_statement_actions(policy, "Lambda")
    missing = REQUIRED_LAMBDA_ACTIONS - allowed
    assert not missing, (
        f"Missing Lambda actions in infra/github-deploy-policy.json: {sorted(missing)}. "
        "These cause 403 AccessDenied during sam deploy. Add them to the Lambda statement."
    )


def test_github_deploy_policy_includes_required_step_functions_actions() -> None:
    """Step Functions statement must include tagging (CFN applies stack tags on create/update)."""
    policy = _load_policy()
    allowed = _get_statement_actions(policy, "StepFunctions")
    missing = REQUIRED_STEP_FUNCTIONS_ACTIONS - allowed
    assert not missing, (
        f"Missing Step Functions actions in infra/github-deploy-policy.json: {sorted(missing)}. "
        "These cause 403 AccessDenied when CloudFormation tags the state machine. "
        "Add them to the StepFunctions statement."
    )


def test_github_deploy_policy_scopes_step_functions_to_pipeline_state_machine() -> None:
    """Step Functions statement should stay scoped to the fide-glicko pipeline state machine."""
    policy = _load_policy()
    statement = _get_statement(policy, "StepFunctions")
    assert statement.get("Resource") == STEP_FUNCTIONS_RESOURCE, (
        "StepFunctions statement in infra/github-deploy-policy.json should stay scoped to "
        f"{STEP_FUNCTIONS_RESOURCE!r}."
    )

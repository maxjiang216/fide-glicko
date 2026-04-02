"""Static validation for template.yaml CloudFormation/SAM template.

Catches deploy failures before they reach CI:
- Duplicate FunctionName values (two logical IDs cannot share a physical name)
- FunctionName moved to a new logical ID (rename conflict: CF refuses to create a
  resource with a name it already owns under a different ID, even mid-changeset)
"""

import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = REPO_ROOT / "template.yaml"
LAMBDA_TYPES = {"AWS::Lambda::Function", "AWS::Serverless::Function"}


def _cf_constructor(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node):
    """Treat all CloudFormation intrinsic function tags as plain Python values."""
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    return loader.construct_mapping(node, deep=True)


def _load_template(text: str) -> dict:
    loader_cls = type("CFLoader", (yaml.SafeLoader,), {})
    yaml.add_multi_constructor("!", _cf_constructor, Loader=loader_cls)
    return yaml.load(text, Loader=loader_cls)


def _function_names(template: dict) -> dict[str, str]:
    """Return {FunctionName: logical_id} for Lambdas with explicit string names."""
    result = {}
    for logical_id, resource in template.get("Resources", {}).items():
        if resource.get("Type") in LAMBDA_TYPES:
            fn_name = resource.get("Properties", {}).get("FunctionName")
            if isinstance(fn_name, str):
                result[fn_name] = logical_id
    return result


def test_no_duplicate_function_names() -> None:
    """No two logical IDs in the template may use the same FunctionName.

    CloudFormation cannot manage two resources with the same physical name,
    even temporarily during a changeset.
    """
    template = _load_template(TEMPLATE_PATH.read_text())
    seen: dict[str, str] = {}
    duplicates: list[str] = []
    for logical_id, resource in template.get("Resources", {}).items():
        if resource.get("Type") in LAMBDA_TYPES:
            fn_name = resource.get("Properties", {}).get("FunctionName")
            if not isinstance(fn_name, str):
                continue
            if fn_name in seen:
                duplicates.append(
                    f"  {fn_name!r}: used by both {seen[fn_name]!r} and {logical_id!r}"
                )
            else:
                seen[fn_name] = logical_id
    assert not duplicates, (
        "Duplicate FunctionNames in template.yaml — CloudFormation cannot manage two "
        "resources with the same physical name:\n" + "\n".join(duplicates)
    )


def test_no_function_name_logical_id_rename() -> None:
    """Detect when a FunctionName is moved from one logical ID to another.

    CloudFormation tracks physical resource names per logical ID. Renaming a
    logical ID while keeping the same FunctionName causes the changeset to fail
    with "already exists in stack" because CF tries to create-before-delete and
    its own internal ownership check rejects the duplicate claim.

    Compares the current template against the previous git commit. Skipped when
    there is no prior commit (e.g. initial repo setup).
    """
    result = subprocess.run(
        ["git", "show", "HEAD~1:template.yaml"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    if result.returncode != 0:
        pytest.skip("No previous commit to compare against")

    prev_names = _function_names(_load_template(result.stdout))
    curr_names = _function_names(_load_template(TEMPLATE_PATH.read_text()))

    renames = {
        fn_name: (old_lid, new_lid)
        for fn_name, new_lid in curr_names.items()
        if fn_name in prev_names and (old_lid := prev_names[fn_name]) != new_lid
    }

    assert not renames, (
        "FunctionName → logical ID renames detected in template.yaml.\n"
        "CloudFormation refuses to create a resource with a name it already owns "
        "under a different logical ID, even within the same changeset.\n"
        "Affected FunctionNames (old logical ID → new):\n"
        + "\n".join(
            f"  {fn!r}: {old!r} → {new!r}" for fn, (old, new) in renames.items()
        )
        + "\n\nTo deploy this rename: delete the CloudFormation stack first, or use "
        "a two-step deploy (rename FunctionName first, then rename the logical ID)."
    )

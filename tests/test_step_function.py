"""Static validation for Step Function pipeline ASL.

Catches issues that cause runtime failures:
- $$.Execution.Input.<field> where field can be defaulted (e.g. run_name via AddDefaultRunName)
- Fail state using Cause.$ instead of CausePath
"""

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_PATH = REPO_ROOT / "infra" / "step-function" / "pipeline.asl.json"

DEFAULTED_FIELDS = {"run_name"}


def _find_defaulted_fields(definition: dict) -> set[str]:
    defaulted = set()
    states = definition.get("States", {})
    for name, state in states.items():
        if state.get("Type") == "Pass":
            params = state.get("Parameters", {})
            for key in params:
                if not key.endswith(".$") and key in DEFAULTED_FIELDS:
                    defaulted.add(key)
        elif state.get("Type") == "Choice":
            for choice in state.get("Choices", []):
                var = choice.get("Variable", "")
                if "IsPresent" in choice and var == "$.run_name":
                    defaulted.add("run_name")
    return defaulted


def _find_execution_input_refs(obj: object, path: str = "") -> list[tuple[str, str]]:
    refs = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith(".$") and isinstance(v, str):
                for m in re.finditer(r"\$\$\.Execution\.Input\.(\w+)", v):
                    refs.append((f"{path}.{k}" if path else k, m.group(1)))
            else:
                refs.extend(_find_execution_input_refs(v, f"{path}.{k}" if path else k))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            refs.extend(_find_execution_input_refs(item, f"{path}[{i}]"))
    return refs


def _find_fail_cause_dollar(obj: object, path: str = "") -> list[str]:
    locations = []
    if isinstance(obj, dict):
        if obj.get("Type") == "Fail" and "Cause.$" in obj:
            locations.append(path or "root")
        for k, v in obj.items():
            locations.extend(_find_fail_cause_dollar(v, f"{path}.{k}" if path else k))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            locations.extend(_find_fail_cause_dollar(item, f"{path}[{i}]"))
    return locations


def _load_pipeline() -> dict:
    with open(PIPELINE_PATH) as f:
        return json.load(f)


def test_no_fail_state_uses_cause_dollar() -> None:
    """Fail states must use CausePath, not Cause.$, for deploy compatibility."""
    definition = _load_pipeline()
    locations = _find_fail_cause_dollar(definition)
    assert (
        not locations
    ), f"Fail states use Cause.$ but Step Functions requires CausePath for dynamic values: {locations}"


def test_no_execution_input_refs_to_defaulted_fields() -> None:
    """ItemSelector must not use $$.Execution.Input for fields defaulted by AddDefaultRunName."""
    definition = _load_pipeline()
    defaulted = _find_defaulted_fields(definition)
    violations = []
    for path, field in _find_execution_input_refs(definition):
        if field in defaulted:
            violations.append(
                f"{path}: References $$.Execution.Input.{field} but '{field}' can be omitted "
                "(defaulted by AddDefaultRunName). Use Map.Item.Value or another source."
            )
    assert not violations, "\n".join(violations)

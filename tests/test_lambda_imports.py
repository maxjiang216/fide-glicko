"""Verify Lambda handlers can import (catches missing deps). Uses SAM build artifacts."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# (SAM logical ID, handler_module)
# Only "light" Lambdas (no pandas/pyarrow) - those with pandas have ABI issues when
# tested with local Python 3.13 vs Lambda 3.12; they are validated by deploy workflow.
LAMBDA_CONFIGS = [
    ("FederationsFunction", "handlers.federations"),
    ("TournamentsFunction", "handlers.tournaments"),
    ("SplitIdsFunction", "handlers.split_ids"),
]


@pytest.mark.parametrize("logical_id,handler_module", LAMBDA_CONFIGS)
def test_lambda_handler_imports(logical_id, handler_module):
    """Build with SAM and verify handler module imports without ImportError."""
    build_dir = REPO_ROOT / ".aws-sam" / "build" / logical_id
    if not build_dir.exists():
        try:
            subprocess.run(
                ["bash", str(REPO_ROOT / "scripts" / "prepare_functions.sh")],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
            )
            result = subprocess.run(
                ["sam", "build"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            pytest.skip("SAM CLI not installed; run: pip install aws-sam-cli")
        if result.returncode != 0:
            err = str(result.stderr or result.stdout or "")
            if result.returncode == 127 or "not found" in err.lower():
                pytest.skip("SAM CLI not installed; run: pip install aws-sam-cli")
            raise AssertionError(
                f"sam build failed:\nstdout={result.stdout}\nstderr={result.stderr}"
            )

    assert build_dir.exists(), f"Expected build at {build_dir}; run sam build first"

    # Run import in isolated subprocess (clean env, no repo path)
    code = f"""
import sys
sys.path.insert(0, {str(build_dir)!r})
mod = __import__({handler_module!r}, fromlist=["lambda_handler"])
assert hasattr(mod, "lambda_handler")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Failed to import {handler_module} from {logical_id}:\n"
        f"stdout={result.stdout}\nstderr={result.stderr}\n"
        f"Ensure all transitive deps are in requirements.txt."
    )

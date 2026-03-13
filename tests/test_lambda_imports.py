"""Verify each Lambda package can import its handler (catches missing deps like raw_utils)."""

import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# (deploy_script_rel, zip_name, handler_module)
# handler_module: Python module to import (e.g. handlers.tournaments); must have lambda_handler
# Only "light" Lambdas (no pandas/numpy) - those with pandas have ABI issues when tested with
# local Python 3.13 vs Lambda 3.12; they are validated by deploy workflows.
LAMBDA_CONFIGS = [
    (
        "infra/deploy_federations_lambda.sh",
        "federations_lambda.zip",
        "handlers.federations",
    ),
    (
        "infra/deploy_tournaments_lambda.sh",
        "tournaments_lambda.zip",
        "handlers.tournaments",
    ),
    ("infra/deploy_split_ids_lambda.sh", "split_ids_lambda.zip", "handlers.split_ids"),
]


@pytest.mark.parametrize("deploy_script,zip_name,handler_module", LAMBDA_CONFIGS)
def test_lambda_handler_imports(deploy_script, zip_name, handler_module):
    """Build Lambda zip and verify handler module imports without ImportError."""
    script_path = REPO_ROOT / deploy_script
    zip_path = REPO_ROOT / "build" / zip_name

    # Build zip (--zip-only, no AWS)
    result = subprocess.run(
        ["bash", str(script_path), "--zip-only"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert (
        result.returncode == 0
    ), f"{deploy_script} failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    assert zip_path.exists(), f"Expected zip at {zip_path}"

    # Extract to a unique temp dir (avoids path pollution from shared dir / pandas/numpy quirks)
    with tempfile.TemporaryDirectory() as tmp:
        extracted = Path(tmp)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extracted)

        # Run import in isolated subprocess to mimic Lambda (clean env, no repo path)
        code = f"""
import sys
sys.path.insert(0, {str(extracted)!r})
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
            f"Failed to import {handler_module} from {zip_name}:\n"
            f"stdout={result.stdout}\nstderr={result.stderr}\n"
            f"Ensure all transitive deps (e.g. raw_utils) are in the deploy script."
        )

"""Verify Lambda handlers can import (catches missing deps). Uses SAM build artifacts."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Handler modules for container (Docker) Lambdas. Verifies scraper modules at /var/task/.
IMAGE_HANDLERS = [
    "handlers.player_list",
    "handlers.details_chunk",
    "handlers.reports_chunk",
    "handlers.merge_chunks",
    "handlers.validate",
]

# (SAM logical ID, handler_module)
# Only "light" Lambdas (no pandas/pyarrow) - those with pandas have ABI issues when
# tested with local Python 3.13 vs Lambda 3.12; they are validated by deploy workflow.
LAMBDA_CONFIGS = [
    ("EnsureRunNameFunction", "handlers.ensure_run_name"),
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


def test_docker_image_handler_imports():
    """Build Docker image and verify all image handlers can import.

    Catches missing scraper modules at /var/task/ (e.g. No module named 'get_player_list').
    Uses the same Dockerfile and context as SAM deploy.
    """
    result = subprocess.run(["docker", "version"], capture_output=True)
    if result.returncode != 0:
        pytest.skip("Docker not available; run in environment with Docker")

    image_tag = "fide-glicko-data:import-test"

    build_result = subprocess.run(
        ["docker", "build", "-f", "docker/Dockerfile", "-t", image_tag, "."],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert (
        build_result.returncode == 0
    ), f"Docker build failed:\n{build_result.stdout}\n{build_result.stderr}"

    import_lines = "\n".join(f"import {m}" for m in IMAGE_HANDLERS)
    run_result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "python3",
            image_tag,
            "-c",
            f"{import_lines}\nprint('ok')",
        ],
        capture_output=True,
        text=True,
    )
    assert run_result.returncode == 0, (
        f"Failed to import image handlers in container:\n"
        f"stdout={run_result.stdout}\nstderr={run_result.stderr}\n"
        f"Ensure scraper modules are copied to /var/task/ in Dockerfile."
    )


# ---------------------------------------------------------------------------
# ZIP Lambdas: simulate their real dependency set (prepare_functions.sh REQS +
# boto3, which the Lambda runtime provides). Imports the handler AND every
# local module it imports lazily inside lambda_handler, with all other
# third-party packages blocked. Catches e.g. a ZIP handler pulling in pyarrow.
# ---------------------------------------------------------------------------
import ast
import re

THIRD_PARTY = {
    "aiohttp",
    "bs4",
    "numpy",
    "pandas",
    "pyarrow",
    "requests",
    "tqdm",
    "lxml",
}
LOCAL_DIRS = [REPO_ROOT / "src" / "scraper", REPO_ROOT / "handlers"]


def _zip_reqs() -> dict[str, set[str]]:
    text = (REPO_ROOT / "scripts" / "prepare_functions.sh").read_text()
    out = {}
    for name, body in re.findall(r'REQS\[(\w+)\]="([^"]*)"', text):
        pkgs = {re.split(r"[<>=]", line)[0].strip() for line in body.splitlines()}
        pkgs = {"bs4" if p == "beautifulsoup4" else p for p in pkgs if p}
        out[name] = pkgs
    return out


def _local(name: str) -> bool:
    top = name.split(".")[0]
    return any((d / f"{top}.py").exists() for d in LOCAL_DIRS)


def _all_imports(path: Path) -> set[str]:
    """Every module imported anywhere in the file (including inside functions)."""
    mods = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            mods.add(node.module.split(".")[0])
    return mods


def _module_path(mod: str) -> Path | None:
    for d in LOCAL_DIRS:
        if (d / f"{mod}.py").exists():
            return d / f"{mod}.py"
    return None


def _modules_used(handler_file: Path) -> set[str]:
    """Local modules reachable from the handler: all imports of the handler and,
    transitively, all imports of the local modules it uses."""
    seen, todo = set(), [m for m in _all_imports(handler_file) if _local(m)]
    while todo:
        m = todo.pop()
        if m in seen:
            continue
        seen.add(m)
        p = _module_path(m)
        if p:
            todo += [x for x in _all_imports(p) if _local(x) and x not in seen]
    return seen


ZIP_HANDLERS = {
    "federations": "handlers.federations",
    "tournaments": "handlers.tournaments",
    "split_ids": "handlers.split_ids",
    "ensure_run_name": "handlers.ensure_run_name",
    "orchestrator": "handlers.orchestrator",
}

# Modules that are reachable through optional/lazy code paths the function never
# runs (e.g. chunk_worker's details/reports adapters, used only by the Docker
# chunk Lambdas). Each entry must say why it is safe to skip.
NOT_USED_AT_RUNTIME = {
    "tournaments": {
        "get_tournament_details",  # chunk_worker details adapter (Docker only)
        "get_tournament_reports",  # chunk_worker reports adapter (Docker only)
        "coverage",  # chunk_worker no longer imports it; reached via reports path
        "split_tournament_ids",
        "get_player_list",
    },
}


@pytest.mark.parametrize("name,handler", sorted(ZIP_HANDLERS.items()))
def test_zip_lambda_dependencies(name, handler):
    allowed = _zip_reqs().get(name, set())
    blocked = sorted(THIRD_PARTY - allowed)
    handler_file = REPO_ROOT / (handler.replace(".", "/") + ".py")
    mods = sorted(_modules_used(handler_file) - NOT_USED_AT_RUNTIME.get(name, set()))
    code = f"""
import sys, importlib.abc
BLOCKED = set({blocked!r})
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise ImportError(f"{{fullname}} is not installed in this ZIP Lambda")
sys.meta_path.insert(0, Block())
sys.path[:0] = [{str(REPO_ROOT)!r}, {str(REPO_ROOT / "src" / "scraper")!r}]
import {handler}
for m in {mods!r}:
    if m in ("chunk_common", "lambda_logging", "pipeline_ssm") or m in {list(ZIP_HANDLERS)!r}:
        __import__("handlers." + m)
    else:
        __import__(m)
"""
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert (
        r.returncode == 0
    ), f"{handler} (ZIP, deps={sorted(allowed)}) cannot import its code:\n{r.stderr}"

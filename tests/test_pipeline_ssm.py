"""Tests for SSM-backed pipeline defaults (ensure_run_name + pipeline_ssm)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import handlers.ensure_run_name as ensure_run_name  # noqa: E402


def test_load_ssm_skips_when_env_unset(monkeypatch):
    monkeypatch.delenv("PIPELINE_CONFIG_SSM_PARAM", raising=False)
    from handlers.pipeline_ssm import load_pipeline_config_from_ssm

    assert load_pipeline_config_from_ssm() == {}


def test_ensure_run_name_applies_ssm_when_key_not_in_input(monkeypatch):
    monkeypatch.setattr(
        ensure_run_name,
        "load_pipeline_config_from_ssm",
        lambda: {
            "chunk_size": 150,
            "max_concurrency": 8,
            "details_rate_limit": 0.4,
            "reports_rate_limit": 0.25,
        },
    )
    out = ensure_run_name.lambda_handler(
        {"input": {"year": 2025, "month": 3, "run_type": "prod"}},
        None,
    )
    assert out["chunk_size"] == 150
    assert out["max_concurrency"] == 8
    assert out["details_rate_limit"] == 0.4
    assert out["reports_rate_limit"] == 0.25


def test_ensure_run_name_execution_input_overrides_ssm(monkeypatch):
    monkeypatch.setattr(
        ensure_run_name,
        "load_pipeline_config_from_ssm",
        lambda: {"chunk_size": 150, "max_concurrency": 10},
    )
    out = ensure_run_name.lambda_handler(
        {
            "input": {
                "year": 2025,
                "month": 3,
                "run_type": "prod",
                "chunk_size": 200,
            }
        },
        None,
    )
    assert out["chunk_size"] == 200
    assert out["max_concurrency"] == 10


def test_ensure_run_name_code_defaults_when_no_ssm(monkeypatch):
    monkeypatch.setattr(ensure_run_name, "load_pipeline_config_from_ssm", lambda: {})
    out = ensure_run_name.lambda_handler(
        {"input": {"year": 2025, "month": 3, "run_type": "prod"}},
        None,
    )
    assert out["chunk_size"] == 300
    assert out["max_concurrency"] == 5
    assert out["details_rate_limit"] == 0.33
    assert out["reports_rate_limit"] == 0.33


def test_load_ssm_skips_without_lambda_env_even_if_param_set(monkeypatch):
    """SSM is only read in Lambda (Step Functions); not from local scripts."""
    monkeypatch.setenv("PIPELINE_CONFIG_SSM_PARAM", "/test/config")
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    from handlers.pipeline_ssm import load_pipeline_config_from_ssm

    assert load_pipeline_config_from_ssm() == {}


def test_load_ssm_invalid_json_raises(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fide-glicko-ensure-run-name")
    monkeypatch.setenv("PIPELINE_CONFIG_SSM_PARAM", "/test/config")

    class FakeSSM:
        def get_parameter(self, Name: str):
            return {"Parameter": {"Value": "not json"}}

    monkeypatch.setattr("handlers.pipeline_ssm._get_ssm_client", lambda: FakeSSM())

    from handlers.pipeline_ssm import load_pipeline_config_from_ssm

    with pytest.raises(ValueError, match="valid JSON"):
        load_pipeline_config_from_ssm()

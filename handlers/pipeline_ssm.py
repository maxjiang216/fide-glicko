"""Load optional pipeline tuning from SSM Parameter Store (JSON object).

Only used when this code runs inside **AWS Lambda** (e.g. EnsureRunName in Step
Functions). Local scripts and tests do not set ``AWS_LAMBDA_FUNCTION_NAME``, so
SSM is never read and boto3 is not required for configuration.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Keys merged from SSM when not set on the execution input (see ensure_run_name).
PIPELINE_CONFIG_KEYS = (
    "chunk_size",
    "max_concurrency",
    "tournaments_max_concurrency",
    "details_rate_limit",
    "reports_rate_limit",
)

_ssm_client = None


def _get_ssm_client():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def load_pipeline_config_from_ssm() -> dict[str, Any]:
    """
    Read JSON from PIPELINE_CONFIG_SSM_PARAM (String parameter).

    Returns {} unless running in Lambda (``AWS_LAMBDA_FUNCTION_NAME`` is set).
    Also returns {} if the parameter name env is unset, parameter is missing, or
    the filtered object is empty. Raises ValueError if the parameter exists but
    JSON is invalid.
    """
    if not os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        return {}
    name = os.environ.get("PIPELINE_CONFIG_SSM_PARAM", "").strip()
    if not name:
        return {}
    try:
        resp = _get_ssm_client().get_parameter(Name=name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ParameterNotFound":
            logger.info(
                "SSM pipeline config not found at %s; using code defaults", name
            )
            return {}
        logger.exception("Failed to read SSM parameter %s", name)
        raise
    raw = resp["Parameter"]["Value"]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"SSM parameter {name} must contain valid JSON object: {e}"
        ) from e
    if not isinstance(data, dict):
        raise ValueError(f"SSM parameter {name} must be a JSON object")
    return {k: data[k] for k in PIPELINE_CONFIG_KEYS if k in data}

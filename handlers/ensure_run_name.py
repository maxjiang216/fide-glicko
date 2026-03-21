"""
Lambda handler to normalize run_name before pipeline execution.

For prod: run_name is always derived from year-month (YYYY-MM) to avoid repeat runs.
For custom: run_name is required (user-defined).
For test: run_name defaults to "test" when omitted.

When the handler runs in **AWS Lambda** (Step Functions), optional tuning is
merged from SSM (``PIPELINE_CONFIG_SSM_PARAM``) for any key omitted from the
execution input: chunk_size, max_concurrency, tournaments_max_concurrency,
details_rate_limit, reports_rate_limit. Local runs do not read SSM.
Precedence: execution input > SSM JSON > code defaults below.

Event shape (passthrough from execution input):
{
    "year": 2025,
    "month": 3,
    "run_type": "prod",
    "run_name": null,
    "bucket": "fide-glicko",
    "override": false,
    "max_concurrency": 5,
    "chunk_size": 300,
    "details_rate_limit": 0.33,
    "reports_rate_limit": 0.33
}

Returns the same input with run_name set. Fails with 400 if validation fails.
"""

from .pipeline_ssm import load_pipeline_config_from_ssm


def lambda_handler(event: dict, context) -> dict:
    """Normalize run_name and return full passthrough for pipeline."""
    # Step Function passes full state as {"input": {...}}
    data = event.get("input", event)
    raw_keys = set(data.keys())
    run_type = data.get("run_type", "custom")
    run_name = data.get("run_name")
    year = data.get("year")
    month = data.get("month")

    if run_type not in ("prod", "custom", "test"):
        raise ValueError(
            f"run_type must be one of prod, custom, test (got {run_type!r})"
        )

    if run_type == "prod":
        if year is None or month is None:
            raise ValueError("year and month are required for prod runs")
        run_name = f"{int(year)}-{int(month):02d}"
    elif run_type == "custom":
        if not run_name:
            raise ValueError("run_name is required for custom runs")
    else:
        run_name = run_name or "test"

    out = dict(data)
    out["run_name"] = run_name

    ssm_config = load_pipeline_config_from_ssm()
    for key, value in ssm_config.items():
        if key not in raw_keys:
            out[key] = value

    # Apply defaults for optional fields (SSM and execution may omit keys)
    out.setdefault("bucket", "fide-glicko")
    out.setdefault("override", False)
    out.setdefault("chunk_size", 300)
    out.setdefault("max_concurrency", 5)
    out.setdefault("details_rate_limit", 0.33)
    out.setdefault("reports_rate_limit", 0.33)
    # Optional; Tournaments Lambda uses default 1 if null (avoid JSONPath missing-key errors)
    if "tournaments_max_concurrency" not in out:
        out["tournaments_max_concurrency"] = None
    return out

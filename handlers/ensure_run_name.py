"""
Lambda handler to normalize run_name before pipeline execution.

For prod: run_name is always derived from year-month (YYYY-MM) to avoid repeat runs.
For custom: run_name is required (user-defined).
For test: run_name defaults to "test" when omitted.

Event shape (passthrough from execution input):
{
    "year": 2025,
    "month": 3,
    "run_type": "prod",
    "run_name": null,
    "bucket": "fide-glicko",
    "override": false,
    "max_concurrency": 10,
    "chunk_size": 300
}

Returns the same input with run_name set. Fails with 400 if validation fails.
"""


def lambda_handler(event: dict, context) -> dict:
    """Normalize run_name and return full passthrough for pipeline."""
    # Step Function passes full state as {"input": {...}}
    data = event.get("input", event)
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
    # Apply defaults for optional fields
    out.setdefault("bucket", "fide-glicko")
    out.setdefault("override", False)
    out.setdefault("chunk_size", 300)
    return out

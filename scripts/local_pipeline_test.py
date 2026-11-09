#!/usr/bin/env python3
"""
Run the real Step Functions pipeline locally, end to end, before deploying.

Starts:
  - `sam local start-lambda` (the built Lambdas from .aws-sam/build, run in the
    Lambda runtime containers, using your local AWS credentials for S3/SSM),
  - AWS Step Functions Local (Docker image amazon/aws-stepfunctions-local),
then loads infra/step-function/pipeline.asl.json with each function ARN pointed
at the local Lambda, starts one execution, and prints every state transition
with the loop counters (remaining/progress), retries and failures.

Prerequisites: Docker, SAM CLI, `bash scripts/prepare_functions.sh && sam build`.

Writes to S3 under the run's prefix like a real run (use run_type=custom), and
like a real run the Federations/PlayerList steps may refresh the shared lists
if they are stale. Real requests go to FIDE at the configured rate.

Example (3 small federations, a few dozen tournaments):
  uv run scripts/local_pipeline_test.py --year 2015 --month 6 \\
      --run-name localtest-2015-06 \\
      --federations-s3-uri s3://fide-glicko/custom/localtest-feds/federations.csv
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

import boto3
import yaml

REPO = Path(__file__).resolve().parent.parent
ASL = REPO / "infra" / "step-function" / "pipeline.asl.json"
TEMPLATE = REPO / "template.yaml"


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# Picked at startup so a port already used by another program can't be
# mistaken for our local Lambda service.
SFN_PORT, LAMBDA_PORT = _free_port(), _free_port()
FAKE_ACCOUNT = "123456789012"
SFN_CONTAINER = "fide-glicko-sfn-local"


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        f"!{suffix} {loader.construct_scalar(node)}"
        if isinstance(node, yaml.ScalarNode)
        else None
    ),
)


def substitutions() -> dict[str, str]:
    """ARN placeholder -> local function ARN (named by SAM logical ID)."""
    tpl = yaml.load(TEMPLATE.read_text(), Loader=_Loader)
    subs = tpl["Resources"]["PipelineStateMachine"]["Properties"][
        "DefinitionSubstitutions"
    ]
    out = {}
    for key, val in subs.items():
        m = re.match(r"!GetAtt (\w+)\.Arn", str(val))
        if not m:
            raise SystemExit(f"Unexpected substitution {key}: {val}")
        out[key] = f"arn:aws:lambda:us-east-1:{FAKE_ACCOUNT}:function:{m.group(1)}"
    return out


def definition() -> str:
    text = ASL.read_text()
    for key, arn in substitutions().items():
        text = text.replace("${" + key + "}", arn)
    left = re.findall(r"\$\{(\w+)\}", text)
    if left:
        raise SystemExit(f"Unsubstituted placeholders: {left}")
    return text


def local_compat(text: str) -> str:
    """
    Step Functions Local (last image ~2 years old) rejects some newer ASL
    fields that real AWS accepts. Rewrite them to older equivalents for the
    local run only; the real definition is checked separately with AWS's
    ValidateStateMachineDefinition (see --skip-aws-validate).
      - Map MaxConcurrencyPath  -> MaxConcurrency 1 (our clamped value)
      - Retry MaxDelaySeconds   -> dropped (only caps backoff waits)
    """
    d = json.loads(text)

    def walk(o):
        if isinstance(o, dict):
            if "MaxConcurrencyPath" in o:
                del o["MaxConcurrencyPath"]
                o["MaxConcurrency"] = 1
            o.pop("MaxDelaySeconds", None)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(d)
    return json.dumps(d)


def aws_validate(text: str) -> None:
    """Validate the real (unmodified) definition with AWS. Read-only."""
    r = boto3.client(
        "stepfunctions", region_name="us-east-1"
    ).validate_state_machine_definition(definition=text, type="STANDARD")
    print(f"AWS ValidateStateMachineDefinition: {r['result']}", flush=True)
    for diag in r.get("diagnostics", []):
        print("  ", diag, flush=True)
    if r["result"] != "OK":
        raise SystemExit("Real definition does not validate")


def wait_port(port: int, timeout: float = 120) -> None:
    import socket

    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(1)
    raise SystemExit(f"Port {port} did not open within {timeout}s")


def summarize_event(e: dict) -> str | None:
    t = e["type"]
    if t.endswith("StateEntered"):
        return f"→ {e['stateEnteredEventDetails']['name']}"
    if t.endswith("StateExited"):
        d = e["stateExitedEventDetails"]
        try:
            out = json.loads(d.get("output") or "{}")
        except ValueError:
            out = {}
        bits = []
        for key in ("tournaments", "details_result", "reports_result", "merge"):
            v = out.get(key) if isinstance(out, dict) else None
            if isinstance(v, dict):
                keep = {
                    k: v[k]
                    for k in (
                        "progress",
                        "remaining",
                        "failed",
                        "stopped",
                        "finalized",
                        "tournaments",
                        "skipped",
                        "rows",
                    )
                    if k in v
                }
                if keep:
                    bits.append(f"{key}={keep}")
        return f"  ✓ {d['name']}" + (f"  {'; '.join(bits)}" if bits else "")
    if t in ("TaskFailed", "LambdaFunctionFailed", "ExecutionFailed", "MapRunFailed"):
        d = next((v for k, v in e.items() if k.endswith("EventDetails")), {})
        return f"  ✗ {t}: {d.get('error')} {str(d.get('cause', ''))[:300]}"
    if t in ("TaskScheduled", "LambdaFunctionScheduled"):
        return None
    if "Retry" in t or t == "TaskTimedOut":
        return f"  ↻ {t}"
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    ap.add_argument("--run-name", required=True, help="custom run name (S3 prefix)")
    ap.add_argument("--bucket", default="fide-glicko")
    ap.add_argument("--federations-s3-uri", default=None)
    ap.add_argument("--extra", default="{}", help="JSON merged into execution input")
    ap.add_argument("--timeout-min", type=float, default=120)
    ap.add_argument(
        "--skip-aws-validate",
        action="store_true",
        help="Skip validating the real definition with AWS (needs credentials)",
    )
    args = ap.parse_args()

    if not (REPO / ".aws-sam" / "build").exists():
        raise SystemExit("Run: bash scripts/prepare_functions.sh && sam build")
    if not args.skip_aws_validate:
        aws_validate(definition())

    procs = []
    subprocess.run(["docker", "rm", "-f", SFN_CONTAINER], capture_output=True)
    try:
        print("Starting sam local start-lambda ...", flush=True)
        procs.append(
            subprocess.Popen(
                [
                    "sam",
                    "local",
                    "start-lambda",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    str(LAMBDA_PORT),
                    "--warm-containers",
                    "LAZY",
                ],
                cwd=REPO,
                stdout=open(REPO / "runs" / "local-lambda.log", "w"),
                stderr=subprocess.STDOUT,
            )
        )
        print("Starting Step Functions Local ...", flush=True)
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                SFN_CONTAINER,
                "-p",
                f"{SFN_PORT}:8083",
                "--add-host",
                "host.docker.internal:host-gateway",
                "-e",
                f"LAMBDA_ENDPOINT=http://host.docker.internal:{LAMBDA_PORT}",
                "-e",
                "AWS_DEFAULT_REGION=us-east-1",
                "-e",
                f"AWS_ACCOUNT_ID={FAKE_ACCOUNT}",
                "amazon/aws-stepfunctions-local",
            ],
            check=True,
            capture_output=True,
        )
        wait_port(LAMBDA_PORT)
        if procs[0].poll() is not None:
            raise SystemExit("sam local start-lambda exited; see runs/local-lambda.log")
        wait_port(SFN_PORT)
        time.sleep(3)

        sfn = boto3.client(
            "stepfunctions",
            endpoint_url=f"http://localhost:{SFN_PORT}",
            region_name="us-east-1",
            aws_access_key_id="local",
            aws_secret_access_key="local",
        )
        sm = sfn.create_state_machine(
            name="fide-glicko-pipeline-local",
            definition=local_compat(definition()),
            roleArn=f"arn:aws:iam::{FAKE_ACCOUNT}:role/local",
        )["stateMachineArn"]
        execution_input = {
            "year": args.year,
            "month": args.month,
            "run_type": "custom",
            "run_name": args.run_name,
            "bucket": args.bucket,
            **(
                {"federations_s3_uri": args.federations_s3_uri}
                if args.federations_s3_uri
                else {}
            ),
            **json.loads(args.extra),
        }
        print("Input:", json.dumps(execution_input), flush=True)
        ex = sfn.start_execution(stateMachineArn=sm, input=json.dumps(execution_input))[
            "executionArn"
        ]

        seen = 0
        end = time.time() + args.timeout_min * 60
        status = "RUNNING"
        while time.time() < end:
            try:
                events = sfn.get_execution_history(executionArn=ex, maxResults=1000)[
                    "events"
                ]
            except sfn.exceptions.ExecutionDoesNotExist:
                # Step Functions Local registers new executions asynchronously.
                time.sleep(2)
                continue
            for e in events[seen:]:
                line = summarize_event(e)
                if line:
                    print(line, flush=True)
            seen = len(events)
            status = sfn.describe_execution(executionArn=ex)["status"]
            if status != "RUNNING":
                break
            time.sleep(5)
        print(f"\nExecution status: {status}", flush=True)
        if status == "SUCCEEDED":
            out = json.loads(
                sfn.describe_execution(executionArn=ex).get("output") or "{}"
            )
            print(
                "Final validate:",
                json.dumps(out.get("validate", {}), default=str)[:800],
            )
        return 0 if status == "SUCCEEDED" else 1
    finally:
        subprocess.run(["docker", "rm", "-f", SFN_CONTAINER], capture_output=True)
        for p in procs:
            p.terminate()
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())

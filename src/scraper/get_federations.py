#!/usr/bin/env python3
"""
Scrape FIDE website to get the list of federations.

Supports flexible output: local path (default) or S3 URI (s3://bucket/key).
Use --output s3://bucket/key for Lambda or remote storage.
"""

import argparse
import csv
import io
import logging
import signal
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

from s3_io import (
    build_federations_data_uri,
    build_local_path_for_run,
    download_to_file,
    get_latest_in_local_prefix,
    get_latest_in_s3_prefix,
    is_s3_path,
    output_exists,
    parse_s3_uri,
    write_output,
    FEDERATIONS_DATA_PREFIX,
    STALE_DAYS,
    is_stale,
)

URL = "https://ratings.fide.com/rated_tournaments.phtml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# State for graceful shutdown
_shutdown_state = {"federations": [], "output_path": None, "completed": False}


def is_valid_federation_code(code: str) -> bool:
    """Validate federation code: 3 uppercase letters (A-Z)."""
    if not code or len(code) != 3:
        return False
    return code.isalpha() and code.isupper()


def get_federations_with_retries(
    max_retries: int = 3, retry_delay: float = 1.0
) -> List[Dict[str, str]]:
    """
    Scrape federations from FIDE website with retry logic.

    Args:
        max_retries: Maximum number of retry attempts
        retry_delay: Delay in seconds between retries

    Returns:
        List of dictionaries with 'code' and 'name' keys
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(URL, timeout=55)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            select = soup.find("select", id="select_country")
            if not select:
                logger.warning("Country selector not found; returning empty list")
                return []

            federations = []

            for option in select.find_all("option"):
                value = (option.get("value") or "").strip()
                name = option.text.strip()

                # Skip the placeholder option
                if not value or value.lower() == "all":
                    continue
                # Normalize to uppercase for validation
                code = value.upper() if len(value) == 3 else value
                if not is_valid_federation_code(code):
                    logger.warning(
                        f"Invalid federation code skipped: {value!r} ({name})"
                    )
                    continue
                federations.append({"code": code, "name": name})

            if not federations:
                logger.warning("No valid federations found in country selector")

            # CGO (Republic of Congo) not on FIDE country selector; add if missing
            codes = {f["code"] for f in federations}
            if "CGO" not in codes:
                federations.append({"code": "CGO", "name": "Republic of the Congo"})
                federations.sort(key=lambda f: f["code"])

            return federations
        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            else:
                raise


def _federations_to_csv(federations: List[Dict[str, str]]) -> str:
    """Convert federations list to CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["code", "name"])
    for fed in federations:
        writer.writerow([fed["code"], fed["name"]])
    return buf.getvalue()


def _parse_federations_from_csv(content: str) -> set[tuple[str, str]]:
    """Parse CSV content into set of (code, name) for order-independent comparison."""
    reader = csv.DictReader(io.StringIO(content))
    return {
        (row["code"].strip(), row["name"].strip()) for row in reader if row.get("code")
    }


def _graceful_shutdown(signum: int, frame) -> None:
    """Save partial results on SIGINT/SIGTERM."""
    global _shutdown_state
    logger.warning("\nReceived interrupt, attempting graceful shutdown...")
    federations = _shutdown_state.get("federations", [])
    output_path = _shutdown_state.get("output_path")
    if federations and output_path:
        try:
            content = _federations_to_csv(federations)
            write_output(content, output_path)
            logger.info("Saved %d federations to %s", len(federations), output_path)
        except Exception as e:
            logger.error("Error saving partial results: %s", e)
    else:
        logger.info("No partial results to save")
    sys.exit(130 if signum == 2 else 0)  # 130 = SIGINT


def run_shared(
    bucket: Optional[str] = None,
    local_root: Optional[str | Path] = None,
    override: bool = False,
    quiet: bool = False,
) -> str:
    """
    Fetch federations and write to shared path (federations/data/federations_{timestamp}.csv).
    Skips fetch if latest exists and is < 2 weeks old (unless override).
    For federations, only writes if content actually changed (order-independent compare).

    Args:
        bucket: S3 bucket (uses shared path). If None, uses local_root.
        local_root: Local root for output (e.g. 'data'). Used when bucket is None.
        override: If True, skip list check and always fetch + write.
        quiet: Reduce log output.

    Returns:
        URI (s3://...) or local path str of the file used.
    """
    from datetime import datetime, timezone

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    use_s3 = bucket is not None

    if use_s3:
        output_uri = build_federations_data_uri(bucket, timestamp)
    else:
        base = Path(local_root or "data") / FEDERATIONS_DATA_PREFIX
        base.mkdir(parents=True, exist_ok=True)
        output_path = base / f"federations_{timestamp}.csv"

    if not override:
        if use_s3:
            latest_uri, last_modified = get_latest_in_s3_prefix(
                bucket, FEDERATIONS_DATA_PREFIX + "/"
            )
            if latest_uri and last_modified and not is_stale(last_modified):
                logger.info("Latest federations < 2 weeks old, reusing %s", latest_uri)
                return latest_uri
        else:
            latest_path, mtime = get_latest_in_local_prefix(
                local_root or "data", FEDERATIONS_DATA_PREFIX
            )
            if latest_path and mtime is not None and not is_stale(mtime):
                logger.info("Latest federations < 2 weeks old, reusing %s", latest_path)
                return str(latest_path)

    logger.info("Fetching federations list from %s...", URL)
    try:
        federations = get_federations_with_retries()
    except Exception as e:
        logger.error("Error fetching federations: %s", e)
        raise RuntimeError(f"Failed to fetch federations: {e}") from e

    if not federations:
        raise RuntimeError("No federations retrieved")

    new_content = _federations_to_csv(federations)

    # Content diff: only write if different (skip when override - we always write)
    if not override:
        new_set = _parse_federations_from_csv(new_content)
        if use_s3:
            latest_uri, _ = get_latest_in_s3_prefix(
                bucket, FEDERATIONS_DATA_PREFIX + "/"
            )
            if latest_uri:
                try:
                    import boto3

                    b, k = parse_s3_uri(latest_uri)
                    s3 = boto3.client("s3")
                    obj = s3.get_object(Bucket=b, Key=k)
                    existing = obj["Body"].read().decode("utf-8")
                    existing_set = _parse_federations_from_csv(existing)
                    if new_set == existing_set:
                        logger.info("Federations unchanged, reusing %s", latest_uri)
                        return latest_uri
                except Exception as e:
                    logger.warning("Could not compare with latest: %s", e)
        else:
            latest_path, _ = get_latest_in_local_prefix(
                local_root or "data", FEDERATIONS_DATA_PREFIX
            )
            if latest_path and latest_path.exists():
                existing_set = _parse_federations_from_csv(
                    latest_path.read_text(encoding="utf-8")
                )
                if new_set == existing_set:
                    logger.info("Federations unchanged, reusing %s", latest_path)
                    return str(latest_path)

    # Write new
    if use_s3:
        write_output(new_content.encode("utf-8"), output_uri)
        logger.info("Saved %d federations to %s", len(federations), output_uri)
        return output_uri
    else:
        output_path.write_text(new_content, encoding="utf-8")
        logger.info("Saved %d federations to %s", len(federations), output_path)
        return str(output_path)


def run(
    output_path: str,
    override: bool = False,
    quiet: bool = False,
) -> int:
    """
    Scrape federations and write to output_path.

    Args:
        output_path: Local path or S3 URI (s3://bucket/key).
        override: If True, overwrite existing file. If False, skip when exists.
        quiet: If True, reduce log output to WARNING only.

    Returns:
        0 on success, 1 on failure.
    """
    global _shutdown_state

    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if output_exists(output_path) and not override:
        logger.info(
            "Output %s already exists. Use override=True to scrape and replace.",
            output_path,
        )
        return 0

    start_time = time.time()
    logger.info("Fetching federations list from %s...", URL)

    try:
        federations = get_federations_with_retries()
    except Exception as e:
        logger.error("Error fetching federations: %s", e)
        return 1

    if not federations:
        logger.error("No federations retrieved")
        return 1

    _shutdown_state["federations"] = federations
    _shutdown_state["output_path"] = output_path

    elapsed_time = time.time() - start_time
    content = _federations_to_csv(federations)
    write_output(content, output_path)

    if not quiet:
        for fed in federations:
            logger.info("%s: %s", fed["code"], fed["name"])
        logger.info("Found %d federations", len(federations))
        logger.info("Time taken: %.2f seconds", elapsed_time)

    logger.info("Saved %d federations to %s", len(federations), output_path)
    _shutdown_state["completed"] = True
    return 0


def main() -> int:
    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    parser = argparse.ArgumentParser(
        description="Scrape FIDE website to get the list of federations"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path: local file or S3 URI. Overrides path-building.",
    )
    parser.add_argument(
        "--local-root",
        type=str,
        default="data",
        help="Local bucket root for run structure (default: data)",
    )
    parser.add_argument(
        "--run-type",
        type=str,
        choices=("prod", "custom", "test"),
        default=None,
        help="Run type (prod/custom/test). With --run-name, builds path.",
    )
    parser.add_argument(
        "--run-name",
        type=str,
        default=None,
        help="Run name (e.g. 2024-01). Required for prod/custom with --run-type.",
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        default="data",
        help="(Legacy) Directory when not using run structure.",
    )
    parser.add_argument(
        "--filename",
        "-f",
        type=str,
        default="federations.csv",
        help="(Legacy) Output filename when not using run structure.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Disable verbose output (default: verbose is enabled)",
    )
    parser.add_argument(
        "--override",
        "-o",
        action="store_true",
        help="Override existing file and scrape again",
    )

    args = parser.parse_args()

    if args.output is not None and is_s3_path(args.output):
        bucket, _ = parse_s3_uri(args.output)
        run_shared(bucket=bucket, override=args.override, quiet=args.quiet)
        return 0
    elif args.run_type:
        if args.run_type in ("prod", "custom") and not args.run_name:
            logger.error("--run-name required when --run-type is prod or custom")
            return 1
        repo_root = Path(__file__).parent.parent.parent
        local_root = repo_root / (args.local_root or "data")
        run_shared(local_root=local_root, override=args.override, quiet=args.quiet)
        return 0
    else:
        repo_root = Path(__file__).parent.parent.parent
        output_dir = repo_root / args.directory
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / args.filename)
        return run(
            output_path=output_path,
            override=args.override,
            quiet=args.quiet,
        )


if __name__ == "__main__":
    sys.exit(main())

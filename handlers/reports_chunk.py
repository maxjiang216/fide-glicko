"""
Lambda handler for one tournament-reports chunk. Resumable and time-budgeted:
see handlers/chunk_common.py for the event shape and return value. Uses the
month's stored details (all chunks and parts) for round-date inference.
"""

from .chunk_common import run_chunk_lambda
from .lambda_logging import configure


def lambda_handler(event: dict, context) -> dict:
    configure()
    return run_chunk_lambda("reports", event, context)

"""
Lambda handler for one tournament-details chunk. Resumable and time-budgeted:
see handlers/chunk_common.py for the event shape and return value.
"""

from .chunk_common import run_chunk_lambda
from .lambda_logging import configure


def lambda_handler(event: dict, context) -> dict:
    configure()
    return run_chunk_lambda("details", event, context)

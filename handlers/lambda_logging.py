"""
Shared logging setup for Lambda handlers.

Ensures stdout/stderr and logging output flush to CloudWatch Logs promptly.
Critical when Lambda times out—buffered output would be lost without this.

Call configure() at the start of each handler.
"""

import logging
import sys


def configure() -> None:
    """
    Configure stdout, stderr, and logging for reliable CloudWatch delivery.

    - Enables line buffering on stdout/stderr so output flushes on newline.
    - Replaces root logger handlers with a flushing handler so logging flushes
      after each record (covers Lambda timeout kills where buffers may not flush).
    """
    # Line buffering: flush on newline
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)

    # Replace root handlers with a flushing one (avoids duplicate lines)
    root = logging.getLogger()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = _FlushingStreamHandler(sys.stderr)
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


class _FlushingStreamHandler(logging.StreamHandler):
    """StreamHandler that flushes after each record (for CloudWatch)."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        if self.stream and hasattr(self.stream, "flush"):
            self.stream.flush()

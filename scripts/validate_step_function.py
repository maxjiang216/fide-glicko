#!/usr/bin/env python3
"""Run Step Function validation tests (thin wrapper for scripts/deploy)."""

import sys

if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", "tests/test_step_function.py"]))

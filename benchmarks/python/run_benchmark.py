#!/usr/bin/env python3
"""Simple CLI runner for benchmarks"""

import asyncio
from pathlib import Path

from quiver_benchmarks.benchmark_runner import app

if __name__ == "__main__":
    app()

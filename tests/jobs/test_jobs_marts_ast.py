"""
Smoke tests for `pipelines/jobs/marts`.

`_db.py` is included in parse checks, but excluded from runnable-shape checks
because it is a utility module.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import assert_runnable_job_shape, parse_module
from tests.common.paths import JOBS_DIR, python_files_under


MARTS_DIR = JOBS_DIR / "marts"
MARTS_FILES = [p for p in python_files_under(MARTS_DIR) if p.name != "__init__.py"]

# This map is intentionally broad and light-touch.
# We only assert each mart job references its core table artifact.
EXPECTED_TABLE_TOKEN = {
    "build_dim_date.py": "dim_date",
    "build_dim_stock.py": "dim_stock",
    "build_dim_universe.py": "dim_universe",
    "build_bridge_stock_universe_membership.py": "bridge_stock_universe_membership",
    "build_fact_price_daily.py": "fact_price_daily",
    "build_fact_fundamentals.py": "fact_fundamentals",
    "build_fact_dividends.py": "fact_dividends",
    "build_fact_earnings.py": "fact_earnings",
    "build_market_signals.py": "fact_market_signals",
    "refresh_stock_symbols.py": "stock_symbols",
}


def test_marts_suite_has_files() -> None:
    # Prevent false-green runs when folder mapping changes.
    assert MARTS_FILES, "No marts job files discovered"


@pytest.mark.parametrize("job_file", MARTS_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_marts_job_parses(job_file: Path) -> None:
    """Mart module should parse and include top-level docstring guidance."""
    parse_module(job_file)


@pytest.mark.parametrize("job_file", MARTS_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_marts_job_runnable_shape(job_file: Path) -> None:
    """Mart module should expose a runnable entrypoint pattern when applicable."""
    assert_runnable_job_shape(job_file)


@pytest.mark.parametrize("job_file", MARTS_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_marts_job_contains_expected_table_token(job_file: Path) -> None:
    """Mart module should reference its expected core table artifact token."""
    # `_db.py` is a utility file with shared connection/table bootstrap helpers.
    if job_file.name == "_db.py":
        return

    token = EXPECTED_TABLE_TOKEN.get(job_file.name)
    # This guard catches newly added marts files not yet represented in tests.
    assert token, f"Missing EXPECTED_TABLE_TOKEN mapping for {job_file.name}"

    source = job_file.read_text(encoding="utf-8")
    assert token in source, f"{job_file} missing expected token '{token}'"

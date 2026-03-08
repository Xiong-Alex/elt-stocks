"""
Smoke tests for `pipelines/jobs/ingest`.

These tests are intentionally simple and structural:
- every ingest script should parse
- every ingest script should have a runnable entry pattern
- every ingest script should contain core ingestion tokens
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import assert_runnable_job_shape, parse_module
from tests.common.paths import JOBS_DIR, python_files_under


INGEST_DIR = JOBS_DIR / "ingest"
INGEST_FILES = [p for p in python_files_under(INGEST_DIR) if p.name != "__init__.py"]

# Lightweight expected tokens by file name.
# These are intentionally simple "presence checks", not deep semantic checks.
EXPECTED_TOKENS = {
    "ingest_yfinance_to_kafka.py": ["yfinance", "KAFKA_TOPIC", "stock_price_events_source"],
    "ingest_yfinance_fundamentals.py": ["yfinance", "fundamentals_raw"],
    "ingest_yfinance_dividends.py": ["yfinance", "dividends_raw"],
    "ingest_yfinance_earnings.py": ["yfinance", "earnings_raw"],
}


def test_ingest_suite_has_files() -> None:
    """
    Guardrail against accidental folder empties/renames.

    If this fails, a file was moved/deleted and tests need updating.
    """
    assert INGEST_FILES, "No ingest job files discovered"


@pytest.mark.parametrize("job_file", INGEST_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_ingest_job_parses(job_file: Path) -> None:
    """Each ingest module should be syntactically valid and documented."""
    # Basic syntax safety check.
    parse_module(job_file)


@pytest.mark.parametrize("job_file", INGEST_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_ingest_job_runnable_shape(job_file: Path) -> None:
    """Each ingest module should follow executable script conventions."""
    # Shared executable-script shape contract.
    assert_runnable_job_shape(job_file)


@pytest.mark.parametrize("job_file", INGEST_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_ingest_job_contains_expected_tokens(job_file: Path) -> None:
    """
    Edge-ish coverage:
    - validates each ingest script still references its expected output artifact.
    - catches accidental table/topic renames early.
    """
    expected = EXPECTED_TOKENS.get(job_file.name, [])
    source = job_file.read_text(encoding="utf-8")
    for token in expected:
        assert token in source, f"{job_file} missing expected token '{token}'"

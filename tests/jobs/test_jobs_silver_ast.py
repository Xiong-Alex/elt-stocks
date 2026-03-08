"""
Smoke tests for `pipelines/jobs/silver`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import assert_runnable_job_shape, parse_module
from tests.common.paths import JOBS_DIR, python_files_under


SILVER_DIR = JOBS_DIR / "silver"
SILVER_FILES = [p for p in python_files_under(SILVER_DIR) if p.name != "__init__.py"]

EXPECTED_TOKENS = {
    "spark_bronze_to_silver.py": ["stock_bars_bronze", "stock_bars_silver"],
}


def test_silver_suite_has_files() -> None:
    # Prevent false-green runs when folder mapping changes.
    assert SILVER_FILES, "No silver job files discovered"


@pytest.mark.parametrize("job_file", SILVER_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_silver_job_parses(job_file: Path) -> None:
    """Silver module should parse and include top-level docstring guidance."""
    parse_module(job_file)


@pytest.mark.parametrize("job_file", SILVER_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_silver_job_runnable_shape(job_file: Path) -> None:
    """Silver module should expose a runnable entrypoint pattern."""
    assert_runnable_job_shape(job_file)


@pytest.mark.parametrize("job_file", SILVER_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_silver_job_contains_expected_tokens(job_file: Path) -> None:
    """Silver module should still reference core source/target artifacts."""
    source = job_file.read_text(encoding="utf-8")
    for token in EXPECTED_TOKENS.get(job_file.name, []):
        assert token in source, f"{job_file} missing expected token '{token}'"

"""
Smoke tests for `pipelines/jobs/gold`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import assert_runnable_job_shape, parse_module
from tests.common.paths import JOBS_DIR, python_files_under


GOLD_DIR = JOBS_DIR / "gold"
GOLD_FILES = [p for p in python_files_under(GOLD_DIR) if p.name != "__init__.py"]

EXPECTED_TOKENS = {
    "spark_silver_to_gold.py": ["stock_bars_silver", "stock_bars_gold"],
}


def test_gold_suite_has_files() -> None:
    # Prevent false-green runs when folder mapping changes.
    assert GOLD_FILES, "No gold job files discovered"


@pytest.mark.parametrize("job_file", GOLD_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_gold_job_parses(job_file: Path) -> None:
    """Gold module should parse and include top-level docstring guidance."""
    parse_module(job_file)


@pytest.mark.parametrize("job_file", GOLD_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_gold_job_runnable_shape(job_file: Path) -> None:
    """Gold module should expose a runnable entrypoint pattern."""
    assert_runnable_job_shape(job_file)


@pytest.mark.parametrize("job_file", GOLD_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_gold_job_contains_expected_tokens(job_file: Path) -> None:
    """Gold module should still reference core source/target artifacts."""
    source = job_file.read_text(encoding="utf-8")
    for token in EXPECTED_TOKENS.get(job_file.name, []):
        assert token in source, f"{job_file} missing expected token '{token}'"

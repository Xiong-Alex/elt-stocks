"""
Smoke tests for `pipelines/jobs/bronze`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import assert_runnable_job_shape, parse_module
from tests.common.paths import JOBS_DIR, python_files_under


BRONZE_DIR = JOBS_DIR / "bronze"
BRONZE_FILES = [p for p in python_files_under(BRONZE_DIR) if p.name != "__init__.py"]

EXPECTED_TOKENS = {
    "spark_kafka_to_bronze.py": ["KafkaConsumer", "stock_bars_bronze", "stock_bars_quarantine"],
}


def test_bronze_suite_has_files() -> None:
    # Prevent silent folder-empty scenarios.
    assert BRONZE_FILES, "No bronze job files discovered"


@pytest.mark.parametrize("job_file", BRONZE_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_bronze_job_parses(job_file: Path) -> None:
    """Bronze module should parse and include top-level docstring guidance."""
    parse_module(job_file)


@pytest.mark.parametrize("job_file", BRONZE_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_bronze_job_runnable_shape(job_file: Path) -> None:
    """Bronze module should expose a runnable entrypoint pattern."""
    assert_runnable_job_shape(job_file)


@pytest.mark.parametrize("job_file", BRONZE_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_bronze_job_contains_expected_tokens(job_file: Path) -> None:
    """Bronze module should still reference its core ingestion/output artifacts."""
    # Token checks catch accidental table/topic/consumer path drift.
    source = job_file.read_text(encoding="utf-8")
    for token in EXPECTED_TOKENS.get(job_file.name, []):
        assert token in source, f"{job_file} missing expected token '{token}'"

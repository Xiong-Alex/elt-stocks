"""
Global pytest setup for this repository.

Important design choice:
- We intentionally keep tests "static-analysis first" (AST + source checks).
- That makes tests fast and deterministic for a demo framework.
- No external services (Airflow, Spark, Kafka, Postgres) are required to run this test layer.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.paths import DAGS_DIR, JOBS_DIR, REPO_ROOT, python_files_under


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """
    Return repository root as a fixture.

    Why fixture?
    - makes tests easy to refactor later if path logic changes
    - keeps tests readable (they can request `repo_root`)
    """
    return REPO_ROOT


@pytest.fixture(scope="session")
def all_dag_files() -> list[Path]:
    """
    Return all DAG Python files discovered under `pipelines/dags`.

    Session scope avoids rescanning filesystem for every test case.
    """
    return python_files_under(DAGS_DIR)


@pytest.fixture(scope="session")
def all_job_files() -> list[Path]:
    """
    Return all job Python files discovered under `pipelines/jobs`.

    Includes utility modules too; individual tests can filter as needed.
    """
    return python_files_under(JOBS_DIR)

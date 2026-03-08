"""
Repository-level smoke tests for demo safety.

These tests ensure core folders/files exist so CI fails early when
critical project structure is accidentally removed or renamed.
"""

from __future__ import annotations

from pathlib import Path

from tests.common.ast_contracts import dag_ids_from_tree, parse_file
from tests.common.paths import DAGS_DIR, JOBS_DIR, REPO_ROOT


def test_core_directories_exist() -> None:
    """Core pipeline directories must exist for the framework to function."""
    # Core pipeline directories must exist.
    assert DAGS_DIR.exists(), "pipelines/dags directory is missing"
    assert JOBS_DIR.exists(), "pipelines/jobs directory is missing"


def test_core_root_files_exist() -> None:
    """Root operational files should remain present for local workflows."""
    # Keep basic developer entrypoints present.
    for rel in ["README.md", "docker-compose.yml", "Makefile"]:
        assert (REPO_ROOT / rel).exists(), f"{rel} is missing"


def test_gitignore_covers_python_cache_artifacts() -> None:
    """
    Edge-case hygiene check:
    - ensure repo ignores cache artifacts even if they appear locally.
    """
    gitignore_path = REPO_ROOT / ".gitignore"
    content = gitignore_path.read_text(encoding="utf-8")
    assert "__pycache__/" in content or "*.pyc" in content, (
        ".gitignore should ignore Python cache artifacts (__pycache__/ or *.pyc)"
    )


def test_dag_ids_are_unique_across_repo() -> None:
    """
    Detect duplicate DAG IDs across files.

    Duplicate dag_id values are a subtle operational bug in Airflow:
    one file can silently shadow another.
    """
    all_dag_ids: list[str] = []
    for dag_file in DAGS_DIR.rglob("*.py"):
        if "__pycache__" in dag_file.parts:
            continue
        all_dag_ids.extend(dag_ids_from_tree(parse_file(dag_file)))

    assert all_dag_ids, "No dag_id values found across DAG files"
    assert len(all_dag_ids) == len(set(all_dag_ids)), (
        "Duplicate dag_id values detected: "
        f"{sorted([d for d in set(all_dag_ids) if all_dag_ids.count(d) > 1])}"
    )

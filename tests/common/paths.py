"""
Shared path helpers for tests.

Keeping this in a dedicated module makes subfolder tests easier to organize.
"""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "pipelines" / "dags"
JOBS_DIR = REPO_ROOT / "pipelines" / "jobs"


def python_files_under(path: Path) -> list[Path]:
    """
    Return all Python files under `path`, excluding cache folders.

    Edge handling:
    - excludes any nested `__pycache__` directory
    - deterministic sorted order keeps parametrized test IDs stable
    """
    return sorted(
        p
        for p in path.rglob("*.py")
        if "__pycache__" not in p.parts
    )

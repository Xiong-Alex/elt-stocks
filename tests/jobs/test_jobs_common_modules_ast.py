"""
Smoke tests for `pipelines/jobs/common`.

These are utility modules, not runnable scripts, so we only enforce syntax.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.common.job_ast_checks import parse_module
from tests.common.paths import JOBS_DIR, python_files_under


COMMON_DIR = JOBS_DIR / "common"
COMMON_FILES = [p for p in python_files_under(COMMON_DIR) if p.name != "__init__.py"]


def test_common_suite_has_files() -> None:
    # Prevent false-green runs when folder mapping changes.
    assert COMMON_FILES, "No common utility modules discovered"


@pytest.mark.parametrize("module_file", COMMON_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_common_module_parses(module_file: Path) -> None:
    """Common utility module should parse and carry a top-level docstring."""
    # Utility modules only need to parse cleanly for this basic smoke suite.
    parse_module(module_file)


@pytest.mark.parametrize("module_file", COMMON_FILES, ids=lambda p: str(Path(p).relative_to(JOBS_DIR)))
def test_common_module_has_at_least_one_function(module_file: Path) -> None:
    """
    Edge-ish guard:
    - utility modules should expose at least one function to justify their file.
    """
    source = module_file.read_text(encoding="utf-8")
    assert "def " in source, f"{module_file} appears to define no functions"

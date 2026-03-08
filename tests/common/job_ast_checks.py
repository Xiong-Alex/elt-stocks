"""
Reusable AST checks for job scripts.

These helpers keep each test module short while still documenting
the expected "shape" of runnable pipeline job files.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


def parse_module(path: Path) -> ast.Module:
    """Parse a Python file and return its AST."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def has_main_guard(tree: ast.Module) -> bool:
    """
    Detect `if __name__ == "__main__": ...` at module level.

    This is the standard executable pattern used in this repository.
    """
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        ):
            return True
    return False


def defined_functions(tree: ast.Module) -> set[str]:
    """Return top-level function names from a parsed module."""
    return {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}


def has_module_docstring(tree: ast.Module) -> bool:
    """
    Return True when module starts with a string docstring.

    This keeps code self-documenting and makes generated test output easier
    for teammates to understand during failures.
    """
    if not tree.body:
        return False
    first = tree.body[0]
    return (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
        and bool(first.value.value.strip())
    )


def assert_runnable_job_shape(job_file: Path) -> None:
    """
    Common shape assertion for runnable jobs.

    Expectations:
    - file parses
    - has a known entry function (run_job/main/refresh_stock_symbols)
    - has a __main__ guard

    Exceptions:
    - `_db.py` is a utility module, not a runnable script
    """
    if job_file.name == "_db.py":
        pytest.skip("utility module, not an executable job script")

    tree = parse_module(job_file)
    fn_names = defined_functions(tree)
    has_entry_fn = bool({"run_job", "main", "refresh_stock_symbols"} & fn_names)
    assert has_entry_fn, f"{job_file} should define run_job()/main()/refresh_stock_symbols()"
    assert has_main_guard(tree), f"{job_file} should include if __name__ == '__main__':"

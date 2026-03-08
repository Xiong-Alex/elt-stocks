"""
DAG-level structure smoke tests.

Goal:
- ensure every DAG file is syntactically valid
- ensure it defines a DAG context and at least one task
- ensure task ids are unique in each file
- ensure core DAG kwargs are explicitly set (schedule + catchup)
- ensure every BashOperator has a non-empty task_id and bash_command

Why AST:
- no Airflow import needed
- catches obvious structural mistakes quickly
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.common.paths import DAGS_DIR, python_files_under


def _is_call_to(node: ast.AST, fn_name: str) -> bool:
    """Return True when node is a call to a specific function/class name."""
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == fn_name
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == fn_name
    return False


def _extract_str_kwarg(call: ast.Call, name: str) -> str | None:
    """Read a string literal keyword argument from a call if present."""
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def _has_kwarg(call: ast.Call, name: str) -> bool:
    """
    Return True when a call provides a specific keyword argument.

    We use this for "contract-ish" DAG checks, e.g. requiring `catchup`.
    """
    return any(kw.arg == name for kw in call.keywords)


@pytest.mark.parametrize("dag_file", python_files_under(DAGS_DIR), ids=lambda p: str(Path(p).relative_to(DAGS_DIR)))
def test_dag_file_has_valid_structure(dag_file: Path) -> None:
    """Every DAG file should satisfy baseline structural and scheduling contracts."""
    # Parse each file to ensure valid syntax.
    source = dag_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(dag_file))

    # Check that the file creates at least one DAG(...) call.
    dag_calls = [n for n in ast.walk(tree) if _is_call_to(n, "DAG")]
    assert dag_calls, f"{dag_file} should declare a DAG(...) call"

    # Verify each DAG(...) call has a dag_id keyword (best-practice sanity check).
    for call in dag_calls:
        dag_id = _extract_str_kwarg(call, "dag_id")
        assert dag_id, f"{dag_file} should set dag_id='...'"
        # Explicitly requiring these keeps scheduling behavior predictable.
        assert _has_kwarg(call, "schedule_interval"), f"{dag_file} should set schedule_interval=..."
        assert _has_kwarg(call, "catchup"), f"{dag_file} should set catchup=..."

    # Check task declarations exist (BashOperator is used in this project).
    task_calls = [n for n in ast.walk(tree) if _is_call_to(n, "BashOperator")]
    assert task_calls, f"{dag_file} should define at least one BashOperator task"

    # Ensure task_id values are unique within the file.
    task_ids = [tid for call in task_calls if (tid := _extract_str_kwarg(call, "task_id"))]
    assert len(task_ids) == len(set(task_ids)), f"{dag_file} has duplicate task_id values"

    # Extra guard: every task should have a non-empty task_id and command.
    for call in task_calls:
        task_id = _extract_str_kwarg(call, "task_id")
        bash_cmd = _extract_str_kwarg(call, "bash_command")
        assert task_id and task_id.strip(), f"{dag_file} has BashOperator with missing/empty task_id"
        assert bash_cmd and bash_cmd.strip(), f"{dag_file}:{task_id} missing/empty bash_command"

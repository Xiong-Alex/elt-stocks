"""
AST helpers used by contract-style tests.

These helpers extract high-level "contracts" from DAG files without importing
Airflow or executing code:
- dag_id values
- BashOperator task_id values
- bash_command string content (best-effort)
"""

from __future__ import annotations

import ast
from pathlib import Path


def parse_file(path: Path) -> ast.Module:
    """Parse a Python file into an AST module."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _call_name(node: ast.AST) -> str | None:
    """Return simple call name (e.g., DAG, BashOperator) from AST call node."""
    if not isinstance(node, ast.Call):
        return None
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _kwarg_value(call: ast.Call, name: str) -> ast.AST | None:
    """Return raw AST node for a keyword argument on a call."""
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _const_str(node: ast.AST | None) -> str | None:
    """
    Best-effort conversion to string for literal-like AST nodes.

    Supports:
    - "literal"
    - "a" + "b"
    - f-string where static parts can be joined
    """
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _const_str(node.left)
        right = _const_str(node.right)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            else:
                # Keep placeholder text for non-literal expression blocks.
                parts.append("{expr}")
        return "".join(parts)
    return None


def dag_ids_from_tree(tree: ast.Module) -> list[str]:
    """Extract all dag_id string literals from DAG(...) calls."""
    ids: list[str] = []
    for node in ast.walk(tree):
        if _call_name(node) != "DAG":
            continue
        dag_id_node = _kwarg_value(node, "dag_id")
        dag_id = _const_str(dag_id_node)
        if dag_id:
            ids.append(dag_id)
    return ids


def bash_tasks_from_tree(tree: ast.Module) -> list[dict[str, str]]:
    """
    Extract BashOperator task contracts.

    Returns list of dicts:
    - task_id
    - bash_command
    """
    tasks: list[dict[str, str]] = []
    for node in ast.walk(tree):
        if _call_name(node) != "BashOperator":
            continue
        task_id = _const_str(_kwarg_value(node, "task_id")) or ""
        bash_command = _const_str(_kwarg_value(node, "bash_command")) or ""
        tasks.append({"task_id": task_id, "bash_command": bash_command})
    return tasks


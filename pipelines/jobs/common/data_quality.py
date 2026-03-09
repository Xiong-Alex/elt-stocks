"""
Shared data quality helpers for Spark jobs.

These helpers intentionally stay lightweight:
- split rows into valid/invalid based on required columns
- optionally deduplicate valid rows
- emit consistent stage metrics to logs
- enforce non-empty expectations when needed
"""

from __future__ import annotations

from typing import Iterable


def split_valid_invalid(df, required_columns: Iterable[str] | None = None, dedupe_keys=None):
    """
    Return `(valid_df, invalid_df)` using null checks and optional dedupe.

    Rules:
    - if `required_columns` is not provided, all columns in `df` are treated as required
    - any row with NULL in a required column is marked invalid
    - valid rows can be de-duplicated with `dedupe_keys`
    """
    try:
        from pyspark.sql import functions as F
    except Exception as exc:
        raise RuntimeError("pyspark is required for data quality helpers.") from exc

    columns = list(df.columns)
    if not columns:
        # No columns means no useful rows to validate.
        return df.limit(0), df

    required = list(required_columns) if required_columns is not None else columns
    required = [c for c in required if c in columns]
    if not required:
        required = columns

    invalid_condition = None
    for col_name in required:
        col_null = F.col(col_name).isNull()
        invalid_condition = col_null if invalid_condition is None else (invalid_condition | col_null)

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    if dedupe_keys:
        valid_df = valid_df.dropDuplicates(list(dedupe_keys))

    return valid_df, invalid_df


def log_quality_summary(stage: str, source_count: int, valid_count: int, invalid_count: int) -> None:
    """
    Emit a small, consistent quality summary.

    This is stdout-based for now so it works in local runs, Docker logs, and Airflow logs.
    """
    valid_pct = (valid_count / source_count * 100.0) if source_count else 0.0
    invalid_pct = (invalid_count / source_count * 100.0) if source_count else 0.0
    print(
        "[DQ] "
        f"stage={stage} "
        f"source={source_count} valid={valid_count} invalid={invalid_count} "
        f"valid_pct={valid_pct:.2f} invalid_pct={invalid_pct:.2f}"
    )


def enforce_non_empty(stage: str, count: int, allow_empty: bool = False) -> None:
    """
    Raise when a stage unexpectedly produces no rows.

    Use `allow_empty=True` for known-empty windows (for example, non-trading periods).
    """
    if count > 0:
        return
    if allow_empty:
        print(f"[DQ] stage={stage} produced 0 rows (allowed)")
        return
    raise ValueError(f"[DQ] stage={stage} produced 0 rows and allow_empty=False")

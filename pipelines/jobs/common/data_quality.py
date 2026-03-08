"""
Data quality framework skeleton.
Implement validation/splitting rules here.
"""


def split_valid_invalid(df, dedupe_keys=None):
    """Return (valid_df, invalid_df)."""
    # TODO: Apply null/type/range checks and split rows into valid + invalid sets.
    # TODO: Deduplicate valid rows using dedupe_keys when provided.
    return df, df.limit(0)


def log_quality_summary(stage: str, source_count: int, valid_count: int, invalid_count: int) -> None:
    """Emit quality metrics/logs."""
    # TODO: Send stage-level metrics to logs/Prometheus/StatsD.
    pass


def enforce_non_empty(stage: str, count: int, allow_empty: bool = False) -> None:
    """Raise if stage output is unexpectedly empty."""
    # TODO: Raise/alert on empty outputs where non-empty is expected.
    pass

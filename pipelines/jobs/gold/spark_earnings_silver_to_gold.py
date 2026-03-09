"""
Earnings Gold load stub.

Planned flow:
- Read curated earnings Silver records.
- Apply final conformance checks.
- Publish Gold-ready earnings dataset for serving/marts.
"""

import argparse


def read_silver_records(run_id: str) -> None:
    """TODO: Read earnings Silver records for this run."""
    print(f"[TODO] read_silver_records not implemented run_id={run_id}")


def enforce_gold_quality(run_id: str) -> None:
    """TODO: Run final quality checks before serving load."""
    print(f"[TODO] enforce_gold_quality not implemented run_id={run_id}")


def write_gold_records(run_id: str) -> None:
    """TODO: Persist earnings Gold records."""
    print(f"[TODO] write_gold_records not implemented run_id={run_id}")


def run_job(run_id: str) -> None:
    """Entrypoint for earnings Gold stage."""
    read_silver_records(run_id)
    enforce_gold_quality(run_id)
    write_gold_records(run_id)
    print(f"[TODO] spark_earnings_silver_to_gold not implemented run_id={run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()
    run_job(args.run_id)

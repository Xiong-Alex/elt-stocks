"""
Earnings Bronze load stub.

Planned flow:
- Read raw earnings ingestion payloads.
- Persist immutable Bronze records to object storage.
"""

import argparse


def read_ingest_payloads(run_id: str) -> None:
    """TODO: Load earnings payloads produced by ingest step."""
    print(f"[TODO] read_ingest_payloads not implemented run_id={run_id}")


def write_bronze_records(run_id: str) -> None:
    """TODO: Write earnings Bronze records to S3/MinIO."""
    print(f"[TODO] write_bronze_records not implemented run_id={run_id}")


def run_job(run_id: str) -> None:
    """Entrypoint for earnings Bronze stage."""
    read_ingest_payloads(run_id)
    write_bronze_records(run_id)
    print(f"[TODO] spark_earnings_to_bronze not implemented run_id={run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()
    run_job(args.run_id)

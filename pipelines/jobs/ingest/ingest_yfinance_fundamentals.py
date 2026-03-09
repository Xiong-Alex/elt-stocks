"""
INGEST JOB STUB: fundamentals raw ingestion.

Implementation notes:
- Resolve active symbols from source/dimension table.
- Fetch fundamentals fields from provider (e.g., yfinance).
- Upsert raw fundamentals table for downstream fact build.
"""


def run_job() -> None:
    """
    TODO: Implement fundamentals ingestion behavior.

    Expected output:
    - refreshed rows in `public.fundamentals_raw`
    """
    print("[TODO] ingest_yfinance_fundamentals not implemented yet")


if __name__ == "__main__":
    run_job()

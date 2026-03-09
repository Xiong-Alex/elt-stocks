"""
INGEST JOB STUB: earnings raw ingestion.

Implementation notes:
- Resolve active symbols from source/dimension table.
- Fetch earnings history/calendar fields from provider.
- Upsert raw earnings table for downstream fact build.
"""


def run_job() -> None:
    """
    TODO: Implement earnings ingestion behavior.

    Expected output:
    - refreshed rows in `public.earnings_raw`
    """
    print("[TODO] ingest_yfinance_earnings not implemented yet")


if __name__ == "__main__":
    run_job()

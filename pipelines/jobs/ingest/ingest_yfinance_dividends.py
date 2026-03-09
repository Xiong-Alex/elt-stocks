"""
INGEST JOB STUB: dividends raw ingestion.

Implementation notes:
- Resolve active symbols from source/dimension table.
- Fetch dividend history/events from provider.
- Upsert raw dividends table for downstream fact build.
"""


def run_job() -> None:
    """
    TODO: Implement dividends ingestion behavior.

    Expected output:
    - refreshed rows in `public.dividends_raw`
    """
    print("[TODO] ingest_yfinance_dividends not implemented yet")


if __name__ == "__main__":
    run_job()

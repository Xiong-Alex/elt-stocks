"""
Dividends fact builder.

Purpose:
- Create/populate `public.fact_dividends` from raw dividends source.
- Compute `dividend_growth` using previous dividend per ticker.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.fact_dividends`.

    Inputs:
    - public.dividends_raw
    """
    conn = connect()
    conn.autocommit = True
    try:
        # Ensure source/raw tables exist for first-run safety.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Create fact table if missing.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.fact_dividends (
                    ticker TEXT NOT NULL,
                    ex_date DATE NOT NULL,
                    dividend_amount DOUBLE PRECISION,
                    dividend_growth DOUBLE PRECISION,
                    PRIMARY KEY (ticker, ex_date)
                );
                """
            )
            # Compute lag-based growth and upsert by (ticker, ex_date).
            cur.execute(
                """
                WITH ordered AS (
                    SELECT
                        symbol AS ticker,
                        ex_date,
                        dividend_amount,
                        LAG(dividend_amount) OVER (
                            PARTITION BY symbol ORDER BY ex_date
                        ) AS prev_dividend
                    FROM public.dividends_raw
                )
                INSERT INTO public.fact_dividends (
                    ticker, ex_date, dividend_amount, dividend_growth
                )
                SELECT
                    ticker,
                    ex_date,
                    dividend_amount,
                    CASE
                        WHEN prev_dividend IS NULL OR prev_dividend = 0 THEN NULL
                        ELSE ((dividend_amount - prev_dividend) / prev_dividend) * 100.0
                    END AS dividend_growth
                FROM ordered
                ON CONFLICT (ticker, ex_date) DO UPDATE
                SET dividend_amount = EXCLUDED.dividend_amount,
                    dividend_growth = EXCLUDED.dividend_growth;
                """
            )
        print("[OK] build_fact_dividends completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

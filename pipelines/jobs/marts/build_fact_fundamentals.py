"""
Fundamentals fact builder.

Purpose:
- Create/populate `public.fact_fundamentals` from raw fundamentals source.
- Keep one row per (ticker, asof_date) with upsert semantics.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.fact_fundamentals`.

    Inputs:
    - public.fundamentals_raw
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
                CREATE TABLE IF NOT EXISTS public.fact_fundamentals (
                    ticker TEXT NOT NULL,
                    asof_date DATE NOT NULL,
                    pe_ratio DOUBLE PRECISION,
                    market_cap DOUBLE PRECISION,
                    beta DOUBLE PRECISION,
                    PRIMARY KEY (ticker, asof_date)
                );
                """
            )
            # Upsert fundamentals rows by business key.
            cur.execute(
                """
                INSERT INTO public.fact_fundamentals (
                    ticker, asof_date, pe_ratio, market_cap, beta
                )
                SELECT symbol, asof_date, pe_ratio, market_cap, beta
                FROM public.fundamentals_raw
                ON CONFLICT (ticker, asof_date) DO UPDATE
                SET pe_ratio = EXCLUDED.pe_ratio,
                    market_cap = EXCLUDED.market_cap,
                    beta = EXCLUDED.beta;
                """
            )
        print("[OK] build_fact_fundamentals completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

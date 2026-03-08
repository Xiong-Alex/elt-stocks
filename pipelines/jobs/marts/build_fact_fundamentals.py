from _db import connect, ensure_source_tables


def run_job() -> None:
    """Build FACT_FUNDAMENTALS from fundamentals_raw."""
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
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

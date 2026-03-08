from _db import connect, ensure_source_tables


def run_job() -> None:
    """Build FACT_EARNINGS from earnings_raw."""
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.fact_earnings (
                    ticker TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    eps_actual DOUBLE PRECISION,
                    eps_estimate DOUBLE PRECISION,
                    surprise_pct DOUBLE PRECISION,
                    PRIMARY KEY (ticker, report_date)
                );
                """
            )
            cur.execute(
                """
                INSERT INTO public.fact_earnings (
                    ticker, report_date, eps_actual, eps_estimate, surprise_pct
                )
                SELECT symbol, report_date, eps_actual, eps_estimate, surprise_pct
                FROM public.earnings_raw
                ON CONFLICT (ticker, report_date) DO UPDATE
                SET eps_actual = EXCLUDED.eps_actual,
                    eps_estimate = EXCLUDED.eps_estimate,
                    surprise_pct = EXCLUDED.surprise_pct;
                """
            )
        print("[OK] build_fact_earnings completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

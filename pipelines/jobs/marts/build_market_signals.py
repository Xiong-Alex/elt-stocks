from _db import connect, ensure_source_tables


def run_job() -> None:
    """Build FACT_MARKET_SIGNALS from FACT_PRICE_DAILY."""
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.fact_market_signals (
                    ticker TEXT NOT NULL,
                    date_key INTEGER NOT NULL,
                    close DOUBLE PRECISION,
                    sma_5 DOUBLE PRECISION,
                    sma_20 DOUBLE PRECISION,
                    momentum_5 DOUBLE PRECISION,
                    signal TEXT,
                    PRIMARY KEY (ticker, date_key)
                );
                """
            )
            cur.execute(
                """
                WITH base AS (
                    SELECT
                        ticker,
                        date_key,
                        close,
                        AVG(close) OVER (
                            PARTITION BY ticker ORDER BY date_key
                            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        ) AS sma_5,
                        AVG(close) OVER (
                            PARTITION BY ticker ORDER BY date_key
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS sma_20,
                        close - LAG(close, 5) OVER (
                            PARTITION BY ticker ORDER BY date_key
                        ) AS momentum_5
                    FROM public.fact_price_daily
                )
                INSERT INTO public.fact_market_signals (
                    ticker, date_key, close, sma_5, sma_20, momentum_5, signal
                )
                SELECT
                    ticker,
                    date_key,
                    close,
                    sma_5,
                    sma_20,
                    momentum_5,
                    CASE
                        WHEN sma_5 > sma_20 THEN 'bullish'
                        WHEN sma_5 < sma_20 THEN 'bearish'
                        ELSE 'neutral'
                    END AS signal
                FROM base
                ON CONFLICT (ticker, date_key) DO UPDATE
                SET close = EXCLUDED.close,
                    sma_5 = EXCLUDED.sma_5,
                    sma_20 = EXCLUDED.sma_20,
                    momentum_5 = EXCLUDED.momentum_5,
                    signal = EXCLUDED.signal;
                """
            )
        print("[OK] build_market_signals completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

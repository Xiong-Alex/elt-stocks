"""
Daily price fact builder.

Purpose:
- Create/populate `public.fact_price_daily` from intraday gold bars.
- Derive daily OHLCV per ticker/date with idempotent upserts.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.fact_price_daily`.

    Inputs:
    - public.stock_bars_gold
    """
    conn = connect()
    conn.autocommit = True
    try:
        # Ensure source/gold tables exist for first-run safety.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Create fact table if missing.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.fact_price_daily (
                    ticker TEXT NOT NULL,
                    date_key INTEGER NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    PRIMARY KEY (ticker, date_key)
                );
                """
            )
            # Aggregate OHLCV daily and upsert by (ticker, date_key).
            cur.execute(
                """
                WITH daily AS (
                    SELECT
                        symbol AS ticker,
                        event_ts::date AS d,
                        (ARRAY_AGG(open ORDER BY event_ts ASC))[1] AS open,
                        MAX(high) AS high,
                        MIN(low) AS low,
                        (ARRAY_AGG(close ORDER BY event_ts DESC))[1] AS close,
                        COALESCE(SUM(volume), 0)::bigint AS volume
                    FROM public.stock_bars_gold
                    GROUP BY symbol, event_ts::date
                )
                INSERT INTO public.fact_price_daily (
                    ticker, date_key, open, high, low, close, volume
                )
                SELECT
                    ticker,
                    TO_CHAR(d, 'YYYYMMDD')::int AS date_key,
                    open, high, low, close, volume
                FROM daily
                ON CONFLICT (ticker, date_key) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """
            )
        print("[OK] build_fact_price_daily completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

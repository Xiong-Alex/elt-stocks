import argparse

from marts._db import connect


def _ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.stock_bars_silver (
            run_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            adj_close DOUBLE PRECISION,
            volume BIGINT,
            source TEXT NOT NULL DEFAULT 'yfinance',
            inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id, symbol, event_ts)
        );
        """
    )


def run_job(run_id: str) -> None:
    conn = connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _ensure_tables(cur)
            if run_id:
                cur.execute(
                    """
                INSERT INTO public.stock_bars_silver (
                    run_id, mode, symbol, event_ts, open, high, low, close, adj_close, volume, source
                )
                SELECT
                    b.run_id,
                    b.mode,
                    b.symbol,
                    b.event_ts,
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    COALESCE(b.adj_close, b.close) AS adj_close,
                    b.volume,
                    b.source
                FROM public.stock_bars_bronze b
                WHERE b.run_id = %s
                  AND b.quality_status = 'valid'
                ON CONFLICT (run_id, symbol, event_ts) DO UPDATE
                SET mode = EXCLUDED.mode,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source,
                    inserted_at = NOW();
                    """,
                    (run_id,),
                )
            else:
                cur.execute(
                    """
                INSERT INTO public.stock_bars_silver (
                    run_id, mode, symbol, event_ts, open, high, low, close, adj_close, volume, source
                )
                SELECT
                    b.run_id,
                    b.mode,
                    b.symbol,
                    b.event_ts,
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    COALESCE(b.adj_close, b.close) AS adj_close,
                    b.volume,
                    b.source
                FROM public.stock_bars_bronze b
                WHERE b.quality_status = 'valid'
                ON CONFLICT (run_id, symbol, event_ts) DO UPDATE
                SET mode = EXCLUDED.mode,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source,
                    inserted_at = NOW();
                    """
                )
            inserted_rows = cur.rowcount if cur.rowcount != -1 else 0

        print(f"[OK] spark_bronze_to_silver run_id={run_id} rows={inserted_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()
    run_job(args.run_id)

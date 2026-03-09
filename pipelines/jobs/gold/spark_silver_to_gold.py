"""
Gold merge job (silver -> gold serving table).

Merges silver rows into `public.stock_bars_gold` keyed by `(symbol, event_ts)`.
"""

import argparse

from marts._db import connect, ensure_source_tables


def run_job(run_id: str) -> None:
    # If run_id is provided, merge only that batch; else merge all silver rows.
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_stock_bars_gold_symbol_event_ts
                ON public.stock_bars_gold (symbol, event_ts);
                """
            )
            if run_id:
                cur.execute(
                    """
                INSERT INTO public.stock_bars_gold (
                    symbol, event_ts, open, high, low, close, volume
                )
                SELECT
                    s.symbol,
                    s.event_ts,
                    s.open,
                    s.high,
                    s.low,
                    COALESCE(s.adj_close, s.close) AS close,
                    s.volume
                FROM public.stock_bars_silver s
                WHERE s.run_id = %s
                ON CONFLICT (symbol, event_ts) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                    """,
                    (run_id,),
                )
            else:
                cur.execute(
                    """
                INSERT INTO public.stock_bars_gold (
                    symbol, event_ts, open, high, low, close, volume
                )
                SELECT
                    s.symbol,
                    s.event_ts,
                    s.open,
                    s.high,
                    s.low,
                    COALESCE(s.adj_close, s.close) AS close,
                    s.volume
                FROM public.stock_bars_silver s
                ON CONFLICT (symbol, event_ts) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                    """
                )
            merged_rows = cur.rowcount if cur.rowcount != -1 else 0
        print(f"[OK] spark_silver_to_gold run_id={run_id} rows={merged_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()
    run_job(args.run_id)

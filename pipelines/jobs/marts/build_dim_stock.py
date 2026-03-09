"""
Stock dimension builder.

Purpose:
- Create/populate `public.dim_stock` from symbol source table.
- Keep stable ticker-level dimensional state for downstream joins.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.dim_stock`.

    Inputs:
    - public.stock_symbols
    """
    conn = connect()
    conn.autocommit = True
    try:
        # Ensure baseline source tables exist for first-run setups.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Create dimension table if needed.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.dim_stock (
                    stock_key BIGSERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL UNIQUE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            # Upsert symbol rows so reruns are idempotent.
            cur.execute(
                """
                INSERT INTO public.dim_stock (ticker, is_active, updated_at)
                SELECT symbol, COALESCE(is_active, TRUE), NOW()
                FROM public.stock_symbols
                ON CONFLICT (ticker) DO UPDATE
                SET is_active = EXCLUDED.is_active,
                    updated_at = NOW();
                """
            )
        print("[OK] build_dim_stock completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

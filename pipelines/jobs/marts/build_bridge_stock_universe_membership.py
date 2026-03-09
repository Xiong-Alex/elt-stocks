"""
Bridge table builder for stock-to-universe membership.

Purpose:
- Map `dim_stock.stock_key` to `dim_universe.universe_key`.
- Track active/inactive membership lifecycle with effective dates.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.bridge_stock_universe_membership`.

    Inputs:
    - public.stock_universe_memberships_source
    - public.dim_stock
    - public.dim_universe
    """
    conn = connect()
    conn.autocommit = True
    try:
        # Ensure base/source tables exist before dimension/bridge work.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Create bridge table once; reruns are upsert-based.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.bridge_stock_universe_membership (
                    stock_key BIGINT NOT NULL REFERENCES public.dim_stock(stock_key),
                    universe_key BIGINT NOT NULL REFERENCES public.dim_universe(universe_key),
                    effective_from DATE NOT NULL DEFAULT CURRENT_DATE,
                    effective_to DATE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (stock_key, universe_key)
                );
                """
            )
            # Join source memberships to dimensions and upsert bridge rows.
            cur.execute(
                """
                WITH src AS (
                    SELECT
                        ds.stock_key,
                        du.universe_key,
                        sus.is_active,
                        sus.source
                    FROM public.stock_universe_memberships_source sus
                    JOIN public.dim_stock ds
                      ON ds.ticker = sus.symbol
                    JOIN public.dim_universe du
                      ON du.universe_code = sus.universe_code
                )
                INSERT INTO public.bridge_stock_universe_membership (
                    stock_key,
                    universe_key,
                    effective_from,
                    effective_to,
                    is_active,
                    source,
                    updated_at
                )
                SELECT
                    stock_key,
                    universe_key,
                    CURRENT_DATE,
                    CASE WHEN is_active THEN NULL ELSE CURRENT_DATE END,
                    is_active,
                    source,
                    NOW()
                FROM src
                ON CONFLICT (stock_key, universe_key) DO UPDATE
                SET is_active = EXCLUDED.is_active,
                    effective_to = CASE
                        WHEN EXCLUDED.is_active THEN NULL
                        ELSE CURRENT_DATE
                    END,
                    effective_from = CASE
                        WHEN public.bridge_stock_universe_membership.is_active = FALSE
                             AND EXCLUDED.is_active = TRUE
                        THEN CURRENT_DATE
                        ELSE public.bridge_stock_universe_membership.effective_from
                    END,
                    source = EXCLUDED.source,
                    updated_at = NOW();
                """
            )
        print("[OK] build_bridge_stock_universe_membership completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

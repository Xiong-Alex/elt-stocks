"""
Universe dimension builder.

Purpose:
- Create/populate `public.dim_universe` from source memberships.
- Keep one row per universe code (for example SP500) with status metadata.
"""

from _db import connect, ensure_source_tables


def run_job() -> None:
    """
    Build/update `public.dim_universe`.

    Inputs:
    - public.stock_universe_memberships_source
    """
    conn = connect()
    conn.autocommit = True
    try:
        # Ensure source tables exist on fresh databases.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Create dimension table once.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.dim_universe (
                    universe_key BIGSERIAL PRIMARY KEY,
                    universe_code TEXT NOT NULL UNIQUE,
                    universe_name TEXT NOT NULL,
                    description TEXT,
                    source TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            # Roll up source memberships to one row per universe_code.
            cur.execute(
                """
                INSERT INTO public.dim_universe (
                    universe_code, universe_name, description, source, is_active, updated_at
                )
                SELECT
                    s.universe_code,
                    CASE
                        WHEN s.universe_code = 'SP500' THEN 'S&P 500'
                        ELSE s.universe_code
                    END AS universe_name,
                    CASE
                        WHEN s.universe_code = 'SP500' THEN 'S&P 500 constituents'
                        ELSE 'Universe: ' || s.universe_code
                    END AS description,
                    MAX(s.source) AS source,
                    BOOL_OR(s.is_active) AS is_active,
                    NOW() AS updated_at
                FROM public.stock_universe_memberships_source s
                GROUP BY s.universe_code
                ON CONFLICT (universe_code) DO UPDATE
                SET universe_name = EXCLUDED.universe_name,
                    description = EXCLUDED.description,
                    source = EXCLUDED.source,
                    is_active = EXCLUDED.is_active,
                    updated_at = NOW();
                """
            )
        print("[OK] build_dim_universe completed")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

import os

import psycopg2


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-analytics"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "analytics"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "analytics"),
    )


def ensure_source_tables(conn) -> None:
    """Create minimal source schemas so mart jobs can run on a fresh DB."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.stock_symbols (
                symbol TEXT PRIMARY KEY,
                is_active BOOLEAN DEFAULT TRUE
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.stock_bars_gold (
                symbol TEXT NOT NULL,
                event_ts TIMESTAMP NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume BIGINT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.fundamentals_raw (
                symbol TEXT NOT NULL,
                asof_date DATE NOT NULL,
                pe_ratio DOUBLE PRECISION,
                market_cap DOUBLE PRECISION,
                beta DOUBLE PRECISION
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.dividends_raw (
                symbol TEXT NOT NULL,
                ex_date DATE NOT NULL,
                dividend_amount DOUBLE PRECISION
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.earnings_raw (
                symbol TEXT NOT NULL,
                report_date DATE NOT NULL,
                eps_actual DOUBLE PRECISION,
                eps_estimate DOUBLE PRECISION,
                surprise_pct DOUBLE PRECISION
            );
            """
        )

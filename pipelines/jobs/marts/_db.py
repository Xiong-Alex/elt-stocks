"""
Shared database helpers for mart jobs.

This module provides:
- `connect()` for psycopg2 connection bootstrap from environment variables.
- `ensure_source_tables()` for minimal schema bootstrap on fresh databases.
"""

import os

import psycopg2


def connect():
    """Create a psycopg2 connection using analytics DB env configuration."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-analytics"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "analytics"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "analytics"),
    )


def ensure_source_tables(conn) -> None:
    """
    Create baseline source tables required by marts.

    Notes:
    - These are lightweight bootstrap tables to make first-run local/dev flows work.
    - Individual mart jobs still own their target dimension/fact table DDL.
    """
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.stock_universe_memberships_source (
                universe_code TEXT NOT NULL,
                symbol TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
                source TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (universe_code, symbol)
            );
            """
        )

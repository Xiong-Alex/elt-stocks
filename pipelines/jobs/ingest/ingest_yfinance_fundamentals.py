from datetime import date

import yfinance as yf
from psycopg2.extras import execute_values

from marts._db import connect, ensure_source_tables


def _resolve_symbols(cur) -> list[str]:
    cur.execute(
        """
        SELECT symbol
        FROM public.stock_symbols
        WHERE COALESCE(is_active, TRUE) = TRUE
        ORDER BY symbol;
        """
    )
    symbols = [r[0] for r in cur.fetchall()]
    if not symbols:
        raise RuntimeError("No active symbols in public.stock_symbols.")
    return symbols


def run_job() -> None:
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fundamentals_raw_symbol_asof
                ON public.fundamentals_raw(symbol, asof_date);
                """
            )
            symbols = _resolve_symbols(cur)

        rows = []
        today = date.today()
        for symbol in symbols:
            info = yf.Ticker(symbol).info or {}
            pe_ratio = info.get("trailingPE")
            market_cap = info.get("marketCap")
            beta = info.get("beta")
            if pe_ratio is None and market_cap is None and beta is None:
                continue
            rows.append((symbol, today, pe_ratio, market_cap, beta))

        if rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.fundamentals_raw (
                        symbol, asof_date, pe_ratio, market_cap, beta
                    )
                    VALUES %s
                    ON CONFLICT (symbol, asof_date) DO UPDATE
                    SET pe_ratio = EXCLUDED.pe_ratio,
                        market_cap = EXCLUDED.market_cap,
                        beta = EXCLUDED.beta;
                    """,
                    rows,
                )
        print(f"[OK] ingest_yfinance_fundamentals symbols={len(symbols)} rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

from datetime import date, timedelta

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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_dividends_raw_symbol_exdate
                ON public.dividends_raw(symbol, ex_date);
                """
            )
            symbols = _resolve_symbols(cur)

        start_date = date.today() - timedelta(days=365 * 5)
        rows = []
        for symbol in symbols:
            series = yf.Ticker(symbol).dividends
            if series is None or series.empty:
                continue
            for ts, amount in series.items():
                ex_date = ts.date()
                if ex_date < start_date:
                    continue
                if amount is None:
                    continue
                rows.append((symbol, ex_date, float(amount)))

        if rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.dividends_raw (symbol, ex_date, dividend_amount)
                    VALUES %s
                    ON CONFLICT (symbol, ex_date) DO UPDATE
                    SET dividend_amount = EXCLUDED.dividend_amount;
                    """,
                    rows,
                )

        print(f"[OK] ingest_yfinance_dividends symbols={len(symbols)} rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

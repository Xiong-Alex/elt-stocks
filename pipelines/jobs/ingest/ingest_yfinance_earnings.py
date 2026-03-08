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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_earnings_raw_symbol_reportdate
                ON public.earnings_raw(symbol, report_date);
                """
            )
            symbols = _resolve_symbols(cur)

        rows = []
        today = date.today()
        for symbol in symbols:
            try:
                earnings_dates = yf.Ticker(symbol).get_earnings_dates(limit=8)
            except Exception:  # noqa: BLE001
                earnings_dates = None
            if earnings_dates is None or earnings_dates.empty:
                continue
            frame = earnings_dates.reset_index()
            date_col = frame.columns[0]
            for _, rec in frame.iterrows():
                report_ts = rec.get(date_col)
                report_date = report_ts.date() if hasattr(report_ts, "date") else today
                eps_actual = rec.get("Reported EPS")
                eps_estimate = rec.get("EPS Estimate")
                surprise_pct = rec.get("Surprise(%)")
                rows.append(
                    (
                        symbol,
                        report_date,
                        float(eps_actual) if eps_actual == eps_actual else None,
                        float(eps_estimate) if eps_estimate == eps_estimate else None,
                        float(surprise_pct) if surprise_pct == surprise_pct else None,
                    )
                )

        if rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.earnings_raw (
                        symbol, report_date, eps_actual, eps_estimate, surprise_pct
                    )
                    VALUES %s
                    ON CONFLICT (symbol, report_date) DO UPDATE
                    SET eps_actual = EXCLUDED.eps_actual,
                        eps_estimate = EXCLUDED.eps_estimate,
                        surprise_pct = EXCLUDED.surprise_pct;
                    """,
                    rows,
                )
        print(f"[OK] ingest_yfinance_earnings symbols={len(symbols)} rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    run_job()

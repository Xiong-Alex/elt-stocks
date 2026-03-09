"""
Universe source refresher.

Purpose:
- Resolve latest symbols for a universe (default S&P 500).
- Keep source-of-truth symbol and membership tables updated and idempotent.
"""

import argparse
import csv
import io
from typing import Iterable
from urllib.request import urlopen

from _db import connect, ensure_source_tables


SP500_CSV_URL = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"


def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
    # Canonicalize symbols to a stable format used across tables/jobs.
    cleaned = {
        s.strip().upper().replace(".", "-")
        for s in symbols
        if s and s.strip()
    }
    return sorted(cleaned)


def _fetch_sp500_symbols() -> list[str]:
    # Pull current S&P 500 constituents from a CSV source.
    with urlopen(SP500_CSV_URL, timeout=30) as resp:
        content = resp.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    symbols = [row.get("Symbol", "") for row in reader]
    return _normalize_symbols(symbols)


def refresh_stock_symbols(
    source: str = "sp500",
    symbols: Iterable[str] | None = None,
    set_inactive_missing: bool = True,
    universe_code: str = "SP500",
) -> None:
    """
    Refresh stock universe source tables.

    Writes/updates:
    - public.stock_symbols
    - public.stock_universe_memberships_source
    """
    # Resolve symbols from configured source.
    if source == "sp500":
        # Resolve latest membership snapshot for the selected source.
        resolved_symbols = _fetch_sp500_symbols()[:10]  # only retrieve first 10, remove slicing later for full list
        upstream_source = "datahub_sp500"
    elif source == "manual":
        resolved_symbols = _normalize_symbols(symbols or [])
        upstream_source = "manual"
    else:
        raise ValueError(f"Unsupported source: {source}")

    if not resolved_symbols:
        raise RuntimeError("No symbols resolved for universe refresh.")

    conn = connect()
    conn.autocommit = True
    try:
        # Ensure baseline source tables exist for first-run bootstraps.
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            # Extend stock_symbols with basic lineage/audit fields.
            cur.execute(
                """
                ALTER TABLE public.stock_symbols
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();
                """
            )
            cur.execute(
                """
                ALTER TABLE public.stock_symbols
                ADD COLUMN IF NOT EXISTS source TEXT;
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

            for sym in resolved_symbols:
                # Upsert symbol-level source of truth.
                cur.execute(
                    """
                    INSERT INTO public.stock_symbols (symbol, is_active, updated_at, source)
                    VALUES (%s, TRUE, NOW(), %s)
                    ON CONFLICT (symbol) DO UPDATE
                    SET is_active = TRUE,
                        updated_at = NOW(),
                        source = EXCLUDED.source;
                    """,
                    (sym, upstream_source),
                )
                # Upsert membership snapshot for the target universe.
                cur.execute(
                    """
                    INSERT INTO public.stock_universe_memberships_source (
                        universe_code, symbol, is_active, as_of_date, source, updated_at
                    )
                    VALUES (%s, %s, TRUE, CURRENT_DATE, %s, NOW())
                    ON CONFLICT (universe_code, symbol) DO UPDATE
                    SET is_active = TRUE,
                        as_of_date = CURRENT_DATE,
                        source = EXCLUDED.source,
                        updated_at = NOW();
                    """,
                    (universe_code, sym, upstream_source),
                )

            if set_inactive_missing:
                # Mark members not present in latest snapshot as inactive.
                cur.execute(
                    """
                    UPDATE public.stock_universe_memberships_source
                    SET is_active = FALSE,
                        as_of_date = CURRENT_DATE,
                        updated_at = NOW()
                    WHERE universe_code = %s
                      AND symbol <> ALL(%s);
                    """,
                    (universe_code, resolved_symbols),
                )

                # Mirror inactive state to stock_symbols for symbols tied to this universe.
                cur.execute(
                    """
                    UPDATE public.stock_symbols ss
                    SET is_active = EXISTS (
                            SELECT 1
                            FROM public.stock_universe_memberships_source sus
                            WHERE sus.symbol = ss.symbol
                              AND sus.is_active = TRUE
                        ),
                        updated_at = NOW(),
                        source = %s
                    WHERE ss.symbol IN (
                        SELECT symbol
                        FROM public.stock_universe_memberships_source
                    );
                    """,
                    (upstream_source,),
                )

        print(
            f"[OK] refresh_stock_symbols completed: universe={universe_code}, "
            f"source={upstream_source}, symbols={len(resolved_symbols)}"
        )
    finally:
        conn.close()


def main() -> None:
    """CLI wrapper so the job can be called from Airflow/BashOperator."""
    parser = argparse.ArgumentParser(description="Refresh stock symbols universe.")
    parser.add_argument("--source", default="sp500", choices=["sp500", "manual"])
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols when --source=manual (e.g., AAPL,MSFT,GOOGL).",
    )
    parser.add_argument("--universe-code", default="SP500")
    parser.add_argument("--no-set-inactive-missing", action="store_true")
    args = parser.parse_args()

    manual_symbols = None
    if args.source == "manual":
        manual_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    refresh_stock_symbols(
        source=args.source,
        symbols=manual_symbols,
        set_inactive_missing=not args.no_set_inactive_missing,
        universe_code=args.universe_code.strip().upper(),
    )


if __name__ == "__main__":
    main()

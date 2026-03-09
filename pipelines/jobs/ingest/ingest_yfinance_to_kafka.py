"""
Price ingest job (yfinance -> Kafka + source table).

Modes:
- intraday: incremental windowing based on latest stored event_ts
- backfill: explicit date window from CLI args
"""

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

import yfinance as yf
from kafka import KafkaProducer
from psycopg2.extras import execute_values

from marts._db import connect, ensure_source_tables


def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
    return sorted(
        {
            s.strip().upper().replace(".", "-")
            for s in symbols
            if s and s.strip()
        }
    )


def _resolve_symbols(cur, symbols_arg: str) -> list[str]:
    # If CLI symbols are supplied, use them. Otherwise resolve active symbols from DB.
    if symbols_arg.strip():
        return _normalize_symbols(symbols_arg.split(","))

    cur.execute(
        """
        SELECT symbol
        FROM public.stock_symbols
        WHERE COALESCE(is_active, TRUE) = TRUE
        ORDER BY symbol;
        """
    )
    from_db = [r[0] for r in cur.fetchall()]
    if from_db:
        return _normalize_symbols(from_db)
    raise RuntimeError("No symbols available. Provide --symbols or populate public.stock_symbols.")


def _ensure_tables(cur) -> None:
    # Source lineage table used for traceability and replay.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.stock_price_events_source (
            run_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            adj_close DOUBLE PRECISION,
            volume BIGINT,
            source TEXT NOT NULL DEFAULT 'yfinance',
            inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id, symbol, event_ts)
        );
        """
    )


def _build_producer():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )


def _resolve_date_window(conn, mode: str, start_date: str, end_date: str) -> tuple[str, str]:
    if mode != "intraday":
        return start_date, end_date

    # Explicit CLI dates always win.
    if start_date.strip() and end_date.strip():
        return start_date, end_date

    today = datetime.now(timezone.utc).date()
    default_start = (today - timedelta(days=3)).isoformat()
    default_end = today.isoformat()

    if start_date.strip() and not end_date.strip():
        return start_date, default_end
    if end_date.strip() and not start_date.strip():
        return default_start, end_date

    # Intraday default: only pull a short recent window.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(event_ts)::date
            FROM public.stock_price_events_source
            WHERE mode = 'intraday';
            """
        )
        last_seen = cur.fetchone()[0]

    if last_seen is None:
        return default_start, default_end

    # Re-read a tiny overlap window to absorb late updates/corrections.
    overlap_start = (last_seen - timedelta(days=2)).isoformat()
    return overlap_start, default_end


def run_job(mode: str, start_date: str, end_date: str, symbols_arg: str, run_id: str) -> None:
    topic = os.getenv("KAFKA_TOPIC", "stock_prices")
    run_ts = datetime.now(timezone.utc).isoformat()

    conn = connect()
    conn.autocommit = True
    producer = None
    inserted = 0
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            _ensure_tables(cur)
            symbols = _resolve_symbols(cur, symbols_arg)
        resolved_start_date, resolved_end_date = _resolve_date_window(conn, mode, start_date, end_date)

        try:
            producer = _build_producer()
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Kafka producer unavailable, continuing without publish: {exc}")

        for symbol in symbols:
            hist = yf.Ticker(symbol).history(
                start=resolved_start_date,
                end=resolved_end_date,
                interval="1d",
                auto_adjust=False,
                actions=False,
            )
            if hist.empty:
                continue

            hist = hist.reset_index()
            rows = []
            for _, row in hist.iterrows():
                event_ts = row["Date"].to_pydatetime().replace(tzinfo=None)
                payload = {
                    "run_id": run_id,
                    "mode": mode,
                    "symbol": symbol,
                    "event_ts": event_ts.isoformat(),
                    "open": float(row["Open"]) if row["Open"] == row["Open"] else None,
                    "high": float(row["High"]) if row["High"] == row["High"] else None,
                    "low": float(row["Low"]) if row["Low"] == row["Low"] else None,
                    "close": float(row["Close"]) if row["Close"] == row["Close"] else None,
                    "adj_close": float(row["Adj Close"]) if row["Adj Close"] == row["Adj Close"] else None,
                    "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
                    "source": "yfinance",
                    "ingested_at_utc": run_ts,
                }
                rows.append(
                    (
                        payload["run_id"],
                        payload["mode"],
                        payload["symbol"],
                        event_ts,
                        payload["open"],
                        payload["high"],
                        payload["low"],
                        payload["close"],
                        payload["adj_close"],
                        payload["volume"],
                        payload["source"],
                    )
                )
                if producer is not None:
                    producer.send(topic, key=symbol, value=payload)

            if not rows:
                continue

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.stock_price_events_source (
                        run_id, mode, symbol, event_ts, open, high, low, close, adj_close, volume, source
                    )
                    VALUES %s
                    ON CONFLICT (run_id, symbol, event_ts) DO UPDATE
                    SET mode = EXCLUDED.mode,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        adj_close = EXCLUDED.adj_close,
                        volume = EXCLUDED.volume,
                        source = EXCLUDED.source,
                        inserted_at = NOW();
                    """,
                    rows,
                )
            inserted += len(rows)

        if producer is not None:
            producer.flush(timeout=30)

        print(
            f"[OK] ingest_yfinance_to_kafka mode={mode} run_id={run_id} "
            f"symbols={len(symbols)} rows={inserted} date_range={resolved_start_date}..{resolved_end_date}"
        )
    finally:
        if producer is not None:
            producer.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="intraday", choices=["intraday", "backfill"])
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    args = parser.parse_args()
    run_job(args.mode, args.start_date, args.end_date, args.symbols, args.run_id)

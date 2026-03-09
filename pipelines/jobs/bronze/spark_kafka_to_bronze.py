"""
Bronze load job (Kafka -> bronze/quarantine tables).

Reads run-scoped events from Kafka, validates payload shape, and writes:
- valid rows to `public.stock_bars_bronze`
- invalid rows to `public.stock_bars_quarantine`
"""

import argparse
import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer
from psycopg2.extras import execute_values

from marts._db import connect, ensure_source_tables


def _ensure_tables(cur) -> None:
    # Bronze target for valid normalized bars.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.stock_bars_bronze (
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
            quality_status TEXT NOT NULL DEFAULT 'valid',
            inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id, symbol, event_ts)
        );
        """
    )
    # Quarantine sink for records that fail validation rules.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.stock_bars_quarantine (
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_ts TIMESTAMP,
            reason TEXT NOT NULL,
            payload JSONB,
            inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """
    )


def _build_consumer() -> KafkaConsumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "stock_prices")
    group_id = os.getenv("KAFKA_BRONZE_CONSUMER_GROUP", "bronze-batch-consumer")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    return consumer


def _consume_for_run(mode: str, run_id: str, idle_seconds: int = 8, max_seconds: int = 180):
    consumer = _build_consumer()
    start = time.time()
    last_seen = start
    matched = []
    scanned = 0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=1000)
            got_any = False
            for _tp, records in batch.items():
                got_any = True
                scanned += len(records)
                for rec in records:
                    payload = rec.value if isinstance(rec.value, dict) else {}
                    if payload.get("run_id") != run_id:
                        continue
                    if payload.get("mode") != mode:
                        continue
                    matched.append(
                        (
                            payload.get("run_id"),
                            payload.get("mode"),
                            payload.get("symbol"),
                            datetime.fromisoformat(payload.get("event_ts")),
                            payload.get("open"),
                            payload.get("high"),
                            payload.get("low"),
                            payload.get("close"),
                            payload.get("adj_close"),
                            payload.get("volume"),
                            payload.get("source", "yfinance"),
                        )
                    )
                    last_seen = time.time()

            now = time.time()
            if got_any:
                continue
            if matched and (now - last_seen) >= idle_seconds:
                break
            if (now - start) >= max_seconds:
                break

        # Commit offsets only after successful polling for this run.
        consumer.commit()
        return matched, scanned
    finally:
        consumer.close()


def run_job(mode: str, run_id: str) -> None:
    conn = connect()
    conn.autocommit = True
    try:
        ensure_source_tables(conn)
        with conn.cursor() as cur:
            _ensure_tables(cur)
        rows, scanned = _consume_for_run(mode=mode, run_id=run_id)

        valid = []
        invalid = []
        for row in rows:
            run_id_v, mode_v, symbol, event_ts, open_v, high_v, low_v, close_v, adj_close, volume, source = row
            reason = None
            if not symbol or event_ts is None:
                reason = "missing_symbol_or_timestamp"
            elif any(v is not None and v < 0 for v in [open_v, high_v, low_v, close_v, adj_close]):
                reason = "negative_price"
            elif volume is not None and volume < 0:
                reason = "negative_volume"

            if reason:
                invalid.append((run_id_v, symbol, event_ts, reason))
            else:
                valid.append(row)

        with conn.cursor() as cur:
            if valid:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.stock_bars_bronze (
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
                        quality_status = 'valid',
                        inserted_at = NOW();
                    """,
                    valid,
                )

            if invalid:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.stock_bars_quarantine (
                        run_id, symbol, event_ts, reason, payload
                    )
                    VALUES %s;
                    """,
                    [
                        (r_id, sym, ts, reason, None)
                        for r_id, sym, ts, reason in invalid
                    ],
                )

        print(
            f"[OK] spark_kafka_to_bronze mode={mode} run_id={run_id} "
            f"kafka_scanned={scanned} source_rows={len(rows)} valid={len(valid)} invalid={len(invalid)}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="intraday", choices=["intraday", "backfill"])
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_job(args.mode, args.run_id)

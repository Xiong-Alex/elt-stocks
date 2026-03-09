# StreamFlow Detailed Technical README

This document explains how the platform works end-to-end, how each technology fits, and what data moves through each stage.

## 1. System Purpose

StreamFlow ingests stock bars from `yfinance`, moves them through Kafka, transforms them with Spark, stores curated layers in MinIO and Postgres, and exposes monitoring/analytics in Streamlit.

Primary runtime model: orchestrated micro-batch with Airflow.

## 2. End-to-End Flow

`yfinance -> Kafka(topic: stock_bars_raw) -> Spark Bronze (MinIO) -> Spark Silver (MinIO) -> Spark Gold (Postgres analytics) -> Streamlit`

## 3. Technology Roles

| Technology | Service | Responsibility | Inputs | Outputs |
|---|---|---|---|---|
| Airflow | `airflow` | Orchestration, scheduling, retries, DAG state | DAG files + env config | Task execution state in Airflow DB |
| Kafka | `kafka` | Event transport buffer between producer and Spark | JSON stock events | Topic partitions/offsets |
| Kafka UI | `kafka-ui` | Topic/debug visibility | Kafka broker metadata | Web UI for topics/messages |
| Spark | `spark-master`, `spark-worker` | Data parsing, validation, dedupe, feature engineering | Kafka + MinIO parquet | Bronze/Silver parquet + Gold writes |
| MinIO (S3) | `minio` | Data lake object storage | Spark outputs | Bronze/Silver/checkpoints/quarantine objects |
| Postgres (Airflow) | `postgres-airflow` | Airflow metadata only | Airflow internals | DAG/task scheduler metadata |
| Postgres (Analytics) | `postgres-analytics` | Serving layer and data products | Spark Gold + symbol updates | `stock_symbols`, `stock_bars_gold`, quarantine table |
| Streamlit | `streamlit` | Data exploration, observability UI | Postgres + MinIO state | Interactive dashboards |

## 4. Kafka Contract

### Topic

- Topic name: `stock_bars_raw`
- Producer key: stock symbol (example: `AAPL`)
- Producer: `pipelines/batch/jobs/ingest/ingest_yfinance_to_kafka.py`
- Consumer: `pipelines/batch/jobs/bronze/spark_kafka_to_bronze.py` (Spark Structured Streaming)

### Message Shape

Each Kafka message value is JSON with:

- `symbol`
- `event_time`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `source` (currently `yfinance`)
- `emitted_at_utc`

## 5. Data Layers and Storage Paths

### Bronze (raw-but-typed)

- Job: `spark_kafka_to_bronze.py`
- Reads: Kafka topic `stock_bars_raw`
- Writes: `s3a://<bucket>/bronze/stock_bars/` partitioned by `year/month/day`
- Checkpoint: `s3a://<bucket>/checkpoints/stock_bars_raw/`
- Trigger modes:
  - `availableNow` for bounded runs (backfill/manual)
  - `processingTime` for long-running intraday stream

Bronze validation behavior:

- Parses JSON into typed columns
- Requires parseable `event_ts`
- Splits valid/invalid rows with shared DQ utility
- Dedupes by `symbol,event_ts`
- Invalid rows are quarantined to:
  - MinIO path: `s3a://<bucket>/quarantine/stock_bars/`
  - Postgres table: `public.stock_bars_quarantine` (when JDBC args are provided)

### Silver (cleaned, deduped)

- Job: `spark_bronze_to_silver.py`
- Reads: Bronze parquet
- Writes: `s3a://<bucket>/silver/stock_bars_clean/` partitioned by `year/month/day`

Silver behavior:

- Enforces core OHLCV completeness
- Coalesces null volume to `0`
- Dedupes by `symbol,event_ts`
- Overwrites silver output path each run

### Gold (serving/features)

- Job: `spark_silver_to_gold.py`
- Reads: Silver parquet
- Writes: Postgres analytics table `public.stock_bars_gold`

Gold behavior:

- Computes features:
  - `return_pct` from previous close per symbol
  - `sma_5` moving average per symbol
- Dedupes by `symbol,event_ts`
- Loads to staging table via JDBC
- Executes Postgres `MERGE` into target table
- Ensures unique index on `(symbol, event_ts)`

## 6. Airflow DAG Responsibilities

### `update_stock_universe_dag`

- Source of truth for active symbols in `public.stock_symbols`
- Reads `STOCK_SYMBOLS` env (fallback default list)
- Marks all symbols inactive, then upserts active set

### `stock_historical_backfill_dag`

- One-time/manual bulk backfill flow
- Uses yfinance with `--period 1y --interval 1h`
- Task chain:
  1. Load active symbols
  2. Ingest yfinance to Kafka
  3. Kafka to Bronze
  4. Bronze to Silver
  5. Silver validation
  6. Silver to Gold
  7. Gold validation

### `stock_intraday_pipeline_dag`

- Recurring incremental flow (`*/30 * * * *`)
- Uses yfinance with `--period 5d --interval 5m`
- Assumes `stock_price_bronze_streaming_dag` is already running for price Kafka->Bronze
- Executes downstream refinement/serving steps on schedule (Silver, Gold, validations)

### `stock_price_bronze_streaming_dag`

- Long-running price-bars stream (`schedule_interval=None`, manually started)
- Runs Kafka -> Bronze for `stock_bars_raw` using processing-time trigger
- Keeps Bronze stock bars current between intraday DAG runs

## 7. Configuration and Control Surface

Primary runtime knobs come from `.env` and are injected into Airflow/Spark jobs:

- Kafka:
  - `KAFKA_BOOTSTRAP_SERVERS`
  - `KAFKA_TOPIC`
- Storage:
  - `S3_ENDPOINT`
  - `S3_ACCESS_KEY`
  - `S3_SECRET_KEY`
  - `S3_DATALAKE_BUCKET`
- Bronze streaming control:
  - `BRONZE_CHECKPOINT_PATH`
  - `BRONZE_STARTING_OFFSETS`
  - `BRONZE_MAX_OFFSETS_PER_TRIGGER`
  - `BRONZE_FAIL_ON_DATA_LOSS`
  - `BRONZE_STREAM_PROCESSING_TIME`
- Analytics DB:
  - `POSTGRES_JDBC_URL`
  - `POSTGRES_USER`
  - `POSTGRES_PASSWORD`

## 8. Operational Notes

- Airflow uses one container for triggerer + scheduler + webserver.
- Startup command clears stale PID files before boot to avoid false "already running" webserver errors.
- Airflow metadata DB and analytics DB are intentionally separate.
- Spark jobs are run from Airflow with `spark-submit` against `spark://spark-master:7077`.

## 9. UI Surfaces

- Airflow: `http://localhost:8082`
- Spark Master UI: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- Streamlit: `http://localhost:8501`

## 10. Quick Verification Checklist

1. `docker compose ps` shows all core services up/healthy.
2. `stock_price_bronze_streaming_dag` is running (for intraday mode).
3. Airflow DAG run succeeds for `stock_intraday_pipeline_dag` or `stock_historical_backfill_dag`.
4. Kafka UI shows topic `stock_bars_raw` with activity.
5. MinIO contains Bronze/Silver objects and checkpoint path.
6. Postgres analytics has rows in `public.stock_bars_gold`.
7. Streamlit charts and monitoring pages show data.

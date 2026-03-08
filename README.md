# StreamFlow Stock Analytics Platform

End-to-end pipeline for market data using Kafka, Spark, MinIO, Postgres, Airflow, and Streamlit.

## Repository Layout

```text
.
|-- platform/
|   |-- docker-compose.yml
|   `-- airflow/Dockerfile
|-- pipelines/
|   |-- batch/
|   |   |-- dags/
|   |   `-- jobs/
|-- streamlit/
`-- docs/
```

## Data Flow

`yfinance -> Kafka(stock_bars_raw) -> Spark Bronze -> Spark Silver -> Spark Gold(Postgres) -> Streamlit`

## Quick Start

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

## Smoke Test

If `make` is installed:

```powershell
make smoke
```

Run the full command from the repo root (works without `make`):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1
```

Fast check without rebuilding images (full command):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1 -NoBuild
```

## Important Config

- Spark jobs submitted by Airflow use Spark cluster mode:
  - `SPARK_MASTER_URL=spark://spark-master:7077`
- Bronze Kafka offset policy is env-driven:
  - `BRONZE_FAIL_ON_DATA_LOSS=false` for local/dev
  - `BRONZE_FAIL_ON_DATA_LOSS=true` for stricter production behavior
- Keep this in `.env` (already present in `.env.example`).

## URLs

1. Airflow: `http://localhost:8082` (`admin/admin`)
2. Spark Master UI: `http://localhost:8081`
3. Kafka UI: `http://localhost:8080`
4. MinIO Console: `http://localhost:9001`
5. Streamlit: `http://localhost:8501`
6. Postgres Airflow metadata: `localhost:5432` (`airflow/airflow`)
7. Postgres analytics: `localhost:5433` (`analytics/analytics`)

## Airflow Services

- Airflow now runs as separate services:
  - `airflow-init` (one-time DB/user bootstrap)
  - `airflow-webserver` (UI/API)
  - `airflow-scheduler` (DAG scheduling)
- For Airflow CLI commands, exec into `airflow-webserver`:
  - `docker compose exec airflow-webserver airflow users list`

## DAG Run Order

1. `update_stock_universe_dag`
2. Start `price_bronze_streaming_dag` (leave running)
3. `historical_backfill_dag` (optional backfill)
4. Enable `intraday_pipeline_dag`

## Key Improvements Included

1. Environment-driven configuration via `.env` and `.env.example`.
2. Scalable Bronze ingestion:
   - `spark_kafka_to_bronze.py` supports:
     - `availableNow` for bounded runs
     - `processing-time` for long-running price stream
   - Offset progress is persisted with Kafka checkpointing (`BRONZE_CHECKPOINT_PATH`).
3. Shared data quality framework:
   - `pipelines/batch/jobs/common/data_quality.py`
4. Bronze quarantine:
   - Invalid OHLCV rows are written to `s3a://<bucket>/quarantine/stock_bars`
   - Includes `quality_failure_reason` and `quarantined_at_utc` columns.
   - Also mirrored to Postgres table `public.stock_bars_quarantine` for UI review.

## Streamlit Enhancements

1. `Quarantine Review` tab with:
   - Date/symbol/reason filters
   - Failure-reason breakdown chart
   - CSV export of filtered rows
2. Sidebar health checks for Analytics DB, Airflow DB, and MinIO/S3.
3. Multipage UI:
   - Home
   - Market Monitor
   - Charts Workbench
   - Replay Lab
   - Quarantine Review
   - SQL Explorer

## Docs

1. Architecture overview: `docs/architecture.md`
2. Original project spec: `docs/architecture-spec.md`
3. Detailed technical walkthrough: `README_DETAILED.md`

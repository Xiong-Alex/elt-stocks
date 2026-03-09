# StreamFlow ELT Pipeline Framework

A local-first ELT framework for stock analytics built with:
- Airflow (orchestration)
- Kafka (event transport)
- Python jobs (ingest + transforms)
- Postgres (analytics warehouse)
- MinIO (lake/object storage service in stack)
- Streamlit (data explorer and dashboards)

The goal is a practical framework you can build on, not a fully production-hardened platform.

## What This Project Does

The pipeline ingests market/reference data, stages and transforms it through Bronze/Silver/Gold style layers, and publishes analytics-ready tables for exploration.

High-level flow:

`yfinance -> Kafka -> Bronze -> Silver -> Gold (Postgres) -> marts/features -> Streamlit`

## Repository Layout

```text
.
|-- airflow/
|   |-- Dockerfile
|   `-- requirements.txt
|-- pipelines/
|   |-- dags/
|   `-- jobs/
|       |-- ingest/
|       |-- bronze/
|       |-- silver/
|       |-- gold/
|       |-- marts/
|       `-- common/
|-- scripts/
|   `-- smoke-test.ps1
|-- streamlit/
|   |-- app.py
|   `-- pages/
|-- tests/
|   |-- contracts/
|   |-- dags/
|   |-- jobs/
|   `-- repo/
|-- docker-compose.yml
|-- Makefile
`-- .env.example
```

## Prerequisites

- Docker Desktop
- Python 3.11 (`C:\Python311\python.exe` in your current setup)
- PowerShell (for Windows commands)

Optional:
- `make` (you can run all commands without it)

## Quick Start

1. Create env file:

```powershell
Copy-Item .env.example .env
```

2. Start stack:

```powershell
docker compose up -d --build
docker compose ps
```

3. Open UIs:
- Airflow: `http://localhost:8082`
- Streamlit: `http://localhost:8501`
- Kafka UI: `http://localhost:8080`
- Spark UI: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`

Default credentials:
- Airflow: `admin / admin`
- Analytics Postgres: `analytics / analytics` on `localhost:5433`
- Airflow Postgres: `airflow / airflow` on `localhost:5432`
- MinIO: `minioadmin / minioadmin`

## Core Services and Ports

From `.env.example` defaults:

- Kafka broker: `9092`
- Kafka UI: `8080`
- Airflow UI: `8082`
- Spark master UI: `8081`
- MinIO API: `9000`
- MinIO console: `9001`
- Streamlit: `8501`
- Postgres (airflow metadata): `5432`
- Postgres (analytics): `5433`

## DAGs and Responsibilities

Located in `pipelines/dags`:

1. `update_stock_universe_dag`
- Refreshes source symbols/universe membership
- Builds `dim_stock`, `dim_universe`, and bridge membership

2. `stock_historical_backfill_dag` (manual)
- Backfills prices for a date range/run_id
- Runs ingest -> bronze -> silver -> gold -> `fact_price_daily`

3. `stock_intraday_pipeline_dag` (scheduled every 30 min)
- Price-focused periodic pipeline
- ingest -> bronze -> silver -> gold

4. `stock_price_bronze_streaming_dag` (manual)
- Manual price ingest + bronze load only
- Useful for price-only runs without silver/gold

5. `company_fundamentals_dag` (daily)
- Fundamentals/dividends/earnings ingest + fact refresh

6. `market_analytics_dag`
- Builds dimensions/facts/features (including market signals)

## Recommended Run Order

For a fresh local setup:

1. `update_stock_universe_dag`
2. `company_fundamentals_dag`
3. `stock_historical_backfill_dag` (optional but recommended initially)
4. `stock_intraday_pipeline_dag`
5. `market_analytics_dag`

Use `stock_price_bronze_streaming_dag` when you specifically want a manual price->bronze run.

## Triggering Backfill with Config

Airflow UI -> `stock_historical_backfill_dag` -> Trigger with config:

```json
{
  "start_date": "2026-03-01",
  "end_date": "2026-03-06",
  "symbols": "AAPL,MSFT",
  "run_id": "backfill_20260306_aapl_msft"
}
```

Notes:
- `symbols` optional: falls back to active symbols in `public.stock_symbols`
- `run_id` optional: defaults to Airflow run timestamp token

## Command Reference

### Make Targets

```powershell
make up
make down
make ps
make logs
make smoke
make smoke-build
```

### Without Make

```powershell
docker compose up -d --build
docker compose down
docker compose ps
docker compose logs -f
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1 -NoBuild
```

### Useful Airflow CLI

```powershell
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags trigger stock_intraday_pipeline_dag
docker compose exec airflow-webserver airflow tasks list stock_intraday_pipeline_dag
```

## Testing

The project includes a lightweight AST/contract-based test suite under `tests/`.

Install pytest (current interpreter):

```powershell
C:\Python311\python.exe -m pip install --user pytest
```

Run tests:

```powershell
C:\Python311\python.exe -m pytest -q
```

Verbose:

```powershell
C:\Python311\python.exe -m pytest -vv -rA
```

## Streamlit Pages

- Market Monitor
- Charts Workbench
- Quarantine Review
- SQL Explorer
- Feature Engineering
- Replay Lab
- Universe Pipeline

## Troubleshooting

### `No module named pytest`

Install pytest for the same interpreter VS Code is using:

```powershell
C:\Python311\python.exe -m pip install --user pytest
```

### Docker container name conflict (`already in use`)

Remove conflicting container or bring stack down first:

```powershell
docker compose down
docker ps -a
docker rm -f <container_name_or_id>
```

### Airflow login issues

Airflow user is created by `airflow-init` service via env vars in `.env`.
If needed, recreate stack:

```powershell
docker compose down
docker compose up -d --build
```

### Streamlit still showing old code

Rebuild/recreate Streamlit container:

```powershell
docker compose up -d --build --force-recreate streamlit
```

## Documentation

- Architecture overview: `docs/architecture.md`
- Detailed architecture spec: `docs/architecture-spec.md`
- Extended notes: `docs/README_DETAILED.md`

## Notes

- This is a framework/demo baseline designed for iterative extension.
- Current tests prioritize structure/contracts over full integration execution.
- Runtime smoke checks (`scripts/smoke-test.ps1`) and `pytest` serve different purposes and both are useful.

# Architecture Overview

## Design

This project uses a layered architecture:

1. `platform`: runtime infrastructure and container orchestration.
2. `pipelines/batch`: Airflow DAGs and Spark batch jobs.
3. `streamlit`: consumer-facing dashboard.

## Runtime Flow

1. Airflow triggers ingestion and Spark jobs.
2. `ingest_yfinance_to_kafka.py` publishes events to Kafka topic `stock_bars_raw`.
3. `spark_kafka_to_bronze.py` incrementally reads Kafka and writes Bronze parquet to MinIO.
4. `spark_bronze_to_silver.py` performs cleaning/quality checks and writes Silver parquet.
5. `spark_silver_to_gold.py` computes features and upserts Gold data into Postgres.
6. Streamlit queries Postgres Gold and MinIO activity for visualization and monitoring.

Postgres is split by responsibility:

- `postgres-airflow`: Airflow metadata (`dag_run`, task state, scheduler state)
- `postgres-analytics`: analytics serving tables (`public.stock_symbols`, `public.stock_bars_gold`)

## Configuration

Airflow DAGs now read key runtime settings from environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `POSTGRES_JDBC_URL`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `BRONZE_CHECKPOINT_PATH`
- `BRONZE_STARTING_OFFSETS`
- `BRONZE_MAX_OFFSETS_PER_TRIGGER`
- `BRONZE_FAIL_ON_DATA_LOSS`
- `BRONZE_STREAM_PROCESSING_TIME`

Data quality is centralized in `pipelines/batch/jobs/common/data_quality.py` and reused by Bronze, Silver, and Gold stages.

## Notes

Current execution model is hybrid:
- Long-running processing-time stream for price Kafka -> Bronze.
- Scheduled micro-batch DAGs for downstream refinement and serving.

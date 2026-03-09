"""
Shared Spark session factory used by pipeline jobs.

This module centralizes Spark bootstrap config so every job gets:
- consistent app naming and master config
- consistent timezone/shuffle defaults
- optional Kafka + S3/MinIO + Postgres connector settings from env
"""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    """Read an env var with a string fallback."""
    value = os.getenv(name)
    return default if value is None or value == "" else value


def create_spark_session(app_name: str, master: str | None = None):
    """
    Create and return a configured SparkSession.

    Env-driven knobs:
    - SPARK_MASTER (default: local[*])
    - SPARK_TIMEZONE (default: UTC)
    - SPARK_SQL_SHUFFLE_PARTITIONS (default: 8)
    - KAFKA_BOOTSTRAP_SERVERS
    - MINIO_ENDPOINT
    - MINIO_ACCESS_KEY
    - MINIO_SECRET_KEY
    - S3_BUCKET
    - POSTGRES_JDBC_URL
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    """
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError(
            "pyspark is required to create a Spark session. "
            "Install pyspark in the runtime environment."
        ) from exc

    resolved_master = master or _env("SPARK_MASTER", "local[*]")
    timezone = _env("SPARK_TIMEZONE", "UTC")
    shuffle_partitions = _env("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Optional package list (left blank by default). If your container/image
    # already has dependencies baked in, do not set this.
    packages = _env("SPARK_JARS_PACKAGES", "")
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    # Optional Kafka hint config (actual read/write code still chooses options).
    kafka_bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "")
    if kafka_bootstrap:
        builder = builder.config("spark.kafka.bootstrap.servers", kafka_bootstrap)

    # Optional MinIO/S3A settings for object storage jobs.
    minio_endpoint = _env("MINIO_ENDPOINT", "")
    if minio_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
        access_key = _env("MINIO_ACCESS_KEY", "")
        secret_key = _env("MINIO_SECRET_KEY", "")
        if access_key:
            builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
        if secret_key:
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    # Optional JDBC defaults so jobs can read these from Spark conf.
    jdbc_url = _env("POSTGRES_JDBC_URL", "")
    if jdbc_url:
        builder = builder.config("spark.pipeline.jdbc.url", jdbc_url)
        builder = builder.config("spark.pipeline.jdbc.user", _env("POSTGRES_USER", ""))
        builder = builder.config("spark.pipeline.jdbc.password", _env("POSTGRES_PASSWORD", ""))

    bucket = _env("S3_BUCKET", "")
    if bucket:
        builder = builder.config("spark.pipeline.s3.bucket", bucket)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(_env("SPARK_LOG_LEVEL", "WARN"))
    return spark

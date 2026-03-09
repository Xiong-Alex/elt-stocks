# Stock Market Data Engineering Pipeline

## Overview

This project implements a scalable financial data platform designed to
ingest, process, and analyze stock market data using modern data
engineering tools.

The platform follows a **Bronze → Silver → Gold architecture** and uses
**Airflow for orchestration, Kafka for streaming ingestion, PySpark for
distributed transformations, and object storage (S3/MinIO) for the data
lake**.

The final output is a **dimensional data warehouse** designed for
analytics, dashboards, and quantitative analysis.

------------------------------------------------------------------------

# Architecture Overview

Data Flow:

Data Sources → Airflow DAGs → Kafka → Spark Jobs → Bronze → Silver →
Gold → Analytics

Core Technologies:

-   Apache Airflow (workflow orchestration)
-   Apache Kafka (event streaming)
-   Apache Spark / PySpark (data processing)
-   S3 or MinIO (data lake storage)
-   Parquet (storage format)
-   Python (data ingestion and transformations)

------------------------------------------------------------------------

# Data Lake Layers

## Bronze Layer -- Raw Data

Purpose: Stores raw ingested data exactly as received from external APIs
or streams.

Characteristics:

-   Immutable raw data
-   Minimal transformations
-   Partitioned by date
-   Stored as Parquet

Example Storage Layout:

bronze/ stock_prices/ year=2026/month=03/day=06/

Data Tables:

-   bronze.stock_prices_raw
-   bronze.fundamentals_raw
-   bronze.dividends_raw
-   bronze.earnings_raw

------------------------------------------------------------------------

## Silver Layer -- Cleaned Data

Purpose: Standardizes, cleans, and validates raw datasets.

Processing Tasks:

-   Remove duplicates
-   Standardize schemas
-   Convert timestamps
-   Normalize numeric fields
-   Handle missing values

Spark Job:

spark_bronze_to_silver.py

Output Tables:

-   silver.stock_prices
-   silver.fundamentals
-   silver.dividends
-   silver.earnings

------------------------------------------------------------------------

## Gold Layer -- Analytics Warehouse

Purpose: Builds a dimensional model optimized for analytics and
querying.

Model Type:

Star Schema

Dimension Tables:

-   DIM_STOCK
-   DIM_DATE
-   DIM_SECTOR

Fact Tables:

-   FACT_PRICE_DAILY
-   FACT_FUNDAMENTALS
-   FACT_DIVIDENDS
-   FACT_EARNINGS
-   FACT_MARKET_SIGNALS

These tables power downstream analytics, dashboards, and quantitative
models.

------------------------------------------------------------------------

# Airflow DAGs

## 1. Update Stock Universe DAG

File:

dags/update_stock_universe_dag.py

Purpose:

Maintains the master list of stocks tracked by the platform.

Tasks:

-   Fetch stock metadata from exchanges or financial APIs
-   Clean and standardize metadata
-   Load dimension table

Output:

gold.dim_stock

Frequency:

Weekly or daily

Spark Job:

build_dim_stock.py

------------------------------------------------------------------------

## 2. Historical Backfill DAG

File:

dags/stock_historical_backfill_dag.py

Purpose:

Performs bulk ingestion of historical stock data for all tracked
securities.

Tasks:

-   Fetch historical price data
-   Fetch historical fundamentals
-   Fetch earnings history
-   Fetch dividend history
-   Publish events to Kafka

Kafka Topics:

-   stock_prices
-   fundamentals
-   earnings
-   dividends

This DAG runs primarily during initial dataset creation.

------------------------------------------------------------------------

## 3. Intraday Market Pipeline DAG

File:

dags/stock_intraday_pipeline_dag.py

Purpose:

Continuously ingests new market data and updates the warehouse.

Tasks:

-   Pull latest stock price data
-   Pull updated fundamentals
-   Pull earnings updates
-   Pull dividend events
-   Publish updates to Kafka
-   Trigger Spark transformations

Frequency:

Every 30 minutes or daily depending on data source.

------------------------------------------------------------------------

## 4. Feature Engineering DAG

File:

dags/market_analytics_dag.py

Purpose:

Computes quantitative signals and engineered financial features used for
analytics.

Tasks:

-   Calculate stock returns
-   Compute price momentum indicators
-   Calculate volatility metrics
-   Derive valuation indicators
-   Calculate growth metrics
-   Generate composite stock scores

Output Table:

gold.fact_market_signals

Frequency:

Runs after daily data ingestion.

------------------------------------------------------------------------

# Spark Processing Jobs

Spark performs the core ETL transformations.

------------------------------------------------------------------------

## Kafka to Bronze Job

File:

jobs/spark/bronze/spark_kafka_to_bronze.py

Purpose:

Consumes financial events from Kafka and writes them to raw Bronze
storage.

Tasks:

-   Read Kafka topics
-   Validate schema
-   Append raw events
-   Write partitioned parquet files

Output:

bronze data lake tables.

------------------------------------------------------------------------

## Bronze to Silver Job

File:

jobs/spark/silver/spark_bronze_to_silver.py

Purpose:

Transforms raw datasets into standardized, clean datasets.

Tasks:

-   Deduplicate records
-   Normalize schemas
-   Convert timestamps
-   Validate data quality

Output:

silver layer tables.

------------------------------------------------------------------------

## Gold Dimension Builders

### Build DIM_DATE

File:

build_dim_date.py

Purpose:

Generates a calendar dimension used across all fact tables.

Fields:

-   date_key
-   year
-   quarter
-   month
-   week
-   day_of_week

------------------------------------------------------------------------

### Build DIM_STOCK

File:

build_dim_stock.py

Purpose:

Creates the master dimension containing stock metadata.

Fields:

-   ticker
-   company name
-   sector
-   industry
-   exchange
-   country
-   IPO year

------------------------------------------------------------------------

## Fact Table Builders

### Build FACT_PRICE_DAILY

File:

build_fact_price_daily.py

Purpose:

Stores daily trading data for each stock.

Fields:

-   open price
-   high price
-   low price
-   close price
-   volume
-   market cap
-   volatility metrics

------------------------------------------------------------------------

### Build FACT_FUNDAMENTALS

File:

build_fact_fundamentals.py

Purpose:

Stores company financial indicators.

Metrics:

-   PE ratio
-   PB ratio
-   PS ratio
-   revenue
-   revenue growth
-   EPS
-   EPS growth
-   margins
-   debt ratios
-   free cash flow

------------------------------------------------------------------------

### Build FACT_DIVIDENDS

File:

build_fact_dividends.py

Purpose:

Tracks dividend payouts and yields over time.

Metrics:

-   dividend amount
-   dividend yield
-   dividend growth

------------------------------------------------------------------------

### Build FACT_EARNINGS

File:

build_fact_earnings.py

Purpose:

Stores company earnings reports.

Metrics:

-   reported EPS
-   expected EPS
-   EPS surprise
-   revenue
-   revenue surprise

------------------------------------------------------------------------

### Build FACT_MARKET_SIGNALS

File:

build_market_signals.py

Purpose:

Computes engineered quantitative features derived from market and
financial data.

Features:

-   price momentum indicators
-   volatility scores
-   risk scores
-   valuation metrics
-   growth indicators
-   financial health scores
-   overall stock strength score

This table enables:

-   stock ranking
-   factor analysis
-   screening for investment strategies

------------------------------------------------------------------------

# Example Project Structure

project/

dags/ update_stock_universe_dag.py stock_historical_backfill_dag.py
stock_intraday_pipeline_dag.py market_analytics_dag.py

jobs/

    ingestion/
        fetch_prices.py
        fetch_fundamentals.py
        fetch_earnings.py
        fetch_dividends.py

    spark/

        bronze/
            spark_kafka_to_bronze.py

        silver/
            spark_bronze_to_silver.py

        gold/
            build_dim_date.py
            build_dim_stock.py
            build_fact_price_daily.py
            build_fact_fundamentals.py
            build_fact_dividends.py
            build_fact_earnings.py
            build_market_signals.py

------------------------------------------------------------------------

# End Goal

The pipeline produces a structured financial analytics warehouse capable
of answering questions such as:

-   Which stocks have the strongest momentum?
-   Which companies show strong financial growth?
-   Which sectors outperform during market volatility?
-   Which stocks are undervalued relative to fundamentals?

This platform enables advanced financial analytics, quantitative
research, and dashboard-driven insights.

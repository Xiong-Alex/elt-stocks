import os
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


DATALAKE_BUCKET = os.getenv("S3_DATALAKE_BUCKET")


def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # Coerce selected fields once to keep chart code simple and consistent.
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def apply_theme(theme_mode: str = "Light") -> None:
    # Keep Streamlit's native look/feel; no custom CSS overrides.
    st.session_state["plotly_template"] = "plotly"


def render_sidebar_controls(default_refresh: int = 30) -> tuple[bool, int, str]:
    # Centralized sidebar UX used by all Streamlit pages.
    with st.sidebar:
        st.subheader("Refresh")
        auto_refresh = st.toggle("Auto refresh", value=True)
        refresh_sec = st.slider("Every (seconds)", min_value=15, max_value=120, value=default_refresh, step=5)
        st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if st.button("Clear Cache"):
            st.cache_data.clear()
            st.cache_resource.clear()

        st.divider()
        st.subheader("Health")
        health = db_health_status()
        st.write(f"Analytics DB: {'UP' if health['analytics_db'] else 'DOWN'}")
        st.write(f"Airflow DB: {'UP' if health['airflow_db'] else 'DOWN'}")
        st.write(f"MinIO/S3: {'UP' if health['s3'] else 'DOWN'}")
    return auto_refresh, refresh_sec, "Default"


def get_db_url() -> str:
    # Analytics database connection string for Gold/quarantine queries.
    host = os.getenv("ANALYTICS_DB_HOST")
    port = os.getenv("ANALYTICS_DB_PORT")
    db = os.getenv("ANALYTICS_DB_NAME")
    user = os.getenv("ANALYTICS_DB_USER")
    password = os.getenv("ANALYTICS_DB_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_resource
def get_engine():
    return create_engine(get_db_url(), pool_pre_ping=True)


def get_airflow_db_url() -> str:
    # Airflow metadata connection string for DAG/task monitoring views.
    host = os.getenv("AIRFLOW_DB_HOST")
    port = os.getenv("AIRFLOW_DB_PORT")
    db = os.getenv("AIRFLOW_DB_NAME")
    user = os.getenv("AIRFLOW_DB_USER")
    password = os.getenv("AIRFLOW_DB_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_resource
def get_airflow_engine():
    return create_engine(get_airflow_db_url(), pool_pre_ping=True)


@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=os.getenv("S3_REGION"),
    )


def query_df(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def query_df_params(sql: str, params: dict) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def query_scalar(sql: str):
    with get_engine().connect() as conn:
        return conn.execute(text(sql)).scalar()


def query_airflow_df(sql: str) -> pd.DataFrame:
    with get_airflow_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def table_exists(schema: str, table: str) -> bool:
    exists_sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema_name
      AND table_name = :table_name
    LIMIT 1
    """
    with get_engine().connect() as conn:
        row = conn.execute(text(exists_sql), {"schema_name": schema, "table_name": table}).first()
    return row is not None


def minio_prefix_snapshot(bucket: str, prefix: str, max_keys: int = 200):
    # Return a quick prefix snapshot without walking the full bucket.
    response = get_s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    contents = response.get("Contents", [])
    count = len(contents)
    latest = None
    latest_key = None
    if contents:
        latest_obj = max(contents, key=lambda x: x["LastModified"])
        latest = latest_obj["LastModified"]
        latest_key = latest_obj["Key"]
    return count, latest, latest_key, contents


def db_health_status() -> dict:
    # Best-effort health probes; failures are surfaced as DOWN in sidebar.
    status = {"analytics_db": False, "airflow_db": False, "s3": False}
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        status["analytics_db"] = True
    except Exception:
        pass
    try:
        with get_airflow_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        status["airflow_db"] = True
    except Exception:
        pass
    try:
        get_s3_client().list_buckets()
        status["s3"] = True
    except Exception:
        pass
    return status


@st.cache_data(ttl=20)
def load_market_snapshot(limit: int = 30) -> pd.DataFrame:
    df = query_df_params(
        """
        WITH ranked AS (
            SELECT
                symbol,
                event_ts,
                close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY event_ts) AS prev_close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_ts DESC) AS rn
            FROM public.stock_bars_gold
        )
        SELECT
            symbol,
            event_ts,
            close,
            prev_close,
            CASE
                WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                ELSE ((close - prev_close) / prev_close) * 100.0
            END AS change_pct
        FROM ranked
        WHERE rn = 1
        ORDER BY symbol
        LIMIT :limit
        """,
        {"limit": limit},
    )
    if not df.empty:
        df = _coerce_numeric_columns(df, ["close", "prev_close", "change_pct"])
        if "event_ts" in df.columns:
            df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    return df


def render_market_ticker(snapshot_df: pd.DataFrame) -> None:
    if snapshot_df.empty:
        st.info("Market snapshot unavailable.")
        return

    cols = st.columns(min(6, max(1, len(snapshot_df))))
    for idx, (_, row) in enumerate(snapshot_df.iterrows()):
        col = cols[idx % len(cols)]
        symbol = row["symbol"]
        close = row["close"]
        change = row["change_pct"]
        delta_txt = f"{change:+.2f}%" if pd.notna(change) else "N/A"
        with col:
            st.metric(label=symbol, value=f"{close:.2f}", delta=delta_txt)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # Compute overlays/lower-panel indicators used across chart pages.
    out = df.copy().sort_values("event_ts").reset_index(drop=True)
    out = _coerce_numeric_columns(out, ["open", "high", "low", "close", "volume"])
    out["sma_5"] = out["close"].rolling(5).mean()
    out["sma_20"] = out["close"].rolling(20).mean()
    out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()

    delta = out["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    rs = gain.rolling(14).mean() / loss.rolling(14).mean().replace(0, pd.NA)
    out["rsi_14"] = 100 - (100 / (1 + rs))

    typical_price = (out["high"] + out["low"] + out["close"]) / 3.0
    vol = out["volume"].fillna(0.0)
    out["vwap"] = (typical_price * vol).cumsum() / vol.cumsum().replace(0, pd.NA)

    bb_basis = out["close"].rolling(20).mean()
    bb_std = out["close"].rolling(20).std()
    out["bb_upper"] = bb_basis + (2 * bb_std)
    out["bb_lower"] = bb_basis - (2 * bb_std)

    ema_12 = out["close"].ewm(span=12, adjust=False).mean()
    ema_26 = out["close"].ewm(span=26, adjust=False).mean()
    out["macd"] = ema_12 - ema_26
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    high_low = out["high"] - out["low"]
    high_prev_close = (out["high"] - out["close"].shift(1)).abs()
    low_prev_close = (out["low"] - out["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    out["atr_14"] = tr.rolling(14).mean()

    plus_dm = out["high"].diff()
    minus_dm = -out["low"].diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    plus_di = 100 * (plus_dm.rolling(14).mean() / out["atr_14"])
    minus_di = 100 * (minus_dm.rolling(14).mean() / out["atr_14"])
    out["adx_14"] = (100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA))).rolling(14).mean()

    lowest_low_14 = out["low"].rolling(14).min()
    highest_high_14 = out["high"].rolling(14).max()
    out["stoch_k"] = 100 * ((out["close"] - lowest_low_14) / (highest_high_14 - lowest_low_14).replace(0, pd.NA))
    out["stoch_d"] = out["stoch_k"].rolling(3).mean()

    sma_tp = typical_price.rolling(20).mean()
    mean_dev = typical_price.rolling(20).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    out["cci_20"] = (typical_price - sma_tp) / (0.015 * mean_dev.replace(0, pd.NA))
    out["stddev_20"] = out["close"].rolling(20).std()
    out["obv"] = (out["close"].diff().fillna(0.0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)) * vol).cumsum()
    out["support_20"] = out["low"].rolling(20).min()
    out["resistance_20"] = out["high"].rolling(20).max()
    return out


def apply_time_axis(fig, compress_gaps: bool, bucket: str):
    # Hide non-trading windows when requested for clearer market charts.
    if not compress_gaps:
        return
    rangebreaks = [dict(bounds=["sat", "mon"])]
    if bucket in ["Raw", "1H"]:
        rangebreaks.append(dict(pattern="hour", bounds=[21, 14.5]))
    fig.update_xaxes(rangebreaks=rangebreaks)


@st.cache_data(ttl=30)
def load_symbol_trend(symbol: str, limit: int = 50000) -> pd.DataFrame:
    df = query_df_params(
        """
        SELECT event_ts, open, high, low, close, volume
        FROM public.stock_bars_gold
        WHERE symbol = :symbol
        ORDER BY event_ts
        LIMIT :limit
        """,
        {"symbol": symbol, "limit": limit},
    )
    if not df.empty:
        df = _coerce_numeric_columns(df, ["open", "high", "low", "close", "volume"])
        df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    return df


def _build_quarantine_where_clauses(params: dict) -> str:
    clauses = ["1=1"]
    if params.get("start_date"):
        clauses.append("DATE(quarantined_at_utc) >= :start_date")
    if params.get("end_date"):
        clauses.append("DATE(quarantined_at_utc) <= :end_date")
    if params.get("symbol"):
        clauses.append("symbol = :symbol")
    if params.get("reason"):
        clauses.append("quality_failure_reason = :reason")
    return " AND ".join(clauses)


@st.cache_data(ttl=30)
def load_quarantine_summary() -> pd.DataFrame:
    return query_df(
        """
        SELECT COUNT(*)::BIGINT AS total_rows,
               COUNT(DISTINCT symbol)::BIGINT AS distinct_symbols,
               MAX(quarantined_at_utc) AS latest_quarantine_ts
        FROM public.stock_bars_quarantine
        """
    )


@st.cache_data(ttl=30)
def load_quarantine_reason_breakdown(limit: int = 20) -> pd.DataFrame:
    return query_df_params(
        """
        SELECT quality_failure_reason, COUNT(*)::BIGINT AS row_count
        FROM public.stock_bars_quarantine
        GROUP BY quality_failure_reason
        ORDER BY row_count DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


@st.cache_data(ttl=30)
def load_quarantine_symbols(limit: int = 1000) -> pd.DataFrame:
    return query_df_params(
        """
        SELECT DISTINCT symbol
        FROM public.stock_bars_quarantine
        WHERE symbol IS NOT NULL
        ORDER BY symbol
        LIMIT :limit
        """,
        {"limit": limit},
    )


@st.cache_data(ttl=30)
def load_quarantine_reasons(limit: int = 1000) -> pd.DataFrame:
    return query_df_params(
        """
        SELECT DISTINCT quality_failure_reason
        FROM public.stock_bars_quarantine
        WHERE quality_failure_reason IS NOT NULL
        ORDER BY quality_failure_reason
        LIMIT :limit
        """,
        {"limit": limit},
    )


@st.cache_data(ttl=30)
def load_quarantine_rows(
    start_date: str | None,
    end_date: str | None,
    symbol: str | None,
    reason: str | None,
    limit: int,
) -> pd.DataFrame:
    params = {"start_date": start_date, "end_date": end_date, "symbol": symbol, "reason": reason, "limit": limit}
    where_clause = _build_quarantine_where_clauses(params)
    return query_df_params(
        f"""
        SELECT symbol, event_ts, open, high, low, close, volume, quality_failure_reason, quarantined_at_utc
        FROM public.stock_bars_quarantine
        WHERE {where_clause}
        ORDER BY quarantined_at_utc DESC
        LIMIT :limit
        """,
        params,
    )


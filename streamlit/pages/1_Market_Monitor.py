import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from shared import (
    DATALAKE_BUCKET,
    add_indicators,
    apply_theme,
    apply_time_axis,
    load_market_snapshot,
    load_symbol_trend,
    minio_prefix_snapshot,
    query_airflow_df,
    query_scalar,
    render_market_ticker,
    render_sidebar_controls,
    table_exists,
)


st.set_page_config(page_title="Market Monitor", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=30)
apply_theme(theme_mode=theme_mode)

st.title("Market Monitor")
st.caption("Live market and pipeline status. No simulated trading controls on this page.")

snapshot_df = pd.DataFrame()
try:
    snapshot_df = load_market_snapshot(limit=100)
except Exception:
    snapshot_df = pd.DataFrame()

if snapshot_df.empty:
    st.warning("Market snapshot unavailable. Run ingestion and transformations, then refresh.")
    st.stop()

snapshot_df = snapshot_df.dropna(subset=["symbol", "close"])
snapshot_df["event_ts"] = pd.to_datetime(snapshot_df["event_ts"], utc=True, errors="coerce")
snapshot_df["change_pct"] = pd.to_numeric(snapshot_df["change_pct"], errors="coerce")

# Watchlist controls the tape and focus context.
st.subheader("Market Tape")
watchlist_symbols = st.multiselect(
    "Watchlist",
    snapshot_df["symbol"].tolist(),
    default=snapshot_df["symbol"].head(8).tolist(),
    help="Choose symbols to pin in the market tape and focus areas.",
)
tape_df = snapshot_df[snapshot_df["symbol"].isin(watchlist_symbols)] if watchlist_symbols else snapshot_df.head(8)
render_market_ticker(tape_df)

advancers = int((snapshot_df["change_pct"] > 0).sum())
decliners = int((snapshot_df["change_pct"] < 0).sum())
avg_move = snapshot_df["change_pct"].dropna().mean()
latest_ts = snapshot_df["event_ts"].max()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Tracked Symbols", f"{snapshot_df['symbol'].nunique():,}")
m2.metric("Advancers / Decliners", f"{advancers} / {decliners}")
m3.metric("Average Move", f"{avg_move:+.2f}%" if pd.notna(avg_move) else "N/A")
m4.metric("Latest Snapshot", str(latest_ts) if pd.notna(latest_ts) else "N/A")

left, right = st.columns([2.2, 1.2])
with left:
    st.subheader("Focus Chart")
    # Primary chart controls for one symbol/time window.
    f1, f2, f3 = st.columns(3)
    focus_symbol = f1.selectbox("Ticker", snapshot_df["symbol"].tolist(), index=0)
    timeframe = f2.selectbox("Range", ["1D", "5D", "1M", "3M", "6M", "1Y"], index=2)
    bucket = f3.selectbox("Interval", ["Raw", "1H", "1D"], index=0)

    f4, f5 = st.columns(2)
    overlays = f4.multiselect(
        "Overlays",
        ["SMA(5)", "SMA(20)", "EMA(20)", "EMA(50)", "VWAP", "BB Upper", "BB Lower", "Support(20)", "Resistance(20)"],
        default=["SMA(20)", "VWAP"],
    )
    compress_gaps = f5.toggle("Compress Market Gaps", value=True)

    trend_df = load_symbol_trend(focus_symbol, limit=50000).sort_values("event_ts")
    lookback_map = {
        "1D": pd.Timedelta(days=1),
        "5D": pd.Timedelta(days=5),
        "1M": pd.Timedelta(days=30),
        "3M": pd.Timedelta(days=90),
        "6M": pd.Timedelta(days=180),
        "1Y": pd.Timedelta(days=365),
    }
    cutoff = pd.Timestamp.now(tz="UTC") - lookback_map[timeframe]
    trend_df = trend_df[trend_df["event_ts"] >= cutoff]
    if bucket != "Raw":
        trend_df = (
            trend_df.set_index("event_ts")
            .resample(bucket)
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
    trend_df = add_indicators(trend_df)

    if trend_df.empty:
        st.info("No data for this selection.")
    else:
        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=trend_df["event_ts"],
                    open=trend_df["open"],
                    high=trend_df["high"],
                    low=trend_df["low"],
                    close=trend_df["close"],
                    name=f"{focus_symbol} OHLC",
                    increasing_line_color="#16a34a",
                    decreasing_line_color="#dc2626",
                )
            ]
        )
        if "SMA(5)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["sma_5"], mode="lines", name="SMA(5)"))
        if "SMA(20)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["sma_20"], mode="lines", name="SMA(20)"))
        if "EMA(20)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["ema_20"], mode="lines", name="EMA(20)"))
        if "EMA(50)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["ema_50"], mode="lines", name="EMA(50)"))
        if "VWAP" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["vwap"], mode="lines", name="VWAP"))
        if "BB Upper" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["bb_upper"], mode="lines", name="BB Upper"))
        if "BB Lower" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["bb_lower"], mode="lines", name="BB Lower"))
        if "Support(20)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["support_20"], mode="lines", name="Support(20)"))
        if "Resistance(20)" in overlays:
            fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["resistance_20"], mode="lines", name="Resistance(20)"))

        fig.update_layout(height=500, margin=dict(l=8, r=8, t=10, b=8), xaxis_rangeslider_visible=False, template="plotly_white")
        apply_time_axis(fig, compress_gaps=compress_gaps, bucket=bucket)
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Live Snapshot")
    # Quick screener view for strongest/weakest movers.
    grid_df = snapshot_df.copy()
    grid_df["direction"] = grid_df["change_pct"].apply(
        lambda x: "UP" if pd.notna(x) and x > 0 else ("DOWN" if pd.notna(x) and x < 0 else "FLAT")
    )
    st.dataframe(
        grid_df[["symbol", "close", "change_pct", "direction", "event_ts"]]
        .sort_values("change_pct", ascending=False)
        .head(20),
        use_container_width=True,
        hide_index=True,
    )

    top_gainers = snapshot_df.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=False).head(5)
    top_losers = snapshot_df.dropna(subset=["change_pct"]).sort_values("change_pct", ascending=True).head(5)
    st.markdown("**Top Gainers**")
    st.dataframe(top_gainers[["symbol", "close", "change_pct"]], use_container_width=True, hide_index=True)
    st.markdown("**Top Losers**")
    st.dataframe(top_losers[["symbol", "close", "change_pct"]], use_container_width=True, hide_index=True)

st.subheader("Market Performance Heatmap")
# Treemap uses close as tile size and percent change as color.
heatmap_df = snapshot_df.dropna(subset=["change_pct"]).copy()
if not heatmap_df.empty:
    heatmap_df["price_change"] = heatmap_df["close"] - heatmap_df["prev_close"]
    heatmap_df["display_change_pct"] = heatmap_df["change_pct"].map(lambda v: f"{v:+.2f}%")
    heatmap_df["display_price_change"] = heatmap_df["price_change"].map(lambda v: f"{v:+.2f}" if pd.notna(v) else "N/A")
    heatmap_df["display_price"] = heatmap_df["close"].map(lambda v: f"{v:.2f}")
    heatmap_fig = px.treemap(
        heatmap_df,
        path=["symbol"],
        values="close",
        color="change_pct",
        color_continuous_scale=["#dc2626", "#f3f4f6", "#16a34a"],
        color_continuous_midpoint=0,
        custom_data=["display_change_pct", "display_price_change", "display_price"],
        template="plotly_white",
    )
    heatmap_fig.update_traces(
        texttemplate="%{label}<br>%{customdata[0]}<br>Delta %{customdata[1]}",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Change: %{customdata[0]}<br>"
            "Price Change: %{customdata[1]}<br>"
            "Current Price: %{customdata[2]}<extra></extra>"
        ),
    )
    heatmap_fig.update_layout(margin=dict(t=10, l=8, r=8, b=8), height=380)
    st.plotly_chart(heatmap_fig, use_container_width=True)
else:
    st.info("Not enough data for heatmap.")

st.subheader("Pipeline Activity")
# Pair orchestration state (Airflow) with storage activity (MinIO).
p1, p2 = st.columns(2)
with p1:
    st.markdown("**Airflow DAG Runs**")
    try:
        dag_runs = query_airflow_df(
            """
            SELECT dag_id, run_id, state, start_date, end_date
            FROM dag_run
            WHERE dag_id IN ('update_stock_universe_dag', 'historical_backfill_dag', 'intraday_pipeline_dag')
            ORDER BY start_date DESC
            LIMIT 30
            """
        )
        st.dataframe(dag_runs, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Unable to query dag_run table: {exc}")

with p2:
    st.markdown("**Lake Storage Activity**")
    try:
        bronze_count, bronze_latest, bronze_key, _ = minio_prefix_snapshot(DATALAKE_BUCKET, "bronze/stock_bars/")
        silver_count, silver_latest, silver_key, _ = minio_prefix_snapshot(DATALAKE_BUCKET, "silver/stock_bars_clean/")
        st.metric("Bronze Objects (sampled)", bronze_count)
        st.metric("Silver Objects (sampled)", silver_count)
        st.write(f"Bronze latest object: `{bronze_key}`" if bronze_key else "Bronze latest object: N/A")
        st.write(f"Bronze latest modified: `{bronze_latest}`" if bronze_latest else "Bronze latest modified: N/A")
        st.write(f"Silver latest object: `{silver_key}`" if silver_key else "Silver latest object: N/A")
        st.write(f"Silver latest modified: `{silver_latest}`" if silver_latest else "Silver latest modified: N/A")
    except Exception as exc:
        st.error(f"Unable to query MinIO: {exc}")

if table_exists("public", "stock_bars_gold"):
    total_rows = query_scalar("SELECT COUNT(*) FROM public.stock_bars_gold")
    st.caption(f"Gold table row count: {total_rows:,}" if total_rows is not None else "Gold table row count: N/A")

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

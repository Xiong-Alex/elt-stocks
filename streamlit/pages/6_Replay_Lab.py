import time
from datetime import datetime, time as dt_time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from shared import add_indicators, apply_theme, apply_time_axis, load_market_snapshot, load_symbol_trend, render_sidebar_controls


OVERLAY_MAP = {
    "SMA(5)": "sma_5",
    "SMA(20)": "sma_20",
    "EMA(20)": "ema_20",
    "EMA(50)": "ema_50",
    "VWAP": "vwap",
    "BB Upper": "bb_upper",
    "BB Lower": "bb_lower",
    "Support(20)": "support_20",
    "Resistance(20)": "resistance_20",
}

LOWER_MAP = {
    "RSI(14)": ["rsi_14"],
    "MACD": ["macd", "macd_signal"],
    "Stochastic": ["stoch_k", "stoch_d"],
    "ADX(14)": ["adx_14"],
    "CCI(20)": ["cci_20"],
    "ATR(14)": ["atr_14"],
    "OBV": ["obv"],
}


def _parse_datetime(date_value, time_value) -> pd.Timestamp:
    # Combine separate date/time inputs into a UTC timestamp boundary.
    return pd.Timestamp(datetime.combine(date_value, time_value), tz="UTC")


def _ensure_state(df_len: int, start_idx: int) -> None:
    # Persist playback cursor across reruns.
    if "replay_playing" not in st.session_state:
        st.session_state["replay_playing"] = False
    if "replay_idx" not in st.session_state:
        st.session_state["replay_idx"] = start_idx
    st.session_state["replay_idx"] = int(min(max(st.session_state["replay_idx"], start_idx), max(df_len - 1, start_idx)))


st.set_page_config(page_title="Replay Lab", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=120)
apply_theme(theme_mode=theme_mode)

st.title("Replay Lab")
st.caption("Select exact time windows and replay one candlestick at a time with indicator overlays and lower panels.")

snapshot_df = load_market_snapshot(limit=100)
if snapshot_df.empty:
    st.warning("No market snapshot available yet.")
    st.stop()

symbols = snapshot_df["symbol"].tolist()
c1, c2, c3 = st.columns([1.2, 1, 1])
symbol = c1.selectbox("Symbol", symbols, index=0)
bucket = c2.selectbox("Replay Interval", ["Raw", "5min", "15min", "1H", "1D"], index=0)
speed_ms = c3.slider("Playback Speed (ms per candle)", min_value=100, max_value=3000, value=800, step=100)

raw_df = load_symbol_trend(symbol, limit=50000).sort_values("event_ts")
if raw_df.empty:
    st.warning("No historical bars for selected symbol.")
    st.stop()

min_ts = pd.to_datetime(raw_df["event_ts"].min(), utc=True)
max_ts = pd.to_datetime(raw_df["event_ts"].max(), utc=True)

d1, d2, d3, d4 = st.columns(4)
default_start = (max_ts - pd.Timedelta(days=30)).date() if max_ts > min_ts else min_ts.date()
start_date = d1.date_input("Start Date (UTC)", value=max(min_ts.date(), default_start), min_value=min_ts.date(), max_value=max_ts.date())
start_time = d2.time_input("Start Time (UTC)", value=dt_time(0, 0))
end_date = d3.date_input("End Date (UTC)", value=max_ts.date(), min_value=min_ts.date(), max_value=max_ts.date())
end_time = d4.time_input("End Time (UTC)", value=dt_time(23, 59))

start_ts = _parse_datetime(start_date, start_time)
end_ts = _parse_datetime(end_date, end_time)
if end_ts <= start_ts:
    st.error("End datetime must be after start datetime.")
    st.stop()

df = raw_df[(raw_df["event_ts"] >= start_ts) & (raw_df["event_ts"] <= end_ts)].copy()
if df.empty:
    st.warning("No bars in selected window.")
    st.stop()

if bucket != "Raw":
    # Aggregate raw candles to the selected replay bucket.
    rule_map = {"5min": "5min", "15min": "15min", "1H": "1H", "1D": "1D"}
    df = (
        df.set_index("event_ts")
        .resample(rule_map[bucket])
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )

df = add_indicators(df)
if len(df) < 5:
    st.warning("Not enough candles in selected window. Widen time range or use a smaller interval.")
    st.stop()

start_idx_default = min(20, len(df) - 1)
_ensure_state(len(df), start_idx_default)

o1, o2, o3 = st.columns(3)
overlay_opts = o1.multiselect("Overlay Indicators", list(OVERLAY_MAP.keys()), default=["SMA(20)", "VWAP"])
lower_opts = o2.multiselect("Lower Panels", list(LOWER_MAP.keys()), default=[])
compress_gaps = o3.toggle("Compress Market Gaps", value=True)

p1, p2, p3, p4, p5 = st.columns(5)
if p1.button("Play", use_container_width=True):
    st.session_state["replay_playing"] = True
if p2.button("Stop", use_container_width=True):
    st.session_state["replay_playing"] = False
if p3.button("Step -1", use_container_width=True):
    st.session_state["replay_playing"] = False
    st.session_state["replay_idx"] = max(st.session_state["replay_idx"] - 1, start_idx_default)
if p4.button("Step +1", use_container_width=True):
    st.session_state["replay_playing"] = False
    st.session_state["replay_idx"] = min(st.session_state["replay_idx"] + 1, len(df) - 1)
if p5.button("Reset", use_container_width=True):
    st.session_state["replay_playing"] = False
    st.session_state["replay_idx"] = start_idx_default

slider_idx = st.slider(
    "Replay Cursor",
    min_value=start_idx_default,
    max_value=len(df) - 1,
    value=st.session_state["replay_idx"],
    step=1,
)
if slider_idx != st.session_state["replay_idx"]:
    st.session_state["replay_playing"] = False
    st.session_state["replay_idx"] = slider_idx

view_df = df.iloc[: st.session_state["replay_idx"] + 1].copy()
last = view_df.iloc[-1]
m1, m2, m3 = st.columns(3)
m1.metric("Replay Price", f"${float(last['close']):,.2f}")
m2.metric("Replay Time (UTC)", str(pd.to_datetime(last["event_ts"])))
m3.metric("Candles Revealed", f"{len(view_df):,}/{len(df):,}")

main_fig = go.Figure(
    data=[
        go.Candlestick(
            x=view_df["event_ts"],
            open=view_df["open"],
            high=view_df["high"],
            low=view_df["low"],
            close=view_df["close"],
            name=f"{symbol} OHLC",
            increasing_line_color="#19a566",
            decreasing_line_color="#d94f66",
        )
    ]
)
# Overlay lines are rendered on top of the replayed candlestick window.
for opt in overlay_opts:
    col = OVERLAY_MAP[opt]
    if col in view_df.columns:
        main_fig.add_trace(go.Scatter(x=view_df["event_ts"], y=view_df[col], mode="lines", name=opt))

main_fig.update_layout(height=500, margin=dict(l=8, r=8, t=8, b=8), xaxis_rangeslider_visible=False, template="plotly_white")
apply_time_axis(main_fig, compress_gaps=compress_gaps, bucket=bucket if bucket != "Raw" else "1H")
st.plotly_chart(main_fig, use_container_width=True)

for panel in lower_opts:
    # Lower panels share the same replay cursor as the main chart.
    fig = go.Figure()
    for col in LOWER_MAP[panel]:
        if col in view_df.columns:
            fig.add_trace(go.Scatter(x=view_df["event_ts"], y=view_df[col], mode="lines", name=col))
    if panel == "RSI(14)":
        fig.add_hline(y=70, line_dash="dash")
        fig.add_hline(y=30, line_dash="dash")
    if panel == "Stochastic":
        fig.add_hline(y=80, line_dash="dash")
        fig.add_hline(y=20, line_dash="dash")
    if panel == "CCI(20)":
        fig.add_hline(y=100, line_dash="dash")
        fig.add_hline(y=-100, line_dash="dash")
    if panel == "ADX(14)":
        fig.add_hline(y=25, line_dash="dash")
    fig.update_layout(title=panel, height=250, margin=dict(l=8, r=8, t=30, b=8), template="plotly_white")
    apply_time_axis(fig, compress_gaps=compress_gaps, bucket=bucket if bucket != "Raw" else "1H")
    st.plotly_chart(fig, use_container_width=True)

if st.session_state["replay_playing"] and st.session_state["replay_idx"] < len(df) - 1:
    # Advance one candle per rerun cycle while in play mode.
    time.sleep(speed_ms / 1000.0)
    st.session_state["replay_idx"] = min(st.session_state["replay_idx"] + 1, len(df) - 1)
    st.rerun()
elif st.session_state["replay_playing"] and st.session_state["replay_idx"] >= len(df) - 1:
    st.session_state["replay_playing"] = False

if auto_refresh and not st.session_state["replay_playing"]:
    time.sleep(refresh_sec)
    st.rerun()

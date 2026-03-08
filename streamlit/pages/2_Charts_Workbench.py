import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from shared import (
    add_indicators,
    apply_theme,
    apply_time_axis,
    load_symbol_trend,
    query_df,
    render_sidebar_controls,
    table_exists,
)


st.set_page_config(page_title="Charts Workbench", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=45)
apply_theme(theme_mode=theme_mode)

st.title("Charts Workbench")
st.caption("Focus-style chart components with per-chart options and multi-symbol comparison.")

COMPARE_INDICATOR_OPTIONS = [
    "SMA(5)",
    "SMA(20)",
    "EMA(20)",
    "EMA(50)",
    "VWAP",
    "BB Upper",
    "BB Lower",
    "Support",
    "Resistance",
]

OVERLAY_INDICATOR_OPTIONS = [
    "SMA(5)",
    "SMA(20)",
    "EMA(20)",
    "EMA(50)",
    "VWAP",
    "BB Upper",
    "BB Lower",
    "Support(20)",
    "Resistance(20)",
]

COMPARE_INDICATOR_COLUMN_MAP = {
    "SMA(5)": "sma_5",
    "SMA(20)": "sma_20",
    "EMA(20)": "ema_20",
    "EMA(50)": "ema_50",
    "VWAP": "vwap",
    "BB Upper": "bb_upper",
    "BB Lower": "bb_lower",
    "Support": "support_20",
    "Resistance": "resistance_20",
}


if not table_exists("public", "stock_bars_gold"):
    st.info(
        "Gold table `public.stock_bars_gold` does not exist yet. "
        "Run `historical_backfill_dag` through `spark_to_gold` first."
    )
else:
    symbols_df = query_df(
        """
        SELECT DISTINCT symbol
        FROM public.stock_bars_gold
        ORDER BY symbol
        """
    )
    if symbols_df.empty:
        st.info("No rows in public.stock_bars_gold yet.")
    else:
        if "chart_components" not in st.session_state:
            st.session_state["chart_components"] = [1]

        if st.button("Add Chart / Graph"):
            st.session_state["chart_components"].append(max(st.session_state["chart_components"]) + 1)

        remove_ids = []
        for comp_id in st.session_state["chart_components"]:
            # Each component is an independent chart workspace.
            st.markdown(f"### Focus Chart Component #{comp_id}")
            c1, c2, c3, c4 = st.columns(4)
            selected_symbol = c1.selectbox("Ticker", symbols_df["symbol"].tolist(), key=f"symbol_{comp_id}")
            timeframe = c2.selectbox("Range", ["1D", "5D", "1M", "3M", "6M", "1Y", "All"], index=4, key=f"timeframe_{comp_id}")
            bucket = c3.selectbox("Interval", ["Raw", "1H", "1D", "1W"], index=0, key=f"bucket_{comp_id}")
            chart_type = c4.selectbox("Chart Type", ["Candlestick", "Line", "Bar"], index=0, key=f"chart_type_{comp_id}")

            c5, c6, c7 = st.columns([2, 1, 1])
            overlay_indicators = c5.multiselect(
                "Overlays",
                OVERLAY_INDICATOR_OPTIONS,
                default=["SMA(20)", "VWAP"],
                key=f"overlay_{comp_id}",
            )
            compress_gaps = c6.toggle("Compress Market Gaps", value=True, key=f"gaps_{comp_id}")
            lower_panels = c7.multiselect(
                "Lower Panels",
                ["RSI(14)", "MACD", "Stochastic", "CCI(20)", "ADX(14)", "ATR(14)", "StdDev(20)", "OBV"],
                default=[],
                key=f"lower_{comp_id}",
            )

            c8, c9, c10 = st.columns(3)
            compare_symbols = c8.multiselect(
                "Compare Symbols",
                [s for s in symbols_df["symbol"].tolist() if s != selected_symbol],
                default=[],
                key=f"compare_{comp_id}",
            )
            normalize_compare = c9.toggle(
                "Normalize Compare (base=100)",
                value=True,
                key=f"norm_{comp_id}",
                help="Scales each symbol to start at 100 for relative performance comparison.",
            )
            compare_chart_type = c10.selectbox(
                "Comparison Chart Type",
                ["Line", "Candlestick"],
                index=0,
                key=f"compare_chart_type_{comp_id}",
            )

            tracked_symbols = [selected_symbol] + [s for s in compare_symbols if s != selected_symbol]
            compare_indicator_map = {}
            if compare_symbols:
                st.markdown("**Comparison Indicators by Symbol**")
                st.caption("Pick overlays per symbol for the comparison chart.")
                for symbol in tracked_symbols:
                    compare_indicator_map[symbol] = st.multiselect(
                        f"{symbol} Indicators",
                        COMPARE_INDICATOR_OPTIONS,
                        default=["SMA(20)"] if symbol == selected_symbol else [],
                        key=f"compare_indicators_{comp_id}_{symbol}",
                    )
            else:
                st.caption("Add one or more symbols in `Compare Symbols` to enable comparison chart/options.")

            if st.button("Remove This Chart", key=f"remove_{comp_id}"):
                remove_ids.append(comp_id)
                st.divider()
                continue

            trend_map = {}
            lookback_map = {
                "1D": pd.Timedelta(days=1),
                "5D": pd.Timedelta(days=5),
                "1M": pd.Timedelta(days=30),
                "3M": pd.Timedelta(days=90),
                "6M": pd.Timedelta(days=180),
                "1Y": pd.Timedelta(days=365),
            }

            for symbol in tracked_symbols:
                # Load and shape each symbol once so all charts reuse the same frame.
                symbol_df = load_symbol_trend(symbol=symbol, limit=50000)
                if symbol_df.empty:
                    continue
                symbol_df["event_ts"] = pd.to_datetime(symbol_df["event_ts"], utc=True)
                if timeframe != "All":
                    cutoff = pd.Timestamp.now(tz="UTC") - lookback_map[timeframe]
                    symbol_df = symbol_df[symbol_df["event_ts"] >= cutoff]
                symbol_df = symbol_df.sort_values("event_ts")
                if bucket != "Raw":
                    symbol_df = (
                        symbol_df.set_index("event_ts")
                        .resample(bucket)
                        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                        .dropna(subset=["open", "high", "low", "close"])
                        .reset_index()
                    )
                symbol_df = add_indicators(symbol_df)
                if not symbol_df.empty:
                    trend_map[symbol] = symbol_df

            primary_df = trend_map.get(selected_symbol)
            if primary_df is None or primary_df.empty:
                st.info("No data found for the selected symbol/timeframe.")
                st.divider()
                continue

            if chart_type == "Candlestick":
                fig = go.Figure(
                    data=[
                        go.Candlestick(
                            x=primary_df["event_ts"],
                            open=primary_df["open"],
                            high=primary_df["high"],
                            low=primary_df["low"],
                            close=primary_df["close"],
                            name=f"{selected_symbol} OHLC",
                        )
                    ]
                )
            elif chart_type == "Bar":
                fig = px.bar(primary_df, x="event_ts", y="close", title=f"{selected_symbol} close ({timeframe}, {bucket})")
            else:
                fig = px.line(primary_df, x="event_ts", y=["close"], title=f"{selected_symbol} close ({timeframe}, {bucket})")

            if "SMA(5)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["sma_5"], mode="lines", name=f"{selected_symbol} SMA(5)"))
            if "SMA(20)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["sma_20"], mode="lines", name=f"{selected_symbol} SMA(20)"))
            if "EMA(20)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["ema_20"], mode="lines", name=f"{selected_symbol} EMA(20)"))
            if "EMA(50)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["ema_50"], mode="lines", name=f"{selected_symbol} EMA(50)"))
            if "VWAP" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["vwap"], mode="lines", name=f"{selected_symbol} VWAP"))
            if "BB Upper" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["bb_upper"], mode="lines", name=f"{selected_symbol} BB Upper"))
            if "BB Lower" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["bb_lower"], mode="lines", name=f"{selected_symbol} BB Lower"))
            if "Support(20)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["support_20"], mode="lines", name=f"{selected_symbol} Support(20)"))
            if "Resistance(20)" in overlay_indicators:
                fig.add_trace(go.Scatter(x=primary_df["event_ts"], y=primary_df["resistance_20"], mode="lines", name=f"{selected_symbol} Resistance(20)"))

            fig.update_layout(xaxis_title="Time", yaxis_title="Price")
            apply_time_axis(fig, compress_gaps=compress_gaps, bucket=bucket)
            st.plotly_chart(fig, use_container_width=True)

            if compare_symbols:
                # Render optional multi-symbol comparison as line or candlestick.
                compare_fig = go.Figure()
                if compare_chart_type == "Candlestick":
                    if normalize_compare:
                        st.caption("Normalization is ignored for candlestick comparison.")
                    for symbol in tracked_symbols:
                        sym_df = trend_map.get(symbol)
                        if sym_df is None or sym_df.empty:
                            continue
                        compare_fig.add_trace(
                            go.Candlestick(
                                x=sym_df["event_ts"],
                                open=sym_df["open"],
                                high=sym_df["high"],
                                low=sym_df["low"],
                                close=sym_df["close"],
                                name=f"{symbol} OHLC",
                                opacity=0.7,
                            )
                        )
                        selected_indicators = compare_indicator_map.get(symbol, [])
                        for indicator_name in selected_indicators:
                            col_name = COMPARE_INDICATOR_COLUMN_MAP.get(indicator_name)
                            if not col_name or col_name not in sym_df.columns:
                                continue
                            compare_fig.add_trace(
                                go.Scatter(
                                    x=sym_df["event_ts"],
                                    y=sym_df[col_name],
                                    mode="lines",
                                    name=f"{symbol} {indicator_name}",
                                    line=dict(dash="dot"),
                                )
                            )
                else:
                    for symbol in tracked_symbols:
                        sym_df = trend_map.get(symbol)
                        if sym_df is None or sym_df.empty:
                            continue
                        close_series = sym_df["close"]
                        base_value = close_series.iloc[0] if len(close_series) > 0 else None

                        y = close_series
                        if normalize_compare and base_value is not None and pd.notna(base_value) and base_value != 0:
                            y = (y / base_value) * 100.0
                        compare_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=y, mode="lines", name=symbol))

                        selected_indicators = compare_indicator_map.get(symbol, [])
                        for indicator_name in selected_indicators:
                            col_name = COMPARE_INDICATOR_COLUMN_MAP.get(indicator_name)
                            if not col_name or col_name not in sym_df.columns:
                                continue
                            ind_y = sym_df[col_name]
                            if normalize_compare and base_value is not None and pd.notna(base_value) and base_value != 0:
                                ind_y = (ind_y / base_value) * 100.0
                            compare_fig.add_trace(
                                go.Scatter(
                                    x=sym_df["event_ts"],
                                    y=ind_y,
                                    mode="lines",
                                    name=f"{symbol} {indicator_name}",
                                    line=dict(dash="dot"),
                                )
                            )
                compare_fig.update_layout(
                    title=f"Symbol Comparison ({compare_chart_type})",
                    xaxis_title="Time",
                    yaxis_title=(
                        "Normalized Close (base=100)"
                        if (normalize_compare and compare_chart_type == "Line")
                        else "Price"
                    ),
                )
                apply_time_axis(compare_fig, compress_gaps=compress_gaps, bucket=bucket)
                st.plotly_chart(compare_fig, use_container_width=True)

            for panel in lower_panels:
                # Lower panels reuse the same filtered time window and symbols.
                panel_fig = go.Figure()
                for symbol in tracked_symbols:
                    sym_df = trend_map.get(symbol)
                    if sym_df is None or sym_df.empty:
                        continue
                    if panel == "RSI(14)":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["rsi_14"], mode="lines", name=symbol))
                    elif panel == "MACD":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["macd"], mode="lines", name=f"{symbol} MACD"))
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["macd_signal"], mode="lines", name=f"{symbol} Signal"))
                    elif panel == "Stochastic":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["stoch_k"], mode="lines", name=f"{symbol} %K"))
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["stoch_d"], mode="lines", name=f"{symbol} %D"))
                    elif panel == "CCI(20)":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["cci_20"], mode="lines", name=symbol))
                    elif panel == "ADX(14)":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["adx_14"], mode="lines", name=symbol))
                    elif panel == "ATR(14)":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["atr_14"], mode="lines", name=symbol))
                    elif panel == "StdDev(20)":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["stddev_20"], mode="lines", name=symbol))
                    elif panel == "OBV":
                        panel_fig.add_trace(go.Scatter(x=sym_df["event_ts"], y=sym_df["obv"], mode="lines", name=symbol))

                if panel == "RSI(14)":
                    panel_fig.add_hline(y=70, line_dash="dash")
                    panel_fig.add_hline(y=30, line_dash="dash")
                if panel == "Stochastic":
                    panel_fig.add_hline(y=80, line_dash="dash")
                    panel_fig.add_hline(y=20, line_dash="dash")
                if panel == "CCI(20)":
                    panel_fig.add_hline(y=100, line_dash="dash")
                    panel_fig.add_hline(y=-100, line_dash="dash")
                if panel == "ADX(14)":
                    panel_fig.add_hline(y=25, line_dash="dash")
                panel_fig.update_layout(title=panel, xaxis_title="Time")
                apply_time_axis(panel_fig, compress_gaps=compress_gaps, bucket=bucket)
                st.plotly_chart(panel_fig, use_container_width=True)

            st.divider()

        if remove_ids:
            st.session_state["chart_components"] = [cid for cid in st.session_state["chart_components"] if cid not in remove_ids]
            st.rerun()

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

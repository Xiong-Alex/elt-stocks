import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from shared import apply_theme, query_df, query_df_params, render_sidebar_controls, table_exists


st.set_page_config(page_title="Feature Engineering", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=45)
apply_theme(theme_mode=theme_mode)

st.title("Feature Engineering")
st.caption("Inspect engineered fact tables, signal scores, and feature readiness.")

required_tables = {
    "fact_price_daily": table_exists("public", "fact_price_daily"),
    "fact_fundamentals": table_exists("public", "fact_fundamentals"),
    "fact_dividends": table_exists("public", "fact_dividends"),
    "fact_earnings": table_exists("public", "fact_earnings"),
    "fact_market_signals": table_exists("public", "fact_market_signals"),
}

status_cols = st.columns(5)
for idx, (table_name, exists) in enumerate(required_tables.items()):
    with status_cols[idx]:
        st.metric(table_name, "READY" if exists else "MISSING")

if not required_tables["fact_market_signals"]:
    st.info(
        "Feature tables are not fully available yet. "
        "Run `intraday_pipeline_dag` and `feature_engineering_dag`, then refresh."
    )
else:
    summary_df = query_df(
        """
        SELECT
            (SELECT MAX(event_date) FROM public.fact_market_signals) AS latest_signal_date,
            (SELECT COUNT(*) FROM public.fact_market_signals) AS signal_rows,
            (SELECT COUNT(DISTINCT ticker) FROM public.fact_market_signals) AS ticker_count,
            (SELECT COUNT(*) FROM public.fact_price_daily) AS price_rows,
            (SELECT COUNT(*) FROM public.fact_fundamentals) AS fundamentals_rows,
            (SELECT COUNT(*) FROM public.fact_dividends) AS dividends_rows,
            (SELECT COUNT(*) FROM public.fact_earnings) AS earnings_rows
        """
    )
    summary = summary_df.iloc[0]
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Latest Signal Date", str(summary["latest_signal_date"]))
    m2.metric("Signal Rows", f"{int(summary['signal_rows']):,}")
    m3.metric("Tickers with Signals", f"{int(summary['ticker_count']):,}")
    m4.metric("Price/Fundamentals Rows", f"{int(summary['price_rows']):,} / {int(summary['fundamentals_rows']):,}")

    st.subheader("Latest Signal Ranking")
    top_n = st.slider("Top N", min_value=10, max_value=100, value=25, step=5)
    ranking_df = query_df_params(
        """
        WITH latest_date AS (
            SELECT MAX(event_date) AS event_date
            FROM public.fact_market_signals
        )
        SELECT
            s.ticker,
            s.event_date,
            s.composite_strength_score,
            s.momentum_5d,
            s.momentum_20d,
            s.volatility_score,
            s.risk_score,
            s.stability_score,
            s.growth_score,
            f.pe_ratio,
            f.revenue_growth,
            f.eps_growth
        FROM public.fact_market_signals s
        LEFT JOIN public.fact_fundamentals f
          ON f.ticker = s.ticker
         AND f.report_date = (
             SELECT MAX(f2.report_date)
             FROM public.fact_fundamentals f2
             WHERE f2.ticker = s.ticker
         )
        WHERE s.event_date = (SELECT event_date FROM latest_date)
        ORDER BY s.composite_strength_score DESC NULLS LAST, s.ticker
        LIMIT :top_n
        """,
        {"top_n": top_n},
    )
    st.dataframe(ranking_df, use_container_width=True, hide_index=True)

    if not ranking_df.empty:
        fig_rank = px.bar(
            ranking_df.head(15),
            x="ticker",
            y="composite_strength_score",
            color="momentum_20d",
            title="Top Composite Strength Scores (Latest Date)",
            color_continuous_scale=["#dc2626", "#f3f4f6", "#16a34a"],
        )
        st.plotly_chart(fig_rank, use_container_width=True)

    st.subheader("Per-Ticker Feature Trend")
    tickers_df = query_df(
        """
        SELECT DISTINCT ticker
        FROM public.fact_market_signals
        ORDER BY ticker
        """
    )
    ticker = st.selectbox("Ticker", tickers_df["ticker"].tolist(), index=0)
    lookback_days = st.selectbox("Lookback", [30, 60, 90, 180, 365], index=2)

    trend_df = query_df_params(
        """
        SELECT
            event_date,
            composite_strength_score,
            momentum_5d,
            momentum_20d,
            volatility_score,
            risk_score,
            stability_score,
            growth_score
        FROM public.fact_market_signals
        WHERE ticker = :ticker
          AND event_date >= CURRENT_DATE - (:lookback_days * INTERVAL '1 day')
        ORDER BY event_date
        """,
        {"ticker": ticker, "lookback_days": lookback_days},
    )
    if trend_df.empty:
        st.info("No feature trend rows for the selected ticker.")
    else:
        trend_df["event_date"] = pd.to_datetime(trend_df["event_date"], errors="coerce")
        metric_cols = [
            "composite_strength_score",
            "momentum_5d",
            "momentum_20d",
            "volatility_score",
            "risk_score",
            "stability_score",
            "growth_score",
        ]
        fig_trend = go.Figure()
        for col in metric_cols:
            if col in trend_df.columns:
                fig_trend.add_trace(
                    go.Scatter(
                        x=trend_df["event_date"],
                        y=trend_df[col],
                        mode="lines",
                        name=col,
                    )
                )
        fig_trend.update_layout(title=f"{ticker} Feature Signals", xaxis_title="Date", yaxis_title="Value")
        st.plotly_chart(fig_trend, use_container_width=True)
        st.dataframe(trend_df.sort_values("event_date", ascending=False), use_container_width=True, hide_index=True)

    st.subheader("Dividend and Earnings Context")
    c1, c2 = st.columns(2)
    with c1:
        dividends_df = query_df_params(
            """
            SELECT ticker, ex_date, dividend_amount, dividend_yield, dividend_growth
            FROM public.fact_dividends
            WHERE ticker = :ticker
            ORDER BY ex_date DESC
            LIMIT 20
            """,
            {"ticker": ticker},
        )
        st.markdown("**Recent Dividends**")
        st.dataframe(dividends_df, use_container_width=True, hide_index=True)
    with c2:
        earnings_df = query_df_params(
            """
            SELECT ticker, report_date, reported_eps, expected_eps, eps_surprise, revenue, revenue_surprise
            FROM public.fact_earnings
            WHERE ticker = :ticker
            ORDER BY report_date DESC
            LIMIT 20
            """,
            {"ticker": ticker},
        )
        st.markdown("**Recent Earnings**")
        st.dataframe(earnings_df, use_container_width=True, hide_index=True)

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

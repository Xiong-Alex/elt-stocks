import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from shared import (
    apply_theme,
    load_quarantine_reason_breakdown,
    load_quarantine_reasons,
    load_quarantine_rows,
    load_quarantine_summary,
    load_quarantine_symbols,
    render_sidebar_controls,
    table_exists,
)


st.set_page_config(page_title="Quarantine Review", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=45)
apply_theme(theme_mode=theme_mode)

st.title("Quarantine Review")
st.caption("Inspect invalid Bronze rows and download filtered datasets.")

quarantine_ready = table_exists("public", "stock_bars_quarantine")
if not quarantine_ready:
    st.info(
        "Table `public.stock_bars_quarantine` does not exist yet. "
        "Run pipeline jobs with invalid rows to populate quarantine."
    )
else:
    # High-level KPI cards for quarantine table health.
    summary_df = load_quarantine_summary()
    summary = summary_df.iloc[0] if not summary_df.empty else None
    q1, q2, q3 = st.columns(3)
    q1.metric("Quarantined Rows", f"{int(summary['total_rows']):,}" if summary is not None else "0")
    q2.metric("Affected Symbols", f"{int(summary['distinct_symbols']):,}" if summary is not None else "0")
    q3.metric("Latest Quarantine", str(summary["latest_quarantine_ts"]) if summary is not None else "N/A")

    col1, col2, col3, col4 = st.columns(4)
    default_start = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)).date()
    start_date = col1.date_input("Start Date", value=default_start)
    end_date = col2.date_input("End Date", value=pd.Timestamp.now(tz="UTC").date())
    symbols_df = load_quarantine_symbols()
    symbol_options = ["All"] + symbols_df["symbol"].tolist() if not symbols_df.empty else ["All"]
    selected_symbol = col3.selectbox("Symbol", symbol_options, index=0)
    reasons_df = load_quarantine_reasons()
    reason_options = ["All"] + reasons_df["quality_failure_reason"].tolist() if not reasons_df.empty else ["All"]
    selected_reason = col4.selectbox("Failure Reason", reason_options, index=0)

    limit = st.slider("Rows to load", min_value=100, max_value=10000, value=1000, step=100)
    # Apply server-side filters so downloads/dataframe reflect the same slice.
    rows_df = load_quarantine_rows(
        start_date=str(start_date),
        end_date=str(end_date),
        symbol=None if selected_symbol == "All" else selected_symbol,
        reason=None if selected_reason == "All" else selected_reason,
        limit=limit,
    )

    st.dataframe(rows_df, use_container_width=True, hide_index=True)
    st.download_button(
        "Download Filtered Quarantine CSV",
        data=rows_df.to_csv(index=False).encode("utf-8"),
        file_name=f"quarantine_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        disabled=rows_df.empty,
    )

    reason_breakdown_df = load_quarantine_reason_breakdown(limit=25)
    if not reason_breakdown_df.empty:
        fig = px.bar(
            reason_breakdown_df,
            x="quality_failure_reason",
            y="row_count",
            title="Top Quarantine Failure Reasons",
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

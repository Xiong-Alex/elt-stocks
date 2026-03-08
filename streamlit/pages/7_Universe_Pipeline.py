import time

import plotly.express as px
import streamlit as st

from shared import apply_theme, query_df, query_df_params, render_sidebar_controls, table_exists


st.set_page_config(page_title="Universe Pipeline", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=45)
apply_theme(theme_mode=theme_mode)

st.title("Universe Pipeline")
st.caption("Inspect each step of update_stock_universe_dag from source snapshot to dimensional bridge.")

required_tables = {
    "stock_symbols": table_exists("public", "stock_symbols"),
    "stock_universe_memberships_source": table_exists("public", "stock_universe_memberships_source"),
    "dim_stock": table_exists("public", "dim_stock"),
    "dim_universe": table_exists("public", "dim_universe"),
    "bridge_stock_universe_membership": table_exists("public", "bridge_stock_universe_membership"),
}

status_cols = st.columns(5)
for idx, (table_name, exists) in enumerate(required_tables.items()):
    with status_cols[idx]:
        st.metric(table_name, "READY" if exists else "MISSING")

if not all(required_tables.values()):
    st.info(
        "Universe tables are not fully available yet. "
        "Run `update_stock_universe_dag`, then refresh this page."
    )
else:
    universe_options_df = query_df(
        """
        SELECT DISTINCT universe_code
        FROM public.stock_universe_memberships_source
        ORDER BY universe_code
        """
    )
    universe_options = universe_options_df["universe_code"].tolist()
    selected_universe = st.selectbox("Universe", universe_options, index=0)

    st.subheader("Step 1: Refresh Source Snapshot")
    source_summary = query_df_params(
        """
        SELECT
            (SELECT COUNT(*) FROM public.stock_symbols) AS stock_symbols_rows,
            (SELECT COUNT(*) FROM public.stock_symbols WHERE is_active) AS stock_symbols_active,
            (SELECT COUNT(*) FROM public.stock_universe_memberships_source WHERE universe_code = :u) AS memberships_rows,
            (SELECT COUNT(*) FROM public.stock_universe_memberships_source WHERE universe_code = :u AND is_active) AS memberships_active,
            (SELECT MAX(updated_at) FROM public.stock_universe_memberships_source WHERE universe_code = :u) AS memberships_latest_update
        """,
        {"u": selected_universe},
    ).iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("stock_symbols rows", f"{int(source_summary['stock_symbols_rows']):,}")
    c2.metric("stock_symbols active", f"{int(source_summary['stock_symbols_active']):,}")
    c3.metric("memberships rows", f"{int(source_summary['memberships_rows']):,}")
    c4.metric("memberships active", f"{int(source_summary['memberships_active']):,}")
    c5.metric("latest membership update", str(source_summary["memberships_latest_update"]))

    source_preview = query_df_params(
        """
        SELECT universe_code, symbol, is_active, as_of_date, source, updated_at
        FROM public.stock_universe_memberships_source
        WHERE universe_code = :u
        ORDER BY is_active DESC, symbol
        LIMIT 200
        """,
        {"u": selected_universe},
    )
    st.dataframe(source_preview, use_container_width=True, hide_index=True)

    st.subheader("Step 2: Build dim_stock")
    dim_stock_summary = query_df(
        """
        SELECT
            COUNT(*) AS dim_stock_rows,
            COUNT(*) FILTER (WHERE is_active) AS dim_stock_active,
            MAX(updated_at) AS dim_stock_latest_update
        FROM public.dim_stock
        """
    ).iloc[0]
    d1, d2, d3 = st.columns(3)
    d1.metric("dim_stock rows", f"{int(dim_stock_summary['dim_stock_rows']):,}")
    d2.metric("dim_stock active", f"{int(dim_stock_summary['dim_stock_active']):,}")
    d3.metric("dim_stock latest update", str(dim_stock_summary["dim_stock_latest_update"]))

    dim_stock_preview = query_df(
        """
        SELECT stock_key, ticker, is_active, updated_at
        FROM public.dim_stock
        ORDER BY updated_at DESC, ticker
        LIMIT 200
        """
    )
    st.dataframe(dim_stock_preview, use_container_width=True, hide_index=True)

    st.subheader("Step 3: Build dim_universe")
    dim_universe_preview = query_df(
        """
        SELECT universe_key, universe_code, universe_name, description, source, is_active, updated_at
        FROM public.dim_universe
        ORDER BY universe_code
        """
    )
    st.dataframe(dim_universe_preview, use_container_width=True, hide_index=True)

    if not dim_universe_preview.empty:
        universe_counts = query_df(
            """
            SELECT
                du.universe_code,
                COUNT(*) FILTER (WHERE b.is_active) AS active_members
            FROM public.bridge_stock_universe_membership b
            JOIN public.dim_universe du
              ON du.universe_key = b.universe_key
            GROUP BY du.universe_code
            ORDER BY du.universe_code
            """
        )
        fig = px.bar(
            universe_counts,
            x="universe_code",
            y="active_members",
            title="Active Members by Universe",
            color="active_members",
            color_continuous_scale=["#f3f4f6", "#1f7a8c"],
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Step 4: Build bridge_stock_universe_membership")
    bridge_summary = query_df_params(
        """
        SELECT
            COUNT(*) AS bridge_rows,
            COUNT(*) FILTER (WHERE b.is_active) AS bridge_active,
            MAX(b.updated_at) AS bridge_latest_update
        FROM public.bridge_stock_universe_membership b
        JOIN public.dim_universe du
          ON du.universe_key = b.universe_key
        WHERE du.universe_code = :u
        """,
        {"u": selected_universe},
    ).iloc[0]

    b1, b2, b3 = st.columns(3)
    b1.metric("bridge rows", f"{int(bridge_summary['bridge_rows']):,}")
    b2.metric("bridge active", f"{int(bridge_summary['bridge_active']):,}")
    b3.metric("bridge latest update", str(bridge_summary["bridge_latest_update"]))

    bridge_preview = query_df_params(
        """
        SELECT
            ds.ticker,
            du.universe_code,
            b.is_active,
            b.effective_from,
            b.effective_to,
            b.source,
            b.updated_at
        FROM public.bridge_stock_universe_membership b
        JOIN public.dim_stock ds
          ON ds.stock_key = b.stock_key
        JOIN public.dim_universe du
          ON du.universe_key = b.universe_key
        WHERE du.universe_code = :u
        ORDER BY b.is_active DESC, ds.ticker
        LIMIT 200
        """,
        {"u": selected_universe},
    )
    st.dataframe(bridge_preview, use_container_width=True, hide_index=True)

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

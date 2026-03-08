import time

import streamlit as st

from shared import apply_theme, query_df, render_sidebar_controls


st.set_page_config(page_title="SQL Explorer", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=60)
apply_theme(theme_mode=theme_mode)

st.title("SQL Explorer")
st.caption("Run ad-hoc SQL against the analytics database.")

sql_default = "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2 LIMIT 100;"
sql = st.text_area("SQL query", value=sql_default, height=160)
run = st.button("Run Query")

if run:
    # Execute user SQL directly against analytics DB and render tabular output.
    try:
        df = query_df(sql)
        st.success(f"Rows returned: {len(df)}")
        st.dataframe(df, use_container_width=True)
    except Exception as exc:
        st.error(f"Query failed: {exc}")

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

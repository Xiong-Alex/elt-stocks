import re
import time

import pandas as pd
import streamlit as st
from sqlalchemy import text

from shared import apply_theme, get_engine, query_df, query_df_params, render_sidebar_controls


READ_ONLY_PREFIXES = ("select", "with", "values", "show", "explain")


def _clean_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


def _looks_read_only(sql: str) -> bool:
    cleaned = _clean_sql(sql).lower()
    return cleaned.startswith(READ_ONLY_PREFIXES)


def _has_limit(sql: str) -> bool:
    return bool(re.search(r"\blimit\s+\d+\b", sql, flags=re.IGNORECASE))


def _apply_limit(sql: str, row_limit: int) -> str:
    cleaned = _clean_sql(sql)
    if _has_limit(cleaned):
        return cleaned
    if _looks_read_only(cleaned):
        return f"SELECT * FROM ({cleaned}) AS q LIMIT {row_limit}"
    return cleaned


def _run_sql(sql: str, timeout_ms: int) -> pd.DataFrame:
    with get_engine().connect() as conn:
        conn.execute(text("SET statement_timeout = :timeout_ms"), {"timeout_ms": timeout_ms})
        return pd.read_sql(text(sql), conn)


st.set_page_config(page_title="SQL Explorer", layout="wide")
auto_refresh, refresh_sec, theme_mode = render_sidebar_controls(default_refresh=120)
apply_theme(theme_mode=theme_mode)

st.title("SQL Explorer")
st.caption("Run ad-hoc SQL against analytics Postgres with guardrails.")

left, right = st.columns([2, 1])

with left:
    presets = {
        "Tables": "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2 LIMIT 200;",
        "Columns (public)": """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            LIMIT 500;
        """,
        "Top rows by table": """
            SELECT schemaname, relname AS table_name, n_live_tup::bigint AS est_rows
            FROM pg_stat_user_tables
            ORDER BY n_live_tup DESC
            LIMIT 50;
        """,
    }
    preset_name = st.selectbox("Preset", list(presets.keys()), index=0)
    if "sql_editor_value" not in st.session_state:
        st.session_state["sql_editor_value"] = presets[preset_name]
    if "pending_sql_editor_value" in st.session_state:
        st.session_state["sql_editor_value"] = st.session_state.pop("pending_sql_editor_value")
    if st.button("Load Preset"):
        st.session_state["pending_sql_editor_value"] = presets[preset_name]
        st.rerun()

    sql = st.text_area("SQL query", key="sql_editor_value", height=220)

with right:
    row_limit = st.number_input("Default row limit", min_value=10, max_value=10000, value=500, step=10)
    timeout_sec = st.number_input("Timeout (seconds)", min_value=1, max_value=120, value=15, step=1)
    read_only_mode = st.toggle("Read-only mode", value=True)
    explain_only = st.toggle("EXPLAIN query", value=False)
    run = st.button("Run Query", type="primary")

st.subheader("Schema Browser")
schemas = query_df(
    """
    SELECT DISTINCT table_schema
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema')
      AND table_schema NOT LIKE 'pg_%'
    ORDER BY table_schema;
    """
)
schema_options = schemas["table_schema"].tolist()
default_schema_idx = schema_options.index("public") if "public" in schema_options else 0
schema_choice = st.selectbox("Schema", schema_options, index=default_schema_idx)
tables = query_df_params(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = :schema_name
      AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name;
    """,
    {"schema_name": schema_choice},
)
table_options = tables["table_name"].tolist()
if not table_options:
    st.warning(f"No tables/views found in schema `{schema_choice}`.")
else:
    table_choice = st.selectbox("Table", table_options, index=0)
    if st.button("Preview Table"):
        st.session_state["pending_sql_editor_value"] = (
            f"SELECT * FROM {schema_choice}.{table_choice} ORDER BY 1 LIMIT {row_limit};"
        )
        st.rerun()

if run:
    try:
        cleaned = _clean_sql(sql)
        if not cleaned:
            st.warning("Enter a SQL query.")
        else:
            if read_only_mode and not _looks_read_only(cleaned):
                st.error("Read-only mode blocks non-SELECT/CTE statements.")
            else:
                final_sql = _apply_limit(cleaned, int(row_limit))
                if explain_only:
                    final_sql = f"EXPLAIN {final_sql}"

                t0 = time.perf_counter()
                df = _run_sql(final_sql, timeout_ms=int(timeout_sec * 1000))
                elapsed_ms = int((time.perf_counter() - t0) * 1000)

                st.success(f"Rows returned: {len(df)} in {elapsed_ms} ms")
                st.code(final_sql, language="sql")
                st.dataframe(df, use_container_width=True, height=420)

                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download CSV",
                    data=csv_bytes,
                    file_name="sql_explorer_results.csv",
                    mime="text/csv",
                )
    except Exception as exc:
        st.error(f"Query failed: {exc}")

if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()

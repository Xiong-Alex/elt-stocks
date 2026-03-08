import streamlit as st

from shared import apply_theme, render_sidebar_controls


# App shell: global page config + shared sidebar controls.
st.set_page_config(page_title="Stock Trading Terminal", layout="wide")
_, _, theme_mode = render_sidebar_controls(default_refresh=30)
apply_theme(theme_mode=theme_mode)

st.title("Stock Trading Terminal")
st.caption("Use the pages in the sidebar to navigate the workspace.")

st.markdown(
    """
### Workspace Pages
- `Market Monitor`: pipeline status, market tape, movers, heatmap.
- `Charts Workbench`: build chart components with independent settings.
- `Feature Engineering`: inspect engineered facts and signal rankings.
- `Universe Pipeline`: inspect stock universe source, dimensions, and bridge step-by-step.
- `Replay Lab`: choose exact time windows and replay candles with indicators.
- `Quarantine Review`: inspect bad rows and export filtered results.
- `SQL Explorer`: run ad-hoc analytics SQL on the analytics database.
"""
)

st.info("Start with `Market Monitor`, then use `Replay Lab` to test decision timing.")

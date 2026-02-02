from __future__ import annotations

import os
import sys
import streamlit as st

# Ensure /app is on PYTHONPATH so pages can import the same modules
APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

from data_access.loaders import load_tables  # noqa: E402

st.set_page_config(
    page_title="AutoScout Marketplace Executive Analytics (EUR)",
    layout="wide",
)

st.title("AutoScout Marketplace Executive Analytics (EUR)")
st.caption("Local mode: reads Gold export CSVs from your disk (single file per table).")

tables = load_tables()

with st.expander("Data health", expanded=False):
    for k, df in tables.items():
        st.write(f"- `{k}`: {len(df):,} rows" if not df.empty else f"- `{k}`: EMPTY / missing")

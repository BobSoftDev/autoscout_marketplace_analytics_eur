from __future__ import annotations

import os
import pandas as pd
import streamlit as st

from config import DATA_DIR, REQUIRED_FILES, OPTIONAL_FILES


@st.cache_data(show_spinner=False)
def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def load_tables() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}

    for f in REQUIRED_FILES:
        p = os.path.join(DATA_DIR, f)
        key = f.replace(".csv", "")
        if not os.path.exists(p):
            st.error(f"Missing REQUIRED file: {p}")
            tables[key] = pd.DataFrame()
        else:
            tables[key] = _read_csv(p)

    for f in OPTIONAL_FILES:
        p = os.path.join(DATA_DIR, f)
        key = f.replace(".csv", "")
        if not os.path.exists(p):
            st.warning(f"Missing optional file: {p} (continuing)")
            continue
        tables[key] = _read_csv(p)

    return tables

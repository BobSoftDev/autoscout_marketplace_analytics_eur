from __future__ import annotations
import pandas as pd
import streamlit as st


def kpi_strip(items: list[tuple[str, str]]) -> None:
    cols = st.columns(len(items))
    for i, (label, value) in enumerate(items):
        cols[i].metric(label, value)


def fmt_int(x: float | int) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "—"


def fmt_pct(x: float) -> str:
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "—"


def fmt_eur(x: float) -> str:
    try:
        return f"€{float(x):,.0f}"
    except Exception:
        return "—"

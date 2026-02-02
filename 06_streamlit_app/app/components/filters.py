from __future__ import annotations
import pandas as pd
import streamlit as st


def month_range_filter(df: pd.DataFrame, month_col: str = "month_key") -> tuple[str, str]:
    if df.empty or month_col not in df.columns:
        return "", ""
    months = sorted(df[month_col].dropna().astype(str).unique().tolist())
    if not months:
        return "", ""
    default_end = months[-1]
    default_start = months[-13] if len(months) >= 13 else months[0]
    start = st.sidebar.selectbox("Start month", months, index=months.index(default_start))
    end = st.sidebar.selectbox("End month", months, index=months.index(default_end))
    return start, end


def multiselect_filter(df: pd.DataFrame, col: str, label: str) -> list[str]:
    if df.empty or col not in df.columns:
        return []
    options = sorted(df[col].dropna().astype(str).unique().tolist())
    selected = st.sidebar.multiselect(label, options, default=[])
    return selected


def apply_month_filter(df: pd.DataFrame, start: str, end: str, month_col: str = "month_key") -> pd.DataFrame:
    if df.empty or month_col not in df.columns or not start or not end:
        return df
    # month_key is 'YYYY-MM' so lexicographic compare works
    return df[(df[month_col].astype(str) >= start) & (df[month_col].astype(str) <= end)].copy()

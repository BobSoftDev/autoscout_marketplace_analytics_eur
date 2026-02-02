from __future__ import annotations

import os, sys
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from data_access.loaders import load_tables
from components.filters import month_range_filter, apply_month_filter
from components.kpi_strip import kpi_strip, fmt_eur
from components.narrative import narrative

st.title("Revenue Mix — What drives MoM and YoY?")

tables = load_tables()
rev = tables.get("agg_revenue_stream_month", pd.DataFrame())
kpi = tables.get("kpi_growth_drivers_month", pd.DataFrame())

if rev.empty:
    st.warning("No data available for agg_revenue_stream_month.")
    st.stop()

start, end = month_range_filter(rev, "month_key")
rev_f = apply_month_filter(rev, start, end, "month_key")

total_rev = float(rev_f["revenue_eur"].sum())
kpi_strip([("Total revenue (selected months)", fmt_eur(total_rev))])

st.subheader("Revenue by stream over time")
pivot = (
    rev_f.pivot_table(index="month_key", columns="revenue_stream", values="revenue_eur", aggfunc="sum")
        .fillna(0.0)
        .sort_index()
)

fig = plt.figure()
for col in pivot.columns:
    plt.plot(pivot.index, pivot[col], label=str(col))
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
st.pyplot(fig)

if not kpi.empty:
    st.subheader("KPI growth drivers (MoM + YoY)")
    kpi_f = apply_month_filter(kpi, start, end, "month_key")
    st.dataframe(kpi_f.sort_values(["kpi_name", "month_key"]))

narrative(
    takeaway="Revenue must be explained by mix and tracked with MoM/YoY to isolate growth and leakage drivers.",
    evidence="Trends show which revenue streams expand or contract across the selected months.",
    so_what="If revenue rises while sales fall, monetization per event improved; if sales rise but revenue lags, fee capture may be leaking.",
    next_steps="Tie revenue shifts back to funnel and dealer changes, then prioritize actions that explain most of the delta."
)

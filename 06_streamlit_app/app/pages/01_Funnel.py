from __future__ import annotations

import os, sys
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from data_access.loaders import load_tables
from components.filters import month_range_filter, multiselect_filter, apply_month_filter
from components.kpi_strip import kpi_strip, fmt_int, fmt_pct
from components.narrative import narrative

st.title("Funnel — Search → Contact → Sale")

tables = load_tables()
df = tables.get("agg_funnel_month", pd.DataFrame())

if df.empty:
    st.warning("No data available for agg_funnel_month.")
    st.stop()

start, end = month_range_filter(df, "month_key")

geo_sel = multiselect_filter(df, "geo", "Geo")
device_sel = multiselect_filter(df, "device", "Device")
user_type_sel = multiselect_filter(df, "user_type", "User type")
seller_type_sel = multiselect_filter(df, "seller_type", "Seller type")
tier_sel = multiselect_filter(df, "dealer_tier", "Dealer tier")

df_f = apply_month_filter(df, start, end, "month_key")
for col, sel in [
    ("geo", geo_sel),
    ("device", device_sel),
    ("user_type", user_type_sel),
    ("seller_type", seller_type_sel),
    ("dealer_tier", tier_sel),
]:
    if sel:
        df_f = df_f[df_f[col].astype(str).isin([str(x) for x in sel])]

searches = float(df_f["searches"].sum())
contacts = float(df_f["contacts"].sum())
sales = float(df_f["sales"].sum())

s2c = (contacts / searches) if searches > 0 else 0.0
c2s = (sales / contacts) if contacts > 0 else 0.0
s2s = (sales / searches) if searches > 0 else 0.0

kpi_strip([
    ("Searches", fmt_int(searches)),
    ("Contacts", fmt_int(contacts)),
    ("Sales", fmt_int(sales)),
    ("Search→Contact", fmt_pct(s2c)),
    ("Contact→Sale", fmt_pct(c2s)),
    ("Search→Sale", fmt_pct(s2s)),
])

trend = df_f.groupby("month_key", as_index=False)[["searches", "contacts", "sales"]].sum().sort_values("month_key")

st.subheader("Trend by month")
fig = plt.figure()
plt.plot(trend["month_key"], trend["searches"], label="Searches")
plt.plot(trend["month_key"], trend["contacts"], label="Contacts")
plt.plot(trend["month_key"], trend["sales"], label="Sales")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
st.pyplot(fig)

st.subheader("Conversion by geo × dealer tier")
pivot = (
    df_f.groupby(["geo", "dealer_tier"], as_index=False)
       .agg(searches=("searches", "sum"), contacts=("contacts", "sum"), sales=("sales", "sum"))
)
pivot["search_to_contact_rate"] = pivot.apply(lambda r: (r["contacts"] / r["searches"]) if r["searches"] else 0.0, axis=1)
pivot["contact_to_sale_rate"] = pivot.apply(lambda r: (r["sales"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)
st.dataframe(pivot.sort_values(["geo", "dealer_tier"]))

narrative(
    takeaway="Funnel performance is measurable end-to-end once scope is fixed.",
    evidence=f"Selected scope: {int(searches):,} searches, {int(contacts):,} contacts, {int(sales):,} sales (Search→Sale {s2s*100:.2f}%).",
    so_what="Prioritize segments with high searches but weak Search→Contact or Contact→Sale conversion.",
    next_steps="Use Listing Quality + Dealer Performance pages to identify whether leakage is driven by inventory, dealer behavior, or pricing."
)

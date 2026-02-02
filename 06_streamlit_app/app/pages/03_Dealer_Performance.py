from __future__ import annotations

import os, sys
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import pandas as pd
import streamlit as st

from data_access.loaders import load_tables
from components.filters import month_range_filter, multiselect_filter, apply_month_filter
from components.kpi_strip import kpi_strip, fmt_int, fmt_pct, fmt_eur
from components.narrative import narrative

st.title("Dealer Performance — Conversion and monetization")

tables = load_tables()
df = tables.get("agg_dealer_month", pd.DataFrame())

if df.empty:
    st.warning("No data available for agg_dealer_month.")
    st.stop()

start, end = month_range_filter(df, "month_key")
tier_sel = multiselect_filter(df, "dealer_tier", "Dealer tier")
geo_sel = multiselect_filter(df, "dealer_geo", "Dealer geo")
sub_sel = multiselect_filter(df, "is_subscribed", "Subscribed (0/1)")

df_f = apply_month_filter(df, start, end, "month_key")
for col, sel in [("dealer_tier", tier_sel), ("dealer_geo", geo_sel), ("is_subscribed", sub_sel)]:
    if sel:
        df_f = df_f[df_f[col].astype(str).isin([str(x) for x in sel])]

active_listings = float(df_f["active_listings"].sum())
contacts = float(df_f["contacts"].sum())
sales = float(df_f["sales"].sum())
revenue = float(df_f["revenue_eur"].sum())

c2s = (sales / contacts) if contacts > 0 else 0.0
rev_per_listing = (revenue / active_listings) if active_listings > 0 else 0.0

kpi_strip([
    ("Active listings", fmt_int(active_listings)),
    ("Contacts", fmt_int(contacts)),
    ("Sales", fmt_int(sales)),
    ("Revenue", fmt_eur(revenue)),
    ("Contact→Sale", fmt_pct(c2s)),
    ("Revenue / listing", fmt_eur(rev_per_listing)),
])

st.subheader("Top dealers by revenue (filtered scope)")
top = (
    df_f.groupby(["dealer_id", "dealer_tier", "is_subscribed"], as_index=False)
       .agg(revenue=("revenue_eur", "sum"), sales=("sales", "sum"), contacts=("contacts", "sum"), listings=("active_listings", "sum"))
)
top["contact_to_sale_rate"] = top.apply(lambda r: (r["sales"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)
top = top.sort_values("revenue", ascending=False).head(30)
st.dataframe(top)

narrative(
    takeaway="Dealer performance differs materially; growth is driven by focusing on the few dealers that matter most.",
    evidence=f"Selected scope: revenue {fmt_eur(revenue)}, sales {int(sales):,}, Contact→Sale {c2s*100:.1f}%.",
    so_what="Target high-volume but low-conversion dealers with operational playbooks (response time, lead handling, pricing guidance).",
    next_steps="Use Pricing Elasticity page to tailor pricing guidance by tier."
)

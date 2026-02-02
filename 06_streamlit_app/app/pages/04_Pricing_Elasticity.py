from __future__ import annotations

import os, sys
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import pandas as pd
import streamlit as st

from data_access.loaders import load_tables
from components.filters import month_range_filter, multiselect_filter, apply_month_filter
from components.narrative import narrative

st.title("Pricing Elasticity (proxy) — Conversion vs price position")

tables = load_tables()
listing = tables.get("agg_listing_month", pd.DataFrame())
dealers = tables.get("dim_dealer", pd.DataFrame())  # optional

if listing.empty:
    st.warning("No data available for agg_listing_month.")
    st.stop()

start, end = month_range_filter(listing, "month_key")
pp_sel = multiselect_filter(listing, "price_position_bucket", "Price position bucket")

df_f = apply_month_filter(listing, start, end, "month_key")
if pp_sel:
    df_f = df_f[df_f["price_position_bucket"].astype(str).isin([str(x) for x in pp_sel])]

if not dealers.empty and "dealer_id" in df_f.columns and "dealer_id" in dealers.columns:
    df_f = df_f.merge(dealers[["dealer_id", "dealer_tier"]], on="dealer_id", how="left")
else:
    df_f["dealer_tier"] = "unknown"

grp = (
    df_f.groupby(["dealer_tier", "price_position_bucket"], as_index=False)
        .agg(contacts=("contacts", "sum"), sales=("sales", "sum"))
)
grp["contact_to_sale_rate"] = grp.apply(lambda r: (r["sales"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)

st.subheader("Contact→Sale rate by price position bucket and dealer tier")
st.dataframe(grp.sort_values(["dealer_tier", "price_position_bucket"]))

narrative(
    takeaway="Price position vs market changes conversion; the effect can differ by dealer cohort.",
    evidence="Table shows Contact→Sale by price bucket; split by dealer tier if dim_dealer.csv exists locally.",
    so_what="Create tier-specific pricing guidance: reduce premium where conversion drops; preserve premium where it stays stable.",
    next_steps="Run pricing nudges as controlled experiments for a few high-volume dealers."
)

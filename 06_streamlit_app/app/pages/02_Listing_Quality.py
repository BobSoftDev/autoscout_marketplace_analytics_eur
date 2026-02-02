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
from components.kpi_strip import kpi_strip, fmt_int, fmt_pct, fmt_eur
from components.narrative import narrative

st.title("Listing Quality — What drives leads and sales?")

tables = load_tables()
df = tables.get("agg_listing_month", pd.DataFrame())

if df.empty:
    st.warning("No data available for agg_listing_month.")
    st.stop()

start, end = month_range_filter(df, "month_key")
make_sel = multiselect_filter(df, "make", "Make")
seller_sel = multiselect_filter(df, "seller_type", "Seller type")
feat_sel = multiselect_filter(df, "is_featured", "Featured (0/1)")
pp_sel = multiselect_filter(df, "price_position_bucket", "Price position bucket")
photo_sel = multiselect_filter(df, "photo_bucket", "Photo bucket")

df_f = apply_month_filter(df, start, end, "month_key")
for col, sel in [
    ("make", make_sel),
    ("seller_type", seller_sel),
    ("is_featured", feat_sel),
    ("price_position_bucket", pp_sel),
    ("photo_bucket", photo_sel),
]:
    if sel:
        df_f = df_f[df_f[col].astype(str).isin([str(x) for x in sel])]

searches = float(df_f["searches"].sum())
contacts = float(df_f["contacts"].sum())
sales = float(df_f["sales"].sum())
revenue = float(df_f["revenue_eur"].sum())

s2c = (contacts / searches) if searches > 0 else 0.0
c2s = (sales / contacts) if contacts > 0 else 0.0
rev_per_lead = (revenue / contacts) if contacts > 0 else 0.0

kpi_strip([
    ("Searches", fmt_int(searches)),
    ("Contacts", fmt_int(contacts)),
    ("Sales", fmt_int(sales)),
    ("Revenue", fmt_eur(revenue)),
    ("Search→Contact", fmt_pct(s2c)),
    ("Contact→Sale", fmt_pct(c2s)),
    ("Revenue / lead", fmt_eur(rev_per_lead)),
])

st.subheader("Featured vs non-featured impact")
by_feat = df_f.groupby("is_featured", as_index=False).agg(
    searches=("searches", "sum"),
    contacts=("contacts", "sum"),
    sales=("sales", "sum"),
    revenue=("revenue_eur", "sum"),
)
by_feat["search_to_contact_rate"] = by_feat.apply(lambda r: (r["contacts"] / r["searches"]) if r["searches"] else 0.0, axis=1)
by_feat["contact_to_sale_rate"] = by_feat.apply(lambda r: (r["sales"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)
by_feat["revenue_per_lead_eur"] = by_feat.apply(lambda r: (r["revenue"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)
st.dataframe(by_feat)

st.subheader("Photo bucket effect on Search→Contact")
photo = df_f.groupby("photo_bucket", as_index=False).agg(searches=("searches", "sum"), contacts=("contacts", "sum"))
photo["rate"] = photo.apply(lambda r: (r["contacts"] / r["searches"]) if r["searches"] else 0.0, axis=1)
photo = photo.sort_values("photo_bucket")

fig = plt.figure()
plt.plot(photo["photo_bucket"], photo["rate"])
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
st.pyplot(fig)

st.subheader("Price position bucket effect on Contact→Sale")
pp = df_f.groupby("price_position_bucket", as_index=False).agg(contacts=("contacts", "sum"), sales=("sales", "sum"))
pp["rate"] = pp.apply(lambda r: (r["sales"] / r["contacts"]) if r["contacts"] else 0.0, axis=1)
pp = pp.sort_values("price_position_bucket")
st.dataframe(pp)

narrative(
    takeaway="Listing attributes create measurable uplift in lead and sale probability.",
    evidence=f"Selected scope: {int(contacts):,} leads, {int(sales):,} sales, revenue {fmt_eur(revenue)}.",
    so_what="If featured and richer photos lift Search→Contact, build seller rules and guidance to raise overall inventory quality.",
    next_steps="Validate whether the uplift differs by seller type and dealer cohorts (Dealer Performance page)."
)

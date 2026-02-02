from __future__ import annotations
import os

DATA_DIR = os.getenv("GOLD_EXPORTS_DIR", "10_delivery/gold_exports_csv")

REQUIRED_FILES = [
    "agg_funnel_month.csv",
    "agg_listing_month.csv",
    "agg_dealer_month.csv",
    "agg_revenue_stream_month.csv",
    "kpi_growth_drivers_month.csv",
]

OPTIONAL_FILES = [
    "dim_date_month.csv",
    "dim_dealer.csv",
    "dim_listing.csv",
]

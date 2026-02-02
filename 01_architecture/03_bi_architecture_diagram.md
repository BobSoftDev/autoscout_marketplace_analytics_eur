# BI Architecture Diagram (Textual)

Users
│
├── Executive Leadership (CEO, GM, CCO)
│      │
│      ├── Tableau (Primary BI)
│      │        ├ Funnel
│      │        ├ Listing Quality
│      │        ├ Dealer Performance
│      │        ├ Pricing Elasticity
│      │        └ Revenue & Growth
│      │
│      └── Streamlit (Interactive Analysis)
│               ├ KPI Drilldowns
│               ├ Segment Filters
│               └ Narratives
│
└── Analytics & Finance Teams
         └── Gold CSV Exports (Single source of truth)

Gold Layer (Databricks SQL)
│
├ agg_funnel_month
├ agg_listing_month
├ agg_dealer_month
├ agg_revenue_stream_month
└ kpi_growth_drivers_month

Silver Layer (Business Entities)
│
├ clean_users
├ clean_dealers
├ clean_listings
├ clean_searches
├ clean_contacts
└ clean_sales

Bronze Layer (Raw)
│
├ raw_users
├ raw_dealers
├ raw_listings
├ raw_searches
├ raw_contacts
├ raw_sales
└ raw_revenue

Inbound CSV Volume
│
└ Python Synthetic Data Generator

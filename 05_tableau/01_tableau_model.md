# Tableau Model Rules

Use Tableau Relationships, not physical joins.

Primary sources:
- Funnel → agg_funnel_month
- Listing → agg_listing_month
- Dealer → agg_dealer_month
- Revenue → agg_revenue_stream_month
- Executive KPIs → kpi_growth_drivers_month

Dimensions:
- dim_date_month on month_key
- dim_dealer on dealer_id
- dim_listing on listing_id

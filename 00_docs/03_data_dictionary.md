# Data Dictionary — Gold Layer

## agg_funnel_month
| Column | Meaning |
|-------|--------|
| month_key | YYYY-MM |
| geo | User country or region |
| device | mobile / desktop |
| user_type | private / dealer buyer |
| seller_type | private / dealer seller |
| dealer_tier | Tier of selling dealer |
| searches | Searches |
| contacts | Leads |
| sales | Transactions |

## agg_listing_month
| Column | Meaning |
|--------|--------|
| month_key | YYYY-MM |
| listing_id | Vehicle listing |
| dealer_id | Seller |
| make | Vehicle brand |
| price_position_bucket | vs market |
| photo_bucket | Listing quality proxy |
| is_featured | Paid placement flag |
| searches | Searches |
| contacts | Leads |
| sales | Transactions |
| revenue_eur | Revenue from this listing |

## agg_dealer_month
| Column | Meaning |
|--------|--------|
| month_key | YYYY-MM |
| dealer_id | Seller |
| dealer_tier | Dealer tier |
| dealer_geo | Dealer region |
| is_subscribed | Paid subscription |
| active_listings | Listings in market |
| contacts | Leads |
| sales | Transactions |
| revenue_eur | Dealer revenue |

## agg_revenue_stream_month
| Column | Meaning |
|--------|--------|
| month_key | YYYY-MM |
| revenue_stream | Subscription, Lead fee, Featured, etc |
| revenue_eur | EUR amount |

## kpi_growth_drivers_month
| Column | Meaning |
|--------|--------|
| kpi_name | KPI |
| month_key | YYYY-MM |
| kpi_value | KPI value |
| mom_abs_change | Absolute MoM |
| mom_pct_change | % MoM |
| yoy_abs_change | Absolute YoY |
| yoy_pct_change | % YoY |

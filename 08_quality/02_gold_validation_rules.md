# Gold Layer Validation Rules

## Funnel
- searches >= contacts >= sales
- contact_to_sale_rate ∈ [0,1]
- search_to_contact_rate ∈ [0,1]

## Listings
- revenue_eur >= 0
- photo_bucket not null
- price_position_bucket in allowed set

## Dealers
- active_listings >= sales
- revenue_per_listing >= 0

## Revenue
- Total revenue = sum of all revenue_stream values

Any violation must trigger a red flag before data is used for reporting.

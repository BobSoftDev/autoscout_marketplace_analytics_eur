-- 03_databricks_sql/02_gold/01_build_gold.sql
-- AutoScout Marketplace Conversion & Monetization Analytics (EUR)
-- STEP 5 — Gold analytics layer (SQL)
--
-- Constraints honored:
-- - SQL only (no PySpark)
-- - All identifiers backticked
-- - Gold includes dimensions, facts, and metrics
-- - Time intelligence: MoM and YoY

USE CATALOG `autoscout_mkt_eur`;

-- =========================================================
-- 1) GOLD DIMENSIONS (conformed, BI-friendly)
-- =========================================================

-- 1.1 Month dimension
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`dim_date_month` AS
SELECT
  `month_key`,
  MIN(`month_start_date`) AS `month_start_date`,
  MIN(`month_end_date`) AS `month_end_date`,
  MIN(`year`) AS `year`,
  MIN(`quarter`) AS `quarter`,
  MIN(`month`) AS `month`
FROM `autoscout_mkt_eur`.`01_silver`.`dim_date`
GROUP BY `month_key`;

-- 1.2 Simple dimensions
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`dim_geo` AS
SELECT DISTINCT `geo`
FROM `autoscout_mkt_eur`.`01_silver`.`dim_user`
WHERE `geo` IS NOT NULL AND TRIM(`geo`) <> '';

CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`dim_make` AS
SELECT DISTINCT `make`
FROM `autoscout_mkt_eur`.`01_silver`.`dim_listing`
WHERE `make` IS NOT NULL AND TRIM(`make`) <> '';

-- 1.3 Dealer + Listing dimensions (BI layer copies)
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`dim_dealer` AS
SELECT
  `dealer_id`,
  `dealer_tier`,
  `dealer_geo`,
  `is_subscribed`,
  `avg_response_time_hours`,
  `dealer_close_skill`,
  `price_elasticity_index`
FROM `autoscout_mkt_eur`.`01_silver`.`dim_dealer`;

CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`dim_listing` AS
SELECT
  `listing_id`,
  `seller_type`,
  `dealer_id`,
  `make`,
  `fuel_type`,
  `transmission`,
  `vehicle_year`,
  `vehicle_age_years`,
  `mileage_km`,
  `listed_price_eur`,
  `base_market_price_eur`,
  `price_position_vs_market`,
  `photo_count`,
  `description_length`,
  `is_featured`,
  `listing_created_date`,
  `listing_delisted_date`,
  `is_active`
FROM `autoscout_mkt_eur`.`01_silver`.`dim_listing`;

-- =========================================================
-- 2) GOLD FUNNEL (Search -> Contact -> Sale) by month + segment
-- =========================================================
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month` AS
WITH `search_base` AS (
  SELECT
    dd.`month_key`,
    u.`geo`,
    u.`device`,
    u.`user_type`,
    l.`seller_type`,
    COALESCE(d.`dealer_tier`, 'unknown') AS `dealer_tier`,
    COUNT(*) AS `searches`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_search` s
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON s.`date_key` = dd.`date_key`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_user` u
    ON s.`user_id` = u.`user_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` l
    ON s.`listing_id` = l.`listing_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_dealer` d
    ON l.`dealer_id` = d.`dealer_id`
  GROUP BY
    dd.`month_key`, u.`geo`, u.`device`, u.`user_type`, l.`seller_type`, COALESCE(d.`dealer_tier`, 'unknown')
),
`contact_base` AS (
  SELECT
    dd.`month_key`,
    u.`geo`,
    u.`device`,
    u.`user_type`,
    l.`seller_type`,
    COALESCE(d.`dealer_tier`, 'unknown') AS `dealer_tier`,
    COUNT(*) AS `contacts`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_contact` c
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON c.`date_key` = dd.`date_key`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_user` u
    ON c.`user_id` = u.`user_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` l
    ON c.`listing_id` = l.`listing_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_dealer` d
    ON l.`dealer_id` = d.`dealer_id`
  GROUP BY
    dd.`month_key`, u.`geo`, u.`device`, u.`user_type`, l.`seller_type`, COALESCE(d.`dealer_tier`, 'unknown')
),
`sale_base` AS (
  SELECT
    dd.`month_key`,
    u.`geo`,
    u.`device`,
    u.`user_type`,
    l.`seller_type`,
    COALESCE(d.`dealer_tier`, 'unknown') AS `dealer_tier`,
    COUNT(*) AS `sales`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_sale` s
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON s.`date_key` = dd.`date_key`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_user` u
    ON s.`user_id` = u.`user_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` l
    ON s.`listing_id` = l.`listing_id`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_dealer` d
    ON l.`dealer_id` = d.`dealer_id`
  GROUP BY
    dd.`month_key`, u.`geo`, u.`device`, u.`user_type`, l.`seller_type`, COALESCE(d.`dealer_tier`, 'unknown')
)
SELECT
  sb.`month_key`,
  sb.`geo`,
  sb.`device`,
  sb.`user_type`,
  sb.`seller_type`,
  sb.`dealer_tier`,
  sb.`searches`,
  COALESCE(cb.`contacts`, 0) AS `contacts`,
  COALESCE(sa.`sales`, 0) AS `sales`,
  CASE WHEN sb.`searches` > 0 THEN COALESCE(cb.`contacts`, 0) / sb.`searches` ELSE 0 END AS `search_to_contact_rate`,
  CASE WHEN COALESCE(cb.`contacts`, 0) > 0 THEN COALESCE(sa.`sales`, 0) / cb.`contacts` ELSE 0 END AS `contact_to_sale_rate`,
  CASE WHEN sb.`searches` > 0 THEN COALESCE(sa.`sales`, 0) / sb.`searches` ELSE 0 END AS `search_to_sale_rate`
FROM `search_base` sb
LEFT JOIN `contact_base` cb
  ON sb.`month_key` = cb.`month_key`
 AND sb.`geo` = cb.`geo`
 AND sb.`device` = cb.`device`
 AND sb.`user_type` = cb.`user_type`
 AND sb.`seller_type` = cb.`seller_type`
 AND sb.`dealer_tier` = cb.`dealer_tier`
LEFT JOIN `sale_base` sa
  ON sb.`month_key` = sa.`month_key`
 AND sb.`geo` = sa.`geo`
 AND sb.`device` = sa.`device`
 AND sb.`user_type` = sa.`user_type`
 AND sb.`seller_type` = sa.`seller_type`
 AND sb.`dealer_tier` = sa.`dealer_tier`;

-- =========================================================
-- 3) GOLD LISTING PERFORMANCE (quality + monetization)
-- =========================================================
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`agg_listing_month` AS
WITH `searches` AS (
  SELECT
    dd.`month_key`,
    s.`listing_id`,
    COUNT(*) AS `searches`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_search` s
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON s.`date_key` = dd.`date_key`
  GROUP BY dd.`month_key`, s.`listing_id`
),
`contacts` AS (
  SELECT
    dd.`month_key`,
    c.`listing_id`,
    COUNT(*) AS `contacts`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_contact` c
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON c.`date_key` = dd.`date_key`
  GROUP BY dd.`month_key`, c.`listing_id`
),
`sales` AS (
  SELECT
    dd.`month_key`,
    sa.`listing_id`,
    COUNT(*) AS `sales`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_sale` sa
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON sa.`date_key` = dd.`date_key`
  GROUP BY dd.`month_key`, sa.`listing_id`
),
`revenue` AS (
  SELECT
    dd.`month_key`,
    r.`listing_id`,
    SUM(r.`amount_eur`) AS `revenue_eur`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_revenue` r
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON r.`date_key` = dd.`date_key`
  WHERE r.`listing_id` IS NOT NULL
  GROUP BY dd.`month_key`, r.`listing_id`
)
SELECT
  s.`month_key`,
  l.`listing_id`,
  l.`dealer_id`,
  l.`seller_type`,
  l.`make`,
  l.`vehicle_age_years`,
  l.`is_featured`,
  CASE
    WHEN l.`photo_count` < 5 THEN '0-4'
    WHEN l.`photo_count` < 10 THEN '5-9'
    WHEN l.`photo_count` < 20 THEN '10-19'
    ELSE '20+'
  END AS `photo_bucket`,
  CASE
    WHEN l.`price_position_vs_market` <= -0.05 THEN 'below_market'
    WHEN l.`price_position_vs_market` < 0.05 THEN 'at_market'
    WHEN l.`price_position_vs_market` < 0.15 THEN 'slightly_above'
    ELSE 'above_market'
  END AS `price_position_bucket`,
  s.`searches`,
  COALESCE(c.`contacts`, 0) AS `contacts`,
  COALESCE(sa.`sales`, 0) AS `sales`,
  COALESCE(r.`revenue_eur`, 0.0) AS `revenue_eur`,
  CASE WHEN 1 = 1 THEN COALESCE(r.`revenue_eur`, 0.0) END AS `revenue_per_listing_eur`,
  CASE WHEN COALESCE(c.`contacts`, 0) > 0 THEN COALESCE(r.`revenue_eur`, 0.0) / c.`contacts` ELSE 0 END AS `revenue_per_lead_eur`,
  CASE WHEN s.`searches` > 0 THEN COALESCE(c.`contacts`, 0) / s.`searches` ELSE 0 END AS `search_to_contact_rate`,
  CASE WHEN COALESCE(c.`contacts`, 0) > 0 THEN COALESCE(sa.`sales`, 0) / c.`contacts` ELSE 0 END AS `contact_to_sale_rate`
FROM `searches` s
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` l
  ON s.`listing_id` = l.`listing_id`
LEFT JOIN `contacts` c
  ON s.`month_key` = c.`month_key` AND s.`listing_id` = c.`listing_id`
LEFT JOIN `sales` sa
  ON s.`month_key` = sa.`month_key` AND s.`listing_id` = sa.`listing_id`
LEFT JOIN `revenue` r
  ON s.`month_key` = r.`month_key` AND s.`listing_id` = r.`listing_id`;

-- =========================================================
-- 4) GOLD DEALER PERFORMANCE (conversion + monetization)
-- =========================================================
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`agg_dealer_month` AS
WITH `active_listings` AS (
  -- Approximate active listings by dealer at month grain:
  -- count listings created on/before month_end and not delisted before month_start.
  SELECT
    m.`month_key`,
    d.`dealer_id`,
    COUNT(*) AS `active_listings`
  FROM `autoscout_mkt_eur`.`02_gold`.`dim_date_month` m
  JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` d
    ON d.`seller_type` = 'dealer'
   AND d.`dealer_id` IS NOT NULL
   AND d.`listing_created_date` <= m.`month_end_date`
   AND (d.`listing_delisted_date` IS NULL OR d.`listing_delisted_date` >= m.`month_start_date`)
  GROUP BY m.`month_key`, d.`dealer_id`
),
`contacts` AS (
  SELECT
    dd.`month_key`,
    c.`dealer_id`,
    COUNT(*) AS `contacts`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_contact` c
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON c.`date_key` = dd.`date_key`
  WHERE c.`dealer_id` IS NOT NULL
  GROUP BY dd.`month_key`, c.`dealer_id`
),
`sales` AS (
  SELECT
    dd.`month_key`,
    s.`dealer_id`,
    COUNT(*) AS `sales`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_sale` s
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON s.`date_key` = dd.`date_key`
  WHERE s.`dealer_id` IS NOT NULL
  GROUP BY dd.`month_key`, s.`dealer_id`
),
`revenue` AS (
  SELECT
    dd.`month_key`,
    r.`dealer_id`,
    SUM(r.`amount_eur`) AS `revenue_eur`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_revenue` r
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON r.`date_key` = dd.`date_key`
  WHERE r.`dealer_id` IS NOT NULL
  GROUP BY dd.`month_key`, r.`dealer_id`
),
`dealer_searches` AS (
  -- searches routed to dealer inventory via listing->dealer mapping
  SELECT
    dd.`month_key`,
    l.`dealer_id`,
    COUNT(*) AS `searches`
  FROM `autoscout_mkt_eur`.`01_silver`.`fact_search` s
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
    ON s.`date_key` = dd.`date_key`
  LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_listing` l
    ON s.`listing_id` = l.`listing_id`
  WHERE l.`dealer_id` IS NOT NULL
  GROUP BY dd.`month_key`, l.`dealer_id`
)
SELECT
  m.`month_key`,
  d.`dealer_id`,
  d.`dealer_tier`,
  d.`dealer_geo`,
  d.`is_subscribed`,
  COALESCE(al.`active_listings`, 0) AS `active_listings`,
  COALESCE(co.`contacts`, 0) AS `contacts`,
  COALESCE(sa.`sales`, 0) AS `sales`,
  COALESCE(rv.`revenue_eur`, 0.0) AS `revenue_eur`,
  CASE WHEN COALESCE(al.`active_listings`, 0) > 0 THEN COALESCE(rv.`revenue_eur`, 0.0) / al.`active_listings` ELSE 0 END AS `revenue_per_listing_eur`,
  CASE WHEN COALESCE(co.`contacts`, 0) > 0 THEN COALESCE(rv.`revenue_eur`, 0.0) / co.`contacts` ELSE 0 END AS `revenue_per_lead_eur`,
  CASE WHEN COALESCE(ds.`searches`, 0) > 0 THEN COALESCE(co.`contacts`, 0) / ds.`searches` ELSE 0 END AS `search_to_contact_rate`,
  CASE WHEN COALESCE(co.`contacts`, 0) > 0 THEN COALESCE(sa.`sales`, 0) / co.`contacts` ELSE 0 END AS `contact_to_sale_rate`
FROM `autoscout_mkt_eur`.`02_gold`.`dim_date_month` m
CROSS JOIN `autoscout_mkt_eur`.`02_gold`.`dim_dealer` d
LEFT JOIN `active_listings` al
  ON m.`month_key` = al.`month_key` AND d.`dealer_id` = al.`dealer_id`
LEFT JOIN `contacts` co
  ON m.`month_key` = co.`month_key` AND d.`dealer_id` = co.`dealer_id`
LEFT JOIN `sales` sa
  ON m.`month_key` = sa.`month_key` AND d.`dealer_id` = sa.`dealer_id`
LEFT JOIN `revenue` rv
  ON m.`month_key` = rv.`month_key` AND d.`dealer_id` = rv.`dealer_id`
LEFT JOIN `dealer_searches` ds
  ON m.`month_key` = ds.`month_key` AND d.`dealer_id` = ds.`dealer_id`;

-- =========================================================
-- 5) GOLD REVENUE STREAMS (month)
-- =========================================================
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`agg_revenue_stream_month` AS
SELECT
  dd.`month_key`,
  r.`revenue_stream`,
  SUM(r.`amount_eur`) AS `revenue_eur`
FROM `autoscout_mkt_eur`.`01_silver`.`fact_revenue` r
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` dd
  ON r.`date_key` = dd.`date_key`
GROUP BY dd.`month_key`, r.`revenue_stream`;

-- =========================================================
-- 6) KPI GROWTH DRIVERS (MoM + YoY)
--    KPI list kept small + executive-relevant.
-- =========================================================
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`02_gold`.`kpi_growth_drivers_month` AS
WITH `kpi_base` AS (
  SELECT
    'searches' AS `kpi_name`,
    `month_key`,
    CAST(SUM(`searches`) AS DOUBLE) AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month`
  GROUP BY `month_key`

  UNION ALL
  SELECT
    'contacts' AS `kpi_name`,
    `month_key`,
    CAST(SUM(`contacts`) AS DOUBLE) AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month`
  GROUP BY `month_key`

  UNION ALL
  SELECT
    'sales' AS `kpi_name`,
    `month_key`,
    CAST(SUM(`sales`) AS DOUBLE) AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month`
  GROUP BY `month_key`

  UNION ALL
  SELECT
    'revenue_eur' AS `kpi_name`,
    `month_key`,
    CAST(SUM(`revenue_eur`) AS DOUBLE) AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_revenue_stream_month`
  GROUP BY `month_key`

  UNION ALL
  SELECT
    'search_to_contact_rate' AS `kpi_name`,
    `month_key`,
    CASE WHEN SUM(`searches`) > 0 THEN CAST(SUM(`contacts`) AS DOUBLE) / CAST(SUM(`searches`) AS DOUBLE) ELSE 0 END AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month`
  GROUP BY `month_key`

  UNION ALL
  SELECT
    'contact_to_sale_rate' AS `kpi_name`,
    `month_key`,
    CASE WHEN SUM(`contacts`) > 0 THEN CAST(SUM(`sales`) AS DOUBLE) / CAST(SUM(`contacts`) AS DOUBLE) ELSE 0 END AS `kpi_value`
  FROM `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month`
  GROUP BY `month_key`
),
`with_lags` AS (
  SELECT
    `kpi_name`,
    `month_key`,
    `kpi_value`,
    LAG(`kpi_value`, 1) OVER (PARTITION BY `kpi_name` ORDER BY `month_key`) AS `prev_month_value`,
    LAG(`kpi_value`, 12) OVER (PARTITION BY `kpi_name` ORDER BY `month_key`) AS `prev_year_value`
  FROM `kpi_base`
)
SELECT
  `kpi_name`,
  `month_key`,
  `kpi_value`,
  (`kpi_value` - `prev_month_value`) AS `mom_abs_change`,
  CASE WHEN `prev_month_value` IS NOT NULL AND `prev_month_value` <> 0 THEN (`kpi_value` - `prev_month_value`) / `prev_month_value` ELSE NULL END AS `mom_pct_change`,
  (`kpi_value` - `prev_year_value`) AS `yoy_abs_change`,
  CASE WHEN `prev_year_value` IS NOT NULL AND `prev_year_value` <> 0 THEN (`kpi_value` - `prev_year_value`) / `prev_year_value` ELSE NULL END AS `yoy_pct_change`
FROM `with_lags`;

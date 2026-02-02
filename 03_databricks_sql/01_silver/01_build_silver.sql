-- 03_databricks_sql/01_silver/01_build_silver.sql
-- AutoScout Marketplace Conversion & Monetization Analytics (EUR)
-- STEP 4 — Silver transformations (SQL)
--
-- Constraints honored:
-- - SQL only (no PySpark here)
-- - All identifiers backticked
-- - Bronze dates are STRING, Silver types them properly

USE CATALOG `autoscout_mkt_eur`;

-- =========================================================
-- 1) SILVER DATE DIMENSION
-- =========================================================
-- Build a date spine from min/max across Bronze event dates.
-- This supports MoM/YoY and joining all facts to one calendar.

CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`dim_date` AS
WITH `bounds` AS (
  SELECT
    LEAST(
      MIN(TO_DATE(`first_seen_date`)),
      MIN(TO_DATE(`listing_created_date`)),
      MIN(TO_DATE(`search_date`)),
      MIN(TO_DATE(`contact_date`)),
      MIN(TO_DATE(`sale_date`)),
      MIN(TO_DATE(`revenue_date`))
    ) AS `min_date`,
    GREATEST(
      MAX(TO_DATE(`first_seen_date`)),
      MAX(TO_DATE(`listing_created_date`)),
      MAX(TO_DATE(`search_date`)),
      MAX(TO_DATE(`contact_date`)),
      MAX(TO_DATE(`sale_date`)),
      MAX(TO_DATE(`revenue_date`))
    ) AS `max_date`
  FROM (
    SELECT `first_seen_date`, NULL AS `listing_created_date`, NULL AS `search_date`, NULL AS `contact_date`, NULL AS `sale_date`, NULL AS `revenue_date`
    FROM `autoscout_mkt_eur`.`00_bronze`.`users`
    UNION ALL
    SELECT NULL, `listing_created_date`, NULL, NULL, NULL, NULL
    FROM `autoscout_mkt_eur`.`00_bronze`.`listings`
    UNION ALL
    SELECT NULL, NULL, `search_date`, NULL, NULL, NULL
    FROM `autoscout_mkt_eur`.`00_bronze`.`searches`
    UNION ALL
    SELECT NULL, NULL, NULL, `contact_date`, NULL, NULL
    FROM `autoscout_mkt_eur`.`00_bronze`.`contacts`
    UNION ALL
    SELECT NULL, NULL, NULL, NULL, `sale_date`, NULL
    FROM `autoscout_mkt_eur`.`00_bronze`.`sales`
    UNION ALL
    SELECT NULL, NULL, NULL, NULL, NULL, `revenue_date`
    FROM `autoscout_mkt_eur`.`00_bronze`.`revenue`
  )
),
`spine` AS (
  SELECT EXPLODE(SEQUENCE(`min_date`, `max_date`, INTERVAL 1 DAY)) AS `date`
  FROM `bounds`
)
SELECT
  CAST(DATE_FORMAT(`date`, 'yyyyMMdd') AS INT) AS `date_key`,
  `date` AS `date`,
  YEAR(`date`) AS `year`,
  QUARTER(`date`) AS `quarter`,
  MONTH(`date`) AS `month`,
  DATE_TRUNC('MONTH', `date`) AS `month_start_date`,
  LAST_DAY(`date`) AS `month_end_date`,
  DATE_FORMAT(`date`, 'yyyy-MM') AS `month_key`,
  WEEKOFYEAR(`date`) AS `week_of_year`,
  DAYOFWEEK(`date`) AS `day_of_week`
FROM `spine`;

-- =========================================================
-- 2) SILVER DIMENSIONS
-- =========================================================

-- Users
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`dim_user` AS
SELECT
  `user_id`,
  TO_DATE(`first_seen_date`) AS `first_seen_date`,
  `geo`,
  `device`,
  `user_type`,
  CAST(`propensity_score` AS DOUBLE) AS `propensity_score`,
  CAST(`is_marketing_opt_in` AS INT) AS `is_marketing_opt_in`
FROM `autoscout_mkt_eur`.`00_bronze`.`users`;

-- Dealers
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`dim_dealer` AS
SELECT
  `dealer_id`,
  `dealer_tier`,
  `dealer_geo`,
  CAST(`is_subscribed` AS INT) AS `is_subscribed`,
  CAST(`avg_response_time_hours` AS DOUBLE) AS `avg_response_time_hours`,
  CAST(`dealer_close_skill` AS DOUBLE) AS `dealer_close_skill`,
  CAST(`price_elasticity_index` AS DOUBLE) AS `price_elasticity_index`
FROM `autoscout_mkt_eur`.`00_bronze`.`dealers`;

-- Listings
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`dim_listing` AS
SELECT
  `listing_id`,
  `seller_type`,
  NULLIF(`dealer_id`, '') AS `dealer_id`,
  `make`,
  `fuel_type`,
  `transmission`,
  CAST(`vehicle_year` AS INT) AS `vehicle_year`,
  CAST(YEAR(CURRENT_DATE()) - CAST(`vehicle_year` AS INT) AS INT) AS `vehicle_age_years`,
  CAST(`mileage_km` AS INT) AS `mileage_km`,
  CAST(`listed_price_eur` AS DOUBLE) AS `listed_price_eur`,
  CAST(`base_market_price_eur` AS DOUBLE) AS `base_market_price_eur`,
  CAST(`price_position_vs_market` AS DOUBLE) AS `price_position_vs_market`,
  CAST(`photo_count` AS INT) AS `photo_count`,
  CAST(`description_length` AS INT) AS `description_length`,
  CAST(`is_featured` AS INT) AS `is_featured`,
  TO_DATE(`listing_created_date`) AS `listing_created_date`,
  CASE WHEN NULLIF(`listing_delisted_date`, '') IS NULL THEN NULL ELSE TO_DATE(`listing_delisted_date`) END AS `listing_delisted_date`,
  CASE
    WHEN NULLIF(`listing_delisted_date`, '') IS NULL THEN 1
    WHEN TO_DATE(`listing_delisted_date`) >= CURRENT_DATE() THEN 1
    ELSE 0
  END AS `is_active`
FROM `autoscout_mkt_eur`.`00_bronze`.`listings`;

-- =========================================================
-- 3) SILVER FACTS (search/contact/sale/revenue)
-- =========================================================

-- Search fact
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`fact_search` AS
SELECT
  s.`search_id`,
  TO_DATE(s.`search_date`) AS `search_date`,
  d.`date_key`,
  s.`user_id`,
  s.`primary_viewed_listing_id` AS `listing_id`,
  s.`search_intent`,
  s.`user_geo`,
  s.`user_device`,
  s.`user_type`,
  CAST(s.`user_propensity_score` AS DOUBLE) AS `user_propensity_score`
FROM `autoscout_mkt_eur`.`00_bronze`.`searches` s
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` d
  ON d.`date` = TO_DATE(s.`search_date`);

-- Contact fact
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`fact_contact` AS
SELECT
  c.`contact_id`,
  TO_DATE(c.`contact_date`) AS `contact_date`,
  CAST(c.`contact_ts` AS TIMESTAMP) AS `contact_ts`,
  d.`date_key`,
  c.`search_id`,
  c.`user_id`,
  c.`listing_id`,
  c.`seller_type`,
  NULLIF(c.`dealer_id`, '') AS `dealer_id`,
  c.`lead_channel`,
  CAST(c.`dealer_response_time_hours` AS DOUBLE) AS `dealer_response_time_hours`
FROM `autoscout_mkt_eur`.`00_bronze`.`contacts` c
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` d
  ON d.`date` = TO_DATE(c.`contact_date`);

-- Sale fact
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`fact_sale` AS
SELECT
  s.`sale_id`,
  TO_DATE(s.`sale_date`) AS `sale_date`,
  d.`date_key`,
  s.`contact_id`,
  s.`search_id`,
  s.`user_id`,
  s.`listing_id`,
  s.`seller_type`,
  NULLIF(s.`dealer_id`, '') AS `dealer_id`,
  CAST(s.`sale_price_eur` AS DOUBLE) AS `sale_price_eur`,
  CAST(s.`listed_price_eur` AS DOUBLE) AS `listed_price_eur`,
  CAST(s.`discount_pct` AS DOUBLE) AS `discount_pct`,
  CAST(s.`is_marketplace_facilitated` AS INT) AS `is_marketplace_facilitated`
FROM `autoscout_mkt_eur`.`00_bronze`.`sales` s
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` d
  ON d.`date` = TO_DATE(s.`sale_date`);

-- Revenue fact
CREATE OR REPLACE TABLE `autoscout_mkt_eur`.`01_silver`.`fact_revenue` AS
SELECT
  r.`revenue_id`,
  TO_DATE(r.`revenue_date`) AS `revenue_date`,
  d.`date_key`,
  r.`revenue_stream`,
  NULLIF(r.`dealer_id`, '') AS `dealer_id`,
  NULLIF(r.`listing_id`, '') AS `listing_id`,
  NULLIF(r.`contact_id`, '') AS `contact_id`,
  NULLIF(r.`sale_id`, '') AS `sale_id`,
  CAST(r.`amount_eur` AS DOUBLE) AS `amount_eur`
FROM `autoscout_mkt_eur`.`00_bronze`.`revenue` r
LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`dim_date` d
  ON d.`date` = TO_DATE(r.`revenue_date`);

-- =========================================================
-- 4) SILVER QUALITY CHECKS (lightweight)
-- =========================================================

-- 4.1 Orphan checks (should be small / zero)
-- These are queries (not tables) you can run to validate.

-- Orphan contacts without searches
-- SELECT COUNT(*) AS orphan_contacts
-- FROM `autoscout_mkt_eur`.`01_silver`.`fact_contact` c
-- LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`fact_search` s
--   ON c.`search_id` = s.`search_id`
-- WHERE s.`search_id` IS NULL;

-- Orphan sales without contacts
-- SELECT COUNT(*) AS orphan_sales
-- FROM `autoscout_mkt_eur`.`01_silver`.`fact_sale` sa
-- LEFT JOIN `autoscout_mkt_eur`.`01_silver`.`fact_contact` c
--   ON sa.`contact_id` = c.`contact_id`
-- WHERE c.`contact_id` IS NULL;

-- Revenue rows without a known stream
-- SELECT COUNT(*) AS bad_revenue_stream
-- FROM `autoscout_mkt_eur`.`01_silver`.`fact_revenue`
-- WHERE `revenue_stream` IS NULL OR TRIM(`revenue_stream`) = '';

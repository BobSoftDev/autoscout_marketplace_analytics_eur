-- 03_databricks_sql/00_ddl/00_marketplace_ddl.sql
-- AutoScout Marketplace Conversion & Monetization Analytics (EUR)
-- STEP 2 — Databricks SQL DDL (Catalog, Schemas, Volumes, Tables)
--
-- Constraints honored:
-- - All identifiers are backticked
-- - Bronze dates/timestamps stored as STRING
-- - DDL only (no transformations here)

-- ----------------------------
-- 0) Catalog
-- ----------------------------
CREATE CATALOG IF NOT EXISTS `autoscout_mkt_eur`;

-- ----------------------------
-- 1) Schemas
-- ----------------------------
CREATE SCHEMA IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`;
CREATE SCHEMA IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`;
CREATE SCHEMA IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`;

-- ----------------------------
-- 2) Volumes
--    - inbound CSV landing
--    - gold exports (single CSV files per table)
-- ----------------------------
CREATE VOLUME IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`inbound_csv`;
CREATE VOLUME IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`gold_exports`;

-- =========================================================
-- 3) BRONZE TABLES (raw-ish; dates as STRING)
-- =========================================================

-- USERS (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`users` (
  `user_id` STRING,
  `first_seen_date` STRING,
  `geo` STRING,
  `device` STRING,
  `user_type` STRING,
  `propensity_score` DOUBLE,
  `is_marketing_opt_in` INT
)
USING DELTA;

-- DEALERS (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`dealers` (
  `dealer_id` STRING,
  `dealer_tier` STRING,
  `dealer_geo` STRING,
  `is_subscribed` INT,
  `avg_response_time_hours` DOUBLE,
  `dealer_close_skill` DOUBLE,
  `price_elasticity_index` DOUBLE
)
USING DELTA;

-- LISTINGS (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`listings` (
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `make` STRING,
  `fuel_type` STRING,
  `transmission` STRING,
  `vehicle_year` INT,
  `mileage_km` INT,
  `listed_price_eur` DOUBLE,
  `base_market_price_eur` DOUBLE,
  `price_position_vs_market` DOUBLE,
  `photo_count` INT,
  `description_length` INT,
  `is_featured` INT,
  `listing_created_date` STRING,
  `listing_delisted_date` STRING
)
USING DELTA;

-- SEARCHES (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`searches` (
  `search_id` STRING,
  `search_date` STRING,
  `user_id` STRING,
  `user_geo` STRING,
  `user_device` STRING,
  `user_type` STRING,
  `user_propensity_score` DOUBLE,
  `search_intent` STRING,
  `primary_viewed_listing_id` STRING
)
USING DELTA;

-- CONTACTS / LEADS (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`contacts` (
  `contact_id` STRING,
  `contact_date` STRING,
  `contact_ts` STRING,
  `search_id` STRING,
  `user_id` STRING,
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `dealer_tier` STRING,
  `listed_price_eur` DOUBLE,
  `is_featured` INT,
  `photo_count` INT,
  `price_position_vs_market` DOUBLE,
  `dealer_response_time_hours` DOUBLE,
  `lead_channel` STRING
)
USING DELTA;

-- SALES (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`sales` (
  `sale_id` STRING,
  `sale_date` STRING,
  `contact_id` STRING,
  `search_id` STRING,
  `user_id` STRING,
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `sale_price_eur` DOUBLE,
  `listed_price_eur` DOUBLE,
  `discount_pct` DOUBLE,
  `is_marketplace_facilitated` INT
)
USING DELTA;

-- REVENUE (Bronze)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`00_bronze`.`revenue` (
  `revenue_id` STRING,
  `revenue_date` STRING,
  `revenue_stream` STRING,
  `dealer_id` STRING,
  `listing_id` STRING,
  `contact_id` STRING,
  `sale_id` STRING,
  `amount_eur` DOUBLE
)
USING DELTA;

-- =========================================================
-- 4) SILVER TABLES (typed & relationalized; created in Step 4)
--    DDL only here, transforms later.
-- =========================================================

-- Dimensions
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`dim_date` (
  `date_key` INT,
  `date` DATE,
  `year` INT,
  `quarter` INT,
  `month` INT,
  `month_start_date` DATE,
  `month_end_date` DATE,
  `month_key` STRING,
  `week_of_year` INT,
  `day_of_week` INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`dim_user` (
  `user_id` STRING,
  `first_seen_date` DATE,
  `geo` STRING,
  `device` STRING,
  `user_type` STRING,
  `propensity_score` DOUBLE,
  `is_marketing_opt_in` INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`dim_dealer` (
  `dealer_id` STRING,
  `dealer_tier` STRING,
  `dealer_geo` STRING,
  `is_subscribed` INT,
  `avg_response_time_hours` DOUBLE,
  `dealer_close_skill` DOUBLE,
  `price_elasticity_index` DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`dim_listing` (
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `make` STRING,
  `fuel_type` STRING,
  `transmission` STRING,
  `vehicle_year` INT,
  `vehicle_age_years` INT,
  `mileage_km` INT,
  `listed_price_eur` DOUBLE,
  `base_market_price_eur` DOUBLE,
  `price_position_vs_market` DOUBLE,
  `photo_count` INT,
  `description_length` INT,
  `is_featured` INT,
  `listing_created_date` DATE,
  `listing_delisted_date` DATE,
  `is_active` INT
)
USING DELTA;

-- Facts
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`fact_search` (
  `search_id` STRING,
  `search_date` DATE,
  `date_key` INT,
  `user_id` STRING,
  `listing_id` STRING,
  `search_intent` STRING,
  `user_geo` STRING,
  `user_device` STRING,
  `user_type` STRING,
  `user_propensity_score` DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`fact_contact` (
  `contact_id` STRING,
  `contact_date` DATE,
  `contact_ts` TIMESTAMP,
  `date_key` INT,
  `search_id` STRING,
  `user_id` STRING,
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `lead_channel` STRING,
  `dealer_response_time_hours` DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`fact_sale` (
  `sale_id` STRING,
  `sale_date` DATE,
  `date_key` INT,
  `contact_id` STRING,
  `search_id` STRING,
  `user_id` STRING,
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `sale_price_eur` DOUBLE,
  `listed_price_eur` DOUBLE,
  `discount_pct` DOUBLE,
  `is_marketplace_facilitated` INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`01_silver`.`fact_revenue` (
  `revenue_id` STRING,
  `revenue_date` DATE,
  `date_key` INT,
  `revenue_stream` STRING,
  `dealer_id` STRING,
  `listing_id` STRING,
  `contact_id` STRING,
  `sale_id` STRING,
  `amount_eur` DOUBLE
)
USING DELTA;

-- =========================================================
-- 5) GOLD TABLES (analytics-ready; created in Step 5)
-- =========================================================

-- Conformed dimensions for BI
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`dim_date_month` (
  `month_key` STRING,
  `month_start_date` DATE,
  `month_end_date` DATE,
  `year` INT,
  `quarter` INT,
  `month` INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`dim_geo` (
  `geo` STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`dim_make` (
  `make` STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`dim_dealer` (
  `dealer_id` STRING,
  `dealer_tier` STRING,
  `dealer_geo` STRING,
  `is_subscribed` INT,
  `avg_response_time_hours` DOUBLE,
  `dealer_close_skill` DOUBLE,
  `price_elasticity_index` DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`dim_listing` (
  `listing_id` STRING,
  `seller_type` STRING,
  `dealer_id` STRING,
  `make` STRING,
  `fuel_type` STRING,
  `transmission` STRING,
  `vehicle_year` INT,
  `vehicle_age_years` INT,
  `mileage_km` INT,
  `listed_price_eur` DOUBLE,
  `base_market_price_eur` DOUBLE,
  `price_position_vs_market` DOUBLE,
  `photo_count` INT,
  `description_length` INT,
  `is_featured` INT,
  `listing_created_date` DATE,
  `listing_delisted_date` DATE,
  `is_active` INT
)
USING DELTA;

-- Funnel grain: month + segment cuts
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`agg_funnel_month` (
  `month_key` STRING,
  `geo` STRING,
  `device` STRING,
  `user_type` STRING,
  `seller_type` STRING,
  `dealer_tier` STRING,
  `searches` BIGINT,
  `contacts` BIGINT,
  `sales` BIGINT,
  `search_to_contact_rate` DOUBLE,
  `contact_to_sale_rate` DOUBLE,
  `search_to_sale_rate` DOUBLE
)
USING DELTA;

-- Listing performance
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`agg_listing_month` (
  `month_key` STRING,
  `listing_id` STRING,
  `dealer_id` STRING,
  `seller_type` STRING,
  `make` STRING,
  `vehicle_age_years` INT,
  `is_featured` INT,
  `photo_bucket` STRING,
  `price_position_bucket` STRING,
  `searches` BIGINT,
  `contacts` BIGINT,
  `sales` BIGINT,
  `revenue_eur` DOUBLE,
  `revenue_per_listing_eur` DOUBLE,
  `revenue_per_lead_eur` DOUBLE,
  `search_to_contact_rate` DOUBLE,
  `contact_to_sale_rate` DOUBLE
)
USING DELTA;

-- Dealer performance
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`agg_dealer_month` (
  `month_key` STRING,
  `dealer_id` STRING,
  `dealer_tier` STRING,
  `dealer_geo` STRING,
  `is_subscribed` INT,
  `active_listings` BIGINT,
  `contacts` BIGINT,
  `sales` BIGINT,
  `revenue_eur` DOUBLE,
  `revenue_per_listing_eur` DOUBLE,
  `revenue_per_lead_eur` DOUBLE,
  `search_to_contact_rate` DOUBLE,
  `contact_to_sale_rate` DOUBLE
)
USING DELTA;

-- Revenue streams by month
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`agg_revenue_stream_month` (
  `month_key` STRING,
  `revenue_stream` STRING,
  `revenue_eur` DOUBLE
)
USING DELTA;

-- MoM / YoY drivers table (general-purpose)
CREATE TABLE IF NOT EXISTS `autoscout_mkt_eur`.`02_gold`.`kpi_growth_drivers_month` (
  `kpi_name` STRING,
  `month_key` STRING,
  `kpi_value` DOUBLE,
  `mom_abs_change` DOUBLE,
  `mom_pct_change` DOUBLE,
  `yoy_abs_change` DOUBLE,
  `yoy_pct_change` DOUBLE
)
USING DELTA;

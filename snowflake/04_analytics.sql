-- ============================================================================
-- Worksheet: 04_analytics
-- Purpose  : Build analytics-ready table from STAGING (one-time load)
--            - Create final fact table
--            - Load clean records only
-- ============================================================================

-- Context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_ECOMMERCE;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA ANALYTICS;

-- --------------------------------------------------------------------------
-- Step 04.1: Final analytics table (fact)
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE FACT_SALES (
  event_id           STRING,
  event_ts           TIMESTAMP_NTZ,
  order_id           STRING,
  customer_id        NUMBER,
  session_id         STRING,
  product_id         NUMBER,
  category           STRING,
  quantity           NUMBER,
  unit_price         NUMBER(10,2),
  total_amount       NUMBER(10,2),
  currency           STRING,
  payment_method     STRING,
  country            STRING,
  device             STRING,
  marketing_channel  STRING,
  source_filename    STRING,
  ingestion_ts       TIMESTAMP_NTZ
);

-- --------------------------------------------------------------------------
-- Step 04.2: Load from STAGING clean table (one-time)
-- --------------------------------------------------------------------------

INSERT INTO FACT_SALES
SELECT
  event_id,
  event_ts,
  order_id,
  customer_id,
  session_id,
  product_id,
  category,
  quantity,
  unit_price,
  total_amount,
  currency,
  payment_method,
  country,
  device,
  marketing_channel,
  source_filename,
  ingestion_ts
FROM ECOMMERCE_DB.STAGING.STG_SALES_CLEAN;

-- --------------------------------------------------------------------------
-- Step 04.3: Quick checks
-- --------------------------------------------------------------------------

SELECT COUNT(*) AS fact_rows FROM FACT_SALES;

-- Optional: simple business summary (example)
SELECT
  DATE_TRUNC('DAY', event_ts) AS day,
  COUNT(*) AS events,
  SUM(quantity) AS units,
  SUM(total_amount) AS revenue
FROM FACT_SALES
GROUP BY 1
ORDER BY 1;

-- --------------------------------------------------------------------------
-- Step 04.4: Analytics Queries 
-- --------------------------------------------------------------------------

-- --------------------------------------------------------------------------
-- Question 1: How many sales and how much revenue do we generate per day?
-- --------------------------------------------------------------------------

SELECT
  DATE_TRUNC('DAY', event_ts) AS day,
  COUNT(*) AS total_sales,
  SUM(total_amount) AS total_revenue
FROM ANALYTICS.FACT_SALES
GROUP BY 1
ORDER BY 1;

-- --------------------------------------------------------------------------
-- Question 2: Which product categories generate the highest revenue?
-- --------------------------------------------------------------------------

SELECT
  category,
  COUNT(*) AS total_sales,
  SUM(quantity) AS total_units,
  SUM(total_amount) AS total_revenue
FROM ANALYTICS.FACT_SALES
GROUP BY category
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------
-- Question 3: What payment methods are most commonly used by customers?
-- --------------------------------------------------------------------------

SELECT
  payment_method,
  COUNT(*) AS total_sales,
  SUM(total_amount) AS total_revenue
FROM ANALYTICS.FACT_SALES
GROUP BY payment_method
ORDER BY total_sales DESC;

-- --------------------------------------------------------------------------
-- Question 4: Which countries generate the most revenue?
-- --------------------------------------------------------------------------

SELECT
  country,
  COUNT(*) AS total_sales,
  SUM(total_amount) AS total_revenue
FROM ANALYTICS.FACT_SALES
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;

-- --------------------------------------------------------------------------
-- Question 5: Which marketing channels generate the highest revenue?
-- --------------------------------------------------------------------------

SELECT
  marketing_channel,
  COUNT(*) AS total_sales,
  SUM(total_amount) AS total_revenue
FROM ANALYTICS.FACT_SALES
GROUP BY marketing_channel
ORDER BY total_revenue DESC;
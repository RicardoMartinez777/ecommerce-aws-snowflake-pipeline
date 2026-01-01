-- ============================================================================
-- Worksheet: 03_staging
-- Purpose  : One-time transformation from RAW to STAGING
--            - Create CLEAN and REJECTS tables
--            - Split RAW events into clean vs rejected rows (basic quality rules)
-- ============================================================================

-- Context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_ECOMMERCE;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA STAGING;

-- --------------------------------------------------------------------------
-- Step 03.1: Create STAGING tables
-- --------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_SALES_CLEAN (
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

CREATE OR REPLACE TABLE STG_SALES_REJECTS (
  reason          STRING,
  raw             VARIANT,
  source_filename STRING,
  ingestion_ts    TIMESTAMP_NTZ
);

-- --------------------------------------------------------------------------
-- Step 03.2: Insert REJECTS (rows that fail quality rules)
-- --------------------------------------------------------------------------

INSERT INTO STG_SALES_REJECTS (reason, raw, source_filename, ingestion_ts)
SELECT
  CASE
    WHEN raw:"event_id" IS NULL THEN 'Missing event_id'
    WHEN TRY_TO_TIMESTAMP_NTZ(raw:"event_ts"::STRING) IS NULL THEN 'Invalid event_ts'
    WHEN TRY_TO_NUMBER(raw:"quantity"::STRING) IS NULL THEN 'Invalid quantity'
    WHEN TRY_TO_NUMBER(raw:"quantity"::STRING) <= 0 THEN 'Non-positive quantity'
    WHEN TRY_TO_NUMBER(raw:"total_amount"::STRING) IS NULL THEN 'Invalid total_amount'
    WHEN TRY_TO_NUMBER(raw:"total_amount"::STRING) < 0 THEN 'Negative total_amount'
    ELSE 'Unknown validation error'
  END AS reason,
  raw,
  source_filename,
  ingestion_ts
FROM ECOMMERCE_DB.RAW.RAW_SALES
WHERE
  raw:"event_id" IS NULL
  OR TRY_TO_TIMESTAMP_NTZ(raw:"event_ts"::STRING) IS NULL
  OR TRY_TO_NUMBER(raw:"quantity"::STRING) IS NULL
  OR TRY_TO_NUMBER(raw:"quantity"::STRING) <= 0
  OR TRY_TO_NUMBER(raw:"total_amount"::STRING) IS NULL
  OR TRY_TO_NUMBER(raw:"total_amount"::STRING) < 0;
  
-- --------------------------------------------------------------------------
-- Step 03.3: Insert CLEAN (rows that pass quality rules + typed columns)
-- --------------------------------------------------------------------------

INSERT INTO STG_SALES_CLEAN (
  event_id, event_ts, order_id, customer_id, session_id, product_id, category,
  quantity, unit_price, total_amount, currency, payment_method, country, device,
  marketing_channel, source_filename, ingestion_ts
)
SELECT
  raw:"event_id"::STRING AS event_id,
  TRY_TO_TIMESTAMP_NTZ(raw:"event_ts"::STRING) AS event_ts,
  raw:"order_id"::STRING AS order_id,
  TRY_TO_NUMBER(raw:"customer_id"::STRING) AS customer_id,
  raw:"session_id"::STRING AS session_id,
  TRY_TO_NUMBER(raw:"product_id"::STRING) AS product_id,
  raw:"category"::STRING AS category,
  TRY_TO_NUMBER(raw:"quantity"::STRING) AS quantity,
  TRY_TO_NUMBER(raw:"unit_price"::STRING) AS unit_price,
  TRY_TO_NUMBER(raw:"total_amount"::STRING) AS total_amount,
  raw:"currency"::STRING AS currency,
  raw:"payment_method"::STRING AS payment_method,
  raw:"country"::STRING AS country,
  raw:"device"::STRING AS device,
  raw:"marketing_channel"::STRING AS marketing_channel,
  source_filename,
  ingestion_ts
FROM ECOMMERCE_DB.RAW.RAW_SALES
WHERE
  raw:"event_id" IS NOT NULL
  AND TRY_TO_TIMESTAMP_NTZ(raw:"event_ts"::STRING) IS NOT NULL
  AND TRY_TO_NUMBER(raw:"quantity"::STRING) IS NOT NULL
  AND TRY_TO_NUMBER(raw:"quantity"::STRING) > 0
  AND TRY_TO_NUMBER(raw:"total_amount"::STRING) IS NOT NULL
  AND TRY_TO_NUMBER(raw:"total_amount"::STRING) >= 0;

-- --------------------------------------------------------------------------
-- Step 03.4: Checks (quick validation)
-- --------------------------------------------------------------------------

SELECT COUNT(*) AS clean_rows FROM STG_SALES_CLEAN;
SELECT COUNT(*) AS reject_rows FROM STG_SALES_REJECTS;

SELECT reason, COUNT(*) AS n
FROM STG_SALES_REJECTS
GROUP BY reason
ORDER BY n DESC;

-- ============================================================================
-- Worksheet: 02_raw
-- Purpose  : Load raw JSON sales files from S3 into Snowflake RAW layer
--            (manual, simple, one-time or batch-based execution)
-- ============================================================================

-- Context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_ECOMMERCE;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- --------------------------------------------------------------------------
-- File format (JSON Lines: one JSON object per line)
-- --------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT JSONL_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

-- --------------------------------------------------------------------------
-- RAW table (store data exactly as it arrives)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_SALES (
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_filename STRING,
  raw VARIANT
);

-- --------------------------------------------------------------------------
-- Storage integration (Snowflake -> S3)
-- --------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION S3_ECOMMERCE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::929368845825:role/role-snowflake-s3-ecommerce-raw'
  STORAGE_ALLOWED_LOCATIONS = ('s3://data-ecommerce-source/raw/sales/');

-- Inspect integration configuration (verification)
DESC INTEGRATION S3_ECOMMERCE_INT;

-- --------------------------------------------------------------------------
-- External stage pointing to S3 folder
-- --------------------------------------------------------------------------
CREATE OR REPLACE STAGE S3_SALES_STAGE
  STORAGE_INTEGRATION = S3_ECOMMERCE_INT
  URL = 's3://data-ecommerce-source/raw/sales/'
  FILE_FORMAT = JSONL_FORMAT;

-- --------------------------------------------------------------------------
-- Load ALL JSON files found in the folder
-- --------------------------------------------------------------------------
COPY INTO RAW_SALES (source_filename, raw)
FROM (
  SELECT METADATA$FILENAME, $1
  FROM @S3_SALES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = JSONL_FORMAT)
ON_ERROR = 'CONTINUE';

-- --------------------------------------------------------------------------
-- Quick validation
-- --------------------------------------------------------------------------
SELECT COUNT(*) AS total_raw_rows FROM RAW_SALES;

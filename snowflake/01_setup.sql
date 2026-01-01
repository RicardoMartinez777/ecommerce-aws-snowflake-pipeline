-- ============================================================================
-- Worksheet: 01_setup
-- Purpose  : Create core Snowflake objects for the project.
--            - Warehouse (cost-controlled)
--            - Database and schemas (RAW / STAGING / ANALYTICS)
-- ============================================================================

-- Context setup: use admin role and a safe starting context
USE ROLE ACCOUNTADMIN;

-- Warehouse (auto-suspend to control costs)
CREATE OR REPLACE WAREHOUSE WH_ECOMMERCE
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database
CREATE OR REPLACE DATABASE ECOMMERCE_DB;

-- Layers
CREATE SCHEMA ECOMMERCE_DB.RAW;
CREATE SCHEMA ECOMMERCE_DB.STAGING;
CREATE SCHEMA ECOMMERCE_DB.ANALYTICS;

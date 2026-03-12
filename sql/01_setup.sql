-- ============================================================
-- PROJECT  : Financial Credit Risk Analytics Pipeline
-- SCRIPT   : 01 — Environment Setup
-- AUTHOR   : Akshay Thakare
-- DATE     : 2026
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

CREATE DATABASE IF NOT EXISTS CREDIT_RISK_DB;

CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.RAW;
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.CLEAN;
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.ANALYTICS;

-- Verify
SHOW SCHEMAS IN DATABASE CREDIT_RISK_DB;

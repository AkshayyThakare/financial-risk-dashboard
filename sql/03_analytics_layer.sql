-- ============================================================
-- PROJECT  : Financial Credit Risk Analytics Pipeline
-- SCRIPT   : 03 — ANALYTICS Layer
-- AUTHOR   : Akshay Thakare
-- DATE     : 2026
--
-- PURPOSE  : Pre-aggregated tables optimized for Power BI
--            4 tables covering all dashboard pages
-- ============================================================

USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE CREDIT_RISK_DB;
USE SCHEMA ANALYTICS;

-- ============================================================
-- TABLE 1: Risk Tier Summary
-- Powers: Risk Overview page KPIs + charts
-- ============================================================
CREATE OR REPLACE TABLE CREDIT_RISK_DB.ANALYTICS.RISK_TIER_SUMMARY AS
SELECT
    RISK_TIER,
    LOAN_GRADE,
    COUNT(*)                                    AS TOTAL_LOANS,
    SUM(DEFAULT_FLAG)                           AS TOTAL_DEFAULTS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)           AS DEFAULT_RATE_PCT,
    ROUND(AVG(LOAN_AMOUNT), 2)                  AS AVG_LOAN_AMOUNT,
    ROUND(AVG(INTEREST_RATE), 2)                AS AVG_INTEREST_RATE,
    ROUND(AVG(RISK_SCORE), 1)                   AS AVG_RISK_SCORE,
    ROUND(AVG(ANNUAL_INCOME), 2)                AS AVG_BORROWER_INCOME,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)         AS AVG_DTI_RATIO,
    ROUND(SUM(LOAN_AMOUNT), 2)                  AS TOTAL_LOAN_EXPOSURE,
    ROUND(SUM(LOAN_AMOUNT) * 100.0 /
        SUM(SUM(LOAN_AMOUNT)) OVER(), 2)        AS EXPOSURE_SHARE_PCT
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY RISK_TIER, LOAN_GRADE
ORDER BY AVG_RISK_SCORE DESC;

-- ============================================================
-- TABLE 2: Loan Purpose Analysis
-- Powers: Loan Performance page
-- ============================================================
CREATE OR REPLACE TABLE CREDIT_RISK_DB.ANALYTICS.LOAN_PURPOSE_ANALYSIS AS
SELECT
    LOAN_PURPOSE,
    LOAN_GRADE,
    COUNT(*)                                    AS TOTAL_LOANS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)           AS DEFAULT_RATE_PCT,
    ROUND(AVG(LOAN_AMOUNT), 2)                  AS AVG_LOAN_AMOUNT,
    ROUND(SUM(LOAN_AMOUNT), 2)                  AS TOTAL_EXPOSURE,
    ROUND(AVG(INTEREST_RATE), 2)                AS AVG_INTEREST_RATE,
    ROUND(AVG(RISK_SCORE), 1)                   AS AVG_RISK_SCORE,
    SUM(DEFAULT_FLAG)                           AS TOTAL_DEFAULTS,
    -- Rank loan purposes by default rate
    RANK() OVER (ORDER BY AVG(DEFAULT_FLAG) DESC) AS DEFAULT_RISK_RANK
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY LOAN_PURPOSE, LOAN_GRADE
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- TABLE 3: Borrower Demographics Analysis
-- Powers: Customer Risk Segments page
-- ============================================================
CREATE OR REPLACE TABLE CREDIT_RISK_DB.ANALYTICS.BORROWER_DEMOGRAPHICS AS
SELECT
    BORROWER_AGE_GROUP,
    HOME_OWNERSHIP,
    INCOME_TIER,
    RISK_TIER,
    COUNT(*)                                    AS TOTAL_BORROWERS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)           AS DEFAULT_RATE_PCT,
    ROUND(AVG(ANNUAL_INCOME), 2)                AS AVG_INCOME,
    ROUND(AVG(LOAN_AMOUNT), 2)                  AS AVG_LOAN_AMOUNT,
    ROUND(AVG(RISK_SCORE), 1)                   AS AVG_RISK_SCORE,
    ROUND(AVG(EMPLOYMENT_YEARS), 1)             AS AVG_EMPLOYMENT_YEARS,
    ROUND(AVG(CREDIT_HISTORY_YEARS), 1)         AS AVG_CREDIT_HISTORY,
    SUM(DEFAULT_FLAG)                           AS TOTAL_DEFAULTS
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY BORROWER_AGE_GROUP, HOME_OWNERSHIP, INCOME_TIER, RISK_TIER
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- TABLE 4: Credit Score Analysis
-- Powers: Credit Score Analysis page
-- ============================================================
CREATE OR REPLACE TABLE CREDIT_RISK_DB.ANALYTICS.CREDIT_ANALYSIS AS
SELECT
    RISK_TIER,
    PRIOR_DEFAULT_ON_FILE,
    LOAN_SIZE_CATEGORY,
    COUNT(*)                                    AS TOTAL_LOANS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)           AS DEFAULT_RATE_PCT,
    ROUND(AVG(RISK_SCORE), 1)                   AS AVG_RISK_SCORE,
    ROUND(MIN(RISK_SCORE), 1)                   AS MIN_RISK_SCORE,
    ROUND(MAX(RISK_SCORE), 1)                   AS MAX_RISK_SCORE,
    ROUND(AVG(CREDIT_HISTORY_YEARS), 1)         AS AVG_CREDIT_HISTORY,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)         AS AVG_DTI,
    ROUND(AVG(ANNUAL_INTEREST_BURDEN), 2)       AS AVG_INTEREST_BURDEN,
    ROUND(AVG(LOAN_AMOUNT), 2)                  AS AVG_LOAN_AMOUNT,
    -- % of total
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER(), 2)                AS PCT_OF_PORTFOLIO
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY RISK_TIER, PRIOR_DEFAULT_ON_FILE, LOAN_SIZE_CATEGORY
ORDER BY AVG_RISK_SCORE DESC;

-- ============================================================
-- VERIFY ALL 4 TABLES
-- ============================================================
SELECT 'RISK_TIER_SUMMARY'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CREDIT_RISK_DB.ANALYTICS.RISK_TIER_SUMMARY      UNION ALL
SELECT 'LOAN_PURPOSE_ANALYSIS'  AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CREDIT_RISK_DB.ANALYTICS.LOAN_PURPOSE_ANALYSIS  UNION ALL
SELECT 'BORROWER_DEMOGRAPHICS'  AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CREDIT_RISK_DB.ANALYTICS.BORROWER_DEMOGRAPHICS  UNION ALL
SELECT 'CREDIT_ANALYSIS'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CREDIT_RISK_DB.ANALYTICS.CREDIT_ANALYSIS;

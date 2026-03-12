-- ============================================================
-- PROJECT  : Financial Credit Risk Analytics Pipeline
-- SCRIPT   : 04 — Business SQL Queries
-- AUTHOR   : Akshay Thakare
-- DATE     : 2026
-- ============================================================

USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE CREDIT_RISK_DB;
USE SCHEMA CLEAN;

-- ============================================================
-- QUERY 1: Portfolio Default Rate by Risk Tier
-- Skill: GROUP BY, aggregation, ORDER BY
-- Business Question: Which risk tier has the highest default rate?
-- ============================================================
SELECT
    RISK_TIER,
    COUNT(*)                                AS TOTAL_LOANS,
    SUM(DEFAULT_FLAG)                       AS TOTAL_DEFAULTS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(LOAN_AMOUNT), 2)              AS AVG_LOAN_AMOUNT,
    ROUND(AVG(INTEREST_RATE), 2)            AS AVG_INTEREST_RATE
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY RISK_TIER
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- QUERY 2: Default Rate by Loan Purpose
-- Skill: GROUP BY, RANK window function
-- Business Question: Which loan purposes carry the most risk?
-- ============================================================
SELECT
    LOAN_PURPOSE,
    COUNT(*)                                AS TOTAL_LOANS,
    SUM(DEFAULT_FLAG)                       AS TOTAL_DEFAULTS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(LOAN_AMOUNT), 2)              AS AVG_LOAN_AMOUNT,
    RANK() OVER (ORDER BY AVG(DEFAULT_FLAG) DESC) AS RISK_RANK
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY LOAN_PURPOSE
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- QUERY 3: Income Tier vs Default Rate
-- Skill: CASE WHEN, GROUP BY, percentage calc
-- Business Question: Do lower income borrowers default more?
-- ============================================================
SELECT
    INCOME_TIER,
    COUNT(*)                                AS TOTAL_BORROWERS,
    SUM(DEFAULT_FLAG)                       AS TOTAL_DEFAULTS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(ANNUAL_INCOME), 2)            AS AVG_INCOME,
    ROUND(AVG(LOAN_AMOUNT), 2)              AS AVG_LOAN_AMOUNT,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)     AS AVG_DTI
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY INCOME_TIER
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- QUERY 4: Loan Grade Risk Analysis
-- Skill: Multi-column GROUP BY, window function
-- Business Question: How does loan grade correlate with default?
-- ============================================================
SELECT
    LOAN_GRADE,
    COUNT(*)                                        AS TOTAL_LOANS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)               AS DEFAULT_RATE_PCT,
    ROUND(AVG(INTEREST_RATE), 2)                    AS AVG_INTEREST_RATE,
    ROUND(AVG(RISK_SCORE), 1)                       AS AVG_RISK_SCORE,
    ROUND(SUM(LOAN_AMOUNT), 2)                      AS TOTAL_EXPOSURE,
    ROUND(SUM(LOAN_AMOUNT) * 100.0 /
        SUM(SUM(LOAN_AMOUNT)) OVER(), 2)            AS EXPOSURE_SHARE_PCT
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY LOAN_GRADE
ORDER BY AVG_RISK_SCORE DESC;

-- ============================================================
-- QUERY 5: High Risk Borrower Profile (CTE)
-- Skill: CTE, subquery, filtering
-- Business Question: What does a typical high risk borrower look like?
-- ============================================================
WITH HIGH_RISK AS (
    SELECT *
    FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
    WHERE RISK_TIER IN ('High Risk', 'Critical Risk')
),
LOW_RISK AS (
    SELECT *
    FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
    WHERE RISK_TIER IN ('Low Risk', 'Medium Risk')
)
SELECT
    'High Risk'                             AS SEGMENT,
    COUNT(*)                                AS TOTAL_BORROWERS,
    ROUND(AVG(BORROWER_AGE), 1)             AS AVG_AGE,
    ROUND(AVG(ANNUAL_INCOME), 0)            AS AVG_INCOME,
    ROUND(AVG(LOAN_AMOUNT), 0)              AS AVG_LOAN,
    ROUND(AVG(INTEREST_RATE), 2)            AS AVG_INTEREST_RATE,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)     AS AVG_DTI,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT
FROM HIGH_RISK
UNION ALL
SELECT
    'Low/Medium Risk'                       AS SEGMENT,
    COUNT(*)                                AS TOTAL_BORROWERS,
    ROUND(AVG(BORROWER_AGE), 1)             AS AVG_AGE,
    ROUND(AVG(ANNUAL_INCOME), 0)            AS AVG_INCOME,
    ROUND(AVG(LOAN_AMOUNT), 0)              AS AVG_LOAN,
    ROUND(AVG(INTEREST_RATE), 2)            AS AVG_INTEREST_RATE,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)     AS AVG_DTI,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT
FROM LOW_RISK;

-- ============================================================
-- QUERY 6: Prior Default Impact Analysis
-- Skill: CASE WHEN, GROUP BY, business insight
-- Business Question: How much does prior default history matter?
-- ============================================================
SELECT
    PRIOR_DEFAULT_ON_FILE,
    RISK_TIER,
    COUNT(*)                                AS TOTAL_LOANS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(RISK_SCORE), 1)               AS AVG_RISK_SCORE,
    ROUND(AVG(LOAN_AMOUNT), 2)              AS AVG_LOAN_AMOUNT,
    ROUND(AVG(INTEREST_RATE), 2)            AS AVG_INTEREST_RATE
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY PRIOR_DEFAULT_ON_FILE, RISK_TIER
ORDER BY PRIOR_DEFAULT_ON_FILE, DEFAULT_RATE_PCT DESC;

-- ============================================================
-- QUERY 7: Debt-to-Income Ratio Bucketing
-- Skill: CASE WHEN bucketing, GROUP BY
-- Business Question: At what DTI level does default risk spike?
-- ============================================================
SELECT
    CASE
        WHEN DEBT_TO_INCOME_RATIO < 10  THEN '1. Low DTI (<10%)'
        WHEN DEBT_TO_INCOME_RATIO < 20  THEN '2. Moderate DTI (10-20%)'
        WHEN DEBT_TO_INCOME_RATIO < 35  THEN '3. High DTI (20-35%)'
        WHEN DEBT_TO_INCOME_RATIO < 50  THEN '4. Very High DTI (35-50%)'
        ELSE                                 '5. Critical DTI (>50%)'
    END                                     AS DTI_BUCKET,
    COUNT(*)                                AS TOTAL_LOANS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(LOAN_AMOUNT), 2)              AS AVG_LOAN_AMOUNT,
    ROUND(AVG(RISK_SCORE), 1)               AS AVG_RISK_SCORE
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY DTI_BUCKET
ORDER BY DTI_BUCKET;

-- ============================================================
-- QUERY 8: Running Total Loan Exposure by Risk Score
-- Skill: Window function, running total, cumulative analysis
-- Business Question: What % of exposure comes from top risk loans?
-- ============================================================
WITH RISK_BANDS AS (
    SELECT
        ROUND(RISK_SCORE / 10) * 10         AS RISK_SCORE_BAND,
        COUNT(*)                            AS TOTAL_LOANS,
        ROUND(SUM(LOAN_AMOUNT), 2)          AS TOTAL_EXPOSURE,
        ROUND(AVG(DEFAULT_FLAG) * 100, 2)   AS DEFAULT_RATE_PCT
    FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
    GROUP BY RISK_SCORE_BAND
)
SELECT
    RISK_SCORE_BAND,
    TOTAL_LOANS,
    TOTAL_EXPOSURE,
    DEFAULT_RATE_PCT,
    ROUND(SUM(TOTAL_EXPOSURE)
        OVER (ORDER BY RISK_SCORE_BAND DESC), 2) AS CUMULATIVE_EXPOSURE,
    ROUND(SUM(TOTAL_EXPOSURE)
        OVER (ORDER BY RISK_SCORE_BAND DESC) * 100.0
        / SUM(TOTAL_EXPOSURE) OVER(), 2)         AS CUMULATIVE_EXPOSURE_PCT
FROM RISK_BANDS
ORDER BY RISK_SCORE_BAND DESC;

-- ============================================================
-- QUERY 9: Age Group Default Analysis
-- Skill: GROUP BY, multi-metric, business storytelling
-- Business Question: Which age group poses the most default risk?
-- ============================================================
SELECT
    BORROWER_AGE_GROUP,
    COUNT(*)                                AS TOTAL_BORROWERS,
    ROUND(AVG(DEFAULT_FLAG) * 100, 2)       AS DEFAULT_RATE_PCT,
    ROUND(AVG(ANNUAL_INCOME), 0)            AS AVG_INCOME,
    ROUND(AVG(LOAN_AMOUNT), 0)              AS AVG_LOAN,
    ROUND(AVG(DEBT_TO_INCOME_RATIO), 2)     AS AVG_DTI,
    ROUND(AVG(CREDIT_HISTORY_YEARS), 1)     AS AVG_CREDIT_HISTORY,
    ROUND(AVG(RISK_SCORE), 1)               AS AVG_RISK_SCORE,
    RANK() OVER (ORDER BY AVG(DEFAULT_FLAG) DESC) AS DEFAULT_RISK_RANK
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
GROUP BY BORROWER_AGE_GROUP
ORDER BY DEFAULT_RATE_PCT DESC;

-- ============================================================
-- QUERY 10: ❄️ Snowflake Time Travel — Risk Data Recovery
-- Skill: Snowflake-specific, data recovery
-- Business Question: Can we recover accidentally deleted risk records?
-- ============================================================

-- Simulate accidental deletion of Critical Risk loans
DELETE FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
WHERE RISK_TIER = 'Critical Risk';

-- Check rows after deletion
SELECT COUNT(*) AS ROWS_AFTER_DELETE
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS;

-- Time Travel: query data as it was 2 minutes ago
SELECT COUNT(*) AS ROWS_BEFORE_DELETE
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
    AT (OFFSET => -120);

-- Restore deleted Critical Risk records
INSERT INTO CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
SELECT * FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS
    AT (OFFSET => -120)
WHERE RISK_TIER = 'Critical Risk';

-- Confirm full restoration
SELECT COUNT(*) AS ROWS_RESTORED
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS;

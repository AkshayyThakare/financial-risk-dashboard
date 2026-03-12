-- ============================================================
-- PROJECT  : Financial Credit Risk Analytics Pipeline
-- SCRIPT   : 02 — CLEAN Layer
-- AUTHOR   : Akshay Thakare
-- DATE     : 2026
-- ============================================================

USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE CREDIT_RISK_DB;
USE SCHEMA CLEAN;

CREATE OR REPLACE TABLE CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS AS
SELECT
    -- ── Borrower Demographics ─────────────────────────────────
    PERSON_AGE                              AS BORROWER_AGE,
    AGE_GROUP                               AS BORROWER_AGE_GROUP,
    UPPER(PERSON_HOME_OWNERSHIP)            AS HOME_OWNERSHIP,
    ROUND(PERSON_INCOME, 2)                 AS ANNUAL_INCOME,
    INCOME_TIER,
    PERSON_EMP_LENGTH                       AS EMPLOYMENT_YEARS,

    -- ── Loan Details ──────────────────────────────────────────
    LOAN_AMNT                               AS LOAN_AMOUNT,
    LOAN_SIZE_CATEGORY,
    ROUND(LOAN_INT_RATE, 2)                 AS INTEREST_RATE,  -- ✅ Fixed
    INITCAP(LOAN_INTENT)                    AS LOAN_PURPOSE,
    UPPER(LOAN_GRADE)                       AS LOAN_GRADE,
    ROUND(LOAN_PERCENT_INCOME, 4)           AS LOAN_PERCENT_INCOME,

    -- ── Risk Metrics ──────────────────────────────────────────
    ROUND(DEBT_TO_INCOME_RATIO, 2)          AS DEBT_TO_INCOME_RATIO,
    ROUND(LOAN_TO_INCOME_RATIO, 4)          AS LOAN_TO_INCOME_RATIO,
    ROUND(ANNUAL_INTEREST_BURDEN, 2)        AS ANNUAL_INTEREST_BURDEN,
    CB_PERSON_CRED_HIST_LENGTH              AS CREDIT_HISTORY_YEARS,
    UPPER(CB_PERSON_DEFAULT_ON_FILE)        AS PRIOR_DEFAULT_ON_FILE,

    -- ── Risk Score & Tier ─────────────────────────────────────
    RISK_SCORE,
    RISK_TIER,

    -- ── Outcome ───────────────────────────────────────────────
    CASE
        WHEN LOAN_STATUS = 1 THEN 'Defaulted'
        ELSE 'Non-Default'
    END                                     AS LOAN_OUTCOME,
    LOAN_STATUS                             AS DEFAULT_FLAG

FROM CREDIT_RISK_DB.RAW.LOAN_APPLICATIONS
WHERE LOAN_AMNT     > 0
  AND PERSON_INCOME > 0
  AND PERSON_AGE    BETWEEN 18 AND 100;

-- ── Verify ────────────────────────────────────────────────────
SELECT COUNT(*) AS TOTAL_CLEAN_ROWS
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS;

SELECT * FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS LIMIT 5;

SELECT COUNT(*) AS TOTAL_CLEAN_ROWS 
FROM CREDIT_RISK_DB.CLEAN.LOAN_APPLICATIONS;

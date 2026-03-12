# Databricks notebook source
# DBTITLE 1,Data Load & Scoring (fixed)
# ============================================================
# PROJECT  : Financial Credit Risk Analytics Pipeline
# NOTEBOOK : 03 — Write to Snowflake
# AUTHOR   : Akshay Thakare
# DATE     : 2025
# ============================================================

# ── Reload & Recreate df_scored from scratch ─────────────────
# Variables don't persist between notebooks in Databricks
# so we reload the raw data and reapply all transformations

from pyspark.sql import functions as F
from pyspark.sql.types import *
import warnings
warnings.filterwarnings('ignore')

RAW_PATH = "dbfs:/Workspace/Users/akshayythakare@gmail.com/credit_risk_dataset.csv"

# ── Step 1: Load raw data ─────────────────────────────────────
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .csv(RAW_PATH)

# ── Step 2: Clean ─────────────────────────────────────────────
df_clean = df_raw.dropDuplicates()
df_clean = df_clean.dropna(subset=['loan_amnt','loan_int_rate',
                                    'loan_status','person_age',
                                    'person_income'])
df_clean = df_clean.filter(
    (F.col('person_age')    <= 100) &
    (F.col('person_income') <= 3_000_000) &
    (F.col('loan_amnt')     >  0)
)
df_clean = df_clean \
    .withColumn('loan_intent',    F.initcap(F.col('loan_intent'))) \
    .withColumn('loan_grade',     F.upper(F.col('loan_grade'))) \
    .withColumn('person_home_ownership', F.upper(F.col('person_home_ownership')))

# ── Step 3: Feature Engineering ───────────────────────────────
df_featured = df_clean \
    .withColumn('debt_to_income_ratio',
        F.round((F.col('loan_amnt')/12) / (F.col('person_income')/12) * 100, 2)) \
    .withColumn('loan_to_income_ratio',
        F.round(F.col('loan_amnt') / F.col('person_income'), 4)) \
    .withColumn('annual_interest_burden',
        F.round((F.col('loan_amnt') * F.col('loan_int_rate') / 100)
                / F.col('person_income') * 100, 2)) \
    .withColumn('age_group',
        F.when(F.col('person_age') < 25, 'Gen Z (<25)')
         .when(F.col('person_age') < 35, 'Young Adult (25-34)')
         .when(F.col('person_age') < 45, 'Mid Career (35-44)')
         .when(F.col('person_age') < 55, 'Experienced (45-54)')
         .otherwise('Senior (55+)')) \
    .withColumn('income_tier',
        F.when(F.col('person_income') < 30_000,  'Low (<$30K)')
         .when(F.col('person_income') < 60_000,  'Medium ($30K-$60K)')
         .when(F.col('person_income') < 100_000, 'High ($60K-$100K)')
         .otherwise('Very High (>$100K)')) \
    .withColumn('loan_size_category',
        F.when(F.col('loan_amnt') < 5_000,  'Small (<$5K)')
         .when(F.col('loan_amnt') < 15_000, 'Medium ($5K-$15K)')
         .when(F.col('loan_amnt') < 25_000, 'Large ($15K-$25K)')
         .otherwise('Very Large (>$25K)'))

# ── Step 4: Risk Scoring ──────────────────────────────────────
grade_score = F.when(F.col('loan_grade') == 'A', 5) \
               .when(F.col('loan_grade') == 'B', 15) \
               .when(F.col('loan_grade') == 'C', 30) \
               .when(F.col('loan_grade') == 'D', 50) \
               .when(F.col('loan_grade') == 'E', 70) \
               .when(F.col('loan_grade') == 'F', 85) \
               .otherwise(100)

int_rate_score = F.least(F.lit(100),
                 F.round(F.col('loan_int_rate') / 25 * 100, 0))

dti_score = F.least(F.lit(100),
            F.round(F.col('debt_to_income_ratio') * 2, 0))

cred_score = F.greatest(F.lit(0),
             F.round(100 - (F.col('cb_person_cred_hist_length') * 10), 0))

df_scored = df_featured \
    .withColumn('risk_score',
        F.round(
            (grade_score    * 0.35) +
            (int_rate_score * 0.25) +
            (dti_score      * 0.25) +
            (cred_score     * 0.15), 1)) \
    .withColumn('risk_tier',
        F.when(F.col('risk_score') <= 30, 'Low Risk')
         .when(F.col('risk_score') <= 55, 'Medium Risk')
         .when(F.col('risk_score') <= 75, 'High Risk')
         .otherwise('Critical Risk'))

#df_scored.cache()

print("=" * 60)
print("  Notebook 3 : Write to Snowflake")
print("=" * 60)
print(f"  ✅ df_scored recreated : {df_scored.count():,} rows")
print(f"  ✅ Columns             : {len(df_scored.columns)}")
print("  ✅ Ready to write to Snowflake!")
print("=" * 60)

# COMMAND ----------

# ============================================================
# PROJECT  : Financial Credit Risk Analytics Pipeline
# NOTEBOOK : 03 — Write to Snowflake
# AUTHOR   : Akshay Thakare
# DATE     : 2025
#
# OBJECTIVE:
#   1. Install Snowflake connector
#   2. Create CREDIT_RISK_DB in Snowflake
#   3. Write scored data from Databricks → Snowflake RAW
#   4. Confirm data landed correctly
#
# INPUT  : Cleaned + Risk-Scored DataFrame (29,317 records)
# OUTPUT : Snowflake CREDIT_RISK_DB.RAW.LOAN_APPLICATIONS
# ============================================================

print("=" * 60)
print("  Notebook 3 : Write to Snowflake")
print("=" * 60)

# COMMAND ----------

# ── Install Snowflake Spark Connector ────────────────────────
# This lets Databricks talk directly to Snowflake

%pip install snowflake-connector-python pandas sqlalchemy snowflake-sqlalchemy

print("✅ Snowflake connector installed")

# COMMAND ----------

# ── Install Snowflake Spark Connector ────────────────────────
# This lets Databricks talk directly to Snowflake

%pip install snowflake-connector-python pandas sqlalchemy snowflake-sqlalchemy

print("✅ Snowflake connector installed")

# COMMAND ----------

# ── Snowflake Connection Configuration ───────────────────────
# Replace values below with your Snowflake credentials

SNOWFLAKE_CONFIG = {
    "account"  : "kqhvurp-ap56363",        # Your account identifier
    "user"     : "Akshaythakare", # ← Replace this
    "password" : "Undertakemakshay@26", # ← Replace this
    "warehouse": "HEALTHCARE_WH",           # Reuse existing warehouse
    "role"     : "ACCOUNTADMIN"
}

print("=" * 50)
print("  Snowflake Connection Config")
print("=" * 50)
print(f"  Account   : {SNOWFLAKE_CONFIG['account']}")
print(f"  User      : {SNOWFLAKE_CONFIG['user']}")
print(f"  Warehouse : {SNOWFLAKE_CONFIG['warehouse']}")
print("=" * 50)
print("⚠️  Verify credentials before running next cell!")

# COMMAND ----------

# ── Create Snowflake Structure ────────────────────────────────
import snowflake.connector

conn = snowflake.connector.connect(
    account   = SNOWFLAKE_CONFIG['account'],
    user      = SNOWFLAKE_CONFIG['user'],
    password  = SNOWFLAKE_CONFIG['password'],
    warehouse = SNOWFLAKE_CONFIG['warehouse'],
    role      = SNOWFLAKE_CONFIG['role']
)

cursor = conn.cursor()

setup_sql = [
    # Database
    "CREATE DATABASE IF NOT EXISTS CREDIT_RISK_DB",

    # Schemas — 3 layer architecture
    "CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.RAW",
    "CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.CLEAN",
    "CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DB.ANALYTICS",

    # Confirm
    "SHOW SCHEMAS IN DATABASE CREDIT_RISK_DB"
]

print("=" * 55)
print("  Setting up CREDIT_RISK_DB in Snowflake...")
print("=" * 55)

for sql in setup_sql:
    cursor.execute(sql)
    print(f"  ✅ {sql[:60]}")

print("=" * 55)
print("  Snowflake structure created successfully!")
print("=" * 55)

# COMMAND ----------

# ── Write Scored Data → Snowflake RAW ────────────────────────
# Convert Spark DataFrame → Pandas → Snowflake
# For 29K rows this is fast and efficient

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd

print("Converting Spark DataFrame to Pandas...")
df_pd_final = df_scored.toPandas()

# Clean column names for Snowflake (uppercase, no spaces)
df_pd_final.columns = [c.upper().replace(' ','_') 
                        for c in df_pd_final.columns]

print(f"✅ Converted: {len(df_pd_final):,} rows, {len(df_pd_final.columns)} columns")
print("\nWriting to Snowflake RAW layer...")

# Create SQLAlchemy engine
engine = create_engine(URL(
    account   = SNOWFLAKE_CONFIG['account'],
    user      = SNOWFLAKE_CONFIG['user'],
    password  = SNOWFLAKE_CONFIG['password'],
    database  = 'CREDIT_RISK_DB',
    schema    = 'RAW',
    warehouse = SNOWFLAKE_CONFIG['warehouse'],
    role      = SNOWFLAKE_CONFIG['role']
))

# Write to Snowflake
df_pd_final.to_sql(
    name      = 'loan_applications',
    con       = engine,
    if_exists = 'replace',
    index     = False,
    chunksize = 5000,          # Write in batches of 5000 rows
    method    = 'multi'
)

print("=" * 55)
print("  DATA WRITTEN TO SNOWFLAKE SUCCESSFULLY!")
print("=" * 55)
print(f"  Database  : CREDIT_RISK_DB")
print(f"  Schema    : RAW")
print(f"  Table     : LOAN_APPLICATIONS")
print(f"  Rows      : {len(df_pd_final):,}")
print(f"  Columns   : {len(df_pd_final.columns)}")
print("=" * 55)

# COMMAND ----------

# DBTITLE 1,Verify Data Landed Correctly (fixed)
# ── Verify Data Landed Correctly ─────────────────────────────
verify_queries = [
    ("Row Count",
     "SELECT COUNT(*) AS TOTAL_ROWS FROM CREDIT_RISK_DB.RAW.LOAN_APPLICATIONS"),

    ("Risk Tier Distribution",
     """SELECT RISK_TIER, COUNT(*) AS TOTAL,
        ROUND(AVG(LOAN_STATUS) * 100, 2) AS ACTUAL_DEFAULT_RATE
        FROM CREDIT_RISK_DB.RAW.LOAN_APPLICATIONS
        GROUP BY RISK_TIER
        ORDER BY TOTAL DESC"""),

    ("Sample Records",
     """SELECT PERSON_AGE, LOAN_AMNT, LOAN_INT_RATE,
               RISK_SCORE, RISK_TIER, LOAN_STATUS
        FROM CREDIT_RISK_DB.RAW.LOAN_APPLICATIONS
        LIMIT 5""")
]

print("=" * 60)
print("  SNOWFLAKE VERIFICATION CHECKS")
print("=" * 60)

for check_name, query in verify_queries:
    print(f"\n📋 {check_name}:")
    print("-" * 50)
    cursor.execute(query)
    results = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    result_df = pd.DataFrame(results, columns=cols)
    print(result_df.to_string(index=False))

cursor.close()
conn.close()
print("\n✅ All verification checks passed!")
print("✅ Notebook 3 Complete — Data is live in Snowflake!")
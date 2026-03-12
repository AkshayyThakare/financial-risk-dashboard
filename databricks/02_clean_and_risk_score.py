# Databricks notebook source
# ============================================================
# PROJECT  : Financial Credit Risk Analytics Pipeline
# NOTEBOOK : 02 — Data Cleaning & Risk Scoring
# AUTHOR   : Akshay Thakare
# DATE     : 2025
#
# OBJECTIVE:
#   1. Clean and validate raw loan data
#   2. Engineer new risk features
#   3. Assign Risk Tier to each customer
#      (Low / Medium / High / Critical)
#   4. Prepare final dataset for Snowflake load
#
# INPUT  : Raw CSV (32,581 records)
# OUTPUT : Cleaned + Risk-Scored Spark DataFrame
#          → Ready to write to Snowflake
# ============================================================

print("=" * 60)
print("  Notebook 2 : Data Cleaning & Risk Scoring")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Imports and Data Load (fixed)
# ── Imports ──────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import warnings
warnings.filterwarnings('ignore')

# ── Plot Styling ─────────────────────────────────────────────
plt.rcParams['figure.facecolor'] = '#0f172a'
plt.rcParams['axes.facecolor']   = '#1e293b'
plt.rcParams['axes.labelcolor']  = '#94a3b8'
plt.rcParams['xtick.color']      = '#64748b'
plt.rcParams['ytick.color']      = '#64748b'
plt.rcParams['text.color']       = '#f1f5f9'
plt.rcParams['grid.color']       = '#334155'
plt.rcParams['grid.alpha']       = 0.4

BLUE   = '#38bdf8'
RED    = '#f87171'
GREEN  = '#34d399'
ORANGE = '#fb923c'
PURPLE = '#a78bfa'

# ── Reload Raw Data ───────────────────────────────────────────
RAW_PATH = "dbfs:/Workspace/Users/akshayythakare@gmail.com/credit_risk_dataset.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .csv(RAW_PATH)

#df_raw.cache()
print(f"✅ Raw data loaded : {df_raw.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Data Cleaning (fixed)
# ── Data Cleaning ─────────────────────────────────────────────
# Step 1: Remove duplicates
# Step 2: Handle nulls
# Step 3: Fix outliers
# Step 4: Standardize text columns

print("── Cleaning Pipeline ────────────────────────────────")

# Step 1: Remove duplicates
before_dedup = df_raw.count()
df_clean = df_raw.dropDuplicates()
after_dedup = df_clean.count()
print(f"  Duplicates removed     : {before_dedup - after_dedup:,}")

# Step 2: Drop rows with nulls in critical columns
critical_cols = ['loan_amnt', 'loan_int_rate', 'loan_status',
                 'person_age', 'person_income']
df_clean = df_clean.dropna(subset=critical_cols)
print(f"  Rows after null drop   : {df_clean.count():,}")

# Step 3: Remove outliers
# Age > 100 is likely data entry error
# Income > 3M is extreme outlier
df_clean = df_clean.filter(
    (F.col('person_age') <= 100) &
    (F.col('person_income') <= 3_000_000) &
    (F.col('loan_amnt') > 0)
)
print(f"  Rows after outlier fix : {df_clean.count():,}")

# Step 4: Standardize text columns
df_clean = df_clean \
    .withColumn('loan_intent',  F.initcap(F.col('loan_intent'))) \
    .withColumn('loan_grade',   F.upper(F.col('loan_grade'))) \
    .withColumn('person_home_ownership', F.upper(F.col('person_home_ownership')))

print(f"  Text columns standardized ✅")
print("─" * 50)
print(f"  Final clean rows       : {df_clean.count():,}")
print("✅ Data cleaning complete")

# COMMAND ----------

# DBTITLE 1,Feature Engineering (fixed)
# ── Feature Engineering ───────────────────────────────────────
# Create new columns that help identify risk
# These are REAL features used by banks

df_featured = df_clean \
    .withColumn(
        # Debt-to-Income Ratio: monthly debt payment vs income
        # Banks use this as primary risk indicator
        'debt_to_income_ratio',
        F.round(
            (F.col('loan_amnt') / 12) / (F.col('person_income') / 12) * 100,
        2)
    ) \
    .withColumn(
        # Loan-to-Income Ratio: total loan vs annual income
        'loan_to_income_ratio',
        F.round(F.col('loan_amnt') / F.col('person_income'), 4)
    ) \
    .withColumn(
        # Interest burden: annual interest cost vs income
        'annual_interest_burden',
        F.round(
            (F.col('loan_amnt') * F.col('loan_int_rate') / 100) 
            / F.col('person_income') * 100,
        2)
    ) \
    .withColumn(
        # Age group segmentation
        'age_group',
        F.when(F.col('person_age') < 25, 'Gen Z (<25)')
         .when(F.col('person_age') < 35, 'Young Adult (25-34)')
         .when(F.col('person_age') < 45, 'Mid Career (35-44)')
         .when(F.col('person_age') < 55, 'Experienced (45-54)')
         .otherwise('Senior (55+)')
    ) \
    .withColumn(
        # Income tier
        'income_tier',
        F.when(F.col('person_income') < 30_000,  'Low (<$30K)')
         .when(F.col('person_income') < 60_000,  'Medium ($30K-$60K)')
         .when(F.col('person_income') < 100_000, 'High ($60K-$100K)')
         .otherwise('Very High (>$100K)')
    ) \
    .withColumn(
        # Loan size category
        'loan_size_category',
        F.when(F.col('loan_amnt') < 5_000,  'Small (<$5K)')
         .when(F.col('loan_amnt') < 15_000, 'Medium ($5K-$15K)')
         .when(F.col('loan_amnt') < 25_000, 'Large ($15K-$25K)')
         .otherwise('Very Large (>$25K)')
    )

print("✅ New features engineered:")
print("   debt_to_income_ratio    — primary bank risk indicator")
print("   loan_to_income_ratio    — total exposure vs income")
print("   annual_interest_burden  — interest cost as % of income")
print("   age_group               — borrower age segmentation")
print("   income_tier             — borrower income segmentation")
print("   loan_size_category      — loan amount buckets")
print(f"\n   Total columns now: {len(df_featured.columns)}")

# COMMAND ----------

# DBTITLE 1,Risk Scoring Engine (fixed)
# ── Risk Scoring Engine ───────────────────────────────────────
# Assign a Risk Score (0-100) to each loan
# Based on 4 weighted risk factors used by real banks
#
# SCORING LOGIC:
#   Factor 1: Loan Grade        (35% weight) — bank's own rating
#   Factor 2: Interest Rate     (25% weight) — higher rate = higher risk
#   Factor 3: Debt-to-Income    (25% weight) — affordability
#   Factor 4: Credit History    (15% weight) — track record
#
# RISK TIERS:
#   0  - 30  → Low Risk      (Green)
#   31 - 55  → Medium Risk   (Yellow)
#   56 - 75  → High Risk     (Orange)
#   76 - 100 → Critical Risk (Red)

# Grade Score: A=5pts, B=15pts, C=30pts, D=50pts, E=70pts, F=85pts, G=100pts
grade_score = F.when(F.col('loan_grade') == 'A', 5) \
               .when(F.col('loan_grade') == 'B', 15) \
               .when(F.col('loan_grade') == 'C', 30) \
               .when(F.col('loan_grade') == 'D', 50) \
               .when(F.col('loan_grade') == 'E', 70) \
               .when(F.col('loan_grade') == 'F', 85) \
               .otherwise(100)

# Interest Rate Score: normalize to 0-100
int_rate_score = F.least(F.lit(100),
                 F.round(F.col('loan_int_rate') / 25 * 100, 0))

# DTI Score: normalize to 0-100
dti_score = F.least(F.lit(100),
            F.round(F.col('debt_to_income_ratio') * 2, 0))

# Credit History Score: longer history = lower risk
cred_score = F.greatest(F.lit(0),
             F.round(100 - (F.col('cb_person_cred_hist_length') * 10), 0))

df_scored = df_featured \
    .withColumn(
        'risk_score',
        F.round(
            (grade_score    * 0.35) +
            (int_rate_score * 0.25) +
            (dti_score      * 0.25) +
            (cred_score     * 0.15),
        1)
    ) \
    .withColumn(
        'risk_tier',
        F.when(F.col('risk_score') <= 30, 'Low Risk')
         .when(F.col('risk_score') <= 55, 'Medium Risk')
         .when(F.col('risk_score') <= 75, 'High Risk')
         .otherwise('Critical Risk')
    )

# ── Risk Tier Distribution ────────────────────────────────────
print("=" * 55)
print("  RISK TIER DISTRIBUTION")
print("=" * 55)
risk_dist = df_scored.groupBy('risk_tier') \
    .agg(
        F.count('*').alias('count'),
        F.round(F.avg('risk_score'), 1).alias('avg_score'),
        F.round(F.avg('loan_status') * 100, 2).alias('actual_default_rate_%')
    ) \
    .orderBy('avg_score')

risk_dist.show()
print("✅ Risk scoring complete")

# COMMAND ----------

# ── Risk Tier Visualization ───────────────────────────────────
risk_pd = df_scored.groupBy('risk_tier', 'loan_status') \
    .count().toPandas()

summary_pd = df_scored.groupBy('risk_tier') \
    .agg(
        F.count('*').alias('total'),
        F.round(F.avg('loan_status') * 100, 1).alias('default_rate'),
        F.round(F.avg('risk_score'), 1).alias('avg_risk_score'),
        F.round(F.avg('loan_amnt'), 0).alias('avg_loan')
    ).toPandas() \
     .sort_values('avg_risk_score')

fig, axes = plt.subplots(1, 3, figsize=(16, 6))
fig.suptitle('Risk Scoring Results — Credit Portfolio',
             fontsize=15, fontweight='bold', color='#f1f5f9')

tier_colors = {
    'Low Risk'     : '#34d399',
    'Medium Risk'  : '#fbbf24',
    'High Risk'    : '#fb923c',
    'Critical Risk': '#f87171'
}
colors = [tier_colors.get(t, '#94a3b8') for t in summary_pd['risk_tier']]

# ── Chart 1: Customer Count per Tier ─────────────────────────
axes[0].barh(summary_pd['risk_tier'], summary_pd['total'],
             color=colors, edgecolor='#0f172a')
axes[0].set_title('Customers per Risk Tier', fontweight='bold', color='#f1f5f9')
axes[0].set_xlabel('Customer Count')
axes[0].xaxis.grid(True)
for i, v in enumerate(summary_pd['total']):
    axes[0].text(v + 50, i, f'{v:,}', va='center',
                 fontsize=10, color='#f1f5f9')

# ── Chart 2: Default Rate per Tier ───────────────────────────
bars = axes[1].barh(summary_pd['risk_tier'], summary_pd['default_rate'],
                    color=colors, edgecolor='#0f172a')
axes[1].set_title('Default Rate by Risk Tier', fontweight='bold', color='#f1f5f9')
axes[1].set_xlabel('Default Rate (%)')
axes[1].xaxis.grid(True)
for i, v in enumerate(summary_pd['default_rate']):
    axes[1].text(v + 0.3, i, f'{v}%', va='center',
                 fontsize=10, color='#f1f5f9')

# ── Chart 3: Avg Risk Score per Tier ─────────────────────────
axes[2].barh(summary_pd['risk_tier'], summary_pd['avg_risk_score'],
             color=colors, edgecolor='#0f172a')
axes[2].set_title('Avg Risk Score by Tier', fontweight='bold', color='#f1f5f9')
axes[2].set_xlabel('Risk Score (0-100)')
axes[2].xaxis.grid(True)
for i, v in enumerate(summary_pd['avg_risk_score']):
    axes[2].text(v + 0.3, i, f'{v}', va='center',
                 fontsize=10, color='#f1f5f9')

plt.tight_layout()
plt.show()
print("✅ Risk visualization complete")

# COMMAND ----------

# ── Final Dataset Summary ─────────────────────────────────────
total_clean = df_scored.count()
total_cols  = len(df_scored.columns)

print("=" * 58)
print("  NOTEBOOK 2 COMPLETE — CLEAN + RISK SCORED DATASET")
print("=" * 58)
print(f"  Total Records     : {total_clean:,}")
print(f"  Total Columns     : {total_cols}")
print(f"  New Features Added: 6 (DTI, LTI, interest burden,")
print(f"                         age group, income tier,")
print(f"                         loan size, risk score, risk tier)")
print("-" * 58)
print("  Risk Tiers assigned:")

tier_counts = df_scored.groupBy('risk_tier') \
    .count().orderBy('count', ascending=False).collect()

for row in tier_counts:
    pct = round(row['count'] / total_clean * 100, 1)
    print(f"    {row['risk_tier']:<15} : {row['count']:>6,}  ({pct}%)")

print("=" * 58)
print("\n✅ Dataset ready for Snowflake load")
print("   Next → Notebook 3: Write to Snowflake")

# ── Save as temp view for Notebook 3 ─────────────────────────
df_scored.createOrReplaceTempView("credit_risk_scored")
print("✅ Temp view 'credit_risk_scored' created")
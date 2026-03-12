# Databricks notebook source
import subprocess
result = subprocess.run(['find', '/Workspace', '-name', '*.csv'], 
                      capture_output=True, text=True)
print(result.stdout)

# COMMAND ----------

# ============================================================
# PROJECT:  Financial Credit Risk Analytics Pipeline
# AUTHOR:   Akshay Thakare
# DATE:     2025
# VERSION:  1.0
#
# DESCRIPTION:
#   End-to-end credit risk analytics pipeline built on
#   Databricks + Snowflake + Power BI. Analyzes 32,000+
#   loan records to identify default risk patterns and
#   segment customers into risk tiers for business decisions.
#
# PIPELINE:
#   Databricks (ingest + clean + risk score)
#   → Snowflake (RAW → CLEAN → ANALYTICS)
#   → Power BI (4-page Risk Dashboard)
#
# DATASET:
#   Source : Kaggle Credit Risk Dataset
#   Rows   : ~32,000 loan records
#   Target : loan_status (1 = Default / 0 = Non-Default)
# ============================================================

print("=" * 60)
print("  Financial Credit Risk Analytics Pipeline")
print("  Databricks + Snowflake + Power BI")
print("=" * 60)
print("  Notebook 1 of 3 : Ingest & Explore")
print("=" * 60)

# COMMAND ----------

# ── Library Imports ──────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ── Plot Styling (dark professional theme) ───────────────────
plt.rcParams['figure.facecolor'] = '#0f172a'
plt.rcParams['axes.facecolor']   = '#1e293b'
plt.rcParams['axes.labelcolor']  = '#94a3b8'
plt.rcParams['xtick.color']      = '#64748b'
plt.rcParams['ytick.color']      = '#64748b'
plt.rcParams['text.color']       = '#f1f5f9'
plt.rcParams['grid.color']       = '#334155'
plt.rcParams['grid.alpha']       = 0.4

print("✅ Libraries loaded successfully")
print(f"   Pandas version : {pd.__version__}")

# COMMAND ----------

# DBTITLE 1,Data Ingestion (fixed)
# ── Data Ingestion ───────────────────────────────────────────
# Loading via Spark = enterprise-scale processing

RAW_PATH = "dbfs:/Workspace/Users/akshayythakare@gmail.com/credit_risk_dataset.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .csv(RAW_PATH)

# Cache for faster repeated operations
#df_raw.cache()

row_count = df_raw.count()
col_count = len(df_raw.columns)

print("=" * 55)
print("  RAW DATA LOADED SUCCESSFULLY")
print("=" * 55)
print(f"  Rows      : {row_count:,}")
print(f"  Columns   : {col_count}")
print(f"  Path      : {RAW_PATH}")
print("=" * 55)
print("\n📋 SCHEMA:")
df_raw.printSchema()

# COMMAND ----------

# ── Data Quality Assessment ──────────────────────────────────
# Real analysts always do this before any analysis

print("=" * 65)
print("  DATA QUALITY REPORT")
print("=" * 65)

total_rows   = df_raw.count()
quality_data = []

for col_name in df_raw.columns:
    null_count    = df_raw.filter(F.col(col_name).isNull()).count()
    null_pct      = round((null_count / total_rows) * 100, 2)
    distinct_vals = df_raw.select(col_name).distinct().count()
    dtype         = dict(df_raw.dtypes)[col_name]

    quality_data.append({
        "Column"          : col_name,
        "Type"            : dtype,
        "Nulls"           : null_count,
        "Null %"          : f"{null_pct}%",
        "Distinct Values" : distinct_vals,
        "Status"          : "⚠️  Needs Attention" if null_pct > 5 else "✅ Good"
    })

quality_df = pd.DataFrame(quality_data)
print(quality_df.to_string(index=False))

needs_attention = sum(1 for r in quality_data if float(r['Null %'].replace('%','')) > 5)
print(f"\n  Total Columns Assessed  : {len(quality_data)}")
print(f"  Columns Needing Cleanup : {needs_attention}")
print("=" * 65)

# COMMAND ----------

# DBTITLE 1,Exploratory Data Analysis (fixed)
# ── Exploratory Data Analysis ────────────────────────────────
df_pd = df_raw.toPandas()

BLUE   = '#38bdf8'
RED    = '#f87171'
GREEN  = '#34d399'
ORANGE = '#fb923c'

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Credit Risk Dataset — Exploratory Analysis',
             fontsize=16, fontweight='bold', color='#f1f5f9')

# ── Plot 1: Default Rate Pie ──────────────────────────────────
loan_counts = df_pd['loan_status'].value_counts()
axes[0,0].pie(
    loan_counts.values,
    labels=['Non-Default', 'Default'],
    colors=[GREEN, RED],
    autopct='%1.1f%%',
    startangle=90,
    textprops={'color': '#f1f5f9', 'fontsize': 11}
)
axes[0,0].set_title('Portfolio Default Rate', fontweight='bold', color='#f1f5f9')

# ── Plot 2: Loan Amount Distribution ─────────────────────────
axes[0,1].hist(df_pd['loan_amnt'].dropna(), bins=40,
               color=BLUE, edgecolor='#0f172a', alpha=0.85)
axes[0,1].set_title('Loan Amount Distribution', fontweight='bold', color='#f1f5f9')
axes[0,1].set_xlabel('Loan Amount ($)')
axes[0,1].set_ylabel('Count')
axes[0,1].yaxis.grid(True)

# ── Plot 3: Interest Rate by Loan Status ─────────────────────
default     = df_pd[df_pd['loan_status'] == 1]['loan_int_rate'].dropna()
non_default = df_pd[df_pd['loan_status'] == 0]['loan_int_rate'].dropna()
axes[1,0].hist(non_default, bins=30, alpha=0.7, color=GREEN, label='Non-Default')
axes[1,0].hist(default,     bins=30, alpha=0.7, color=RED,   label='Default')
axes[1,0].set_title('Interest Rate by Loan Status', fontweight='bold', color='#f1f5f9')
axes[1,0].set_xlabel('Interest Rate (%)')
axes[1,0].set_ylabel('Count')
axes[1,0].legend(facecolor='#1e293b', labelcolor='#f1f5f9')
axes[1,0].yaxis.grid(True)

# ── Plot 4: Borrower Age Distribution ────────────────────────
axes[1,1].hist(df_pd['person_age'].dropna(), bins=30,
               color=ORANGE, edgecolor='#0f172a', alpha=0.85)
axes[1,1].set_title('Borrower Age Distribution', fontweight='bold', color='#f1f5f9')
axes[1,1].set_xlabel('Age')
axes[1,1].set_ylabel('Count')
axes[1,1].yaxis.grid(True)

plt.tight_layout()
plt.show()
print("✅ EDA visualizations generated")

# COMMAND ----------

# DBTITLE 1,Executive Summary (fixed)
# ── Executive Summary ────────────────────────────────────────
# What a real analyst presents to stakeholders

total        = len(df_pd)
defaults     = int(df_pd['loan_status'].sum())
default_rate = round((defaults / total) * 100, 2)
avg_loan     = round(df_pd['loan_amnt'].mean(), 2)
avg_income   = round(df_pd['person_income'].mean(), 2)
avg_int_rate = round(df_pd['loan_int_rate'].mean(), 2)
avg_age      = round(df_pd['person_age'].mean(), 1)
avg_credit   = round(df_pd['cb_person_cred_hist_length'].mean(), 1)

print("=" * 58)
print("  EXECUTIVE SUMMARY — CREDIT RISK PORTFOLIO")
print("=" * 58)
print(f"  Total Loan Records       : {total:>10,}")
print(f"  Total Defaults           : {defaults:>10,}")
print(f"  Portfolio Default Rate   : {default_rate:>9}%")
print("-" * 58)
print(f"  Avg Loan Amount          : ${avg_loan:>10,.2f}")
print(f"  Avg Borrower Income      : ${avg_income:>10,.2f}")
print(f"  Avg Interest Rate        : {avg_int_rate:>9}%")
print(f"  Avg Borrower Age         : {avg_age:>10} years")
print(f"  Avg Credit History       : {avg_credit:>10} years")
print("=" * 58)
print(f"\n  ⚠️  KEY INSIGHT:")
print(f"  1 in every {round(100/default_rate,1)} loans in this portfolio defaults.")
print(f"  High-interest loans show significantly elevated risk.")
print("=" * 58)
print("\n✅ Notebook 1 Complete — Ready for Notebook 2: Data Cleaning")
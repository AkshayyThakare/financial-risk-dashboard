# 🏦 Financial Credit Risk Dashboard
## Databricks | Snowflake | SQL | Power BI | DAX

An end-to-end credit risk analytics pipeline analyzing 32,581 loan 
records to identify default patterns, score borrower risk, and 
visualize portfolio health for banking stakeholders.

---

## 🏗️ Architecture
```
CSV Dataset → Databricks (PySpark) → Snowflake (RAW → CLEAN → ANALYTICS) → Power BI Dashboard
```

## 🛠️ Tools Used
- **Databricks** — PySpark notebooks for large-scale data processing
- **Snowflake** — Cloud data warehouse (3-layer architecture)
- **SQL** — Data transformation & 10 business queries
- **Power BI** — 4-page interactive dashboard
- **DAX** — 8 custom KPI measures
- **GitHub** — Version control & documentation

## 📁 Project Structure
```
├── databricks/
│   ├── 01_ingest_and_explore.py      # Data ingestion + EDA
│   ├── 02_clean_and_risk_score.py    # Cleaning + Risk Scoring Engine
│   └── 03_write_to_snowflake.py      # Databricks → Snowflake pipeline
├── sql/
│   ├── 01_setup.sql                  # Warehouse, DB, schemas
│   ├── 02_clean_layer.sql            # Data cleaning & transformation
│   ├── 03_analytics_layer.sql        # Pre-aggregated analytics tables
│   └── 04_business_queries.sql       # 10 business SQL queries
├── powerbi/
│   └── financial_risk_dashboard.pbix # 4-page Power BI dashboard
└── data/
    └── credit_risk_dataset.csv       # Source dataset (32,581 rows)
```

## 📊 Dashboard Pages
1. **Risk Overview** — KPIs: Total Loans, Default Rate, Total Exposure, High Risk Rate
2. **Loan Performance** — Default rate by purpose, loan grade analysis
3. **Customer Segments** — Demographics: age group, income tier, home ownership
4. **Credit Analysis** — DTI analysis, prior default impact, portfolio distribution

## 🎯 Risk Scoring Engine
Custom risk scoring model built in PySpark assigning each borrower
a Risk Score (0-100) based on 4 weighted factors:

| Factor | Weight | Rationale |
|---|---|---|
| Loan Grade | 35% | Bank's own credit assessment |
| Interest Rate | 25% | Higher rate = higher perceived risk |
| Debt-to-Income | 25% | Measures affordability |
| Credit History | 15% | Track record of repayment |

**Risk Tiers:**
- 🟢 Low Risk (0-30) — 42.1% of portfolio
- 🟡 Medium Risk (31-55) — 52.2% of portfolio
- 🟠 High Risk (56-75) — 5.5% of portfolio
- 🔴 Critical Risk (76-100) — 0.3% of portfolio

## 🔍 Key SQL Concepts Demonstrated
- CTEs (Common Table Expressions)
- Window functions (RANK, running totals, cumulative %)
- CASE WHEN bucketing (DTI bands, risk tiers)
- Multi-table aggregations
- ❄️ Snowflake Time Travel for data recovery

## 💡 Key Business Insights
- Portfolio default rate: **21.82%** — 1 in every 4.6 loans defaults
- Critical Risk loans (0.3% of volume) require immediate collections attention
- Prior default history significantly increases default probability
- Gen Z borrowers show highest default rates due to lower credit history

## 🚀 How to Reproduce
1. Sign up for Databricks Community Edition (free)
2. Sign up for Snowflake free trial (free)
3. Download dataset from Kaggle: Credit Risk Dataset
4. Run Databricks notebooks in order (01 → 03)
5. Run SQL scripts in order (01 → 04)
6. Open Power BI file and update Snowflake connection details

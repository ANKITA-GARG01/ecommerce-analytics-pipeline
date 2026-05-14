# 🛍️ E-Commerce Analytics Pipeline + FCR/AML Risk Engine

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

**End-to-end data engineering pipeline processing 100,000+ real e-commerce orders —**
**from raw CSVs to analytics dashboard to financial crime risk detection.**

[View Pipeline Code](#️-project-structure) • [SQL Queries](#-sql-analytics-10-queries) • [FCR Module](#-fcraml-risk-engine) • [Dashboard](#-power-bi-dashboard)

</div>

---

## 📌 What This Project Does

Most data projects show you how to clean a CSV and make a chart.

This one builds what a **real data engineering team** actually ships:

- A **modular ETL pipeline** that extracts, cleans, and loads 100K+ records
- A **Star Schema** in MS SQL Server with enforced foreign key constraints
- **10 advanced SQL queries** using window functions, CTEs, and LAG/LEAD
- A **3-page Power BI dashboard** with KPI cards, maps, and trend charts
- A **Financial Crime Risk (FCR/AML) scoring engine** that flags suspicious customers

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         7 Raw CSV Files  ·  100,000+ Brazilian E-Commerce Orders│
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     EXTRACT  (extract.py)                       │
│         Read 7 CSVs → Python Dictionary of DataFrames           │
│         Log shape of each table · Verify all 7 files loaded     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TRANSFORM  (transform.py)                    │
│                                                                 │
│  ✦ Fix date columns (object → datetime64)                       │
│  ✦ Handle nulls (drop / fill / ignore — by strategy)            │
│  ✦ Standardize text (Title Case cities, UPPER states)           │
│  ✦ Remove duplicates (customer_id grain preserved for FK)       │
│  ✦ Engineer KPI columns:                                        │
│      delivery_days · is_late_delivery · sentiment               │
│      order_year · order_month · order_quarter · revenue         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      LOAD  (load.py)                            │
│         SQLAlchemy → MS SQL Server (ecommerce_db)               │
│         Dimensions first → Facts second (FK dependency order)   │
│         Chunked inserts (1,000 rows/batch) · Row count verify   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   MS SQL SERVER  (Star Schema)                   │
│                                                                  │
│              ┌─────────────────────────┐                        │
│              │      fact_orders        │  ← 99,441 rows         │
│              │  (center of the star)   │                        │
│              └───────────┬─────────────┘                        │
│          ┌───────────────┼──────────────────┐                   │
│          ▼               ▼                  ▼                   │
│   dim_customers    dim_products        dim_sellers              │
│   (96,096 rows)    (32,951 rows)       (3,095 rows)             │
│                                                                  │
│   + fact_order_items  (112,650 rows)  ← revenue lives here      │
│   + order_payments    (103,877 rows)                            │
│   + order_reviews     (98,673 rows)                             │
└────────────────────────────┬─────────────────────────────────────┘
                             │
              ┌──────────────┴───────────────┐
              ▼                              ▼
┌─────────────────────┐         ┌────────────────────────┐
│   SQL ANALYTICS     │         │   FCR / AML ENGINE     │
│   10 Business       │         │   fcr_transform.py     │
│   Queries in SSMS   │         │   Risk Scoring +       │
│   Window Functions  │         │   AML Alert System     │
│   CTEs · LAG · RANK │         │   4 New SQL Tables     │
└──────────┬──────────┘         └───────────┬────────────┘
           │                                │
           └──────────────┬─────────────────┘
                          ▼
           ┌──────────────────────────────┐
           │      POWER BI DASHBOARD      │
           │  Page 1: Executive Overview  │
           │  Page 2: Products/Customers  │
           │  Page 3: Operations          │
           │  Page 4: FCR / AML Risk      │
           └──────────────────────────────┘
```

---

## 🔢 Project Numbers

| Metric | Value |
|--------|-------|
| Total orders processed | 99,441 |
| Total items analyzed | 112,650 |
| Unique customers | 96,096 |
| Total revenue analyzed | R$ 13.6M+ |
| Late deliveries flagged | 7,827 (7.9%) |
| Avg delivery time | 12.1 days |
| Positive review rate | 77.1% |
| AML alerts triggered | See FCR module |
| SQL queries written | 16 (10 analytics + 6 FCR) |
| Power BI pages | 4 |

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Language | Python 3.11 | Pipeline orchestration |
| Data Processing | Pandas, NumPy | Transform & feature engineering |
| Database ORM | SQLAlchemy + pyodbc | Python → SQL Server bridge |
| Database | MS SQL Server (SSMS) | Analytical data warehouse |
| Schema Design | Star Schema | Optimized for analytical queries |
| SQL Dialect | T-SQL | Window functions, CTEs, analytics |
| Visualization | Power BI Desktop | 4-page interactive dashboard |
| Version Control | Git + GitHub | Source control |
| Environment | Python venv + dotenv | Dependency isolation + secrets |

---

## 📁 Project Structure

```
ecommerce-analytics-pipeline/
│
├── data/
│   ├── raw/                    ← Original CSVs (7 files, never modified)
│   └── processed/              ← Intermediate cleaned files
│
├── scripts/
│   ├── extract.py              ← E: reads 7 CSVs into DataFrames dict
│   ├── transform.py            ← T: cleans, enriches, engineers features
│   ├── load.py                 ← L: pushes to SQL Server via SQLAlchemy
│   ├── pipeline.py             ← Master orchestrator (runs E→T→L)
│   └── fcr_transform.py        ← FCR/AML risk scoring engine
│
├── sql/
│   ├── creating tables.sql       ← Star Schema DDL (7 tables + FK constraints)
│   ├── QUERIES ON ECOMMERCE DATA.sql   ← FCR tables DDL (4 risk tables)
│   └── Risk analysis.sql            ← 16 business + FCR queries
│
├── dashboard/
│   └── screenshots/            ← Power BI dashboard exports
│
├── notebooks/
│   ├── eda.ipynb               ← Exploratory data analysis (9 cells)
│   └── test_connection.ipynb   ← SQL Server connection test
│
├── .env                        ← DB credentials (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚙️ How to Run

### Prerequisites
```
Python 3.11+
MS SQL Server Express + SSMS
ODBC Driver 17 for SQL Server
Power BI Desktop
```

### 1 — Clone & Setup
```bash
git clone https://github.com/ANKITA-GARG01/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate          # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2 — Configure Environment
Create a `.env` file in the root:
```
DB_SERVER=localhost\SQLEXPRESS
DB_NAME=ecommerce_db
DB_DRIVER=ODBC Driver 17 for SQL Server
```

### 3 — Set Up Database
```sql
-- Run in SSMS:
CREATE DATABASE ecommerce_db;
```
Then run `sql/create_tables.sql` and `sql/create_fcr_tables.sql` in SSMS.

### 4 — Add Dataset
Download the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.
Place all 7 CSV files into `data/raw/`.

### 5 — Run the Pipeline
```bash
cd scripts
python pipeline.py
```

### 6 — Run FCR Engine
```bash
python fcr_transform.py
```

### 7 — Open Dashboard
Open Power BI Desktop → Get Data → SQL Server → `localhost\SQLEXPRESS` → `ecommerce_db`

---

## 🔄 Transform: What Gets Cleaned

| Table | Rows In | Rows Out | Key Transformations |
|-------|---------|----------|-------------------|
| orders | 99,441 | 99,441 | 5 date columns fixed · delivery_days · is_late_delivery · time dimensions extracted |
| items | 112,650 | 112,650 | shipping_limit_date fixed · revenue = price + freight |
| customers | 99,441 | 96,096 | 3,345 duplicates removed · city/state standardized |
| products | 32,951 | 32,951 | 610 null categories → 'Unknown' · underscores removed |
| sellers | 3,095 | 3,095 | city/state standardized |
| payments | 103,886 | 103,877 | 9 zero-value ghost transactions removed |
| reviews | 99,224 | 98,673 | 551 duplicates removed · sentiment label added |

---

## 📊 SQL Analytics (10 Queries)

Every query answers a real business question:

| # | Business Question | Key Concept Used |
|---|-------------------|-----------------|
| 1 | What is our total revenue and avg order value? | Multi-table JOIN + aggregation |
| 2 | Which months performed best? | GROUP BY year/month + ORDER |
| 3 | How is cumulative revenue growing? | `SUM() OVER()` running total + `LAG()` MoM growth |
| 4 | Which product categories drive the most revenue? | `SUM(SUM()) OVER()` for % share |
| 5 | Which seller states cause the most delays? | `HAVING` + late rate calculation |
| 6 | Which categories have the happiest customers? | `CASE WHEN` conditional aggregation + `RANK()` |
| 7 | How do customers prefer to pay? | `SUM(COUNT()) OVER()` for % of transactions |
| 8 | Where are customers concentrated geographically? | Multi-table JOIN + `RANK() OVER()` |
| 9 | Who are our best and worst performing sellers? | CTE + composite ranking |
| 10 | Which days of the week see the most orders? | `CASE WHEN` for custom sort order |

---

## 🏦 FCR/AML Risk Engine

Extended the pipeline with a Financial Crime Risk scoring engine that analyzes every customer across 3 risk dimensions.

### Why FCR on E-Commerce Data?

E-commerce platforms are real targets for financial crime:
- **Money laundering** → buying/returning products to clean dirty money
- **Structuring** → splitting large amounts into small transactions below thresholds
- **Velocity abuse** → placing many orders in rapid succession
- **Refund fraud** → buying expensive items then filing 1-star "never arrived" complaints

### Risk Signals

| Signal | What We Detect | Weight |
|--------|---------------|--------|
| **Velocity** | Multiple orders in rapid succession · high total spend (top 5%) | 35% |
| **Structuring** | Round-number transactions · just-below-threshold amounts (R$990, R$1990) · unusual installment ratios | 40% |
| **Behavioral** | Late-night orders (2–5am) · high-value weekend transactions · 1-star reviews on expensive orders | 25% |

### Risk Output

```
Each customer receives:
  ├── velocity_risk_score      (0–100)
  ├── structuring_risk_score   (0–100)
  ├── behavioral_risk_score    (0–100)
  ├── composite_risk_score     (weighted average, 0–100)
  ├── risk_tier                (LOW / MEDIUM / HIGH / CRITICAL)
  ├── total_flags_fired        (0–4)
  └── aml_alert                (1 = two or more signals fired simultaneously)
```

### FCR SQL Queries (6 Investigation Queries)

| # | Query Purpose |
|---|--------------|
| 1 | Risk tier summary — count and avg score per tier |
| 2 | Top 20 highest risk customers with all flags |
| 3 | What did flagged customers actually buy? |
| 4 | Structuring pattern deep-dive |
| 5 | Geographic risk heatmap by state |
| 6 | Monthly AML alert trend — is crime increasing? |

---

## 📈 Power BI Dashboard

4 pages, connected directly to MS SQL Server:

**Page 1 — Executive Overview**
Total Revenue · Total Orders · Avg Delivery Days · Revenue by Month (line) · Order Status (donut)

**Page 2 — Products & Customers**
Top 10 Categories by Revenue · Avg Review Score by Category · Revenue by State (map)

**Page 3 — Operations**
Late Delivery Rate by Seller State · Payment Method Split · Running Total Revenue (area)

**Page 4 — FCR / AML Risk**
Risk Tier Distribution · AML Alerts by State · Monthly Alert Trend · Top 20 Risk Customers table

---

## 🧠 Key Engineering Decisions

**1. Expected Nulls vs Data Quality Nulls**
Delivery date nulls in cancelled/in-transit orders are NOT a data quality issue — they are business reality. These rows are kept intact. Only purchase timestamp nulls (truly useless rows) are dropped.

**2. Vectorized Operations Over .apply()**
`np.where()` used for `is_late_delivery` and all FCR flag columns. Operates on entire columns at C-speed instead of looping row-by-row — critical at 100K+ row scale.

**3. customer_id vs customer_unique_id**
Deduplicated by `customer_id` (not `customer_unique_id`) to preserve all foreign key references in `fact_orders`. One real person can have multiple `customer_id` values (one per order).

**4. Dimension-First Load Order**
Dimensions loaded before fact tables to satisfy SQL Server foreign key constraints. Loading `fact_orders` before `dim_customers` causes IntegrityError on every row.

**5. Chunked Inserts**
`chunksize=1000` in `df.to_sql()` prevents memory overflow and connection timeouts when loading 100K+ rows.

**6. Truncate Before Load**
`DELETE FROM` runs on all tables before each pipeline execution to prevent primary key conflicts on re-runs. Executed in reverse FK order (facts → dimensions).

---

## 📦 Requirements

```
pandas
numpy
sqlalchemy
pyodbc
python-dotenv
jupyter
```

Install: `pip install -r requirements.txt`

---

## 📂 Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| File | Rows | Description |
|------|------|-------------|
| olist_orders_dataset.csv | 99,441 | All orders placed |
| olist_customers_dataset.csv | 99,441 | Customer info + location |
| olist_order_items_dataset.csv | 112,650 | Items + revenue per order |
| olist_products_dataset.csv | 32,951 | Product details + category |
| olist_sellers_dataset.csv | 3,095 | Seller info + location |
| olist_order_payments_dataset.csv | 103,886 | Payment method + value |
| olist_order_reviews_dataset.csv | 99,224 | Review scores + comments |

---

## 👩‍💻 Author

**Ankita Garg**
Data Analyst · Data Engineer · CS Undergraduate (CGPA: 8.98/10)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin)](https://linkedin.com/in/ankita-garg-a80817212)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github)](https://github.com/ANKITA-GARG01)

---


---

<div align="center">
  <i>Built from scratch — every line written, debugged, and understood.</i>
</div>

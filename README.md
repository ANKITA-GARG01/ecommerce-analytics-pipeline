# ecommerce-analytics-pipeline
□ What I'm Building (Big Picture First)
Before touching any code, understand what this system does and why:
  RAW DATA (CSV files)
      ↓
  PYTHON (Extract → Clean → Transform)    ← "ETL" = Extract, Transform, Load
      ↓
  PostgreSQL DATABASE (Store structured data)
      ↓
  SQL QUERIES (Answer business questions)
      ↓
  POWER BI / TABLEAU DASHBOARD (Visualize insights)
Every company in the world does some version of this. 
I'm building a miniature version of what Amazon, Flipkart, and Zomato's data teams do every single day.

STEP-1 MAIN CONCEPTS

□ What is ETL?
   ETL = Extract, Transform, Load
                 What it means                                            In our project
   Extract       Pull raw data from a source                            Read CSV files with Pandas 
   Transform    Clean, reshape, enrich the data            Fix nulls, parse dates, calculate new columns
   Load           Push clean data into a destination                   Insert into PostgreSQL tables

□ What is a Data Pipeline?
   A pipeline is a series of steps where data flows from one stage to the next — like an assembly line in a factory.
   Source → Step 1 → Step 2 → Step 3 → Destination
   CSV    → Extract → Clean →  Load  → PostgreSQL
If any step breaks, you fix just that step. The rest stays intact.

□ What is a Data Warehouse vs a Database?
                   Regular Database(OLTP)                               Data Warehouse(OLAP)
   Purpose            Store live transactions                              Analyze historical data
   Example            Your bank recording a payment                      Analyst checking monthly revenue
   Optimized for      Writing fast                                        Reading/querying fast
My project uses MS SQL as a simple warehouse
I have built a lightweight analytical database — not a transaction system.

□ What is a Schema / Data Model?
   A schema is the blueprint of your database — what tables exist and how they connect.
    We'll use a Star Schema (most common in analytics):
                          FACT_ORDERS (center)
                     /      |          \       \
         DIM_CUSTOMERS  DIM_PRODUCTS  DIM_DATE  DIM_SELLERS

Fact table = the main events (orders, transactions) — has numbers/metrics
Dimension tables = context (who, what, when, where) — has descriptive info

STEP-2 contains following steps:
□ Python 3.11 installed and verified
□ VS Code installed with all 5 extensions
□ SQL Server Express installed
□ SSMS installed and connected to localhost\SQLEXPRESS
□ ODBC Driver 17 installed
□ Git configured with your name and email
□ GitHub repo created and cloned locally
□ Folder structure created
□ Virtual environment created and activated
□ All libraries installed (pandas, sqlalchemy, pyodbc...)
□ 7 CSV files in data/raw/
□ ecommerce_db created in SSMS
□ .env configured
□ test_connection.ipynb shows all


STEP3: checklist
□  All row data converted into dataframes dic{"name":"dataframe"}
□ https://ap.wps.com/l/cbCaeoHhQVThe4iF
□ extract.py runs and shows all 7 ✅ tables
□ eda.ipynb has all 9 cells run successfully
□ You know which columns have nulls in each table
□ You understand that revenue lives in ITEMS not ORDERS
□ You understand why items has more rows than orders
□ You can explain the Star Schema relationships (order→customer→product→seller)
□ You've written your own EDA notes in Cell 9


STEP4
✅ All 7 tables transformed successfully
✅ 7/7 loaded in clean data dictionary
✅ Dates fixed → datetime64
✅ delivery_days calculated correctly
✅ is_late_delivery: 7,827 late orders flagged
✅ Duplicates removed from customers and reviews
✅ Nulls handled in products
✅ Revenue column added to items
✅ Sentiment labels added to reviews

STEP 5 Load into SQL Server
"The L in ETL — making data permanent"
□ create_tables.sql ran in SSMS — 7 tables visible
□ .env file has correct DB_SERVER, DB_NAME, DB_DRIVER
□ load.py connection test shows ✅ Connected
□ All 7 tables loaded without ❌ errors
□ verify_counts matches Python output
□ SSMS count query confirms all row counts


STEP 6 — SQL Analytics in SSMS
"Turning raw data into business insights"

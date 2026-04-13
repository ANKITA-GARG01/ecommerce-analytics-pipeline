# scripts/load.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()  # reads your .env file


# ============================================================
# CONNECTION
# ============================================================

def get_engine():
    """
    Creates a SQLAlchemy engine — the bridge between Python and SQL Server.

    WHY SQLAlchemy?
    It's an abstraction layer. Instead of writing raw database connection
    code, SQLAlchemy handles it. df.to_sql() needs this engine to know
    WHERE to send the data.

    trusted_connection=yes → uses your Windows login (no password needed)
    This is called Windows Authentication — standard in corporate environments.
    """
    server   = os.getenv('DB_SERVER', r'localhost\SQLEXPRESS')
    database = os.getenv('DB_NAME',   'ecommerce_db')
    driver   = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

    # Build connection string
    # Format: mssql+pyodbc://server/database?driver=...&trusted_connection=yes
    conn_str = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
        f"&trusted_connection=yes"
    )

    engine = create_engine(
        conn_str,
        fast_executemany=True,  # ← makes bulk inserts 10x faster
        pool_pre_ping=True,     # ← checks if connection is alive before using it
        pool_reset_on_return='rollback'# ← rolls back any uncommitted transactions when connection is returned to pool
    )
    return engine


def test_connection(engine):
    """Quick check — can we actually reach the database?"""
    try:
        with engine.begin() as conn:
            result = conn.execute(text("SELECT DB_NAME() AS current_db"))
            db_name = result.fetchone()[0]
            print(f"  ✅ Connected to: {db_name}")
            return True
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        return False

# ============================================================
# LOAD
# ============================================================

def load_table(df, table_name, engine):
    """
    Loads a single DataFrame into a SQL Server table.

    Parameters:
        df         → the clean DataFrame from transform.py
        table_name → name of the SQL table to load into
        engine     → SQLAlchemy connection

    if_exists='append' → adds rows to existing table
    WHY append and not replace?
        Because we already created tables with proper
        PRIMARY KEYS and FOREIGN KEYS in SSMS.
        'replace' would drop and recreate the table
        WITHOUT those constraints — destroying our schema.

    index=False → don't write the pandas row numbers as a column
    chunksize=1000 → insert 1000 rows at a time
        WHY? Loading all 112,650 rows at once can timeout or
        run out of memory. Chunks are safer and more stable.
    """
    try:
        print(f"  ⏳ Loading {table_name} ({len(df):,} rows)...", end='', flush=True)
       
        with engine.begin() as conn:  # ← transaction scope for this load
         df.to_sql(
            name       = table_name,
            con        = conn,
            index      = False, #index=False → don't write the pandas row numbers as a column
            if_exists  = 'append',
            chunksize = 1000     # ← increase from 1000 to 5000

        )
        print(f"  ✅ {table_name:<20} → {len(df):>7,} rows loaded out of {df.shape[0]:,}")
    except Exception as e:
        print(f"  ❌ {table_name:<20} → FAILED: {e}")

# In load.py, add this function:
def truncate_all_tables(engine):
    """
    Clears all tables before loading fresh data.
    ORDER MATTERS — truncate facts before dimensions
    (reverse of load order) because of FK constraints.
    """
    print("\n🧹 Clearing existing data...")

    tables_in_order = [
        # Facts first (they reference dimensions)
        'fact_order_items',
        'fact_orders',
        'order_payments',
        'order_reviews',
        # Dimensions last
        'dim_customers',
        'dim_products',
        'dim_sellers',
    ]

    with engine.begin() as conn:
        for table in tables_in_order:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
                print(f"  🗑️  Cleared {table}")
            except Exception as e:
                print(f"  ⚠️  Could not clear {table}: {e}")

    print("  ✅ All tables cleared\n")


def load_all(clean_data):
    """
    Master load function.
    Called by pipeline.py after transform is complete.

    ORDER MATTERS here:
    Dimension tables FIRST → Fact tables SECOND

    WHY?
    Because fact tables have FOREIGN KEYS pointing to dimension tables.
    If you load fact_orders before dim_customers exists,
    SQL Server will reject it — the customer_id has nothing to point to.

    Think of it like:
    You can't reference a book that hasn't been published yet.
    Publish the book (dimension) first, then reference it (fact).
    """

    engine = get_engine()

    print("\n🔌 Testing connection...")
    if not test_connection(engine):
        return

    # ← ADD THIS LINE
    truncate_all_tables(engine)
    # ... rest of your existing code
    print("\n" + "="*50)
    print("📤 LOAD — Pushing data into SQL Server...")
    print("="*50)

    print("\n📦 Loading DIMENSION tables first...")
    # Dimensions first — these are referenced by fact tables
    load_table(clean_data['customers'], 'dim_customers', engine)
    load_table(clean_data['products'],  'dim_products',  engine)
    load_table(clean_data['sellers'],   'dim_sellers',   engine)

    print("\n📦 Loading FACT tables...")
    # Facts second — they reference the dimensions above
    load_table(clean_data['orders'],  'fact_orders',      engine)
    load_table(clean_data['items'],   'fact_order_items', engine)

    print("\n📦 Loading SUPPORTING tables...")
    load_table(clean_data['payments'], 'order_payments', engine)
    load_table(clean_data['reviews'],  'order_reviews',  engine)

    # Final verification — count rows in each table
    print("\n🔍 Verifying row counts in SQL Server...")
    verify_counts(engine)


def verify_counts(engine):
    """
    After loading, query SQL Server to confirm row counts match.
    WHY: df.to_sql() can silently skip rows on errors.
    Always verify after loading — this is professional practice.
    """
    tables = [
        'dim_customers',
        'dim_products',
        'dim_sellers',
        'fact_orders',
        'fact_order_items',
        'order_payments',
        'order_reviews'
    ]

    print()
    with engine.begin() as conn:
        for table in tables:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            )
            count = result.fetchone()[0]
            print(f"  📊 {table:<22} → {count:>7,} rows in SQL Server")


# ── Test independently ───────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.append('.')
    from extract import extract_all
    from transform import run_all_transforms
    from fcr_transform import run_fcr_transform

    run_fcr_transform()
    raw   = extract_all()
    clean_data = run_all_transforms(raw)
    
    load_all(clean_data)
    
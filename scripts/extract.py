# scripts/extract.py
import pandas as pd
import os

# Path to raw data folder
RAW_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')


def extract_all():
    """
    EXTRACT step — reads all 7 raw CSV files into Pandas DataFrames.

    Think of this like picking up raw ingredients from a store.
    We don't cook anything here — just bring everything in.

    Returns a dictionary: { 'table_name': DataFrame }
    """

    print("=" * 50)
    print(" EXTRACT — Loading raw CSV files...")
    print("=" * 50)

    files = {
        'orders'  : 'olist_orders_dataset.csv',
        'customers': 'olist_customers_dataset.csv',
        'items'   : 'olist_order_items_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'sellers' : 'olist_sellers_dataset.csv',
        'payments': 'olist_order_payments_dataset.csv',
        'reviews' : 'olist_order_reviews_dataset.csv',
    }

    dataframes = {}
    
    for name, filename in files.items():
        filepath = os.path.join(RAW_PATH, filename)

        # Check file exists before reading
        if not os.path.exists(filepath):
            print(f"   MISSING: {filename} — check your data/raw/ folder")
            continue

        df = pd.read_csv(filepath)
        dataframes[name] = df

        print(f"   {name:<12} → {df.shape[0]:>7,} rows  ×  {df.shape[1]:>2} columns")

    print(f"\n   Total tables loaded: {len(dataframes)}/7")
    return dataframes


# ── Run this file directly to test ──────────────────────────
if __name__ == "__main__":
    data = extract_all()

    # Quick sanity check — print column names of each table
    print("\n COLUMN NAMES PER TABLE:")
    print("=" * 50)
    for name, df in data.items():
        print(f"\n{name.upper()}:")
        print(f"  {list(df.columns)}")
import pandas as pd
import numpy as np
  
def log(message):
    '''Simple logger function to print messages with a timestamp.'''
    print(f"->{message}")

# ============================================================
# TRANSFORM: ORDERS
# The most important table — our FACT table
# ============================================================

def transform_orders(df):
    log("Transforming ORDERS...")
    df = df.copy()
    # 1. Convert date columns to datetime
    date_cols = ['order_purchase_timestamp', 
                 'order_approved_at', 
                 'order_delivered_carrier_date', 
                 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    log(f"Fixed {len(date_cols)} date columns → datetime64")

    # 2 Handle nulls
    # Drop rows with no purchase timestamp — these are completely useless
    before = len(df)
    df = df.dropna(subset=['order_purchase_timestamp'])
    after = len(df)
    log(f"Dropped {before - after} rows with null purchase timestamp")
    # NOTE: We do NOT drop rows with null delivery dates.Cancelled/in-transit orders SHOULD have null delivery dates
    # These are "expected nulls" — dropping them would be wrong


    # 3. Extract time parts from purchase date
    # WHY: these become very powerful GROUP BY dimensions in SQL
    df['order_year']= df['order_purchase_timestamp'].dt.year
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_quarter'] = df['order_purchase_timestamp'].dt.quarter
    df['order_day_of_week'] = df['order_purchase_timestamp'].dt.day_name()
    log("Extracted year, month, quarter, day_of_week from purchase date")

    # 4. Calculate how many days delivery actually took
    # WHY: key operational KPI — how fast are we delivering?
    # .dt.days converts timedelta to integer number of days
    df['delivery_days']=(df['order_delivered_customer_date']-df['order_purchase_timestamp']).dt.days
    log(f"Calculated delivery_days — avg: {df['delivery_days'].mean():.1f} days")

    # 5. Was the order delivered late?
    # WHY: measures seller/logistics performance
    # Logic: if actual delivery > estimated delivery → late (1), else on time (0)
    df['is_late_delivery'] = np.where(
        df['order_delivered_customer_date'] > df['order_estimated_delivery_date'],
        1,  # late
        0   # on time
    )
    log(f"Calculated is_late_delivery flag (1=late, 0=on time): {df['is_late_delivery'].sum()} late deliveries")

    df = df[[
        'order_id',
        'customer_id',
        'order_status',
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
        'order_year',
        'order_month',
        'order_quarter',
        'order_day_of_week',
        'delivery_days',
        'is_late_delivery'
    ]]

    #dropped columns: 'order_approved_at', 'order_delivered_carrier_date' — these are less relevant for our analysis and have many nulls

    print(f"  ✅ ORDERS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df

# ============================================================
# TRANSFORM: ORDER ITEMS
# Revenue lives here — price + freight per item
# ============================================================


def transform_items(df):
  log("Transforming ITEMS...")
  df = df.copy()
  # 1. Convert shipping_limit_date to datetime
  df['shipping_limit_date']=pd.to_datetime(df['shipping_limit_date'])
  log("Converted shipping_limit_date to datetime64")

  # 2. Handle nulls
  null_count=df.isnull().sum()
  log(f"Null values per column:\n{null_count}")


  #find out that there are no nulls in items table, so we can skip dropping or filling nulls
  # 3. Calculate total revenue per item (price + freight_value)
  df['revenue']=df["price"]+df["freight_value"]
  log("Calculated revenue = price + freight_value")
  print(f"  ✅ ITEMS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
  return df
# ============================================================
# TRANSFORM: CUSTOMERS
# Dimension table — who are our customers?
# ============================================================

def transform_customers(df):
    log("Transforming CUSTOMERS...")
    df=df.copy()
   #since we have no nulls to fix
   #1. standardise names
    df['customer_city']  = df['customer_city'].str.strip().str.title()
    df['customer_state'] = df['customer_state'].str.strip().str.upper()
    log("Standardized city (Title Case) and state (UPPER CASE)")

    #handling duplicates
    before=len(df)
    df=df.drop_duplicates(subset='customer_id', keep='last')
    after=len(df)
    log(f"Removed {before - after} duplicate customers")


    print(f"  ✅ CUSTOMERS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


# ============================================================
# TRANSFORM: PRODUCTS
# Dimension table — what are we selling?
# ============================================================

def transform_products(df):
    print("\n🔄 Transforming PRODUCTS...")
    df = df.copy()

    # ── STEP 1: Handle nulls ──────────────────────────────────

    # Fill missing category with 'Unknown'
    # WHY: we can't drop these — the product still exists and was sold
    null_cats = df['product_category_name'].isnull().sum()
    df['product_category_name'] = df['product_category_name'].fillna('Unknown')
    log(f"Filled {null_cats} null category names with 'Unknown'")

    # Fill missing physical dimensions with 0
    # WHY: not critical for our revenue/delivery analysis
    dim_cols = [
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ]
    for col in dim_cols:
        df[col] = df[col].fillna(0)
    log("Filled null dimension columns with 0")

    # ── STEP 2: Standardize category names ───────────────────
    # Raw data has underscores: "health_beauty" → "Health Beauty"
    # WHY: cleaner display in Power BI dashboard
    df['product_category_name'] = (
        df['product_category_name']
        .str.replace('_', ' ')   # remove underscores
        .str.strip()             # remove leading/trailing spaces
        .str.title()             # Title Case
    )
    log("Cleaned product_category_name")

    print(f"  ✅ PRODUCTS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    
    # Add this step — drop columns not in our SQL schema
    df = df.drop(columns=[
        'product_name_lenght',         # typo in original dataset (should be "length")
        'product_description_lenght',  # typo in original dataset
        'product_photos_qty'           # not needed for our analysis
    ])
    return df


# ============================================================
# TRANSFORM: REVIEWS
# Sentiment analysis — how happy are customers?
# ============================================================

def transform_reviews(df):
    print("\n🔄 Transforming REVIEWS...")
    df = df.copy()

    # ── STEP 1: Keep only what we need ───────────────────────
    # review_comment columns have 60-90% nulls — not useful for us
    # We only need order_id and review_score for our analysis
    df = df[['order_id', 'review_score']].copy()
    log("Kept only order_id and review_score — dropped comment columns")

    # ── STEP 2: Remove duplicate reviews per order ───────────
    # Some orders have multiple reviews — keep the latest (last one)
    before = len(df)
    df = df.drop_duplicates(subset='order_id', keep='last')
    log(f"Removed {before - len(df)} duplicate reviews")

    # ── STEP 3: Add sentiment label ──────────────────────────
    # WHY: a text label is more readable in dashboards than a number
    # Business rule: 4-5 = Positive, 3 = Neutral, 1-2 = Negative
    def get_sentiment(score):
        if score >= 4:
            return 'Positive'
        elif score == 3:
            return 'Neutral'
        else:
            return 'Negative'

    df['sentiment'] = df['review_score'].apply(get_sentiment)
    log("Added sentiment column (Positive / Neutral / Negative)")

    # Verify distribution
    dist = df['sentiment'].value_counts()
    log(f"Sentiment distribution: {dict(dist)}")

    print(f"  ✅ REVIEWS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


# ============================================================
# TRANSFORM: SELLERS
# Already clean — just standardize text
# ============================================================

def transform_sellers(df):
    print("\n🔄 Transforming SELLERS...")
    df = df.copy()

    df['seller_city']  = df['seller_city'].str.strip().str.title()
    df['seller_state'] = df['seller_state'].str.strip().str.upper()
    log("Standardized city and state formatting")

    print(f"  ✅ SELLERS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


# ============================================================
# TRANSFORM: PAYMENTS
# Already clean — no transformation needed
# ============================================================

def transform_payments(df):
    print("\n🔄 Transforming PAYMENTS...")
    df = df.copy()

    # Remove invalid payment rows (payment_value = 0)
    before = len(df)
    df = df[df['payment_value'] > 0]
    log(f"Removed {before - len(df)} zero-value payment rows")

    print(f"  ✅ PAYMENTS done → {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


# ============================================================
# MASTER FUNCTION — called by pipeline.py
# ============================================================

def run_all_transforms(raw_data):
    """
    Takes the raw data dictionary from extract.py
    Returns a clean data dictionary ready for load.py
    """
    print("\n" + "="*50)
    print("🔄 TRANSFORM — Cleaning all tables...")
    print("="*50)

    clean_data = {
        'orders'   : transform_orders(raw_data['orders']),
        'items'    : transform_items(raw_data['items']),
        'customers': transform_customers(raw_data['customers']),
        'products' : transform_products(raw_data['products']),
        'sellers'  : transform_sellers(raw_data['sellers']),
        'payments' : transform_payments(raw_data['payments']),
        'reviews'  : transform_reviews(raw_data['reviews']),
    }

    print("\n" + "="*50)
    print("✅ ALL TRANSFORMS COMPLETE")
    print("="*50)

    return clean_data


# ── Test this file independently ────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.append('.')
    from extract import extract_all

    raw = extract_all()
    clean = run_all_transforms(raw)

    # Final summary
    print("\n📊 CLEAN DATA SUMMARY:")
    for name, df in clean.items():
        print(f"  {name:<12} → {df.shape[0]:>7,} rows × {df.shape[1]:>2} columns")

  
    
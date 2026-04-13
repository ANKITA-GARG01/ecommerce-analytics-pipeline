 # scripts/fcr_transform.py
# ============================================================
# FCR / AML FEATURE ENGINEERING
# Builds risk indicators from existing clean data
# ============================================================
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

def get_engine():
    server   = os.getenv('DB_SERVER', r'localhost\SQLEXPRESS')
    database = os.getenv('DB_NAME', 'ecommerce_db')
    driver   = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    conn_str = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
        f"&trusted_connection=yes"
    )
    return create_engine(conn_str, fast_executemany=True)


# ============================================================
# FEATURE 1 — Transaction Velocity
# Flag customers placing too many orders too fast
# AML Signal: rapid repeated transactions = structuring or fraud
# ============================================================
def build_velocity_features(engine):
    print("\n🔄 Building VELOCITY features...")

    query = """
        SELECT
            o.customer_id,
            o.order_id,
            o.order_purchase_timestamp,
            p.payment_value,
            p.payment_type
        FROM fact_orders o
        JOIN order_payments p ON o.order_id = p.order_id
    """
    df = pd.read_sql(query, engine)
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df = df.sort_values(['customer_id', 'order_purchase_timestamp'])

    # Orders per customer
    customer_order_counts = (
        df.groupby('customer_id')['order_id']
        .nunique()
        .reset_index()
        .rename(columns={'order_id': 'total_orders'})
    )

    # Time between orders (days)
    df['prev_order_time'] = df.groupby('customer_id')['order_purchase_timestamp'].shift(1)
    df['days_since_last_order'] = (
        df['order_purchase_timestamp'] - df['prev_order_time']
    ).dt.days

    # Average days between orders per customer
    avg_gap = (
        df.groupby('customer_id')['days_since_last_order']
        .mean()
        .reset_index()
        .rename(columns={'days_since_last_order': 'avg_days_between_orders'})
    )

    # Total spend per customer
    total_spend = (
        df.groupby('customer_id')['payment_value']
        .sum()
        .reset_index()
        .rename(columns={'payment_value': 'total_spend'})
    )

    # Merge all
    velocity = customer_order_counts.merge(avg_gap, on='customer_id', how='left')
    velocity = velocity.merge(total_spend, on='customer_id', how='left')

    # ── RISK FLAGS ────────────────────────────────────────────
    # High velocity: multiple orders with very short gaps
    velocity['high_velocity_flag'] = np.where(
        (velocity['total_orders'] >= 3) &
        (velocity['avg_days_between_orders'] < 2),
        1, 0
    )

    # High spender flag (top 5% by total spend)
    spend_threshold = velocity['total_spend'].quantile(0.95)
    velocity['high_value_flag'] = np.where(
        velocity['total_spend'] >= spend_threshold,
        1, 0
    )

    # Risk score (simple additive)
    velocity['velocity_risk_score'] = (
        velocity['high_velocity_flag'] * 40 +
        velocity['high_value_flag'] * 30
    )

    print(f"  ✅ Velocity features built: {len(velocity):,} customers")
    print(f"  🚨 High velocity flagged: {velocity['high_velocity_flag'].sum():,}")
    print(f"  💰 High value flagged:    {velocity['high_value_flag'].sum():,}")
    return velocity


# ============================================================
# FEATURE 2 — Structuring Detection
# Structuring = breaking large amounts into small transactions
# AML Signal: many transactions just below round number thresholds
# ============================================================
def build_structuring_features(engine):
    print("\n🔄 Building STRUCTURING features...")

    query = """
        SELECT
            o.customer_id,
            p.order_id,
            p.payment_value,
            p.payment_type,
            p.payment_installments,
            o.order_purchase_timestamp
        FROM order_payments p
        JOIN fact_orders o ON p.order_id = o.order_id
    """
    df = pd.read_sql(query, engine)

    # ── Red Flag 1: Round number transactions ─────────────────
    # Criminals often use exact round numbers
    df['is_round_number'] = (df['payment_value'] % 100 == 0).astype(int)

    # ── Red Flag 2: Just-below-threshold transactions ─────────
    # Classic structuring: keeping amounts just under reporting thresholds
    # e.g., R$990, R$1990 instead of R$1000, R$2000
    df['is_just_below_threshold'] = np.where(
        (df['payment_value'] % 1000).between(900, 999),
        1, 0
    )

    # ── Red Flag 3: Abnormal installment count ─────────────────
    # Very high installments on small amounts = unusual behavior
    df['installment_per_value'] = df['payment_installments'] / (df['payment_value'] + 1)
    df['unusual_installments'] = np.where(
        df['installment_per_value'] > 0.05,
        1, 0
    )

    # Aggregate per customer
    structuring = df.groupby('customer_id').agg(
        total_transactions   = ('order_id', 'count'),
        total_spend          = ('payment_value', 'sum'),
        avg_transaction_value= ('payment_value', 'mean'),
        round_number_txns    = ('is_round_number', 'sum'),
        below_threshold_txns = ('is_just_below_threshold', 'sum'),
        unusual_installment_txns = ('unusual_installments', 'sum'),
    ).reset_index()

    # Structuring risk score
    structuring['structuring_risk_score'] = (
        structuring['round_number_txns']        * 15 +
        structuring['below_threshold_txns']     * 35 +
        structuring['unusual_installment_txns'] * 20
    )

    # Flag high-risk structuring customers
    structuring['structuring_flag'] = np.where(
        structuring['structuring_risk_score'] >= 35,
        1, 0
    )

    print(f"  ✅ Structuring features built: {len(structuring):,} customers")
    print(f"  🚨 Structuring flagged: {structuring['structuring_flag'].sum():,}")
    return structuring


# ============================================================
# FEATURE 3 — Behavioral Anomaly Detection
# Unusual patterns that deviate from normal customer behavior
# AML Signal: sudden behavior changes, odd timing, geography mismatches
# ============================================================
def build_behavioral_features(engine):
    print("\n🔄 Building BEHAVIORAL anomaly features...")

    query = """
        SELECT
            o.customer_id,
            o.order_id,
            o.order_purchase_timestamp,
            o.order_day_of_week,
            o.order_year,
            o.order_month,
            c.customer_state,
            p.payment_value,
            p.payment_type,
            r.review_score,
            o.is_late_delivery
        FROM fact_orders o
        JOIN dim_customers c    ON o.customer_id  = c.customer_id
        JOIN order_payments p   ON o.order_id     = p.order_id
        LEFT JOIN order_reviews r ON o.order_id   = r.order_id
    """
    df = pd.read_sql(query, engine)
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

    # Hour of day
    df['order_hour'] = df['order_purchase_timestamp'].dt.hour

    # ── Red Flag 1: Late night orders (2am-5am) ──────────────
    # Unusual hours can signal automated/fraudulent activity
    df['late_night_order'] = np.where(
        df['order_hour'].between(2, 5),
        1, 0
    )

    # ── Red Flag 2: Weekend high-value orders ─────────────────
    df['is_weekend'] = np.where(
        df['order_day_of_week'].isin(['Saturday', 'Sunday']),
        1, 0
    )
    value_threshold = df['payment_value'].quantile(0.90)
    df['weekend_high_value'] = np.where(
        (df['is_weekend'] == 1) & (df['payment_value'] >= value_threshold),
        1, 0
    )

    # ── Red Flag 3: 1-star review after high-value order ──────
    # Could indicate product never arrived (fraud for return/refund)
    df['suspicious_review'] = np.where(
        (df['review_score'] == 1) & (df['payment_value'] >= value_threshold),
        1, 0
    )

    # Aggregate per customer
    behavioral = df.groupby('customer_id').agg(
        total_orders          = ('order_id', 'nunique'),
        late_night_orders     = ('late_night_order', 'sum'),
        weekend_high_value    = ('weekend_high_value', 'sum'),
        suspicious_reviews    = ('suspicious_review', 'sum'),
        avg_payment_value     = ('payment_value', 'mean'),
        dominant_state        = ('customer_state', lambda x: x.mode()[0])
    ).reset_index()

    # Behavioral risk score
    behavioral['behavioral_risk_score'] = (
        behavioral['late_night_orders']   * 20 +
        behavioral['weekend_high_value']  * 25 +
        behavioral['suspicious_reviews']  * 35
    )

    behavioral['behavioral_flag'] = np.where(
        behavioral['behavioral_risk_score'] >= 35,
        1, 0
    )

    print(f"  ✅ Behavioral features built: {len(behavioral):,} customers")
    print(f"  🚨 Behavioral anomalies: {behavioral['behavioral_flag'].sum():,}")
    return behavioral


# ============================================================
# FEATURE 4 — Combine into Master FCR Risk Table
# ============================================================
def build_fcr_master(velocity, structuring, behavioral):
    print("\n🔄 Building FCR MASTER risk table...")

    # Merge all three feature sets
    fcr = velocity[['customer_id', 'total_orders', 'total_spend',
                    'high_velocity_flag', 'high_value_flag',
                    'velocity_risk_score']].merge(
        structuring[['customer_id', 'structuring_flag',
                     'structuring_risk_score', 'round_number_txns',
                     'below_threshold_txns']],
        on='customer_id', how='left'
    ).merge(
        behavioral[['customer_id', 'behavioral_flag',
                    'behavioral_risk_score', 'late_night_orders',
                    'suspicious_reviews', 'dominant_state']],
        on='customer_id', how='left'
    )

    # Fill nulls from left joins
    fcr = fcr.fillna(0)

    # ── COMPOSITE RISK SCORE (0–100) ──────────────────────────
    fcr['composite_risk_score'] = (
        fcr['velocity_risk_score']    * 0.35 +
        fcr['structuring_risk_score'] * 0.40 +
        fcr['behavioral_risk_score']  * 0.25
    ).clip(0, 100).round(2)

    # ── RISK TIER ─────────────────────────────────────────────
    def assign_tier(score):
        if score >= 70:   return 'CRITICAL'
        elif score >= 45: return 'HIGH'
        elif score >= 20: return 'MEDIUM'
        else:             return 'LOW'

    fcr['risk_tier'] = fcr['composite_risk_score'].apply(assign_tier)

    # ── ALERT FLAG ────────────────────────────────────────────
    # Triggered when 2+ individual flags fire simultaneously
    fcr['total_flags_fired'] = (
        fcr['high_velocity_flag'].astype(int) +
        fcr['high_value_flag'].astype(int) +
        fcr['structuring_flag'].astype(int) +
        fcr['behavioral_flag'].astype(int)
    )
    fcr['aml_alert'] = np.where(fcr['total_flags_fired'] >= 2, 1, 0)

    print(f"  ✅ FCR Master table: {len(fcr):,} customers scored")
    print(f"\n  📊 RISK TIER DISTRIBUTION:")
    print(fcr['risk_tier'].value_counts().to_string())
    print(f"\n  🚨 AML ALERTS TRIGGERED: {fcr['aml_alert'].sum():,}")
    return fcr

def truncate_all_tables(engine):
    """
    Clears all tables before loading fresh data.
    ORDER MATTERS — truncate facts before dimensions
    (reverse of load order) because of FK constraints.
    """
    print("\n🧹 Clearing existing data...")

    tables_in_order = [
        'fcr_master_risk_table',
        'fcr_velocity_features',
        'fcr_structuring_features',
        'fcr_behavioral_features',
        # Facts first (they reference dimensions)
      
    ]

    with engine.begin() as conn:
        for table in tables_in_order:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
                print(f"  🗑️  Cleared {table}")
            except Exception as e:
                print(f"  ⚠️  Could not clear {table}: {e}")

    print("  ✅ All fcr tables cleared\n")

# ============================================================
# LOAD FCR TABLES INTO SQL SERVER
# ============================================================
def load_fcr_tables(velocity, structuring, behavioral, fcr_master, engine):
    
    print("\n📤 Loading FCR tables into SQL Server...")

    tables = {
        'fcr_velocity_features'    : velocity,
        'fcr_structuring_features' : structuring,
        'fcr_behavioral_features'  : behavioral,
        'fcr_master_risk_table'    : fcr_master,
    }

    for table_name, df in tables.items():
        df.to_sql(
            name      = table_name,
            con       = engine,
            if_exists = 'replace',
            index     = False,
            chunksize = 1000
        )
        print(f"  ✅ {table_name:<35} → {len(df):,} rows")



def run_fcr_transform():
    engine = get_engine()
    truncate_all_tables(engine)
    velocity    = build_velocity_features(engine)
    structuring = build_structuring_features(engine)
    behavioral  = build_behavioral_features(engine)
    fcr_master  = build_fcr_master(velocity, structuring, behavioral)

    load_fcr_tables(velocity, structuring, behavioral, fcr_master, engine)

    print("\n🎉 FCR/AML Pipeline Complete!")

# ============================================================
# RUN EVERYTHING
# ============================================================
if __name__ == "__main__":
    run_fcr_transform()



    
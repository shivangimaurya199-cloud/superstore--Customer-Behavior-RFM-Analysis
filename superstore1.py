# =============================================================================
# STEP 1: PYTHON → MYSQL CONNECTION
# =============================================================================

import pandas as pd
from sqlalchemy import create_engine
import datetime as dt

# Create MySQL connection
engine = create_engine(
    "mysql+pymysql://root:Rcmk%401234@localhost:3306/dakshitadb"
)

# SQL query to fetch required data
query = """
SELECT
    f.order_id,
    f.order_date,
    f.customer_id,
    f.product_id,
    f.sales_amount,
    f.quantity
FROM fact_sales f;
"""

# Load data into Pandas DataFrame
df = pd.read_sql(query, engine)

# Convert order_date to datetime (IMPORTANT)
df['order_date'] = pd.to_datetime(df['order_date'])

print("Data Loaded Successfully")
print(df.head())

# =============================================================================
# STEP 2: RFM CALCULATION (CORE BUSINESS LOGIC)
# =============================================================================

# Snapshot date = one day after last order date
snapshot_date = df['order_date'].max() + dt.timedelta(days=1)

# Calculate RFM metrics
rfm = df.groupby('customer_id').agg({
    'order_date': lambda x: (snapshot_date - x.max()).days,  # Recency
    'order_id': 'nunique',                                   # Frequency
    'sales_amount': 'sum'                                    # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'Recency', 'Frequency', 'Monetary']

print("\nRFM Table:")
print(rfm.head())

# =============================================================================
# STEP 3: RFM SCORING (QUANTILE-BASED 1–5 SCALE)
# =============================================================================

rfm['R_score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
rfm['F_score'] = pd.qcut(
    rfm['Frequency'].rank(method='first'),
    5,
    labels=[1, 2, 3, 4, 5]
)
rfm['M_score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

# Total RFM Score
rfm['RFM_Score'] = (
    rfm['R_score'].astype(int) +
    rfm['F_score'].astype(int) +
    rfm['M_score'].astype(int)
)

# =============================================================================
# STEP 4: CUSTOMER SEGMENT ASSIGNMENT
# =============================================================================

def segment_customer(row):
    if row['RFM_Score'] >= 13:
        return 'Champion'
    elif row['RFM_Score'] >= 10:
        return 'Loyal'
    elif row['RFM_Score'] >= 7:
        return 'Potential'
    else:
        return 'Hibernating'

rfm['Segment'] = rfm.apply(segment_customer, axis=1)

print("\nCustomer Segments:")
print(rfm.head())

# =============================================================================
# STEP 5: VALIDATION CHECK (BUSINESS CONFIRMATION)
# =============================================================================

# Champions should have highest average spend
validation = (
    rfm.groupby('Segment')['Monetary']
    .mean()
    .sort_values(ascending=False)
)

print("\nValidation Check – Avg Monetary by Segment:")
print(validation)

# =============================================================================
# STEP 6: MARKET BASKET ANALYSIS (APRIORI)
# =============================================================================

from mlxtend.frequent_patterns import apriori, association_rules

# Create basket: Orders × Products
basket = (
    df
    .groupby(['order_id', 'product_id'])['quantity']
    .sum()
    .unstack()
    .fillna(0)
)

# Convert quantities to binary (0/1)
basket = basket.map(lambda x: 1 if x > 0 else 0)

print("\nBasket Shape:", basket.shape)

# Generate frequent itemsets
frequent_items = apriori(
    basket,
    min_support=0.01,  # Lower support to avoid empty result
    use_colnames=True
)

print("\nFrequent Itemsets:")
print(frequent_items.head())

# Generate association rules
rules = association_rules(
    frequent_items,
    metric="lift",
    min_threshold=1
)

print("\nAssociation Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head())

# =============================================================================
# END OF SCRIPT
# =============================================================================

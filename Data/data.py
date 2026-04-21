import pandas as pd
from sqlalchemy import create_engine

# Database Connection
engine = create_engine('postgresql://postgres:130111@localhost:5432/sar_db')

# Load your CSV data
df = pd.read_csv('Data/data.csv')

# --- 1. POPULATE CUSTOMERS ---
# We extract unique 'Case_ID' + 'Customer_Occupation' + 'Risk_Score' 
# In this project, Case_ID acts as our link to the specific subject
customers_df = df[['Case_ID', 'Customer_Occupation', 'Risk_Score']].drop_duplicates()
customers_df.rename(columns={
    'Customer_Occupation': 'occupation',
    'Risk_Score': 'risk_score',
    'Case_ID': 'case_id'
}, inplace=True)
customers_df.to_sql('customers', engine, if_exists='append', index=False)

# --- 2. POPULATE TRANSACTIONS ---
df[['Transaction_ID', 'Case_ID', 'Transaction_Date', 'Amount_INR', 
    'Transaction_Type', 'Counterparty', 'Anomaly_Flag']].to_sql('transactions', engine, if_exists='append', index=False)

# --- 3. POPULATE ALERTS ---
alerts_df = df[['Case_ID', 'Primary_Typology']].drop_duplicates()
alerts_df.rename(columns={'Primary_Typology': 'primary_typology'}, inplace=True)
alerts_df.to_sql('alerts', engine, if_exists='append', index=False)

# --- 4. INITIALIZE AUDIT LOGS ---
# We don't import data here yet. Instead, we create a 'System Initialization' log
# to prove the audit trail is active.
audit_init = pd.DataFrame({
    'case_id': ['SYSTEM'],
    'step_name': ['Data Import'],
    'reasoning_trace': ['Successfully imported 85+ scenarios into the relational database.'],
    'performed_by': ['System_Admin']
})
audit_init.to_sql('audit_logs', engine, if_exists='append', index=False)

print("🚀 Full Data Migration Complete: Customers, Transactions, Alerts, and Audit Init.")
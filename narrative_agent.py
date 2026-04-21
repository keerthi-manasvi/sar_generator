from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:130111@localhost:5432/sar_db')

def fetch_case_data(case_id):
    query = f"""
    SELECT 
        t."Transaction_ID", t."Amount_INR", t."Transaction_Date", 
        t."Transaction_Type", t."Counterparty",
        a."Primary_Typology"
    FROM transactions t
    JOIN alerts a ON t."Case_ID" = a."Case_ID"
    WHERE t."Case_ID" = '{case_id}';
    """
    
    df = pd.read_sql(query, engine)
    return df

# Test it
try:
    data = fetch_case_data('CASE-0064')
    print("✅ Data Fetched Successfully:")
    print(data)
except Exception as e:
    print(f"❌ Error: {e}")

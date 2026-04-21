-- 1. Customers Table (Derived from your Occupation and Risk Score data)
CREATE TABLE Customers (
    Customer_ID SERIAL PRIMARY KEY,
    Occupation VARCHAR(100),
    Risk_Score INT,
    Expected_Activity_Level VARCHAR(50) DEFAULT 'Normal'
);

-- 2. Transactions Table (Directly from your CSV)
CREATE TABLE Transactions (
    Transaction_ID VARCHAR(20) PRIMARY KEY,
    Case_ID VARCHAR(20),
    Transaction_Date DATE,
    Amount_INR DECIMAL(15, 2),
    Transaction_Type VARCHAR(50),
    Counterparty VARCHAR(100),
    Anomaly_Flag VARCHAR(5) -- 'YES' or 'NO'
);

-- 3. Alerts Table (To trigger the SAR process)
CREATE TABLE Alerts (
    Alert_ID SERIAL PRIMARY KEY,
    Case_ID VARCHAR(20),
    Primary_Typology VARCHAR(100),
    Detection_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Status VARCHAR(20) DEFAULT 'Pending' -- Pending, Under Review, Filed
);

-- 4. Audit_Logs Table (The "Reasoning Trace")
CREATE TABLE Audit_Logs (
    Audit_ID SERIAL PRIMARY KEY,
    Case_ID VARCHAR(20),
    Step_Name VARCHAR(100), -- e.g., 'Data Retrieval', 'LLM Draft', 'Analyst Edit'
    Reasoning_Trace TEXT,   -- Where the LLM explains "Why it wrote what it wrote"
    Performed_By VARCHAR(50),
    Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

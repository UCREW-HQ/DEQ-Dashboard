import os
import json
import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
SHEET_NAME = "BERP AR Tracking"

# --- 1. AUTHENTICATE ---
raw_creds = os.environ.get("GCP_SERVICE_ACCOUNT")
if not raw_creds:
    print("Error: GCP_SERVICE_ACCOUNT secret is missing.")
    exit(1)

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(raw_creds)
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# --- 2. LOAD DATA ---
try:
    print(f"Opening Google Sheet: '{SHEET_NAME}'...")
    sheet = client.open(SHEET_NAME).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df.columns = df.columns.astype(str).str.strip()
    print("Data loaded successfully.")
except Exception as e:
    print(f"Error loading sheet: {e}")
    exit(1)

# --- 3. SANITIZE ---
if 'Company' in df.columns:
    df = df.drop(columns=['Company'])

# --- 4. CLEAN DATA ---
# We clean these columns to ensure they are numbers, not text
target_cols = [
    'Gas Savings (MMBtu/yr)', 
    'Electric Savings (kWh/yr)', 
    'Total Cost Savings', 
    'Implementation Costs', 
    'Electricity Equivalent CO2 Savings - LOW (lb/year)',
    'Electricity Equivalent CO2 Savings - HIGH (lb/year)',
    'Electricity NOx Savings LOW (lb/yr)',
    'Electricity NOx Savings HIGH (lb/yr)',
    'Electricity SO2 Savings LOW',
    'Electricity SO2 Savings HIGH (lb/yr)',
    'Electricity PM2.5 Savings LOW (lb/yr)',
    'Electricity PM2.5 Savings HIGH (lb/yr)'
]

for col in target_cols:
    if col not in df.columns: df[col] = 0
    # Force conversion to number (handle "$1,000", "TBD", empty strings)
    df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,%]', '', regex=True), errors='coerce').fillna(0)

# --- 5. CALCULATIONS (The Hybrid Approach) ---

# CONSTANTS (Standard EPA Factors)
GAS_CO2_FACTOR = 117.0   # lb/MMBtu
GAS_NOX_FACTOR = 0.092   # lb/MMBtu
GAS_SO2_FACTOR = 0.0006  # lb/MMBtu
GAS_PM25_FACTOR = 0.007  # lb/MMBtu

# A. GAS CALCULATIONS (Calculated from MMBtu)
# We calculate this here to avoid errors if the sheet column is blank
df['Gas_CO2_lb'] = df['Gas Savings (MMBtu/yr)'] * GAS_CO2_FACTOR
df['Gas_NOx_lb'] = df['Gas Savings (MMBtu/yr)'] * GAS_NOX_FACTOR
df['Gas_SO2_lb'] = df['Gas Savings (MMBtu/yr)'] * GAS_SO2_FACTOR
df['Gas_PM25_lb'] = df['Gas Savings (MMBtu/yr)'] * GAS_PM25_FACTOR

# B. ELECTRIC CALCULATIONS (Read & Average)
# 1. CO2
df['Elec_CO2_Avg'] = (df['Electricity Equivalent CO2 Savings - LOW (lb/year)'] + 
                      df['Electricity Equivalent CO2 Savings - HIGH (lb/year)']) / 2

# FALLBACK CHECK: If the sheet had 0 for CO2 but DOES have kWh savings, calculate it manually
# (Using a conservative ~1.5 lb/kWh factor for Utah grid)
mask_missing_co2 = (df['Elec_CO2_Avg'] == 0) & (df['Electric Savings (kWh/yr)'] > 0)
if mask_missing_co2.any():
    print(f"Note: Calculated fallback CO2 for {mask_missing_co2.sum()} rows.")
    df.loc[mask_missing_co2, 'Elec_CO2_Avg'] = df.loc[mask_missing_co2, 'Electric Savings (kWh/yr)'] * 1.5

# 2. Other Pollutants (Averaging)
df['Elec_NOx_Avg'] = (df['Electricity NOx Savings LOW (lb/yr)'] + df['Electricity NOx Savings HIGH (lb/yr)']) / 2
df['Elec_SO2_Avg'] = (df['Electricity SO2 Savings LOW'] + df['Electricity SO2 Savings HIGH (lb/yr)']) / 2
df['Elec_PM25_Avg'] = (df['Electricity PM2.5 Savings LOW (lb/yr)'] + df['Electricity PM2.5 Savings HIGH (lb/yr)']) / 2

# C. TOTALS
df['Total_CO2_Tons'] = (df['Gas_CO2_lb'] + df['Elec_CO2_Avg']) / 2000.0
df['Total_NOx_lb'] = df['Gas_NOx_lb'] + df['Elec_NOx_Avg']
df['Total_SO2_lb'] = df['Gas_SO2_lb'] + df['Elec_SO2_Avg']
df['Total_PM25_lb'] = df['Gas_PM25_lb'] + df['Elec_PM25_Avg']

# Equivalency (Cars)
df['Cars_Equivalent'] = df['Total_CO2_Tons'] / 5.07

# --- 6. METADATA (Year & FIPS) ---
# Year
if 'Date of Assessment' in df.columns:
    date_col = df['Date of Assessment'].astype(str)
    df['Date_Obj'] = pd.to_datetime(date_col, errors='coerce')
    df['Year'] = df['Date_Obj'].dt.year.fillna(0).astype(int)
    # Fallback regex
    mask_zero = (df['Year'] == 0)
    if mask_zero.any():
        df.loc[mask_zero, 'Year'] = pd.to_numeric(date_col[mask_zero].str.extract(r'(\d{4})')[0], errors='coerce').fillna(0).astype(int)
    if 'Date_Obj' in df.columns: df = df.drop(columns=['Date_Obj'])
else:
    df['Year'] = 0

# FIPS
if 'FIPS' in df.columns:
    df['FIPS'] = pd.to_numeric(df['FIPS'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(5)

# --- 7. EXPORT ---
json_output = df.to_json(orient='records')
with open('site_data.json', 'w') as f:
    f.write(json_output)
    print("Success: site_data.json saved.")

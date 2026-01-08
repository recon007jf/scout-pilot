import pandas as pd
import numpy as np
import glob
import os
import re
import string
import json

# ==========================================
# 1. CONFIGURATION
# ==========================================
OUTPUT_SYSTEM = 'artifacts/Master_Hunting_List_Production_v3_SYSTEM.csv'
OUTPUT_ANDREW = 'artifacts/Master_Hunting_List_Production_v3_ANDREW.csv'
LIVES_THRESHOLD = 150

# Map for BF_RAW_Leads.csv
COL_MAP = {
    'FIRM': 'PROVIDER_NAME_NORM',
    'FIRM_RAW': 'Company Name',
    'CONTACT': 'Contact Full Name',
    'EMAIL': 'Contact Email',
    'TITLE': 'Contact Job Title',
    'CLIENT': 'SPONSOR_NAME',
    'LIVES': 'LIVES',
    'STATE': 'PROVIDER_STATE'
}

# ==========================================
# 2. LOAD DATA (Hardcoded for Canonical)
# ==========================================
print("STEP 1: Loading Golden Source...")

# A. Leads
# Explicitly pointing to the confirmed location from Inventory Scan
LEAD_PATH = 'backend/data/Input/BF_RAW_Leads.csv'
if not os.path.exists(LEAD_PATH):
    # Fallback search if path is different
    candidates = glob.glob('**/BF_RAW_Leads.csv', recursive=True)
    if candidates: LEAD_PATH = candidates[0]

print(f"  -> Reading Leads from: {LEAD_PATH}")
df_leads = pd.read_csv(LEAD_PATH, low_memory=False)
print(f"  -> Leads Loaded: {len(df_leads)} rows")

# B. Cache
df_cache = pd.DataFrame()
if os.path.exists('broker_location_cache.csv'):
    df_cache = pd.read_csv('broker_location_cache.csv')
elif os.path.exists('artifacts/broker_location_cache.csv'):
    df_cache = pd.read_csv('artifacts/broker_location_cache.csv')
print(f"  -> Cache Loaded: {len(df_cache)} rows")

# C. DOL (Auto-Find Recursive)
dol_files = glob.glob('**/*5500*.csv', recursive=True)
sch_a_files = glob.glob('**/*SCH_A*.csv', recursive=True) + glob.glob('**/*sched_a*.csv', recursive=True)
# Filter out artifacts or outputs if possible, but keep it simple as per request
# Just verify we found something
if not sch_a_files:
    print("  ! WARNING: No Schedule A files found.")
else:
    print(f"  -> Found Schedule A: {sch_a_files[0]}")
    df_sch_a = pd.read_csv(sch_a_files[0], low_memory=False)

# ==========================================
# 3. TERRITORY JOIN
# ==========================================
print("STEP 2: Territory Resolution...")

# Normalize function
def norm_firm(x):
    if not isinstance(x, str): return ''
    x = x.lower().strip().translate(str.maketrans('', '', string.punctuation))
    return ' '.join([w for w in x.split() if w not in ['inc','llc','corp','group']])

# Check if columns exist
if COL_MAP['FIRM'] not in df_leads.columns:
    # Try alternate column name if mapped one missing
    print(f"Column {COL_MAP['FIRM']} missing. Available: {df_leads.columns.tolist()[:5]}")
    # Fallback for BF_RAW_Leads might have different headers? 
    # The user script implies these columns exist. 
    # Let's check 'Company Name'
    if 'Company Name' in df_leads.columns:
        COL_MAP['FIRM'] = 'Company Name' # Use raw as norm base if needed
        print(f"  -> Switched Norm Base to: {COL_MAP['FIRM']}")

df_leads['firm_norm'] = df_leads[COL_MAP['FIRM']].apply(norm_firm)

if not df_cache.empty:
    cache_firm_col = [c for c in df_cache.columns if 'firm' in c.lower() or 'target' in c.lower()][0]
    df_cache['firm_norm'] = df_cache[cache_firm_col].apply(norm_firm)
    
    # Filter Cache to In-Territory
    if 'firm_state_class' in df_cache.columns:
        cache_valid = df_cache[df_cache['firm_state_class'] == 'IN_TERRITORY'].copy()
        cache_valid = cache_valid.drop_duplicates(subset=['firm_norm'])
        
        # Join
        df_final = pd.merge(df_leads, cache_valid, on='firm_norm', how='left')
        
        # Filter Leads
        df_final = df_final[df_final['firm_state_class'] == 'IN_TERRITORY'].copy()
    else:
         print("  ! WARNING: Cache missing 'firm_state_class'. No filtering applied.")
         df_final = df_leads.copy()
else:
    df_final = df_leads.copy() # Fallback if no cache
    print("  ! WARNING: No Cache used.")

print(f"  -> Sales List Size: {len(df_final)}")

# ==========================================
# 4. FUNDING LOGIC (Broker Bridge)
# ==========================================
print("STEP 3: Funding Logic...")

# Build Broker Map from Sch A
broker_map = {}
if 'df_sch_a' in locals() and not df_sch_a.empty:
    # Find Broker Column
    br_cols = [c for c in df_sch_a.columns if 'AGENT' in c.upper() or 'BROKER' in c.upper()]
    if br_cols:
        br_col = br_cols[0]
        # Find Stop Loss Rows
        sl_mask = df_sch_a.astype(str).apply(lambda x: x.str.contains('STOP|SL', case=False)).any(axis=1)
        sl_brokers = df_sch_a.loc[sl_mask, br_col].dropna().unique()
        
        for b in sl_brokers:
            broker_map[norm_firm(b)] = ("Verified Self-Funded Player", "High", "DOL_SCH_A_SL")

def get_funding(row):
    # 1. Broker Check
    if row.get('firm_norm') in broker_map:
        return broker_map[row['firm_norm']]
    
    # 2. Lives Fallback
    try:
        lives = float(row.get(COL_MAP['LIVES'], 0))
    except: lives = 0
    
    if lives >= LIVES_THRESHOLD: return "Likely Self-Funded", "Low", "LIVES_FALLBACK"
    elif lives > 0: return "Likely Fully Insured", "Low", "LIVES_FALLBACK"
    return "Unknown", "None", "NO_DATA"

res = df_final.apply(get_funding, axis=1, result_type='expand')
df_final[['Funding_Status_Est', 'Funding_Confidence', 'Funding_Source']] = res

# ==========================================
# 5. EXPORT
# ==========================================
print("STEP 4: Exporting...")
if not os.path.exists('artifacts'): os.makedirs('artifacts')

# System View
df_final.to_csv(OUTPUT_SYSTEM, index=False)

# Andrew View
andrew = pd.DataFrame()
andrew['Broker_Firm'] = df_final[COL_MAP['FIRM_RAW']]
andrew['Contact_Name'] = df_final[COL_MAP['CONTACT']]
andrew['Title'] = df_final.get(COL_MAP['TITLE'], "")
andrew['Email'] = df_final[COL_MAP['EMAIL']]
andrew['LinkedIn'] = "" # Placeholder for Enrichment
andrew['Client_Firm'] = df_final[COL_MAP['CLIENT']]
andrew['Lives'] = df_final[COL_MAP['LIVES']]
andrew['Funding_Status'] = df_final['Funding_Status_Est']
andrew['Funding_Confidence'] = df_final['Funding_Confidence']
andrew['Data_Source'] = 'CANONICAL_BF_RAW'

if 'firm_state' in df_final.columns:
    andrew['Broker_State'] = df_final['firm_state']
else:
    andrew['Broker_State'] = df_final.get(COL_MAP['STATE'], "")

andrew.to_csv(OUTPUT_ANDREW, index=False)
print(f"\nSUCCESS: Generated {OUTPUT_ANDREW} with {len(andrew)} rows.")

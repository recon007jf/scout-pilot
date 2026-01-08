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
LIVES_THRESHOLD = 150
OUTPUT_SYSTEM = 'artifacts/Master_Hunting_List_Production_v3_SYSTEM.csv'
OUTPUT_ANDREW = 'artifacts/Master_Hunting_List_Production_v3_ANDREW.csv'

PATTERNS = {
    'FIRM': ['TARGET_FIRM', 'FIRM', 'BROKER', 'AGENCY', 'AGENT_FIRM_NAME'],
    'CONTACT': ['CONTACT', 'NAME', 'FULL_NAME'],
    'EMAIL': ['EMAIL'],
    'TITLE': ['TITLE', 'POSITION', 'JOB'],
    'LINKEDIN': ['LINKEDIN'],
    'EIN': ['EIN', 'TAX', 'FEIN'],
    'LIVES': ['LIVES', 'PARTIC', 'COUNT', 'EMPLOYEE', 'ENROLL'],
    'PLAN': ['PLAN_NAME', 'PLAN'],
    'CLIENT': ['CLIENT', 'SPONSOR', 'COMPANY', 'SPONSOR_NAME'],
    'ACK_ID': ['ACK_ID', 'FILING_ID'],
    'STATE': ['STATE', 'ST']
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def normalize_firm_name(text):
    if not isinstance(text, str): return ''
    text = text.lower().strip()
    text = text.translate(str.maketrans('', '', string.punctuation))
    suffixes = ['inc', 'llc', 'ltd', 'corp', 'company', 'group', 'agency', 'insurance', 'services']
    words = [w for w in text.split() if w not in suffixes]
    return ' '.join(words)

def normalize_ack(val):
    if pd.isna(val): return None
    s = str(val).strip()
    if s.endswith('.0'): s = s[:-2]
    return s

def find_col(df, pattern_list):
    if df is None or df.empty: return None
    cols = df.columns
    for pat in pattern_list:
        matches = [c for c in cols if pat in c.upper()]
        if matches: return matches[0]
    return None

# ==========================================
# 3. DATA DISCOVERY & LOADING
# ==========================================
print("STEP 1: Data Discovery...")

all_files = []
for root, dirs, files in os.walk('.'):
    for f in files:
        # STRICT EXCLUSION: Only skip known generated artifacts by name
        if 'Master_Hunting_List' in f: continue
        if 'Target_Hunting_List' in f: continue
        if 'SYSTEM.csv' in f or 'ANDREW.csv' in f: continue
        
        if f.endswith('.csv') or f.endswith('.json'):
            all_files.append(os.path.join(root, f))

# A. RAW LEADS
lead_candidates = [f for f in all_files if f.endswith('.csv') 
                   and 'cache' not in f.lower() 
                   and '5500' not in f and 'sch' not in f.lower()]

df_leads = pd.DataFrame()
for f in lead_candidates:
    try:
        # Strict Header Check: Must have FIRM + (CONTACT or EMAIL)
        header = pd.read_csv(f, nrows=0).columns.tolist()
        col_str = " ".join([c.upper() for c in header])
        
        has_firm = any(p in col_str for p in PATTERNS['FIRM'])
        has_contact = any(p in col_str for p in PATTERNS['CONTACT'])
        has_email = any(p in col_str for p in PATTERNS['EMAIL'])
        
        if has_firm and (has_contact or has_email):
            temp = pd.read_csv(f, low_memory=False)
            temp['Source_File'] = os.path.basename(f)
            df_leads = pd.concat([df_leads, temp], ignore_index=True)
            print(f"  -> Ingested Lead Source: {f} ({len(temp)} rows)")
    except: pass

if df_leads.empty:
    raise ValueError("CRITICAL: No valid Lead Files found (Requires Firm + Contact/Email).")

# B. CACHE (Robust Loading)
df_cache = pd.DataFrame()
json_cache = [f for f in all_files if 'territory_resolution.json' in f]
csv_cache = [f for f in all_files if 'broker_location_cache.csv' in f]

if json_cache:
    try:
        with open(json_cache[0], 'r') as jf:
            data = json.load(jf)
        # Handle Dict of Dicts (Key = Firm Name)
        if isinstance(data, dict):
            # Convert { "Firm A": { "state": "CA", ... } } -> DataFrame
            df_cache = pd.DataFrame.from_dict(data, orient='index')
            # Ensure the Key (Firm Name) is preserved as a column
            df_cache['target_firm_raw'] = df_cache.index
            print(f"  -> Cache Loaded (JSON Dict): {len(df_cache)} rows")
        elif isinstance(data, list):
            df_cache = pd.DataFrame(data)
            print(f"  -> Cache Loaded (JSON List): {len(df_cache)} rows")
    except Exception as e: print(f"  ! JSON Cache Error: {e}")
elif csv_cache:
    df_cache = pd.read_csv(csv_cache[0])
    print(f"  -> Cache Loaded (CSV): {len(df_cache)} rows")

# Validate Cache Schema
if not df_cache.empty:
    cache_firm_col = find_col(df_cache, PATTERNS['FIRM']) or 'target_firm_raw' # Fallback to our new key
    if cache_firm_col not in df_cache.columns:
        print("  ! WARNING: Cache missing Firm Name column. Territory filtering disabled.")
        df_cache = pd.DataFrame() 
    else:
        # Check for Territory Flag
        status_col = find_col(df_cache, ['FIRM_STATE_CLASS', 'STATE_CLASS'])
        if status_col:
            df_cache = df_cache[df_cache[status_col] == 'IN_TERRITORY'].copy()
            print(f"  -> Cache Validated & Filtered: {len(df_cache)} In-Territory Firms")
        else:
            print("  ! WARNING: Cache missing 'Firm_State_Class'. Filtering disabled.")
            df_cache = pd.DataFrame()

# C. DOL DATA (Correct Discovery)
# Looking for 'sch_a', 'schedule_a', OR 'sched_a'
def is_sch_a(fname):
    n = fname.lower()
    return ('sch_a' in n or 'schedule_a' in n or 'sched_a' in n) and n.endswith('.csv')

is_dummy = False
dol_5500_files = [f for f in all_files if '5500' in f and f.endswith('.csv')]
sch_a_files = [f for f in all_files if is_sch_a(f)]
sch_c_files = [f for f in all_files if ('sch_c' in f.lower() or 'schedule_c' in f.lower()) and f.endswith('.csv')]

if any('dummy' in f.lower() for f in dol_5500_files + sch_a_files + sch_c_files):
    is_dummy = True
    print("  -> NOTICE: Detected DUMMY/TEST DOL data.")

df_5500 = pd.read_csv(dol_5500_files[0], low_memory=False) if dol_5500_files else pd.DataFrame()
df_sch_a = pd.read_csv(sch_a_files[0], low_memory=False) if sch_a_files else pd.DataFrame()
df_sch_c = pd.read_csv(sch_c_files[0], low_memory=False) if sch_c_files else pd.DataFrame()

# ==========================================
# 4. TERRITORY JOIN (Lead Firm -> Cache)
# ==========================================
print("STEP 2: Linking Territory...")

lead_firm_col = find_col(df_leads, PATTERNS['FIRM'])
if not lead_firm_col: raise ValueError("CRITICAL: Leads missing Firm Name column.")

df_leads['firm_norm'] = df_leads[lead_firm_col].apply(normalize_firm_name)

if not df_cache.empty:
    cache_firm_col = find_col(df_cache, PATTERNS['FIRM']) or 'target_firm_raw'
    df_cache['firm_norm'] = df_cache[cache_firm_col].apply(normalize_firm_name)
    
    # Select columns to merge
    cache_cols = [c for c in df_cache.columns if 'state' in c.lower() or 'class' in c.lower()]
    valid_cache = df_cache[['firm_norm'] + cache_cols].drop_duplicates(subset=['firm_norm'])
    
    df_merged = pd.merge(df_leads, valid_cache, on='firm_norm', how='left')
    
    # Filter to STRICT In-Territory
    status_col = find_col(df_merged, ['FIRM_STATE_CLASS', 'STATE_CLASS'])
    if status_col:
        df_final = df_merged[df_merged[status_col] == 'IN_TERRITORY'].copy()
    else:
        # Fallback (Should be caught by cache validation, but just in case)
        df_final = df_merged.copy() 
else:
    print("  ! Skipping Territory Filter (Cache Invalid)")
    df_final = df_leads.copy()

print(f"  -> Sales List Size (Post-Filter): {len(df_final)}")

# ==========================================
# 5. FUNDING INTELLIGENCE (Broker Bridge)
# ==========================================
print("STEP 3: Applying Broker-Centric Funding...")

broker_funding_map = {}

if not df_sch_a.empty:
    # Look for AGENT_FIRM_NAME first (most specific)
    broker_col = find_col(df_sch_a, ['AGENT_FIRM_NAME']) 
    if not broker_col:
        broker_col = find_col(df_sch_a, ['BROKER', 'AGENT', 'FIRM'])
    
    if broker_col:
        # Identify Self-Funded ACKs
        type_col = find_col(df_sch_a, ['TYPE', 'CODE'])
        name_col = find_col(df_sch_a, ['NAME', 'CONTRACT'])
        
        mask_sl = pd.Series(False, index=df_sch_a.index)
        if type_col: mask_sl |= (df_sch_a[type_col].astype(str).str.upper() == 'SL')
        if name_col: mask_sl |= (df_sch_a[name_col].astype(str).str.contains('STOP', case=False, na=False))
        
        sl_brokers = df_sch_a[mask_sl][broker_col].dropna().unique()
        
        for raw_name in sl_brokers:
            norm_name = normalize_firm_name(raw_name)
            if norm_name:
                broker_funding_map[norm_name] = ("Verified Self-Funded Player", "High", "DOL_SCH_A_SL")

    print(f"  -> Identified {len(broker_funding_map)} Brokers with Self-Funded Plans.")

# Apply to Leads
lead_lives_col = find_col(df_final, PATTERNS['LIVES'])

def get_funding(row):
    # A. Broker Check (The Bridge)
    f_norm = row.get('firm_norm', '')
    if f_norm in broker_funding_map:
        return broker_funding_map[f_norm]
    
    # B. Lives Fallback
    try:
        lives = float(row.get(lead_lives_col, 0))
    except:
        lives = 0
    
    if lives >= LIVES_THRESHOLD:
        return "Likely Self-Funded", "Low", "LIVES_FALLBACK"
    elif lives > 0:
        return "Likely Fully Insured", "Low", "LIVES_FALLBACK"
        
    return "Unknown", "None", "NO_DATA"

results = df_final.apply(get_funding, axis=1, result_type='expand')
df_final[['Funding_Status_Est', 'Funding_Confidence', 'Funding_Source']] = results

# Provenance Flag
if is_dummy:
    df_final['Data_Provenance'] = 'TEST_DATA_DOL'
else:
    df_final['Data_Provenance'] = 'PRODUCTION_DOL'

# ==========================================
# 6. EXPORT
# ==========================================
print("STEP 4: Exporting...")
if not os.path.exists('artifacts'): os.makedirs('artifacts')

# System View
df_final.to_csv(OUTPUT_SYSTEM, index=False)
print(f"  -> Saved System View: {OUTPUT_SYSTEM}")

# Andrew View
andrew_map = {
    'Broker_Firm': find_col(df_final, PATTERNS['FIRM']),
    'Contact_Name': find_col(df_final, PATTERNS['CONTACT']),
    'Title': find_col(df_final, PATTERNS['TITLE']),
    'Email': find_col(df_final, PATTERNS['EMAIL']),
    'LinkedIn': find_col(df_final, PATTERNS['LINKEDIN']),
    'Client_Firm': find_col(df_final, PATTERNS['CLIENT']),
    'Lives': lead_lives_col,
    'Funding_Status': 'Funding_Status_Est',
    'Funding_Confidence': 'Funding_Confidence',
    'Data_Source': 'Data_Provenance'
}

andrew_df = pd.DataFrame()
for target, source in andrew_map.items():
    if source and source in df_final.columns:
        andrew_df[target] = df_final[source]
    else:
        andrew_df[target] = ""

state_col = find_col(df_final, ['FIRM_STATE', 'BROKER_STATE'])
if state_col: andrew_df['Broker_State'] = df_final[state_col]

andrew_df.to_csv(OUTPUT_ANDREW, index=False)
print(f"  -> Saved Andrew View: {OUTPUT_ANDREW}")
print("Funding Mix:")
print(andrew_df['Funding_Status'].value_counts())

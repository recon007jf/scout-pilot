import pandas as pd
import glob
import os
import re
import string

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_DIR = '/Users/josephlf/.gemini/antigravity/scratch/backend/data/input'
ARTIFACTS_DIR = 'artifacts'

SYSTEM_INPUT = os.path.join(ARTIFACTS_DIR, 'Master_Hunting_List_Production_v3_SYSTEM_ENRICHED.csv')
SYSTEM_OUTPUT = os.path.join(ARTIFACTS_DIR, 'Master_Hunting_List_Production_v3_SYSTEM_ENRICHED_FUNDING_PATCHED.csv')
ANDREW_OUTPUT = os.path.join(ARTIFACTS_DIR, 'Master_Hunting_List_Production_v3_ANDREW_ENRICHED_FUNDING_PATCHED.csv')

# Generic Name Blacklist
GENERIC_BLACKLIST = {
    'group', 'inc', 'llc', 'company', 'corporation', 'benefits', 'insurance',
    'services', 'consulting', 'management', 'partners', 'associates', 'holdings',
    'enterprises', 'solutions', 'advisors', 'agency', 'fund', 'trust', 'plan'
}

print("=== FORENSIC FUNDING PATCH: FINAL ENGINEERING GRADE ===")

# ==========================================
# 2. ROBUST HELPERS
# ==========================================
def resolve_header(headers, strict_list, fuzz_list=None):
    """
    Clean header resolution. Returns the actual column name.
    """
    upper_map = {h.upper(): h for h in headers}
    
    # 1. Strict
    for s in strict_list:
        if s.upper() in upper_map:
            return upper_map[s.upper()]
            
    # 2. Fuzz
    if fuzz_list:
        for h_orig in headers:
            h_up = h_orig.upper()
            for f in fuzz_list:
                if f.upper() in h_up:
                    return h_orig
    return None

def clean_ein(val):
    """
    Parses EIN as string.
    PM Fix: Strict length enforcement. Do not truncate >9 digits.
    """
    if pd.isna(val) or str(val).strip() == '': return None
    s = str(val).split('.')[0] # Remove decimal
    s = re.sub(r'[^0-9]', '', s)
    
    if not s: return None
    
    # Pad if plausibly short (e.g. lost leading zero)
    if len(s) < 9: 
        s = s.zfill(9)
        
    # Strict Length Check
    if len(s) != 9: 
        return None
        
    return s

def clean_sponsor_name(val):
    """
    Normalize + Blacklist Generics.
    """
    if not isinstance(val, str): return None
    s = val.lower().strip()
    
    # Remove punctuation
    s = re.sub(r'[^a-z0-9\s]', '', s)
    
    # Remove suffixes
    s = re.sub(r'\s(inc|llc|ltd|corp|corporation|company|co|group|trust|fund)$', '', s)
    s = s.strip()
    
    # Safety Checks
    if len(s) < 3: return None
    if s in GENERIC_BLACKLIST: return None
    
    return s

def score_file(filename, targets, year='2023'):
    """Scores files for discovery ranking."""
    f = filename.lower()
    score = 0
    for t in targets:
        if t in f: score += 10
    if year in f: score += 5
    if 'latest' in f: score += 5
    if 'dummy' in f or 'test' in f: score -= 100
    return score

# ==========================================
# 3. DISCOVERY & LOAD
# ==========================================
print("STEP 1: Locating Data Assets...")

# Scan Input
all_files = []
# Fixed: Ensure we scan the absolute path correctly, or relative if fails
scan_dirs = [INPUT_DIR, 'backend/Scout_Data_Artifacts', 'backend/data/Input']

for d in scan_dirs:
    if os.path.exists(d):
        print(f"   -> Scanning: {d}")
        for root, dirs, files in os.walk(d):
            for f in files:
                if f.endswith('.csv'):
                    all_files.append(os.path.join(root, f))

# A. Locate Schedule A
sch_a_candidates = []
for f in all_files:
    # Look for SCH_A, SCHEDULE_A, SCHED_A
    if re.search(r'sch(ed)?(ule)?_?a', os.path.basename(f).lower()):
        score = score_file(os.path.basename(f), ['sch', 'a'], '2023')
        if score > 0: sch_a_candidates.append((score, f))

if not sch_a_candidates:
    # Fallback search in artifacts if input invalid
    alt_files = glob.glob('backend/Scout_Data_Artifacts/*.csv')
    for f in alt_files:
        if re.search(r'sch(ed)?(ule)?_?a', os.path.basename(f).lower()):
             sch_a_candidates.append((10, f))

if not sch_a_candidates:
    raise FileNotFoundError("CRITICAL: No Schedule A file found.")
    
sch_a_path = sorted(sch_a_candidates, key=lambda x: x[0], reverse=True)[0][1]
print(f"   -> Locked Schedule A: {os.path.basename(sch_a_path)}")

# B. Locate Spine (Explicit Override)
spine_candidates = []
# Try direct look first
known_spine = 'backend/Scout_Data_Artifacts/f_5500_2023_latest.csv'
if os.path.exists(known_spine):
    spine_candidates.append((100, known_spine))

if not spine_candidates:
    for f in all_files:
        name = os.path.basename(f).lower()
        if '5500' in name and 'sch' not in name:
            score = score_file(name, ['5500', 'f_5500'], '2023')
            if score > 0: spine_candidates.append((score, f))

spine_path = None
if spine_candidates:
    spine_path = sorted(spine_candidates, key=lambda x: x[0], reverse=True)[0][1]
    print(f"   -> Locked Spine: {os.path.basename(spine_path)}")
else:
    print("   -> WARNING: No 2023 5500 Spine found. EIN matching limited to Sch A data.")

# C. Load System
if not os.path.exists(SYSTEM_INPUT):
    if os.path.exists(os.path.basename(SYSTEM_INPUT)):
        SYSTEM_INPUT = os.path.basename(SYSTEM_INPUT)
    else:
        # Check relative
        if os.path.exists('artifacts/' + os.path.basename(SYSTEM_INPUT)):
             SYSTEM_INPUT = 'artifacts/' + os.path.basename(SYSTEM_INPUT)
        else:
            raise FileNotFoundError(f"CRITICAL: System file not found: {SYSTEM_INPUT}")

df_sys = pd.read_csv(SYSTEM_INPUT, low_memory=False)
print(f"   -> Loaded System: {len(df_sys)} rows")

# ==========================================
# 4. STOP-LOSS DETECTION
# ==========================================
print("\nSTEP 2: Detecting Stop-Loss Signal...")

# Load Sch A (Surgical)
header_a = pd.read_csv(sch_a_path, nrows=0).columns.tolist()

col_type = resolve_header(header_a, ['TYPE_INS_CONTRACT_CD', 'TYPE_CODE', 'CONTRACT_TYPE_CODE'])
col_name = resolve_header(header_a, ['INS_CONTRACT_NAME', 'CONTRACT_NAME', 'PLAN_NAME'])
col_ack = resolve_header(header_a, ['ACK_ID'])
col_ein_a = resolve_header(header_a, ['EIN', 'SPONSOR_EIN', 'SCH_A_EIN'])
col_sponsor_a = resolve_header(header_a, ['SPONSOR_NAME', 'EMPLOYER_NAME', 'INS_CARRIER_NAME'])
col_sl_ind = resolve_header(header_a, ['WLFR_BNFT_STOP_LOSS_IND'])
col_text = resolve_header(header_a, ['WLFR_TYPE_BNFT_OTH_TEXT', 'BENEFIT_TYPE_TEXT'])

# Only load columns that exist
use_cols = [c for c in [col_type, col_name, col_ack, col_ein_a, col_sponsor_a, col_sl_ind, col_text] if c]
if not use_cols:
    print("CRITICAL: Schedule A has none of the required columns.")
    exit()
    
df_a = pd.read_csv(sch_a_path, usecols=use_cols, low_memory=False)

mask_sl = pd.Series(False, index=df_a.index)

# 1. DOL Flag Match (The most explicit signal)
if col_sl_ind:
    # "1" = Stop Loss
    vals = df_a[col_sl_ind].astype(str).str.strip()
    mask_sl |= vals.isin(['1', '1.0', 'Y', 'YES', 'TRUE'])

# 2. Code Match
if col_type:
    vals = df_a[col_type].astype(str).str.upper().str.strip()
    mask_sl |= vals.isin(['SL', 'S/L', 'STOP LOSS', 'STOP-LOSS'])

# 3. Keyword Match (Name or Text)
keywords = ['STOP', 'EXCESS', 'SPECIFIC', 'AGGREGATE', 'REINSURANCE']
check_cols = [c for c in [col_name, col_text] if c]

for c in check_cols:
    vals = df_a[c].astype(str).str.upper()
    for k in keywords:
        mask_sl |= vals.str.contains(k, na=False)

df_sl = df_a[mask_sl].copy()
print(f"   -> Raw Schedule A Rows: {len(df_a)}")
print(f"   -> Verified Stop-Loss Rows: {len(df_sl)}")

if len(df_sl) == 0:
    print("❌ ABORT: No Stop-Loss signals found in Schedule A.")
    exit()

# Link to Spine (Entity Resolution)
if spine_path and col_ack:
    # Try reading header first
    try:
        header_s = pd.read_csv(spine_path, nrows=0).columns.tolist()
        col_ack_s = resolve_header(header_s, ['ACK_ID'])
        col_ein_s = resolve_header(header_s, ['EIN', 'SPONSOR_EIN'])
        col_sponsor_s = resolve_header(header_s, ['SPONSOR_NAME', 'PLAN_SPONSOR_NAME', 'SPONSOR_DFE_NAME'])
        
        print(f"   [Debug] Spine Columns: ACK='{col_ack_s}', Sponsor='{col_sponsor_s}'")

        if col_ack_s and (col_ein_s or col_sponsor_s):
            use_cols_s = [c for c in [col_ack_s, col_ein_s, col_sponsor_s] if c]
            df_spine = pd.read_csv(spine_path, usecols=use_cols_s, low_memory=False)
            
            df_sl = df_sl.merge(df_spine, left_on=col_ack, right_on=col_ack_s, how='left', suffixes=('', '_S'))
            
            # Coalesce
            if col_sponsor_s: 
                 df_sl['final_sponsor'] = df_sl[col_sponsor_s].fillna(df_sl.get(col_sponsor_a))
            else: 
                 df_sl['final_sponsor'] = df_sl.get(col_sponsor_a)
        else:
            df_sl['final_ein'] = df_sl.get(col_ein_a)
            df_sl['final_sponsor'] = df_sl.get(col_sponsor_a)
    except:
        print("  ! Error reading Spine file. Skipping enrichment.")
        df_sl['final_ein'] = df_sl.get(col_ein_a)
        df_sl['final_sponsor'] = df_sl.get(col_sponsor_a)
else:
    df_sl['final_ein'] = df_sl.get(col_ein_a)
    df_sl['final_sponsor'] = df_sl.get(col_sponsor_a)

# Evidence
df_sl['evidence_code'] = df_sl[col_type] if col_type else None
df_sl['evidence_name'] = df_sl[col_name] if col_name else None
df_sl['evidence_ack'] = df_sl[col_ack] if col_ack else None

# ==========================================
# 5. DETERMINISTIC JOIN
# ==========================================
print("\nSTEP 3: Executing Safe Join...")

# PM Fix: Explicit list cast for safe resolution
sys_cols = list(df_sys.columns)
col_sys_ein = resolve_header(sys_cols, ['EIN', 'TAX_ID', 'FEIN'])
col_sys_sponsor = resolve_header(sys_cols, ['SPONSOR_NAME', 'Primary_Client', 'Client_Firm'])

print(f"   [Debug] System Columns Resolved: Sponsor='{col_sys_sponsor}', EIN='{col_sys_ein}'")

matched_indices_ein = []
matched_indices_name = []

# Strategy 1: EIN (String Match)
if col_sys_ein:
    df_sl['clean_ein'] = df_sl['final_ein'].apply(clean_ein)
    df_sys['clean_ein'] = df_sys[col_sys_ein].apply(clean_ein)
    
    valid_sl_eins = df_sl.dropna(subset=['clean_ein'])
    # Dedupe: 1 EIN -> Best Row
    ein_map = valid_sl_eins.drop_duplicates('clean_ein').set_index('clean_ein').to_dict('index')
    
    matches_ein = df_sys[df_sys['clean_ein'].isin(ein_map.keys())].index
    matched_indices_ein = matches_ein.tolist()
    print(f"   -> EIN Matches: {len(matched_indices_ein)}")

# Strategy 2: Safe Name Match (Fallback)
if col_sys_sponsor:
    df_sl['norm_name'] = df_sl['final_sponsor'].apply(clean_sponsor_name)
    df_sys['norm_name'] = df_sys[col_sys_sponsor].apply(clean_sponsor_name)
    
    # Debug Norm Strings
    print(f"   [Debug] Sample SL Norm Names: {df_sl['norm_name'].dropna().head(5).tolist()}")
    print(f"   [Debug] Sample Sys Norm Names: {df_sys['norm_name'].dropna().head(5).tolist()}")
    
    # Ambiguity Kill-Switch
    sl_counts = df_sl.groupby('norm_name')['final_sponsor'].nunique()
    bad_sl = sl_counts[sl_counts > 1].index
    
    sys_counts = df_sys.groupby('norm_name')[col_sys_sponsor].nunique()
    bad_sys = sys_counts[sys_counts > 1].index
    
    valid_sl = df_sl[(~df_sl['norm_name'].isin(bad_sl)) & (df_sl['norm_name'].notna())]
    valid_sys = df_sys[(~df_sys['norm_name'].isin(bad_sys)) & (df_sys['norm_name'].notna())]
    
    name_map = valid_sl.drop_duplicates('norm_name').set_index('norm_name').to_dict('index')
    
    # Only match rows not already matched by EIN
    sys_unmatched = valid_sys[~valid_sys.index.isin(matched_indices_ein)]
    matches_name = sys_unmatched[sys_unmatched['norm_name'].isin(name_map.keys())].index
    matched_indices_name = matches_name.tolist()
    
    print(f"   -> Safe Name Matches: {len(matched_indices_name)}")

all_matches = list(set(matched_indices_ein) | set(matched_indices_name))
print(f"   -> TOTAL UNIQUE MATCHES: {len(all_matches)}")

if len(all_matches) == 0:
    print("❌ STOP: No matches found. Pipeline halts to prevent empty patch.")
    # We exit, but let's verify if we should produce empty output or not
    # User said "Pipeline halts", so we exit
    exit()

# ==========================================
# 6. APPLY PATCH
# ==========================================
print("\nSTEP 4: Applying Patch...")

# Init Evidence Cols
cols = ['StopLoss_Verified', 'StopLoss_Evidence_TypeCode', 'StopLoss_Evidence_ContractName', 'StopLoss_Evidence_ACK_ID', 'StopLoss_Evidence_MatchMethod']
for c in cols:
    if c not in df_sys.columns: df_sys[c] = None

for idx in all_matches:
    row = df_sys.loc[idx]
    match = None
    method = None
    
    # Look up match data
    if col_sys_ein and row.get('clean_ein') in ein_map:
        match = ein_map[row['clean_ein']]
        method = 'EIN'
    elif row.get('norm_name') in name_map:
        match = name_map[row['norm_name']]
        method = 'Safe_Name'
        
    if match:
        df_sys.at[idx, 'Funding_Status_Est'] = 'Verified Self-Funded Player'
        df_sys.at[idx, 'Funding_Confidence'] = 'High'
        df_sys.at[idx, 'Funding_Source'] = 'DOL_SCH_A_STOP_LOSS'
        
        df_sys.at[idx, 'StopLoss_Verified'] = True
        df_sys.at[idx, 'StopLoss_Evidence_TypeCode'] = match.get('evidence_code')
        df_sys.at[idx, 'StopLoss_Evidence_ContractName'] = match.get('evidence_name')
        df_sys.at[idx, 'StopLoss_Evidence_ACK_ID'] = match.get('evidence_ack')
        df_sys.at[idx, 'StopLoss_Evidence_MatchMethod'] = method

# Cleanup
df_sys.drop(columns=['clean_ein', 'norm_name'], inplace=True, errors='ignore')

# Save
df_sys.to_csv(SYSTEM_OUTPUT, index=False)
print(f"   -> Saved Patched System: {os.path.basename(SYSTEM_OUTPUT)}")

# Audit Sample
print("\n[Audit] Sample Patched Rows:")
sample_cols = [col_sys_sponsor, 'Funding_Status_Est', 'StopLoss_Evidence_MatchMethod']
try:
    print(df_sys.loc[all_matches[:5], sample_cols].to_string(index=False))
except:
    pass

# ==========================================
# 7. REGENERATE ANDREW
# ==========================================
print("\nSTEP 5: Regenerating Andrew View...")

df_andrew = pd.DataFrame()
def get_val(df, candidates):
    for c in candidates:
        if c in df.columns: return df[c]
    return ""

df_andrew['Broker_Firm'] = get_val(df_sys, ['firm_name_raw', 'Company Name', 'PROVIDER_NAME_NORM', 'Target_Firm'])
df_andrew['Contact_Name'] = get_val(df_sys, ['Contact Full Name', 'Broker_Name', 'Contact_Name'])
df_andrew['Title'] = get_val(df_sys, ['Contact Job Title', 'Job_Title', 'Title'])
df_andrew['Email'] = get_val(df_sys, ['Contact Email', 'Work_Email', 'Email'])
df_andrew['LinkedIn'] = get_val(df_sys, ['LinkedIn_URL', 'LinkedIn'])
df_andrew['Client_Firm'] = get_val(df_sys, ['SPONSOR_NAME', 'Primary_Client', 'Client_Firm'])
df_andrew['Lives'] = get_val(df_sys, ['LIVES', 'Lives'])

# Patched
df_andrew['Funding_Status'] = df_sys['Funding_Status_Est']
df_andrew['Funding_Confidence'] = df_sys['Funding_Confidence']

df_andrew['Broker_State'] = get_val(df_sys, ['firm_state', 'PROVIDER_STATE', 'Broker_State'])
df_andrew['Data_Source'] = get_val(df_sys, ['Data_Provenance', 'Data_Source'])
df_andrew['Funding_Source'] = df_sys['Funding_Source']

df_andrew.to_csv(ANDREW_OUTPUT, index=False)
print(f"   -> Saved Patched Andrew View: {os.path.basename(ANDREW_OUTPUT)}")

print("\n=== PATCH SUCCESSFUL ===")

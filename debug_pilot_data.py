import pandas as pd
import os

# CONFIG
F5500_PATH = "data/master_5500.csv"
CHUNK_SIZE = 5000
PILOT_MODE = True
MIN_LIVES = 100
MAX_LIVES = 100000

print(f"--- DIAGNOSTIC START ---")
print(f"Checking File: {F5500_PATH}")

if not os.path.exists(F5500_PATH):
    print(f"❌ File Not Found!")
    exit()

print(f"✅ File Exists. Size: {os.path.getsize(F5500_PATH)/1024/1024:.2f} MB")

# Peek Columns
df_head = pd.read_csv(F5500_PATH, nrows=5)
print(f"Columns Found: {list(df_head.columns)}")
df_head.columns = [c.upper() for c in df_head.columns]
cols = df_head.columns

# Logic Re-Run
counts = {
    'total_rows': 0,
    'passed_state': 0,
    'passed_lives': 0,
    'passed_funding': 0,
    'passed_final': 0
}

west_states = ['CA']
print(f"Targeting States: {west_states}")

chunk_iter = pd.read_csv(F5500_PATH, chunksize=CHUNK_SIZE, low_memory=False)

for i, chunk in enumerate(chunk_iter):
    chunk.columns = [c.upper() for c in chunk.columns]
    counts['total_rows'] += len(chunk)
    
    # 1. State Filter
    if 'SPONSOR_US_ADDRESS_STATE' not in chunk.columns:
        print("❌ CRITICAL: 'SPONSOR_US_ADDRESS_STATE' Missing!")
        break
        
    mask_state = chunk['SPONSOR_US_ADDRESS_STATE'].isin(west_states)
    counts['passed_state'] += mask_state.sum()
    
    # 2. Lives Filter
    part_col = None
    if 'TOT_PARTCP_BOY_CNT' in chunk.columns: part_col = 'TOT_PARTCP_BOY_CNT'
    elif 'TOT_PARTCP_EOY_CNT' in chunk.columns: part_col = 'TOT_PARTCP_EOY_CNT'
    
    if part_col:
        chunk[part_col] = pd.to_numeric(chunk[part_col], errors='coerce').fillna(0)
        if PILOT_MODE:
             # Pilot removes upper cap? logic check
             mask_size = (chunk[part_col] >= MIN_LIVES) 
        else:
             mask_size = (chunk[part_col] >= MIN_LIVES) & (chunk[part_col] <= MAX_LIVES)
    else:
        mask_size = pd.Series([False] * len(chunk))
    
    counts['passed_lives'] += (mask_state & mask_size).sum() # cumulative check

    # 3. Funding Filter
    target_col = 'FUNDING_ARRANGEMENT' if 'FUNDING_ARRANGEMENT' in chunk.columns else 'PLAN_FUNDING_ARRANGEMENT'
    if target_col:
        chunk['Funding_Code'] = chunk[target_col].astype(str)
        # Regex for 3 or 4
        mask_fund = chunk['Funding_Code'].str.contains(r'[34]', regex=True, na=False)
    else:
        mask_fund = pd.Series([False] * len(chunk))
        
    counts['passed_funding'] += (mask_state & mask_size & mask_fund).sum()
    counts['passed_final'] += (mask_state & mask_size & mask_fund).sum()
    
    if i > 5: break # Only check first 6 chunks (30k rows) for speed

print(f"--- RESULTS (First {counts['total_rows']} rows) ---")
print(f"Total Rows Checked: {counts['total_rows']}")
print(f"Passed State (CA): {counts['passed_state']}")
print(f"Passed Lives (>100): {counts['passed_lives']} (Cumulative)")
print(f"Passed Funding (Self): {counts['passed_funding']} (Cumulative)")
print(f"Final Leads Qualified: {counts['passed_final']}")

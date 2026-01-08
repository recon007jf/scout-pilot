import pandas as pd
import glob
import os

print("=== SCHEMA AUDIT: WIDE NET + SUBSTRING MATCHING ===")

# 1. Broad File Discovery
# Search multiple locations to ensure no source file is hidden
search_patterns = [
    '*.csv',             # Root
    'artifacts/*.csv',   # Standard output folder
    'data/*.csv',        # Potential data folder
    'backend/data/input/*.csv', # Explicit Input
    'backend/data/output/*.csv',
    '*/*.csv'            # Generic one-level deep
]

all_files = []
for p in search_patterns:
    all_files.extend(glob.glob(p))

# Deduplicate paths
all_files = sorted(list(set(all_files)))

# 2. Strict Exclusion Logic (Only exclude OUR generated outputs)
def is_generated_artifact(filepath):
    fname = os.path.basename(filepath)
    # Only ignore the final lists we are trying to build/rebuild
    if fname.startswith('Master_Hunting_List'): return True
    if fname.startswith('Target_Hunting_List'): return True
    return False

# 3. Execution
print(f"Found {len(all_files)} total CSV candidates.")

for filepath in all_files:
    if is_generated_artifact(filepath):
        continue
    
    try:
        # Read Header Only with encoding fallback
        try:
             df = pd.read_csv(filepath, nrows=0)
        except UnicodeDecodeError:
             df = pd.read_csv(filepath, nrows=0, encoding='latin1')
             
        cols = sorted(list(df.columns))
        col_upper = [str(c).upper() for c in cols]
        fname = os.path.basename(filepath)
        
        # Categorize (Heuristic for readability)
        tag = "UNKNOWN"
        if 'Benefit' in fname or 'Lead' in fname or 'Andrew' in fname: tag = "LEAD_SOURCE"
        elif '5500' in fname: tag = "DOL_5500"
        elif 'SCH_A' in fname or 'Schedule_A' in fname: tag = "DOL_SCH_A"
        elif 'SCH_C' in fname or 'Schedule_C' in fname: tag = "DOL_SCH_C"
        elif 'cache' in fname: tag = "CACHE"
        
        print(f"\n[{tag}] File: {filepath}")
        print(f"  -> Columns: {cols}")
        
        # Critical Key Detection (Substring based)
        # Checks if any keyword is part of any column name
        
        ein_keywords = ['EIN', 'TAX', 'FEIN', 'ACK_ID']
        firm_keywords = ['FIRM', 'BROKER', 'AGENCY', 'PRODUCER', 'TARGET', 'COMPANY']
        lives_keywords = ['LIVES', 'PARTIC', 'EMPLOY', 'ENROLL', 'COUNT', 'EE_']
        
        has_ein = any(k in c for c in col_upper for k in ein_keywords)
        has_firm = any(k in c for c in col_upper for k in firm_keywords)
        has_lives = any(k in c for c in col_upper for k in lives_keywords)
        
        print(f"  -> Signals: EIN={has_ein}, FIRM={has_firm}, LIVES={has_lives}")

    except Exception as e:
        print(f"  ! Error reading {filepath}: {e}")

print("\n=== AUDIT COMPLETE ===")

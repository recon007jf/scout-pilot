import pandas as pd
import glob
import os

print("=== SYSTEM INVENTORY & SCHEMA AUDIT (STRICT V2) ===")

def get_file_category(fname):
    fname_lower = fname.lower()
    
    # 1. ARTIFACTS (Outputs - Do not ingest as raw data)
    # Fixed: Removed generic 'output' exclusion to avoid hiding real files
    if 'master_hunting_list' in fname_lower: return 'ARTIFACT (OUTPUT)'
    if 'target_hunting_list' in fname_lower: return 'ARTIFACT (OUTPUT)'
    if 'system.csv' in fname_lower or 'andrew.csv' in fname_lower: return 'ARTIFACT (OUTPUT)'
    
    # 2. CACHE (Critical Input)
    if 'broker_location_cache' in fname_lower: return 'TERRITORY_CACHE'
    # Adding a check for our known cache name just in case they meant the JSON but treating as CSV? 
    # No, stick to user script. But maybe their 'broker_location_cache' is a file I haven't seen.
    
    # 3. DOL DATA (Funding Spine)
    if '5500' in fname_lower: return 'DOL_5500'
    if 'sch_a' in fname_lower or 'schedule_a' in fname_lower: return 'DOL_SCH_A'
    if 'sch_c' in fname_lower or 'schedule_c' in fname_lower: return 'DOL_SCH_C'
    
    # 4. RAW LEADS (Context)
    if 'benefit' in fname_lower: return 'RAW_LEADS_BF'
    if 'andrew' in fname_lower: return 'RAW_LEADS_ANDREW'
    if 'lead' in fname_lower: return 'RAW_LEADS_GENERIC'
    
    return 'UNKNOWN / OTHER'

# Recursive search for all CSVs
all_csvs = []
for root, dirs, files in os.walk('.'):
    for f in files:
        if f.endswith('.csv'):
            all_csvs.append(os.path.join(root, f))

# Sort for readability
all_csvs = sorted(all_csvs)
cache_found = False

print(f"Total CSVs Found: {len(all_csvs)}")

for fpath in all_csvs:
    fname = os.path.basename(fpath)
    category = get_file_category(fname)
    
    try:
        # Read Header Only
        # Handle potential encoding issues blindly
        try:
            df = pd.read_csv(fpath, nrows=0)
        except UnicodeDecodeError:
            df = pd.read_csv(fpath, nrows=0, encoding='latin1')
            
        cols = list(df.columns)
        
        print(f"\n[{category}] {fpath}")
        print(f"  Columns: {cols}")
        
        # Specific Validation for Cache
        if category == 'TERRITORY_CACHE':
            cols_l = [c.lower() for c in cols]
            
            # Strict Logic: Must have Firm State Class AND Firm State
            has_class = any(c == 'firm_state_class' or ('firm' in c and 'state' in c and 'class' in c) for c in cols_l)
            has_state = any(c == 'firm_state' or ('firm' in c and 'state' in c and 'class' not in c and 'evidence' not in c) for c in cols_l)
            
            if has_class and has_state:
                print("  ✅ CACHE VALIDATION: Passed (Contains critical territory fields)")
                cache_found = True
            else:
                print("  ❌ CACHE VALIDATION: Failed (Missing 'Firm_State' or 'Firm_State_Class')")

    except Exception as e:
        print(f"\n[{category}] {fpath} - Read Error: {e}")

print("\n=== SUMMARY ===")
if cache_found:
    print("Territory Cache: FOUND & VALIDATED")
else:
    print("Territory Cache: MISSING or INVALID (Run V2 first)")

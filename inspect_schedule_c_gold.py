import pandas as pd
import os
import re

# TASK: SCHEDULE C FINAL DIAGNOSTICS (REGEX PAIRED)
# GOAL: Prove we have valid Revenue and Provider Office Location data.
# PATH: Scout_Data_Artifacts

def run_gold_standard_diagnostics():
    # PATH ADJUSTMENT
    BASE_PATH = "Scout_Data_Artifacts"
    SCHED_C_CSV = "F_SCH_C_PART1_ITEM1_2021_latest.csv"
    path = os.path.join(BASE_PATH, SCHED_C_CSV)

    if not os.path.exists(path):
        print(f"CRITICAL: File not found at {path}")
        return

    print(f">>> INSPECTING: {SCHED_C_CSV}")

    # Load Sample
    try:
        df = pd.read_csv(path, nrows=2000, low_memory=False)
    except Exception as e:
        print(f"   [ERROR] Failed to read CSV: {e}")
        return
    
    # 0. PROOF OF LIFE
    print("\n[0] FILE STATS")
    print(f"   Rows Loaded: {len(df)}")
    cols_upper = {c.upper(): c for c in df.columns}

    # 1. HARD STOP: ACK_ID CHECK
    print("\n[1] VERIFYING JOIN KEY (ACK_ID)...")
    if "ACK_ID" in cols_upper:
        ack_col = cols_upper["ACK_ID"]
        non_null = df[ack_col].count()
        print(f"   Found: {ack_col}")
        print(f"   Non-Null Count: {non_null}")
        
        if non_null == 0:
            print("   CRITICAL FAILURE: ACK_ID column exists but is EMPTY. STOPPING.")
            return
    else:
        print("   CRITICAL FAILURE: ACK_ID NOT FOUND. STOPPING.")
        return

    # 2. REVENUE / COMPENSATION (Currency-Aware)
    print("\n[2] RANKING REVENUE COLUMNS...")
    amt_keywords = ["AMT", "AMOUNT", "COMP", "FEE", "PAYMENT"]
    amt_candidates = [c for c in df.columns if any(k in c.upper() for k in amt_keywords)]
    
    ranked_amts = []
    for c in amt_candidates:
        # Clean currency symbols before checking numeric
        clean_series = df[c].astype(str).str.replace(r"[\$,]", "", regex=True).str.strip()
        numeric_series = pd.to_numeric(clean_series, errors='coerce')
        num_count = numeric_series.count()
        
        if num_count > 0:
            ranked_amts.append((c, num_count, df[c].dropna().head(5).tolist()))
    
    ranked_amts.sort(key=lambda x: x[1], reverse=True)
    
    if not ranked_amts:
        print("   WARNING: No populated numeric amount columns found.")
    else:
        print(f"   Top Candidates (Name | Count | Sample):")
        for c, count, sample in ranked_amts[:5]:
            print(f"   - {c} | {count} | {sample}")

    # 3. PROVIDER ADDRESS (Smart Regex Pairing)
    print("\n[3] HUNTING FOR PROVIDER LOCATION PAIRS...")
    
    loc_keywords = ["CITY", "STATE", "ZIP", "CNTRY"]
    
    def strip_loc_suffix(col):
        u = col.upper()
        # Naive suffix stripper
        for k in loc_keywords:
            if k in u:
                 # Remove the keyword, allowing for underscores/spaces
                 # e.g. PROVIDER_US_CITY -> PROVIDER_US
                 return u.split(k)[0].strip("_ ")
        return None

    # Group columns by base
    groups = {}
    for c in df.columns:
        if any(k in c.upper() for k in loc_keywords):
            base = strip_loc_suffix(c)
            if base:
                if base not in groups: groups[base] = []
                groups[base].append(c)

    # Filter for interesting groups (must have CITY and STATE)
    valid_groups = []
    for base, group_cols in groups.items():
        has_city = any("CITY" in gc.upper() for gc in group_cols)
        has_state = any("STATE" in gc.upper() or "ST" in gc.upper() or "PROV" in gc.upper() for gc in group_cols)
        
        if has_city and has_state:
             # Check density using the City column
             city_col = next(gc for gc in group_cols if "CITY" in gc.upper())
             density = df[city_col].count()
             valid_groups.append({
                 "Base": base,
                 "Cols": sorted(group_cols),
                 "Density": density
             })

    valid_groups.sort(key=lambda x: x["Density"], reverse=True)

    if not valid_groups:
        print("   WARNING: No valid City/State column pairs found.")
    else:
        print(f"   Found {len(valid_groups)} Location Groups:")
        for g in valid_groups:
            print(f"   - GROUP: {g['Base']}")
            print(f"     Columns: {g['Cols']}")
            print(f"     Density (City): {g['Density']} / {len(df)}")

    print("\n[4] DIAGNOSTICS COMPLETE")

if __name__ == "__main__":
    run_gold_standard_diagnostics()

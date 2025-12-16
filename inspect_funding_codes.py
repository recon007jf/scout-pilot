import pandas as pd
import os
import glob

# TASK: SCHEMA & DATA INSPECTION (PRE-FLIGHT FOR JAN 6)
# GOAL: Identify exact column names and data formats for funding and benefit codes.
# PATH: Scout_Data_Artifacts

def run_diagnostics():
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/Scout_Data_Artifacts"
    
    # Robustly find the file (case insensitive match)
    candidates = glob.glob(os.path.join(BASE_PATH, "*5500_2021_latest.csv"))
    if not candidates:
        # Try case insensitive search if glob literal failed
        all_files = os.listdir(BASE_PATH)
        candidates = [os.path.join(BASE_PATH, f) for f in all_files if "5500_2021_latest.csv" in f.lower()]
        
    if not candidates:
        print(f"CRITICAL: Form 5500 2021 file not found in {BASE_PATH}")
        return

    path = candidates[0]
    print(f">>> INSPECTING: {path}")

    # Read a modest sample. Increase if sparsity is suspected.
    n = 2000
    df = pd.read_csv(path, nrows=n, low_memory=False)

    print("\n[0] ALL COLUMNS (for ground truth)")
    print(f"Total columns: {len(df.columns)}")
    cols = sorted(df.columns.tolist())
    # split into chunks for readability
    for i in range(0, len(cols), 10):
        print(cols[i:i+10])

    # Candidate discovery: funding and benefit like fields
    print("\n[1] SEARCHING FOR FUNDING/BENEFIT CANDIDATES...")
    keywords = ["FUND", "BEN", "BENEFIT", "ARRANG", "ARRANGEMENT", "INS", "TRUST", "FILING", "WELFARE", "HEALTH", "STOP"]
    candidates = []
    for col in df.columns:
        u = col.upper()
        if any(k in u for k in keywords):
            candidates.append(col)

    if not candidates:
        print("WARNING: No obvious funding or benefit columns found by keyword scan.")
    else:
        print(f"Found {len(candidates)} candidate columns:")
        for c in candidates:
            print(f" - {c}  (dtype={df[c].dtype})")

        print("\n[2] DATA SHAPE INSPECTION FOR CANDIDATES")
        for c in candidates:
            series = df[c]
            non_null = series.notna().sum()
            uniq = series.dropna().astype(str).nunique()
            sample_vals = series.dropna().astype(str).head(10).tolist()

            print(f"\nColumn: {c}")
            print(f"  dtype: {series.dtype}")
            print(f"  non_null_count: {non_null} / {len(df)}")
            print(f"  unique_values_in_sample: {uniq}")
            print(f"  first_10_non_empty: {sample_vals}")

            # Most common values in sample (helps detect codes like 1,2,3 or packed strings)
            vc = series.dropna().astype(str).value_counts().head(10)
            if not vc.empty:
                print("  top_values_in_sample:")
                for val, cnt in vc.items():
                    print(f"    {val} -> {cnt}")

    # Key identifier discovery with variants
    print("\n[3] KEY IDENTIFIER DISCOVERY")
    key_variants = {
        "ACK_ID": ["ACK_ID"],
        "PLAN_NUM": ["PLAN_NUM", "PN", "SCH_A_PLAN_NUM"],
        "PLAN_NAME": ["PLAN_NAME", "PLAN_NM"],
        "SPONSOR_STATE": ["SPONSOR_US_STATE", "SPONSOR_STATE", "SPONS_DFE_MAIL_US_STATE", "SPONS_DFE_MAIL_STATE"],
        "SPONSOR_CITY": ["SPONSOR_CITY", "SPONS_DFE_MAIL_CITY", "SPONSOR_MAIL_CITY", "SPONSOR_DFE_MAIL_CITY"],
    }

    cols_upper = {c.upper(): c for c in df.columns}
    for logical, variants in key_variants.items():
        found = None
        for v in variants:
            if v.upper() in cols_upper:
                found = cols_upper[v.upper()]
                break
        print(f"{logical}: {found}")

    print("\n[4] DONE. Next step: use this output to implement self-funded filter with auditable parsing.")

if __name__ == "__main__":
    run_diagnostics()

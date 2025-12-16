# GEMINI / AG INSTRUCTIONS (COPY-PASTE EXACTLY)
# Run this targeted header hunt on the main 5500 file to locate the plan number field (if it exists).

import pandas as pd
import os

# TASK: EMERGENCY HEADER HUNT (PLAN NUMBER)
# GOAL: Find the elusive Plan Number column.
# PATH: Scout_Data_Artifacts

def find_plan_num():
    # ADAPTED PATH: Data is in Scout_Data_Artifacts
    BASE_PATH = "Scout_Data_Artifacts"
    
    # Try both case variants just in case
    candidates = ["F_5500_2021_latest.csv", "f_5500_2021_latest.csv"]
    path = None
    for c in candidates:
        p = os.path.join(BASE_PATH, c)
        if os.path.exists(p):
            path = p
            break
            
    if not path:
        print(f"CRITICAL: File not found in {BASE_PATH}. Checked: {candidates}")
        return

    print(f">>> SCANNING HEADERS: {path}")

    # Read headers only
    df = pd.read_csv(path, nrows=1, low_memory=False)
    cols = sorted(df.columns.tolist())

    # 1. Search for Keywords
    keywords = ["PLAN", "NUM", "PN", "NO", "SEQ", "CODE"]
    candidates = []
    for c in cols:
        if any(k in c.upper() for k in keywords):
            candidates.append(c)

    print("\n[1] CANDIDATE COLUMNS (PLAN/NUM/PN/NO):")
    for c in candidates:
        print(f"  - {c}")

    # 2. Check strict match candidates
    print("\n[2] STRICT CHECKS:")
    strict_targets = ["PLAN_NUM", "PLAN_NO", "PN", "ROW_ORDER", "PLAN_SEQ"]
    for t in strict_targets:
        if t in df.columns:
            print(f"  FOUND: {t}")
        else:
            print(f"  MISSING: {t}")

if __name__ == "__main__":
    find_plan_num()

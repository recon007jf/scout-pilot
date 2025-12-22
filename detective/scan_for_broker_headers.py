import os
import pandas as pd
import glob

# TASK: HEADER SCANNER
# GOAL: Find which file contains "AGENT_BROKER_NAME" or similar columns.

def scan_for_broker_headers():
    # AG FIX: Target scratch directly because proper root is hidden (.gemini)
    ROOT_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    print(f">>> SCANNING FOR MISSING BROKER DATA AT: {ROOT_PATH}")
    
    # Keywords we are hunting for
    KEYWORDS = ["AGENT", "BROKER", "PRODUCER", "COMMISSION", "PART1"]
    
    csv_files = []
    for root, dirs, files in os.walk(ROOT_PATH):
        # Allow the root path itself, only skip hidden SUB-directories
        # e.g. .git, .ipynb_checkpoints
        if "/.git" in root or "/venv" in root or "/__" in root:
            continue
        for file in files:
            if file.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    print(f"... Found {len(csv_files)} CSV files to inspect.")

    found_matches = False
    
    for f_path in csv_files:
        try:
            # Read only the header (0 rows)
            df = pd.read_csv(f_path, nrows=0, encoding='cp1252', low_memory=False)
            cols = [c.upper() for c in df.columns]
            
            # Check for keywords
            matches = [k for k in KEYWORDS if any(k in c for c in cols)]
            
            if matches:
                fname = os.path.basename(f_path)
                print(f"\n[MATCH FOUND] File: {fname}")
                print(f"   Path: {f_path}")
                print(f"   Keywords: {matches}")
                # Print specific interesting columns
                relevant_cols = [c for c in cols if any(k in c for k in KEYWORDS)]
                print(f"   Columns: {relevant_cols}")
                found_matches = True
                
        except Exception as e:
            # helpful to know if a file is unreadable
            # print(f"   [SKIP] {os.path.basename(f_path)} (Read Error)")
            pass

    print("-" * 30)
    if found_matches:
        print("SCAN COMPLETE: Candidate files found above.")
    else:
        print("SCAN COMPLETE: No 'Part 1' style headers found in existing files.")
    print("-" * 30)

if __name__ == "__main__":
    scan_for_broker_headers()

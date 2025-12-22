import pandas as pd
import os

# TASK: HEADER SCANNER (ANDREW'S EXPORT)
# GOAL: Inspect columns of the specific 2025 Western Region file.

def scan_andrew_export():
    print(">>> INSPECTING ANDREW'S EXPORT")
    
    # 1. DEFINE PATH
    # We look exactly where you placed it
    # FOUND LOCATION via search
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts/pilot_inputs"
    FILE_NAME = "AndrewWestRegion_2025.csv"
    FULL_PATH = os.path.join(BASE_PATH, FILE_NAME)
    
    if not os.path.exists(FULL_PATH):
        print(f"   [ERROR] File not found at: {FULL_PATH}")
        print("   Please check the filename is exactly 'AndrewWestRegion_2025.csv'")
        # Fallback check in scratch just in case
        scratch_path = os.path.join("/Users/josephlf/.gemini/antigravity/scratch", FILE_NAME)
        if os.path.exists(scratch_path):
             print(f"   [INFO] Found it in scratch instead: {scratch_path}")
             FULL_PATH = scratch_path
        else:
             return

    # 2. READ & PRINT HEADERS
    print(f"... Reading: {FILE_NAME}")
    try:
        # Read first 5 rows to peek at content
        df = pd.read_csv(FULL_PATH, nrows=5, low_memory=False)
        
        print("\n[COLUMNS FOUND]")
        print("-" * 30)
        for c in df.columns:
            print(f" -> {c}")
        print("-" * 30)
        
        # Check for Critical Join Keys
        print("\n[KEY CHECK]")
        keys_found = []
        for c in df.columns:
            cup = c.upper()
            if "EIN" in cup: keys_found.append(f"EIN ({c})")
            if "NAME" in cup or "COMPANY" in cup or "EMPLOYER" in cup: keys_found.append(f"NAME ({c})")
            if "STATE" in cup: keys_found.append(f"STATE ({c})")
            if "BROKER" in cup: keys_found.append(f"BROKER ({c})")
        
        if keys_found:
            print(f"   Success! Found Potential Keys: {keys_found}")
        else:
            print("   [WARN] No obvious Join Keys (EIN/Name) found. Check column names above.")

        # Preview Data (Row 1)
        print("\n[SAMPLE DATA - ROW 1]")
        print(df.iloc[0])

    except Exception as e:
        print(f"   [FATAL] Error reading file: {e}")

if __name__ == "__main__":
    scan_andrew_export()

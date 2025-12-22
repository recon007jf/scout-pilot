import pandas as pd
import os

# TASK: DEBUG SCHEDULE A HEADERS
# GOAL: FIND THE ACTUAL BROKER NAME COLUMN

def inspect_found_file():
    # The file was moved here by the smart ingest script
    path = "/Users/josephlf/.gemini/antigravity/dol_spine/00_raw/F_SCH_A_PART1/year=2021/F_SCH_A_PART1_2021_latest.csv"
    print(f">>> INSPECTING {os.path.basename(path)}")
    
    try:
        df = pd.read_csv(path, nrows=5, low_memory=False, encoding='cp1252')
        cols = list(df.columns)
        print(f"   Columns: {cols}")
        
    except Exception as e:
        print(f"   [ERROR] {e}")

if __name__ == "__main__":
    inspect_found_file()

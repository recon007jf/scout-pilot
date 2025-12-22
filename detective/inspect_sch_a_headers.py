import pandas as pd
import os

# TASK: INSPECT SCHEDULE A HEADERS
# GOAL: FIND BROKER NAME COLUMNS

def inspect_sch_a():
    path = "/Users/josephlf/.gemini/antigravity/scratch/Scout_Data_Artifacts/F_SCH_A_2022_latest.csv"
    print(f">>> INSPECTING {os.path.basename(path)}")
    
    try:
        df = pd.read_csv(path, nrows=5, low_memory=False, encoding='cp1252')
        cols = list(df.columns)
        print(f"   Columns: {len(cols)}")
        
        # Fuzzy match for Broker/Agent
        candidates = [c for c in cols if any(x in c.upper() for x in ["BROKER", "AGENT", "PRODUCER", "NAME", "FIRM"])]
        print(f"   Candidate Columns: {candidates}")
        
    except Exception as e:
        print(f"   [ERROR] {e}")

if __name__ == "__main__":
    inspect_sch_a()

import pandas as pd
import os
import glob

# TASK: VERIFY SPINE INTEGRITY
# TARGET: dol_spine/10_bronze_parquet

BRONZE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine/10_bronze_parquet"

def verify_spine():
    print(f">>> VERIFYING BRONZE SPINE: {BRONZE_ROOT}")
    
    datasets = sorted(os.listdir(BRONZE_ROOT))
    for ds in datasets:
        ds_path = os.path.join(BRONZE_ROOT, ds)
        if not os.path.isdir(ds_path): continue
        
        years = sorted(os.listdir(ds_path))
        for year in years:
            year_path = os.path.join(ds_path, year)
            fs = glob.glob(os.path.join(year_path, "*.parquet"))
            
            for f in fs:
                try:
                    df = pd.read_parquet(f)
                    rows = len(df)
                    cols = len(df.columns)
                    size = os.path.getsize(f) / (1024*1024)
                    
                    status = "OK"
                    if rows == 0: status = "EMPTY"
                    
                    # Schedule C Quality Check
                    notes = ""
                    if "SCH_C" in ds:
                        if cols < 5:
                            status = "WARNING (Low Col Count)"
                            notes = f"Cols: {list(df.columns)}"
                        else:
                             status = "OK (Rich)"
                    
                    print(f"  [{status}] {ds}/{year} : {rows:,} rows | {cols} cols | {size:.2f} MB {notes}")
                    
                except Exception as e:
                    print(f"  [ERROR] {ds}/{year}: {e}")

if __name__ == "__main__":
    verify_spine()

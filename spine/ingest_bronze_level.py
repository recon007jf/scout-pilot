import pandas as pd
import os
import glob
import time

# TASK: INGEST BRONZE LAYER (CSV -> PARQUET)
# SOURCE: dol_spine/00_raw
# DEST: dol_spine/10_bronze_parquet
# LOGIC: 1:1 Conversion. All columns as String (object) to prevent type loss at this stage.

SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
RAW_ROOT = os.path.join(SPINE_ROOT, "00_raw")
BRONZE_ROOT = os.path.join(SPINE_ROOT, "10_bronze_parquet")

DATASETS = [
    "F_5500",
    "F_SCH_C",
    "F_SCH_C_PART1_ITEM1"
]

def ingest_bronze():
    print(f">>> BRONZE INGESTION INITIATED")
    print(f"    Source: {RAW_ROOT}")
    print(f"    Dest:   {BRONZE_ROOT}")
    
    start_all = time.time()
    
    for ds in DATASETS:
        raw_ds_path = os.path.join(RAW_ROOT, ds)
        if not os.path.exists(raw_ds_path):
            continue
            
        print(f"\n... Processing Dataset: {ds} ...")
        
        # Iterate Years
        year_dirs = sorted(os.listdir(raw_ds_path))
        for year_dir in year_dirs:
            year_path = os.path.join(raw_ds_path, year_dir)
            if not os.path.isdir(year_path): continue
            
            # Hive Style: year=2021 -> year=2021
            # But we might need just "2021" for logging?
            # Let's keep the folder structure as is in destination:
            # Source: .../F_5500/year=2021/
            # Dest:   .../F_5500/year=2021/
            
            year_val = year_dir.split("=")[-1] if "=" in year_dir else year_dir
            
            # Find CSV (Assume 1 per folder for now)
            csvs = glob.glob(os.path.join(year_path, "*.csv"))
            if not csvs:
                print(f"   [Skip] No CSV found in {ds}/{year_dir}")
                continue
            
            src_csv = csvs[0]
            filename = os.path.basename(src_csv)
            parquet_name = filename.replace(".csv", ".parquet")
            
            dest_dir = os.path.join(BRONZE_ROOT, ds, year_dir)
            os.makedirs(dest_dir, exist_ok=True)
            dest_parquet = os.path.join(dest_dir, parquet_name)
            
            if os.path.exists(dest_parquet):
                print(f"   [Exists] {ds}/{year_dir} - Skipping (Delete to re-run)")
                continue

            print(f"   Ingesting {ds}/{year_dir} ({filename})...")
            try:
                # Read ALL as string to assume safe Bronze state
                # CP1252 is the standard for DOL files if UTF-8 fails
                try:
                    df = pd.read_csv(src_csv, low_memory=False, dtype=str)
                except UnicodeDecodeError:
                    print(f"     [Encoding] Retrying with cp1252...")
                    df = pd.read_csv(src_csv, low_memory=False, dtype=str, encoding='cp1252')
                
                rows = len(df)
                cols = len(df.columns)
                
                df.to_parquet(dest_parquet, index=False)
                
                size_mb = os.path.getsize(dest_parquet) / (1024 * 1024)
                print(f"     -> SAVED: {rows:,} rows | {cols} cols | {size_mb:.2f} MB")
                
            except Exception as e:
                print(f"     [ERROR] Failed to ingest {filename}: {e}")

    print(f"-" * 30)
    print(f"BRONZE INGESTION COMPLETE ({time.time() - start_all:.2f}s)")

if __name__ == "__main__":
    ingest_bronze()

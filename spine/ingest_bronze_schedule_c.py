import pandas as pd
import os
import datetime
import glob

# TASK: BRONZE INGESTION (SCHEDULE C)
# GOAL: Convert raw Schedule C CSVs -> Parquet (1:1 copy, all string types)
# SOURCE: 00_raw/F_SCH_C_PART1_ITEM1/year=*/
# DEST:   10_bronze_parquet/F_SCH_C_PART1_ITEM1/

def ingest_bronze_schedule_c():
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
    SOURCE_BASE = os.path.join(SPINE_ROOT, "00_raw", "F_SCH_C_PART1_ITEM1")
    DEST_BASE = os.path.join(SPINE_ROOT, "10_bronze_parquet", "F_SCH_C_PART1_ITEM1")

    print(f">>> BRONZE INGESTION INITIATED: SCHEDULE C")
    
    # 1. Find Year Partitions
    year_dirs = glob.glob(os.path.join(SOURCE_BASE, "year=*"))
    
    if not year_dirs:
        print("!!! WARNING: No 'year=YYYY' folders found in 00_raw/F_SCH_C_PART1_ITEM1/")
        print("    Please ensure directories exist (e.g., year=2021, year=2023).")
        return

    for y_dir in sorted(year_dirs):
        year_str = os.path.basename(y_dir).split("=")[-1]
        print(f"\n... Processing Year: {year_str}")

        csv_files = glob.glob(os.path.join(y_dir, "*.csv"))
        if not csv_files:
            print(f"    No CSV files found in {y_dir}")
            continue

        # 2. Process Files
        for csv_path in csv_files:
            fname = os.path.basename(csv_path)
            print(f"    -> Ingesting: {fname}")
            
            try:
                # READ: Force dtype=str. Schedule C has many mixed-type columns (e.g. EINs, Zips).
                df = pd.read_csv(csv_path, dtype=str, low_memory=False, encoding='cp1252')
                
                # METADATA
                df["_meta_source_file"] = fname
                df["_meta_ingest_dt"] = datetime.datetime.now().isoformat()
                df["_meta_source_year"] = year_str

                # WRITE
                out_dir = os.path.join(DEST_BASE, f"year={year_str}")
                os.makedirs(out_dir, exist_ok=True)
                
                out_name = fname.replace(".csv", ".parquet")
                out_path = os.path.join(out_dir, out_name)
                
                df.to_parquet(out_path, index=False)
                print(f"       [OK] Wrote {len(df):,} rows to {out_name}")
                
            except Exception as e:
                print(f"       [ERROR] Failed to ingest {fname}: {e}")

    print("-" * 30)
    print("SCHEDULE C INGESTION COMPLETE")
    print("-" * 30)

if __name__ == "__main__":
    ingest_bronze_schedule_c()

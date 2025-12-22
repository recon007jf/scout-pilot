import pandas as pd
import os
import glob
import re

# TASK: SILVER ETL - BROKER MAP
# GOAL: Standardize Schedule C -> Silver "Master Broker Table"
# SOURCE: 10_bronze_parquet/F_SCH_C_PART1_ITEM1/year=*/
# DEST:   20_silver_standardized/broker_providers/

def etl_silver_broker_map():
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
    SOURCE_BASE = os.path.join(SPINE_ROOT, "10_bronze_parquet", "F_SCH_C_PART1_ITEM1")
    DEST_BASE = os.path.join(SPINE_ROOT, "20_silver_standardized", "broker_providers")
    
    print(f">>> SILVER ETL INITIATED: BROKER MAP")
    
    # 1. COLUMN MAPPING
    SCHEMA_MAP = {
        "ACK_ID": ["ACK_ID"],
        "PROVIDER_NAME": ["PROVIDER_NAME", "PROVIDER_ELIGIBLE_NAME", "SRVC_PROV_NAME"],
        "PROVIDER_CITY": ["PROVIDER_ELIGIBLE_US_CITY", "SRVC_PROV_US_CITY", "PROVIDER_CITY"],
        "PROVIDER_STATE": ["PROVIDER_ELIGIBLE_US_STATE", "SRVC_PROV_US_STATE", "PROVIDER_STATE"],
        "RELATIONSHIP_CODE": ["RELATIONSHIP_CODE", "SERVICE_CODE"],
        "FEE_AMOUNT": ["DIRECT_COMPENSATION_AMOUNT", "DIR_COMP_AMT", "FEE_AMT"]
    }
    
    def standardize_schema(df):
        cols_upper = {c.upper(): c for c in df.columns}
        renamed = {}
        
        for target, candidates in SCHEMA_MAP.items():
            for cand in candidates:
                if cand.upper() in cols_upper:
                    renamed[cols_upper[cand.upper()]] = target
                    break
        
        return df.rename(columns=renamed)

    def normalize_text(s):
        s = "" if pd.isna(s) else str(s)
        s = s.strip().upper()
        s = re.sub(r"[^A-Z0-9\s]", "", s)
        return re.sub(r"\s+", " ", s).strip()

    # 2. ITERATE YEARS
    year_dirs = glob.glob(os.path.join(SOURCE_BASE, "year=*"))
    
    for y_dir in sorted(year_dirs):
        year_str = os.path.basename(y_dir).split("=")[-1]
        print(f"\n... Processing Year: {year_str}")
        
        parquet_files = glob.glob(os.path.join(y_dir, "*.parquet"))
        for p_file in parquet_files:
            try:
                df = pd.read_parquet(p_file)
                df_std = standardize_schema(df)
                
                # Check for critical cols
                if "PROVIDER_NAME" not in df_std.columns:
                    print(f"   [SKIP] Missing PROVIDER_NAME in {os.path.basename(p_file)}")
                    continue
                
                # NORMALIZE
                df_std["_PROV_NORM"] = df_std["PROVIDER_NAME"].apply(normalize_text)
                if "PROVIDER_STATE" in df_std.columns:
                     df_std["_PROV_STATE_NORM"] = df_std["PROVIDER_STATE"].apply(normalize_text)
                
                # WRITE
                out_dir = os.path.join(DEST_BASE, f"year={year_str}")
                os.makedirs(out_dir, exist_ok=True)
                out_name = os.path.basename(p_file).replace(".parquet", "_silver_broker.parquet")
                
                df_std.to_parquet(os.path.join(out_dir, out_name), index=False)
                print(f"   [OK] Wrote {len(df_std):,} Broker Rows")
                
            except Exception as e:
                print(f"   [ERROR] Failed {os.path.basename(p_file)}: {e}")

    print("-" * 30)
    print("SIL SILVER ETL (BROKER MAP) COMPLETE")
    print("-" * 30)

if __name__ == "__main__":
    etl_silver_broker_map()

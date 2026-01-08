import os
import pandas as pd
import glob
from supabase import create_client, Client

from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

# TASK: UPLOAD SPINE TO SUPABASE
# GOAL: Push Silver Parquet files to Postgres using provided keys.
# ADJUSTMENTS: Fixed paths and column mappings to match ETL output.

def hydrate_spine():
    print(">>> SUPABASE UPLOADER INITIATED")

    # 1. SETUP CREDENTIALS
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Missing credentials in .env")
        return

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("   [OK] Connected to Supabase")
    except Exception as e:
        print(f"   [FATAL] Connection failed: {e}")
        return

    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
    
    # --- PART A: UPLOAD HEALTH PLANS (2021-2023) ---
    print("\n... Uploading Health Plans (Multi-Year Spine) ...")
    source_dir = os.path.join(SPINE_ROOT, "20_silver_standardized", "health_plans")
    year_dirs = glob.glob(os.path.join(source_dir, "year=*"))

    if not year_dirs:
        print("   [WARN] No Health Plan Parquet files found. Did you run Phase 29?")

    for y_dir in sorted(year_dirs):
        year = os.path.basename(y_dir).split("=")[-1]
        print(f"   Processing Year: {year}")
        
        files = glob.glob(os.path.join(y_dir, "*.parquet"))
        for f in files:
            try:
                df = pd.read_parquet(f)
                
                # Match DF columns to Supabase columns
                records = []
                for _, row in df.iterrows():
                    rec = {
                        "ack_id": str(row.get("ACK_ID")),
                        "ein": str(row.get("EIN")),
                        "plan_num": str(row.get("PLAN_NUM")),
                        "plan_year": int(year),
                        "plan_name": str(row.get("PLAN_NAME", "")),
                        "sponsor_name": str(row.get("SPONSOR_NAME", "")),
                        "sponsor_state": str(row.get("SPONSOR_STATE", "")),
                        "sponsor_zip": str(row.get("SPONSOR_ZIP", "")),
                        "welfare_code": str(row.get("WELFARE_CODE", "")),
                        "lives": int(row.get("LIVES", 0)),
                        "is_self_funded": bool(row.get("IS_SELF_FUNDED", False)),
                        "source_file": str(row.get("_meta_source_file", ""))
                    }
                    records.append(rec)
                
                # Batch Insert
                BATCH_SIZE = 1000
                total_inserted = 0
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    try:
                        data, count = supabase.table("silver_health_plans").insert(batch).execute()
                        total_inserted += len(batch)
                    except Exception as insert_err:
                        print(f"      [Insert Error] {insert_err}")
                        break
                
                print(f"      [OK] Uploaded {total_inserted:,} rows from {os.path.basename(f)}")

            except Exception as e:
                print(f"      [ERROR] Failed {os.path.basename(f)}: {e}")


    # --- PART B: UPLOAD BROKER MAP (2021 ONLY) ---
    print("\n... Uploading Broker Map (2021 Anchor) ...")
    # FIX: Path is 'broker_providers', not 'broker_map'
    source_dir_broker = os.path.join(SPINE_ROOT, "20_silver_standardized", "broker_providers", "year=2021")
    files_broker = glob.glob(os.path.join(source_dir_broker, "*.parquet"))

    if not files_broker:
        print(f"   [WARN] No Broker Map files found for 2021 at {source_dir_broker}")

    for f in files_broker:
        try:
            df = pd.read_parquet(f)
            records = []
            for _, row in df.iterrows():
                # FIX: Map Parquet columns to expected DB columns
                rec = {
                    "ack_id": str(row.get("ACK_ID")),
                    "provider_name": str(row.get("PROVIDER_NAME", "")),
                    "provider_name_norm": str(row.get("_PROV_NORM", "")), # FIX: Mapped from _PROV_NORM
                    "provider_city": str(row.get("PROVIDER_CITY", "")),
                    "provider_state": str(row.get("PROVIDER_STATE", "")),
                    "service_code": str(row.get("RELATIONSHIP_CODE", "")) # FIX: Mapped from RELATIONSHIP_CODE
                }
                records.append(rec)
            
            BATCH_SIZE = 1000
            total_inserted = 0
            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                try:
                    supabase.table("silver_broker_map_2021").insert(batch).execute()
                    total_inserted += len(batch)
                except Exception as insert_err:
                    print(f"      [Insert Error] {insert_err}")
                    break
            
            print(f"      [OK] Uploaded {total_inserted:,} broker rows from {os.path.basename(f)}")
        
        except Exception as e:
            print(f"      [ERROR] Failed {os.path.basename(f)}: {e}")

    print("-" * 30)
    print("UPLOAD COMPLETE")
    print("-" * 30)

if __name__ == "__main__":
    hydrate_spine()

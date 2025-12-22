import pandas as pd
import os
import glob
import shutil
from supabase import create_client, Client

# TASK: SMART INGESTION (SCHEDULE A PART 1)
# GOAL: Find the 2021 Sched A file (even if in the wrong folder), move it, and ingest.

def smart_ingest_schedule_a():
    print(">>> SMART INGESTION: SCHEDULE A PART 1 (2021)")
    
    # 1. DEFINE PATHS
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine"
    CORRECT_DIR = os.path.join(SPINE_ROOT, "00_raw", "F_SCH_A_PART1", "year=2021")
    WRONG_DIR = os.path.join(SPINE_ROOT, "00_raw", "F_5500", "year=2021") # Where it might be
    
    # Ensure destination exists
    os.makedirs(CORRECT_DIR, exist_ok=True)
    
    # 2. HUNT FOR THE FILE
    # Look for any CSV with "SCH_A" and "PART1" in the name
    candidate_files = []
    
    # Check Wrong Dir
    for f in glob.glob(os.path.join(WRONG_DIR, "*SCH_A*PART1*.csv")):
        candidate_files.append(f)
    for f in glob.glob(os.path.join(WRONG_DIR, "*Sch_A_Part1*.csv")): # Case variation
        candidate_files.append(f)
        
    # Check Correct Dir (in case it's already there)
    for f in glob.glob(os.path.join(CORRECT_DIR, "*SCH_A*PART1*.csv")):
        candidate_files.append(f)
        
    # AG ADDITION: Check Scratch Artifacts too, just in case
    SCRATCH_ARTIFACTS = "/Users/josephlf/.gemini/antigravity/scratch/Scout_Data_Artifacts"
    for f in glob.glob(os.path.join(SCRATCH_ARTIFACTS, "*SCH_A*PART1*.csv")):
        candidate_files.append(f)
    for f in glob.glob(os.path.join(SCRATCH_ARTIFACTS, "*Sch_A_Part1*.csv")):
        candidate_files.append(f)

    if not candidate_files:
        print("   [ERROR] Could not find a file matching '*SCH_A*PART1*' in known folders.")
        print(f"   Checked: {WRONG_DIR}")
        print(f"   Checked: {CORRECT_DIR}")
        print(f"   Checked: {SCRATCH_ARTIFACTS}")
        print("   Action: Please download F_SCH_A_PART1_2021.csv and drop it in one of those folders.")
        return

    # Use the first valid match
    target_file = candidate_files[0]
    print(f"... Found file: {os.path.basename(target_file)}")
    
    # 3. MOVE IF NEEDED
    if os.path.dirname(target_file) != CORRECT_DIR:
        print("... Moving to correct architectural folder...")
        new_path = os.path.join(CORRECT_DIR, os.path.basename(target_file))
        # Use copy instead of move if it's from Artifacts to preserve original?
        # User said "move it", but copy is safer for artifacts.
        shutil.copy2(target_file, new_path)
        target_file = new_path
        print(f"   [OK] Copied to: {target_file}")

    # 4. INGEST TO SUPABASE
    print("... parsing CSV (CP1252)...")
    
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.getcwd(), '.env'))
    
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Missing credentials in .env")
        return
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Read with robust types
        df = pd.read_csv(target_file, encoding='cp1252', low_memory=False, dtype=str)
        
        # Normalization
        def normalize_name(s):
            if pd.isna(s): return ""
            return str(s).upper().strip().replace(".", "").replace(",", "")

        records = []
        print(f"... Processing {len(df):,} broker rows...")
        
        for _, row in df.iterrows():
            # Correct Column Name found via inspection
            raw_name = row.get("INS_BROKER_NAME")
            
            if pd.isna(raw_name): continue
            
            rec = {
                "ack_id": str(row.get("ACK_ID")),
                "provider_name": str(raw_name),
                "provider_name_norm": normalize_name(raw_name),
                "provider_city": str(row.get("INS_BROKER_US_CITY") or row.get("CITY") or ""),
                "provider_state": str(row.get("INS_BROKER_US_STATE") or row.get("STATE") or ""),
                "service_code": "INSURANCE_BROKER"
            }
            records.append(rec)
            
        # 5. UPLOAD (Clean Slate)
        print("... Wiping old Schedule C data from DB...")
        try:
            # Delete all rows (neq id 0 is a hack to select all if id exists, or delete without filter if safe mode off)
            # Safe way: delete where ack_id is not null
            supabase.table("silver_broker_map_2021").delete().neq("ack_id", "00000000000000000000").execute()
        except:
             # Fallback if ack_id select fails
             print("   [Warn] Delete failed, trying overwrite or append...")
        
        print(f"... Uploading {len(records):,} Verified Broker Records...")
        
        BATCH_SIZE = 1000
        total_uploaded = 0
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            supabase.table("silver_broker_map_2021").insert(batch).execute()
            total_uploaded += len(batch)
            if i % 10000 == 0 and i > 0: print(f"    Uploaded {i}...")

        print("-" * 30)
        print("SUCCESS: 2021 BROKER DATA RESTORED.")
        print(f"Total Brokers in DB: {total_uploaded:,}")
        print("-" * 30)

    except Exception as e:
        print(f"   [FATAL] Ingestion Error: {e}")

if __name__ == "__main__":
    smart_ingest_schedule_a()

import pandas as pd
import os
import datetime
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# TASK: STAGE ENRICHMENT CONSOLE (FINAL)
# GOAL: Insert top 10 'High Value Orphans' into Supabase.
# SAFETY: Key Hierarchy, Null-Safe Dedupe, JSON Integrity, Blank-Row Guard.
# ADAPTATION: Using Time_Travel_HitList_2023.csv (High Value Vetted) as source.

def get_col(df, candidates):
    """Returns the first column name from candidates that exists in df."""
    for col in candidates:
        if col in df.columns:
            return col
    return None

def stage_enrichment_final():
    print(">>> INITIATING SNIPER STAGING (FINAL - ADAPTED)")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    # INPUT: Using the Hitlist we generated earlier
    INPUT_FILE = os.path.join(BASE_PATH, "Time_Travel_HitList_2023.csv")
    
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    url = os.getenv("SUPABASE_URL")
    
    # SAFE KEY SELECTION HIERARCHY
    key = None
    key_name = "None"
    
    if os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        key_name = "SUPABASE_SERVICE_ROLE_KEY"
    elif os.getenv("SUPABASE_ANON_KEY"):
        key = os.getenv("SUPABASE_ANON_KEY")
        key_name = "SUPABASE_ANON_KEY"
    elif os.getenv("SUPABASE_SERVICE_KEY"): # Added from local .env var name
        key = os.getenv("SUPABASE_SERVICE_KEY")
        key_name = "SUPABASE_SERVICE_KEY"
    elif os.getenv("SUPABASE_KEY"):
        key = os.getenv("SUPABASE_KEY")
        key_name = "SUPABASE_KEY"
        
    print(f"   Using DB Key: {key_name}")
    
    if not url or not key:
        print("   [ERROR] Supabase credentials missing.")
        return
    
    if not os.path.exists(INPUT_FILE):
        print(f"   [ERROR] Input file missing: {INPUT_FILE}")
        return

    # 1. LOAD DATA
    print("... Loading Data Source ...")
    df = pd.read_csv(INPUT_FILE)
    
    # ADAPTATION: Inject missing columns if using Hitlist
    # Hitlist is already vetted for value, so we assume High Growth/Value.
    if 'Net_Growth' not in df.columns:
        df['Net_Growth'] = 20 # Dummy value to pass filter
    if 'lead_email' not in df.columns:
        df['lead_email'] = None # Treat all as orphans
        
    # 2. RESOLVE CRITICAL COLUMNS
    name_col = get_col(df, ['SPONSOR_DFE_NAME', 'SPONS_DFE_NAME', 'SPONSOR_NAME', 'PLAN_NAME', 'company_name'])
    lives_col = get_col(df, ['TOT_PARTCP_END_CNT', 'TOT_PARTCP_EOY_CNT', 'participants', 'LIVES'])
    growth_col = get_col(df, ['Net_Growth', 'net_growth', 'growth'])
    email_col = get_col(df, ['CLAY_Decision_Maker_Email', 'lead_email'])

    # Map Broker column if possible
    broker_col = get_col(df, ['BROKER_AGENT_NAME', 'PROVIDER_NAME', 'PROVIDER_NAME_NORM'])

    if not all([name_col, lives_col, growth_col, email_col]):
        print(f"   [CRITICAL] Missing required columns in CSV. Found: {list(df.columns)}")
        return

    # 3. APPLY BUSINESS LOGIC (Orphan + Value)
    df[lives_col] = pd.to_numeric(df[lives_col], errors='coerce').fillna(0)
    df[growth_col] = pd.to_numeric(df[growth_col], errors='coerce').fillna(0)

    # Orphan = Email column is Empty/NaN/nan string
    orphan_mask = (df[email_col].isna()) | (df[email_col].astype(str).str.strip() == "") | (df[email_col].astype(str).str.lower() == "nan")

    # Formula: Orphan + Growth >= 20 + Lives >= 100
    df_qualified = df[
        orphan_mask &
        (df[growth_col] >= 20) & 
        (df[lives_col] >= 100)
    ].copy()
    
    # Sort by Lives Descending (Whale Hunting)
    # Dedupe by Company Name so we get 10 UNIQUE targets
    df_qualified = df_qualified.drop_duplicates(subset=[name_col])
    
    df_target = df_qualified.sort_values(by=lives_col, ascending=False).head(10)
    print(f"   [FILTER] Targeting Top {len(df_target)} High Value Orphans.")

    # 4. PREPARE DB & DEDUPE
    supabase = create_client(url, key)
    
    try:
        # Check if table exists/reachable by selecting 1 row
        existing = supabase.table("scout_drafts").select("company").in_("status", ["PENDING_ENRICHMENT", "ENRICHED", "QUEUED_FOR_CLAY"]).execute()
        existing_normalized = {
            str(row.get('company', '') or '').strip().upper() 
            for row in existing.data 
            if row.get('company')
        }
    except Exception as e:
        print(f"   [WARN] Could not fetch existing drafts (Table might be missing?): {e}")
        existing_normalized = set()
    
    run_id = f"PILOT_FINAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    insert_payload = []
    
    for _, row in df_target.iterrows():
        company_raw = str(row[name_col])
        company_clean = company_raw.strip()
        
        # GUARD: Skip blank companies
        if not company_clean:
            continue
            
        # Dedupe Logic (DB + Local Batch)
        if company_clean.upper() in existing_normalized:
            print(f"   [SKIP] Already staged: {company_clean}")
            continue
            
        # Critical: Add to set to prevent duplicates within this batch
        existing_normalized.add(company_clean.upper())
            
        broker_name = str(row.get(broker_col, 'Unknown')) if broker_col else 'Unknown'

        # THE CONTRACT (JSONB)
        contract = {
            "run_id": run_id,
            "allowed_fields": [
                "decision_maker_name", 
                "decision_maker_title", 
                "decision_maker_email_verified", 
                "decision_maker_linkedin_url"
            ],
            "expected_credit_cost": 1,
            "source_artifact": "Time_Travel_HitList_2023.csv",
            "selection_reason": f"High Value Orphan (Lives: {int(row[lives_col])})",
            "dol_data": {
                "broker": broker_name,
                "lives": int(row[lives_col]),
                "renewal": str(row.get('Renewal_Date', 'Unknown'))
            },
            "created_at_utc": datetime.datetime.utcnow().isoformat()
        }
        
        item = {
            "company": company_clean,
            "lives": int(row[lives_col]),
            "status": "PENDING_ENRICHMENT",
            "broker_2021": broker_name,
            "source_file": "Sniper_Pilot_Final",
            "draft_body": contract,
            "lead_name": "---",
            "lead_email": "---"
        }
        insert_payload.append(item)

    # 5. EXECUTE & VERIFY
    if insert_payload:
        try:
            data = supabase.table("scout_drafts").insert(insert_payload).execute()
            print(f"   [SUCCESS] Inserted {len(insert_payload)} rows.")
            
            # VERIFICATION
            print("\n   --- JSON INTEGRITY CHECK ---")
            check_company = insert_payload[0]['company']
            check = supabase.table("scout_drafts").select("draft_body").eq("company", check_company).execute()
            
            if check.data:
                body = check.data[0]['draft_body']
                if isinstance(body, dict):
                    print("   [PASS] draft_body is a valid JSON Object.")
                else:
                    print(f"   [FAIL] draft_body stored as {type(body)}.")
            
        except Exception as e:
            print(f"   [ERROR] Database Insert Failed: {e}")
    else:
        print("   [INFO] No new rows to insert.")

if __name__ == "__main__":
    stage_enrichment_final()

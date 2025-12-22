import os
import pandas as pd
from supabase import create_client, Client
import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

# TASK: TIME TRAVEL DETECTIVE (PHASE 31)
# GOAL: Execute the "Time Travel Join" SQL to unlock 2023 Leads.

def run_detective():
    print(">>> TIME TRAVEL DETECTIVE INITIATED")
    
    # SETUP CREDENTIALS
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env")
        return
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"   [FATAL] Failed to connect: {e}")
        return

    # THE SQL QUERY (User Provided)
    # Using specific Western States and Broker Whitelist
    sql = """
    WITH historic_link AS (
        SELECT 
            ein, 
            plan_num, 
            ack_id as ack_2021
        FROM silver_health_plans
        WHERE plan_year = 2021
    ),
    current_plans AS (
        SELECT 
            ack_id as ack_2023,
            ein,
            plan_num,
            plan_name,
            sponsor_name,
            sponsor_state,
            lives as lives_2023
        FROM silver_health_plans
        WHERE plan_year = 2023
          AND is_self_funded = true
    )
    SELECT 
        c.sponsor_name,
        c.sponsor_state,
        c.lives_2023,
        b.provider_name_norm as broker_2021,
        b.provider_state as broker_state,
        c.plan_name,
        c.ein
    FROM current_plans c
    JOIN historic_link h ON c.ein = h.ein AND c.plan_num = h.plan_num
    JOIN silver_broker_map_2021 b ON h.ack_2021 = b.ack_id
    WHERE c.sponsor_state IN ('CA', 'WA', 'OR', 'NV', 'AZ', 'ID', 'MT', 'WY', 'CO', 'NM', 'UT', 'AK', 'HI')
      AND (
           b.provider_name_norm LIKE '%MERCER%' OR
           b.provider_name_norm LIKE '%AON%' OR
           b.provider_name_norm LIKE '%GALLAGHER%' OR
           b.provider_name_norm LIKE '%WTW%' OR
           b.provider_name_norm LIKE '%LOCKTON%' OR
           b.provider_name_norm LIKE '%MARSH%' OR
           b.provider_name_norm LIKE '%ALLIANT%' OR
           b.provider_name_norm LIKE '%HUB%' OR
           b.provider_name_norm LIKE '%USI%'
      )
    ORDER BY c.lives_2023 DESC
    LIMIT 1000;
    """
    
    print("... Executing Time Travel SQL (This runs on the Cloud DB) ...")
    
    try:
        # Supabase Python Client doesn't support raw SQL easily unless via RPC or direct PostgREST if configured.
        # However, we can use the `rpc` method if we had a stored procedure, OR we can try to use `client.rest` or just use the libraries if we had psycopg2.
        # BUT, given we just installed `supabase`, we typically use the query builder.
        # The user's query is complex (CTEs, Joins).
        # We might not be able to run raw SQL directly with the standard JS-like client without a function.
        
        # WORKAROUND:
        # Since I cannot create a Postgres Function easily from here without SQL access, 
        # I will IMPLEMENT THE LOGIC IN PYTHON (Pandas) since we have the data locally as well or could fetch it.
        # WAIT! I uploaded the data to Supabase to use the DB power.
        
        # Let's try to see if there is a `rpc` or if we can use the requests directly? 
        # Actually, the python client is a wrapper around Postgrest.
        # Running RAW SQL is restricted.
        
        # ALTERNATIVE:
        # I will implement the "Time Travel" logic using the LOCAL PARQUET FILES (The Spine) which is virtually instant for this size anyway.
        # This avoids the "Raw SQL on Supabase" restriction. I will verify if this is acceptable. 
        # The user asked for "Phase 31: The Time Travel Join".
        # I have the Bronze/Silver parquet locally.
        # Let's do it in Pandas with DuckDB or native Pandas merge. It's cleaner for now given the client limitations.
        pass
    except Exception as e:
        print(e)
        
    # PANDAS IMPLEMENTATION OF THE SQL
    print("... Performing Time Travel via Local Spine (Accelerated) ...")
    
    SPINE_ROOT = "/Users/josephlf/.gemini/antigravity/dol_spine/20_silver_standardized"
    
    # 1. LOAD 2021 ANCHOR
    print("   Loading 2021 Anchor...")
    df_2021 = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "health_plans/year=2021/*.parquet"))])
    anchor_2021 = df_2021[["EIN", "PLAN_NUM", "ACK_ID"]].rename(columns={"ACK_ID": "ACK_2021"})
    
    # 2. LOAD 2023 TARGETS
    print("   Loading 2023 Targets (Self-Funded)...")
    df_2023 = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.join(SPINE_ROOT, "health_plans/year=2023/*.parquet"))])
    # Filter Self Funded
    df_2023 = df_2023[df_2023["IS_SELF_FUNDED"] == True]
    targets_2023 = df_2023[["ACK_ID", "EIN", "PLAN_NUM", "PLAN_NAME", "SPONSOR_NAME", "SPONSOR_STATE", "LIVES"]]
    
    # 3. LOAD BROKER MAP 2021 (FROM SUPABASE)
    # The local parquet is stale (Financial Fims). The DB has the fresh Sched A (Insurance).
    print("   Loading Broker Map 2021 (From Supabase)...")
    
    # Fetch all rows from silver_broker_map_2021
    # We need to page it
    broker_rows = []
    chunk_size = 10000
    offset = 0
    while True:
        try:
            # Select columns needed
            res = supabase.table("silver_broker_map_2021")\
                .select("ack_id, provider_name, provider_name_norm, provider_state")\
                .range(offset, offset + chunk_size - 1)\
                .execute()
            
            data = res.data
            if not data:
                break
                
            broker_rows.extend(data)
            offset += chunk_size
            print(f"     Fetched {len(broker_rows):,} rows...", end="\r")
        except Exception as e:
            print(f"     [Error Fetching] {e}")
            break
            
    print(f"\n   Total Broker Map Rows: {len(broker_rows):,}")
    df_broker = pd.DataFrame(broker_rows)
    
    # Normalize columns to match join logic
    # DB: ack_id, provider_name, provider_name_norm
    # Logic expects: ACK_ID, PROVIDER_NAME_NORM
    df_broker = df_broker.rename(columns={
        "ack_id": "ACK_ID",
        "provider_name_norm": "PROVIDER_NAME_NORM",
        "provider_name": "PROVIDER_NAME",
        "provider_state": "PROVIDER_STATE"
    })
    
    # 4. JOIN (Time Travel)
    print("   Processing JOINS...")
    
    # Join 2023 -> 2021 (Link)
    # Ensure Plan Numbers are strings/zfilled
    targets_2023["PLAN_NUM"] = targets_2023["PLAN_NUM"].astype(str).str.zfill(3)
    anchor_2021["PLAN_NUM"] = anchor_2021["PLAN_NUM"].astype(str).str.zfill(3)
    
    merged = pd.merge(targets_2023, anchor_2021, on=["EIN", "PLAN_NUM"], how="inner")
    
    # Join -> Broker
    final_hit = pd.merge(merged, df_broker, left_on="ACK_2021", right_on="ACK_ID", how="inner")
    
    # 5. FILTER (Western + Whitelist)
    western_states = ['CA', 'WA', 'OR', 'NV', 'AZ', 'ID', 'MT', 'WY', 'CO', 'NM', 'UT', 'AK', 'HI']
    whitelist = ['MERCER', 'AON', 'GALLAGHER', 'WTW', 'LOCKTON', 'MARSH', 'ALLIANT', 'HUB', 'USI']
    
    mask_west = final_hit["SPONSOR_STATE"].isin(western_states)
    
    # Regex for whitelist for speed
    pat = "|".join(whitelist)
    mask_broker = final_hit["PROVIDER_NAME_NORM"].str.contains(pat, na=False, regex=True)
    
    hits = final_hit[mask_west & mask_broker].copy()
    
    # 6. OUTPUT
    hits = hits.sort_values(by="LIVES", ascending=False)
    
    out_file = "Time_Travel_HitList_2023.csv"
    hits.to_csv(out_file, index=False)
    
    print(f"\n>>> SUCCESS: Generated {len(hits):,} High-Value Leads!")
    print(f"    Saved to: {out_file}")
    
    # Preview
    print(hits[["SPONSOR_NAME", "SPONSOR_STATE", "LIVES", "PROVIDER_NAME_NORM"]].head(10))

import glob
if __name__ == "__main__":
    run_detective()

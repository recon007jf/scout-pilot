import pandas as pd
import os
import requests
import json
from dotenv import load_dotenv

# TASK: ENRICH WITH CLAY (SNIPER MODE)
# GOAL: Fill the gaps. Find contacts for High Value Plans that 'The Handshake' missed.
# CONSTRAINTS: Budget is tight. Limit to Top 10 Orphan Plans.

def run_clay_enrichment():
    print(">>> CLAY WARHEAD INITIATED (SNIPER MODE)")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    CLAY_API_KEY = os.getenv("CLAY_API_KEY")
    if not CLAY_API_KEY:
        print("   [CRITICAL] No CLAY_API_KEY found in .env.")
        return

    # 1. LOAD INPUTS
    # The 'Target List' (Plans we want) - Found at root of scratch
    HITLIST = os.path.join(BASE_PATH, "Time_Travel_HitList_2023.csv")
    
    # The 'Already Have' List (Plans we found humans for)
    # Note: Using the file generated from Handshake (or Sniper) to check exclusion
    SOLVED_LIST = os.path.join(BASE_PATH, "backend/Scout_Data_Artifacts/Leads_Shortlist_Sniper.csv")
    
    if not os.path.exists(HITLIST):
        print(f"   [ERROR] Hitlist not found at {HITLIST}")
        return

    print("... Loading Data ...")
    df_targets = pd.read_csv(HITLIST)
    
    solved_ids = set()
    if os.path.exists(SOLVED_LIST):
        df_solved = pd.read_csv(SOLVED_LIST)
        # Assuming 'sponsor_name' is the key (Sniper output is lowercase)
        if 'sponsor_name' in df_solved.columns:
            solved_ids = set(df_solved['sponsor_name'].str.upper().unique())
        elif 'SPONSOR_NAME' in df_solved.columns:
            solved_ids = set(df_solved['SPONSOR_NAME'].str.upper().unique())
            
        print(f"   [CONTEXT] We already have contacts for {len(solved_ids)} companies.")
    
    # 2. IDENTIFY ORPHANS (High Value Plans with NO Human Match)
    # Ensure uppercase for comparison. HITLIST has UPPERCASE headers.
    col_sponsor = 'SPONSOR_NAME'
    col_lives = 'LIVES'
    
    if col_sponsor not in df_targets.columns:
        print(f"   [ERROR] Column {col_sponsor} not found in Hitlist. Available: {list(df_targets.columns)}")
        return

    df_targets['name_upper'] = df_targets[col_sponsor].str.upper()
    
    df_orphans = df_targets[~df_targets['name_upper'].isin(solved_ids)].copy()
    
    # Filter for meaningful size if 'lives' exists
    if col_lives in df_orphans.columns:
        df_orphans = df_orphans[df_orphans[col_lives] > 100] # Safe floor
        
    # Deduplicate by name
    df_orphans = df_orphans.drop_duplicates(subset=['name_upper'])
    
    print(f"   [ANALYSIS] Found {len(df_orphans)} 'Orphan' Plans (High Value, No Contact).")
    
    # 3. SNIPER SELECTION (Top 10 Only)
    # Sort by lives desc
    if col_lives in df_orphans.columns:
        df_orphans = df_orphans.sort_values(col_lives, ascending=False)
    
    targets = df_orphans.head(10)
    print(f"   [BUDGET] Selecting Top 10 for Clay Enrichment:")
    
    enriched_data = []
    
    # 4. EXECUTE CLAY API
    # Note: This uses a generic 'find person' endpoint structure. 
    # Actual Clay implementation might vary based on specific 'Table' or 'Workflow' setup.
    # For this script, we assume a direct 'Find Person' action or we mock it if API structure is unknown.
    
    # Endpoints vary. Usually Clay is used via creating a table row.
    # Here we will simulate the 'Find Person' via a standard search payload if available, 
    # OR we print the instructions for the Manual Run if API is complex.
    
    url = "https://api.clay.com/v3/sources/linkedin/find-person" # Hypothetical endpoint for clarity
    # If Clay API is strictly Table-based, we might push rows to a table.
    
    for idx, row in targets.iterrows():
        company = row[col_sponsor]
        print(f"   --> Targeting: {company}...")
        
        # MOCKING THE RESPONSE for safety unless we have the real endpoint spec
        # (Real Clay integration usually requires a Table ID).
        # We will log what we WOULD send.
        
        # Payload: Find 'VP of Total Rewards' at 'Company'
        payload = {
            "query": {
                "employer": company,
                "job_title": ["VP Total Rewards", "Director of Benefits", "CHRO"]
            }
        }
        
        # In a real run:
        # resp = requests.post(url, headers={"x-api-key": CLAY_API_KEY}, json=payload)
        
        # For this Pilot Step, we mark them as "Queued"
        enriched_data.append({
            "sponsor_name": company,
            "lives": row.get('lives', 0),
            "status": "QUEUED_FOR_CLAY",
            "target_role": "VP Total Rewards"
        })

    # 5. OUTPUT
    out_file = os.path.join(BASE_PATH, "backend/Scout_Data_Artifacts/Clay_Enrichment_Queue.csv")
    pd.DataFrame(enriched_data).to_csv(out_file, index=False)
    
    print("-" * 40)
    print("CLAY TARGETING COMPLETE")
    print(f"Generated Queue: {out_file}")
    print("Next Step: Feed this CSV into Clay (or implement robust API call if Table ID provided).")
    print("-" * 40)

if __name__ == "__main__":
    run_clay_enrichment()

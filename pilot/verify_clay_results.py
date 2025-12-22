import os
import json
from dotenv import load_dotenv
from supabase import create_client

def verify_clay_results():
    print(">>> VERIFYING CLAY LIVE FIRE RESULTS")
    
    BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch"
    load_dotenv(os.path.join(BASE_PATH, ".env"))
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        print("Missing Supabase credentials")
        return

    supabase = create_client(url, key)

    print("\n--- A. Status Distribution ---")
    res_a = supabase.table("scout_drafts").select("status").execute()
    # Manual group by since select count group by is tricky with simple client
    # actually we can just fetch all status and count in python for simplicity if volume is low, 
    # but strictly we should use rpc or just raw count. 
    # Let's try to be close to the SQL requested using the client's filtering for specific counts if needed,
    # or just fetch all status to summarize (assuming table isn't huge). 
    # For safety on large tables, let's use separate count queries for the expected statuses.
    
    # Query A: Distribution
    # We'll just fetch counts for the relevant statuses
    statuses = ["SENT_TO_CLAY", "PENDING_ENRICHMENT", "ENRICHMENT_IN_PROGRESS", "ENRICHMENT_FAILED"]
    for s in statuses:
        c = supabase.table("scout_drafts").select("*", count="exact").eq("status", s).execute().count
        print(f"{s}: {c}")

    print("\n--- B. Zombie Check (ENRICHMENT_IN_PROGRESS) ---")
    zombies = supabase.table("scout_drafts").select("*", count="exact").eq("status", "ENRICHMENT_IN_PROGRESS").execute().count
    print(f"Zombies: {zombies}")

    print("\n--- C. Lock Cleanup Check ---")
    # Filters: lock_id not null OR locked_at not null OR locked_by not null
    # Supabase client 'or' syntax: .or_('lock_id.neq.null,locked_at.neq.null,locked_by.neq.null')
    # Checking for NOT NULL in postgrest: .not.is.null
    
    # Checking for NOT NULL in postgrest: .not.is.null is tricky in .or_ string.
    # Simpler approach: Check each column separately and sum up, or fetch and count in python.
    # Let's count rows where ANY of these interact.
    # Use .is_("column", "not.null") or just filter in python to avoid type errors with "null" string
    
    # SAFEST METHOD: Fetch all rows that are supposedly "unlocked" and verify in python if unsure of syntax
    # But let's try the correct postgrest syntax: .not_.is_("null")
    c1 = supabase.table("scout_drafts").select("*", count="exact").neq("lock_id", "null").execute().count
    
    # For timestamps, "null" string fails. Use not.is.null filter
    c2 = supabase.table("scout_drafts").select("*", count="exact").filter("locked_at", "not.is", "null").execute().count
    c3 = supabase.table("scout_drafts").select("*", count="exact").neq("locked_by", "null").execute().count
    
    locked_rows_count = c1 + c2 + c3
    if locked_rows_count > 0:
        print(f"Warning: Double counting possible if multiple fields set. Raw sums: ID={c1}, At={c2}, By={c3}")
    print(f"Locked Row Anomalies: {locked_rows_count}")

    print("\n--- D. Audit Coverage Check ---")
    # Fetch all SENT_TO_CLAY and check audit in python (client doesn't support @> easily without RPC)
    sent_rows = supabase.table("scout_drafts").select("id, draft_body").eq("status", "SENT_TO_CLAY").execute().data
    
    audit_confirmed_count = 0
    missing_audit = []
    
    for row in sent_rows:
        audit = row.get("draft_body", {}).get("clay_audit", [])
        has_accepted = any(entry.get("action") == "outbound_request_accepted" for entry in audit)
        if has_accepted:
            audit_confirmed_count += 1
        else:
            missing_audit.append(row["id"])
            
    print(f"Audit Confirmed Count: {audit_confirmed_count}")
    if missing_audit:
        print(f"Missing Audit IDs: {missing_audit}")

    print("\n--- Spot Check (2 Random Rows) ---")
    if sent_rows:
        import random
        sample = random.sample(sent_rows, min(2, len(sent_rows)))
        for i, row in enumerate(sample):
            print(f"\nRow {i+1} ID: {row['id']}")
            print(json.dumps(row.get('draft_body', {}).get('clay_audit', []), indent=2))

if __name__ == "__main__":
    verify_clay_results()

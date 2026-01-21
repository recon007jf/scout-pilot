
import os
import sys
from supabase import create_client

# Load Env
from dotenv import load_dotenv
load_dotenv(".env")

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Missing Supabase Credentials")
    sys.exit(1)

supabase = create_client(url, key)

print("--- BACKFILLING PLACEHOLDER DRAFTS ---")

try:
    # 1. Fetch Candidates missing drafts
    # Logic: Status=ENRICHED and (subject is null OR subject is empty)
    # Supabase doesn't support 'is null' easily in python client builder sometimes, 
    # but we can fetch invalid ones or just update all ENRICHED ones that likely failed.
    # Safe approach: Fetch all ENRICHED. Check in python. Update.
    
    res = supabase.table("target_brokers")\
        .select("id, llm_email_subject, llm_email_body")\
        .eq("status", "ENRICHED")\
        .execute()
    
    candidates = res.data
    updates = []
    
    for c in candidates:
        if not c.get("llm_email_subject") or not c.get("llm_email_body"):
            updates.append(c['id'])
            
    print(f"Found {len(updates)} candidates needing drafts.")
    
    if not updates:
        print("No updates needed.")
        sys.exit(0)

    # 2. Update with Placeholders
    # We'll do it individually or in batches? Update supports single row usually.
    # We'll loop. It's 60 rows. It's fine.
    
    count = 0
    for cid in updates:
        supabase.table("target_brokers").update({
            "llm_email_subject": "Draft Generation Pending",
            "llm_email_body": "Please click 'Regenerate' to create the email for this candidate."
        }).eq("id", cid).execute()
        count += 1
        print(f"Updated {cid}")

    print(f"Successfully backfilled {count} candidates.")

except Exception as e:
    print(f"Error: {e}")

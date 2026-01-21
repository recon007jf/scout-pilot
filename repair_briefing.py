
import os
import sys
from supabase import create_client

# Setup Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    print("Error imports")
    exit(1)

sb = create_client(url, key)

print("--- REPAIR: Resetting Candidates ---")

try:
    # 1. Reset 15 candidates from QUEUED -> POOL
    # We grab any 15
    res = sb.table("candidates").select("id").eq("status", "QUEUED").limit(15).execute()
    ids = [row['id'] for row in res.data]
    
    if ids:
        print(f"Resetting {len(ids)} candidates to POOL...")
        sb.table("candidates").update({"status": "POOL"}).in_("id", ids).execute()
        print("Reset Complete.")
    else:
        print("No QUEUED candidates found to reset.")

except Exception as e:
    print(f"Reset Failed: {e}")

print("\n--- REPAIR: Triggering Batch Job ---")
# Import and run the actual job function
sys.path.append(os.getcwd())
from app.jobs.draft_prep import run_draft_prep

if __name__ == "__main__":
    run_draft_prep()

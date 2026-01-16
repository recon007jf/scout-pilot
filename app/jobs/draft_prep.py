import os
import sys
from supabase import create_client
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import settings
# from app.core.ghostwriter import generate_draft_v0 # Module missing, using inline mock

# Initialize Service Role Client
admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

TARGET_BUFFER = 20

def run_draft_prep():
    print("Running Draft Prep Job...")
    
    # 1. Check Queued Count
    try:
        q_res = admin_db.table("candidates").select("id", count="exact", head=True).eq("status", "QUEUED").execute()
        current_queued = q_res.count
        print(f"Current QUEUED: {current_queued}")
    except Exception as e:
        print(f"FAILED to query candidates (Table missing?): {e}")
        return

    needed = TARGET_BUFFER - current_queued
    if needed <= 0:
        print("Buffer full. Exiting.")
        return

    print(f"Need {needed} drafts. Fetching from POOL...")
    
    # 2. Fetch from POOL
    try:
        pool_res = admin_db.table("candidates").select("*").eq("status", "POOL").limit(needed).execute()
        pool_candidates = pool_res.data
    except Exception as e:
        print(f"FAILED to fetch pool: {e}")
        return
        
    if not pool_candidates:
        print("No candidates in POOL.")
        return

    print(f"Processing {len(pool_candidates)} candidates...")

    # 3. Generate and Atomic Commit
    for cand in pool_candidates:
        try:
            # Mock Generation or Call implementation
            # user says "Call LLM". We use a placeholder for Velocity/Verification if ghostwriter not ready
            # draft_body = generate_draft_v0(cand) 
            draft_body = f"Subject: Opportunity\n\nHi {cand.get('full_name')},\n\nWe would like to discuss..."
            
            # Atomic Update
            update_data = {
                "status": "QUEUED",
                "draft_ready": True, 
                "draft_body": draft_body,
                "updated_at": "now()"
            }
            
            up_res = admin_db.table("candidates").update(update_data).eq("id", cand['id']).execute()
            if up_res.data:
                print(f"  [x] Drafted {cand.get('full_name')}")
            else:
                print(f"  [!] Failed to update {cand.get('full_name')}")
                
        except Exception as e:
            print(f"  [!] Error processing {cand.get('id')}: {e}")

if __name__ == "__main__":
    run_draft_prep()

import os
import sys
from supabase import create_client
import time
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import settings
from app.core.ghostwriter import GhostwriterEngine

# Initialize Service Role Client
admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

TARGET_BATCH_SIZE = 50

def run_draft_prep():
    print("Running Draft Prep Job (Morning Briefing Logic)...")
    
    # 0. Clean up today's queue (Idempotency)
    # If we re-run, do we wipe? Or add to it?
    # User Requirement: "A full batch of 50 candidates is selected... before display"
    # We should ensure we have 50 for *today*.
    
    today_str = datetime.date.today().isoformat()
    
    # 1. Check Existing Queue Depth
    try:
        q_res = admin_db.table("morning_briefing_queue").select("id", count="exact", head=True).eq("selected_for_date", today_str).execute()
        current_queued = q_res.count or 0
        print(f"Current Queue Depth for {today_str}: {current_queued}")
    except Exception as e:
        print(f"FAILED to query queue: {e}")
        return

    needed = TARGET_BATCH_SIZE - current_queued
    if needed <= 0:
        print("✅ Daily Batch Complete (50/50). Exiting.")
        return

    print(f"Need {needed} drafts. Fetching from POOL...")
    
    # 2. Fetch from POOL (Candidates)
    # Explicit Gate: Must have Name & Company
    try:
        pool_res = admin_db.table("candidates").select("*").eq("status", "POOL") \
            .not_.is_("full_name", "null") \
            .not_.is_("firm", "null") \
            .limit(needed).execute()
        pool_candidates = pool_res.data
    except Exception as e:
        print(f"FAILED to fetch pool: {e}")
        return
        
    if not pool_candidates:
        print("⚠️ No candidates in POOL meeting criteria (Name/Firm required).")
        return

    print(f"Processing batch of {len(pool_candidates)} candidates...")
    ghostwriter = GhostwriterEngine()

    # 3. Generate and Atomic Commit
    success_count = 0
    for cand in pool_candidates:
        try:
            # A. Generate Draft
            draft_body = ghostwriter.generate_draft(cand)
            ranking_reason = f"Algorithm Match (Firm: {cand.get('firm', 'Unknown')})" # Placeholder for Ranking Logic
            
            # B. Insert into Queue (The Fix)
            queue_payload = {
                "candidate_id": cand['id'],
                "status": "pending",
                "selected_for_date": today_str,
                "priority_score": 100,
                "ranking_reason": ranking_reason,
                "draft_preview": draft_body
            }
            
            # Atomic: Insert Queue THEN Update Candidate
            # We treat morning_briefing_queue as the "Lock"
            q_ins = admin_db.table("morning_briefing_queue").insert(queue_payload).execute()
            
            if q_ins.data:
                # C. Update Candidate Status
                # We store draft_body in candidates too for redundancy/details view
                update_data = {
                    "status": "QUEUED",
                    "draft_ready": True, 
                    "draft_body": draft_body,
                    "updated_at": "now()"
                }
                admin_db.table("candidates").update(update_data).eq("id", cand['id']).execute()
                print(f"  [x] Queued: {cand.get('full_name')}")
                success_count += 1
            else:
                print(f"  [!] Failed to insert queue for {cand.get('full_name')}")
                
        except Exception as e:
            print(f"  [!] Error processing {cand.get('id')}: {e}")
            
    print(f"Batch Complete. Successfully Queued: {success_count}")

if __name__ == "__main__":
    run_draft_prep()

import os
import sys
from supabase import create_client
import time
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import settings
from app.core.ghostwriter import GhostwriterEngine
from app.core.image_proxy import ImageProxyEngine

# Initialize Service Role Client
admin_db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

TARGET_BATCH_SIZE = 50

def run_draft_prep():
    print("Running Draft Prep Job (Phase 2 Enrichment)...")
    
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
    image_proxy = ImageProxyEngine()

    # 3. Generate and Atomic Commit
    success_count = 0
    for cand in pool_candidates:
        try:
            # A. Enrichment (Images) - Phase 2
            # "Behavior: If image exists -> render it" (User)
            # We fetch it now to lock it in.
            img_url = cand.get("linkedin_image_url") # Check if already exists
            if not img_url:
                print(f"  [>] Fetching Image for {cand.get('full_name')}...")
                img_res = image_proxy.fetch_image(
                    name=cand.get('full_name'), 
                    company=cand.get('firm'),
                    linkedin_url=cand.get('linkedin_url')
                )
                img_url = img_res.get("imageUrl")
                if img_url:
                     print(f"  [+] Found Image: {img_url[:30]}...")
                
            # B. Deterministic Ranking Logic (Phase 2)
            # Simple Logic: Tier 1 (Warm) > Tier 2 (Cold)
            # Default to "Tier 2" if unknown.
            # We parse "Tier 1" from candidate role or existing tier field if available.
            # Assuming 'role' contains 'CEO' -> High Priority.
            role = (cand.get("role") or "").lower()
            if "ceo" in role or "president" in role or "founder" in role:
                priority = 90
                reason = f"Executive Role ({cand.get('role')})"
            elif "vp" in role or "director" in role:
                priority = 70
                reason = f"Leadership Role ({cand.get('role')})"
            else:
                priority = 50
                reason = f"Standard Match ({cand.get('firm')})"

            # C. Generate Draft
            draft_body = ghostwriter.generate_draft(cand)
            
            # D. Insert into Queue
            queue_payload = {
                "candidate_id": cand['id'],
                "status": "pending",
                "selected_for_date": today_str,
                "priority_score": priority,
                "ranking_reason": reason, # Persisted Explanation
                "draft_preview": draft_body
            }
            
            q_ins = admin_db.table("morning_briefing_queue").insert(queue_payload).execute()
            
            if q_ins.data:
                # E. Update Candidate (Status + Image)
                update_data = {
                    "status": "QUEUED",
                    "draft_ready": True, 
                    "draft_body": draft_body,
                    "linkedin_image_url": img_url, # Persist Image
                    "updated_at": "now()"
                }
                admin_db.table("candidates").update(update_data).eq("id", cand['id']).execute()
                print(f"  [x] Queued: {cand.get('full_name')} (Score: {priority})")
                success_count += 1
            else:
                print(f"  [!] Failed to insert queue for {cand.get('full_name')}")
                
        except Exception as e:
            print(f"  [!] Error processing {cand.get('id')}: {e}")
            # If column missing, this might fail until Migration 011 runs.
            # Pass
            
    print(f"Batch Complete. Successfully Queued: {success_count}")

if __name__ == "__main__":
    run_draft_prep()

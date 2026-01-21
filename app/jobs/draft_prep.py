import os
import sys
from supabase import create_client
import datetime
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import settings
from app.core.ghostwriter import GhostwriterEngine
from app.core.image_proxy import ImageProxyEngine
from app.lib.safety import sanitize_external_string_for_db

# Initialize Service Role Client
# HARDCODED FIX: Bypass flaky .env loading
url = "https://ojzqwwaebxvpnenkthaw.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"
admin_db = create_client(url, key)

TARGET_DAILY_TOTAL = 50
BATCH_SIZE = 10

def run_draft_prep():
    print("--- OVERNIGHT BAKER: Generating Morning Briefing (5 Blocks of 10) ---")
    
    today_str = datetime.date.today().isoformat()
    
    # 1. Check if already populated
    try:
        q_res = admin_db.table("morning_briefing_queue").select("id", count="exact", head=True).eq("selected_for_date", today_str).execute()
        if q_res.count and q_res.count >= TARGET_DAILY_TOTAL:
            print(f"✅ Morning Briefing already fully baked for {today_str} ({q_res.count} items). Exiting.")
            return
    except Exception as e:
        print(f"Queue check failed: {e}")
        return

    
    # 2. HUNT LOOP (Keep fetching until we have 50)
    # We query the current count inside the loop (or track it locally)
    # But to be safe against race conditions (unlikely here), local tracking is fine.
    
    current_count = q_res.count or 0
    needed = TARGET_DAILY_TOTAL - current_count
    
    print(f"Need {needed} fresh candidates to reach {TARGET_DAILY_TOTAL}...")
    
    ghostwriter = GhostwriterEngine()
    image_proxy = ImageProxyEngine()
    
    # INTELLIGENCE ENGINES
    from app.core.signal_analyst import SignalAnalyst
    from app.core.event_scout import EventScout
    from app.core.liveness import EmploymentLivenessCheck
    
    signals_engine = SignalAnalyst()
    event_scout = EventScout()
    liveness_checker = EmploymentLivenessCheck()
    
    attempts = 0
    MAX_ATTEMPTS = 50 # Increased safety break
    
    # MOVED OUTSIDE LOOP: Track firms across all batches
    firm_counts = {}

    while needed > 0 and attempts < MAX_ATTEMPTS:
        attempts += 1
        print(f"--- Hunt Batch {attempts} (Need {needed}) ---")
        
        try:
             # Fetch from POOL with Offset to traverse inventory
            offset_val = (attempts - 1) * 200
            print(f"  Fetching POOL offset {offset_val}...")
            
            pool_res = admin_db.table("candidates").select("*").eq("status", "POOL") \
                .range(offset_val, offset_val + 199).execute()
            candidates = pool_res.data
            
            # SHUFFLE candidates
            import random
            random.shuffle(candidates)
            
        except Exception as e:
            print(f"Pool fetch failed: {e}")
            break
            
        if not candidates:
            print("Pool Exhausted.")
            break
        
        candidates_processed_in_batch = 0
        
        for cand in candidates:
            if needed <= 0:
                break
                
            # DIVERSITY CHECK
            firm = cand.get("firm", "Unknown")
            if firm_counts.get(firm, 0) >= 2:
                # Just skip locally. 
                # Since we use OFFSET now, we won't see this candidate again in next loops.
                # So no need to change DB status.
                continue
            
            # ... (Rest of validation logic) ...
            
            # STRICT VALIDATION
            is_valid = True
            missing = []
            if not cand.get("full_name"): missing.append("name")
            if not cand.get("firm"): missing.append("firm")
            if not cand.get("email") and not cand.get("work_email"): missing.append("email")
            
            # Image Check
            # SANITIZE existing URLs first (Blast Radius Defense)
            raw_img_url = cand.get("linkedin_image_url") or cand.get("profile_image")
            img_url = sanitize_external_string_for_db(raw_img_url)

            # FORCE VALIDATION OF EXISTING URL
            if img_url:
                if not image_proxy.verify_accessibility(img_url):
                    print(f"  [Img] Existing URL blocked (403): {img_url}")
                    img_url = None # Force re-fetch

            # Attempt Image Fetch if missing or broken
            if not img_url and is_valid: # Only fetch image if other stuff valid
                print(f"  [Img] Fetching for {cand.get('full_name')}...")
                try:
                    img_res = image_proxy.fetch_image(
                        name=cand.get('full_name'), 
                        company=cand.get('firm'),
                        linkedin_url=cand.get('linkedin_url')
                    )
                    # SANITIZE fetched URL (Blast Radius Defense)
                    img_url = sanitize_external_string_for_db(img_res.get("imageUrl"))
                    # Update local obj
                    cand['linkedin_image_url'] = img_url
                    
                    # Validate the NEW image immediately
                    if img_url and not image_proxy.verify_accessibility(img_url):
                         print(f"  [Img] New fetch produced broken URL: {img_url}")
                         img_url = None
                         
                except:
                    pass
            
            # POLICY CHANGE: Do not skip candidate if image is missing.
            # Just proceed with Initials.
            if not img_url: 
                print(f"  [Warn] No valid image for {cand.get('full_name')}. Proceeding with Initials.")
                # missing.append("image") # REMOVED strict check
            
            if missing:
                print(f"  [Skip] {cand.get('full_name')} missing: {missing}")
                # MARK AS FAILED (Constraint: status IN ('POOL', 'QUEUED', 'SENT', 'FAILED'))
                admin_db.table("candidates").update({"status": "FAILED"}).eq("id", cand['id']).execute()
                continue

            # === LIVENESS GATE (Hard Block) ===
            # Protect Reputation. Check for departures.
            liveness = liveness_checker.check_status(cand)
            is_blocked = False
            blocked_reason = ""
            
            if liveness["is_departure"]:
                print(f"  [GATE] BLOCKED: {cand.get('full_name')} - {liveness['risk_reason']}")
                is_blocked = True
                blocked_reason = liveness["risk_reason"]
                
                # Update Master Status
                # Note: 'BLOCKED_BOUNCE_RISK' might be a new enum value. Ensure DB allows text or update enum.
                # Assuming text field or compatible.
                admin_db.table("candidates").update({"status": "FAILED", "draft_body": f"BLOCKED: {blocked_reason}"}).eq("id", cand['id']).execute()
                
            # === VALID CANDIDATE (OR BLOCKED VISIBLE) ===
            
            try:
                # 1. INTELLIGENCE GATHERING (New Layer)
                # SKIP if blocked to save tokens/time
                cand_signals_context = []
                cand_events_context = []
                
                if not is_blocked:
                    print(f"  [Intel] Scanning Signals & Events for {cand.get('firm')}...")
                    
                    # A. Signals (Reactive)
                    signals = signals_engine.scan_and_analyze(cand.get("firm"))
                cand_signals_context = []
                
                if signals:
                    print(f"       -> Found {len(signals)} Signals.")
                    for sig in signals:
                        # Store in DB
                        try:
                            payload = sig.copy()
                            payload["candidate_id"] = cand['id']
                            admin_db.table("candidate_signals").insert(payload).execute()
                            
                            # Add to Context for Draft
                            cand_signals_context.append(f"SIGNAL: {sig['signal_type']} - {sig['title']} ({sig['analysis']})")
                        except Exception as e:
                            print(f"Failed to save signal: {e}")
                            
                # B. Events (Proactive)
                events = event_scout.check_events(cand)
                cand_events_context = []
                if events:
                    print(f"       -> Found {len(events)} Event Hooks.")
                    for evt in events:
                        cand_events_context.append(f"EVENT: {evt['event_name']} ({evt['match_reason']}) - Hook: '{evt['hook_text']}'")

                # 2. Determine Batch
                batch_num = (current_count // BATCH_SIZE) + 1
                
                print(f"  [Draft] Generating for {cand.get('full_name')} (Batch {batch_num})...")
                
                # PREPARE CONTEXT FOR GHOSTWRITER
                body = ""
                subj = "Subject: Connect"
                
                if is_blocked:
                     body = f"BLOCKED: {blocked_reason}"
                     subj = "Alert: High Bounce Risk"
                else:
                    # Run Ghostwriter
                    intel_context = "\n".join(cand_signals_context + cand_events_context)
                    draft_body = ghostwriter.generate_draft(cand, context=intel_context)
                    
                    body = draft_body
                    if "\n" in draft_body:
                        parts = draft_body.split("\n", 1)
                        if parts[0].lower().startswith("subject:"):
                            subj = parts[0]
                            body = parts[1].strip()
                        
                # 1. Update target_brokers (Legacy/Mirror)
                cand_update = {
                    "llm_email_body": body,
                    "llm_email_subject": subj,
                    "profile_image": img_url
                }
                admin_db.table("target_brokers").update(cand_update).eq("id", cand['id']).execute()
                
                # 2. Update candidates (Master) - Mark as QUEUED
                candidate_payload = {
                    "status": "QUEUED",
                    "draft_body": body,
                    "linkedin_image_url": img_url,
                    "updated_at": "now()"
                }
                admin_db.table("candidates").update(candidate_payload).eq("id", cand['id']).execute()
                
                # 3. Insert Queue
                queue_payload = {
                    "candidate_id": cand['id'],
                    "status": "pending",
                    "selected_for_date": today_str,
                    "priority_score": batch_num,
                    "ranking_reason": f"Batch {batch_num}",
                    "draft_preview": body,
                    # TODO: Store signals summary here if schema allows?
                }
                admin_db.table("morning_briefing_queue").insert(queue_payload).execute()
                
                print(f"  [✅] Baked: {cand.get('full_name')} -> Batch {batch_num}")
                
                # INCREMENT FIRM COUNT (GLOBAL)
                firm_counts[firm] = firm_counts.get(firm, 0) + 1
                
                current_count += 1
                needed -= 1
                candidates_processed_in_batch += 1
                
            except Exception as e:
                print(f"  [❌] Processing Failed {cand.get('full_name')}: {e}")
        
        # Check progress inside loop
        if candidates_processed_in_batch == 0:
             print("Warning: No candidates processed in this batch. Queue might be stuck on duplicates.")
    
    print(f"Baking Complete. Queue Size: {current_count}")
        
if __name__ == "__main__":
    run_draft_prep()

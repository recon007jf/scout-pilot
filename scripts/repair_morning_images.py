import datetime
from supabase import create_client
from app.config import settings
from app.core.image_proxy import ImageProxyEngine
from app.lib.safety import sanitize_external_string_for_db

def repair_images():
    print("--- MORNING BRIEFING IMAGE REPAIR ---")
    
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_KEY
    db = create_client(url, key)
    engine = ImageProxyEngine()
    
    # Get today's queue
    today = datetime.date.today().isoformat()
    # Note: query queue, get candidate_id
    q_res = db.table("morning_briefing_queue").select("candidate_id").eq("selected_for_date", today).execute()
    
    if not q_res.data:
        print("No items in queue for today.")
        return

    cand_ids = [row['candidate_id'] for row in q_res.data]
    
    # Get Candidates
    # Supabase 'in' filter
    c_res = db.table("candidates").select("*").in_("id", cand_ids).execute()
    candidates = c_res.data
    
    print(f"Checking {len(candidates)} candidates for missing images...")
    
    repaired_count = 0
    
    for c in candidates:
        has_img = c.get("linkedin_image_url") or c.get("profile_image")
        
        # 2. Check for "Stripped Token" Corruption (The Safety.py Bug)
        # LinkedIn URLs MUST have query params (?e=, ?t=). If missing, it's broken.
        if has_img and "licdn.com" in has_img and "?" not in has_img:
            print(f"  [BROKEN TOKEN] {c['full_name']} -> Re-fetching...")
            has_img = None # Force re-fetch to get fresh token

        if not has_img:
            print(f"  [MISSING] {c['full_name']} ({c['firm']})")
            
            try:
                # Fetch
                res = engine.fetch_image(c['full_name'], c['firm'])
                raw_url = res.get("imageUrl")
                
                if raw_url:
                    # Sanitize
                    final_url = sanitize_external_string_for_db(raw_url)
                    
                    if final_url:
                        print(f"    -> FOUND: {final_url[:50]}...")
                        
                        # Update DB
                        # 1. Candidates
                        db.table("candidates").update({"linkedin_image_url": final_url}).eq("id", c['id']).execute()
                        
                        # 2. Target Brokers (Mirror)
                        db.table("target_brokers").update({"profile_image": final_url}).eq("id", c['id']).execute()
                        
                        repaired_count += 1
                    else:
                        print("    -> Sanitization failed.")
                else:
                    print(f"    -> Not found by Serper. ({res.get('reason')})")
                    
            except Exception as e:
                print(f"    -> Error: {e}")
                
    print(f"\nRepair Complete. Fixed {repaired_count} profiles.")

if __name__ == "__main__":
    repair_images()

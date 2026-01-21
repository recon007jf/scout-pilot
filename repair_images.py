
import os
import sys
import datetime
from supabase import create_client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app.core.image_proxy import ImageProxyEngine
from app.lib.safety import sanitize_external_string_for_db

# Hardcoded Service Role Key
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def repair_queue_images():
    print("--- REPAIRING QUEUE IMAGES ---")
    sb = create_client(URL, KEY)
    proxy = ImageProxyEngine()
    
    today_str = datetime.date.today().isoformat()
    
    # 1. Fetch Queue for Today
    q_res = sb.table("morning_briefing_queue").select("candidate_id, priority_score").eq("selected_for_date", today_str).execute()
    queue = q_res.data
    print(f"Checking {len(queue)} candidates in queue...")
    
    fixed_count = 0
    
    for item in queue:
        cid = item["candidate_id"]
        # Fetch Candidate
        res = sb.table("candidates").select("*").eq("id", cid).execute()
        if not res.data: continue
        cand = res.data[0]
        
        current_img = cand.get("linkedin_image_url")
        name = cand.get("full_name")
        firm = cand.get("firm")
        
        is_bad = False
        if not current_img:
            is_bad = True
        else:
            # Check validity using our new method
            if not proxy.verify_accessibility(current_img):
                print(f"  [X] Broken Image for {name}: {current_img[:30]}...")
                is_bad = True
            else:
                # Valid
                print(f"  [OK] {name}")
                continue
                
        if is_bad:
            print(f"  [Repairing] Fetching new image for {name} ({firm})...")
            new_img_res = proxy.fetch_image(name, firm)
            new_img = new_img_res.get("imageUrl")
            
            if new_img:
                print(f"    -> Found: {new_img[:40]}...")
                # Sanitize
                clean_url = sanitize_external_string_for_db(new_img)
                
                # Update DBs
                # 1. candidate
                sb.table("candidates").update({"linkedin_image_url": clean_url}).eq("id", cid).execute()
                # 2. target_broker
                sb.table("target_brokers").update({"profile_image": clean_url}).eq("id", cid).execute()
                fixed_count += 1
            else:
                print("    -> No replacement found.")
                
    print(f"Repaired {fixed_count} images.")

if __name__ == "__main__":
    repair_queue_images()

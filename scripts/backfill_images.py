import os
import sys
from supabase import create_client

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from app.config import settings
from app.core.image_proxy import ImageProxyEngine

db = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)

def backfill_images():
    print("Backfilling Images for Today's Queue...")
    today = "2026-01-17" # Hardcode or dynamic
    
    # Get Queue
    res = db.table("morning_briefing_queue").select("*, candidates(*)").eq("selected_for_date", today).execute()
    items = res.data
    
    proxy = ImageProxyEngine()
    
    for item in items:
        cand = item.get("candidates")
        if not cand: continue
        
        if not cand.get("linkedin_image_url"):
            print(f"Fetching for {cand.get('full_name')}...")
            img_res = proxy.fetch_image(cand.get("full_name"), cand.get("firm"), cand.get("linkedin_url"))
            url = img_res.get("imageUrl")
            
            if url:
                print(f"  Found: {url[:30]}...")
                db.table("candidates").update({"linkedin_image_url": url}).eq("id", cand['id']).execute()
            else:
                print("  No image found.")
        else:
            print(f"Skipping {cand.get('full_name')} (Already has image)")

if __name__ == "__main__":
    backfill_images()

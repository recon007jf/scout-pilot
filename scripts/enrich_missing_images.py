import os
import requests
import json
import time
from dotenv import load_dotenv
from supabase import create_client

# Load env vars
load_dotenv()

# Configuration
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([SERPER_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Missing API Keys")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
serper_url = "https://google.serper.dev/images"

def get_real_image_from_serper(name, firm):
    query = f"{name} {firm} LinkedIn profile photo"
    print(f"   🔍 Querying Serper: '{query}'")
    
    payload = {
        "q": query,
        "num": 10,
        "gl": "us",
        "hl": "en",
        "autocorrect": True
    }
    
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(serper_url, headers=headers, json=payload)
        if response.status_code != 200:
            print(f"      ❌ Serper Error: {response.status_code}")
            return None
            
        data = response.json()
        images = data.get("images", [])
        
        for idx, img in enumerate(images):
            link = img.get("imageUrl", "")
            source_page = img.get("link", "") 
            title = img.get("title", "")
            w = img.get("imageWidth", 0)
            h = img.get("imageHeight", 0)
            
            # 1. Size Check
            if w < 100 or h < 100:
                continue
            
            # 2. Aspect Ratio 
            ratio = w / h if h > 0 else 0
            if not (0.75 <= ratio <= 1.3):
                continue

            # 3. ANTI-FEED Logic (Strict)
            if "feedshare" in link or "feedshare" in source_page:
                continue
            
            # 4. Strict Profile Validation
            # Must be from a profile page
            if "/in/" not in source_page and "linkedin.com" in source_page:
                 continue

            print(f"      ✅ STRICT MATCH Found [{idx}]:")
            print(f"         URL: {link[:60]}...")
            return link

        print("      ⚠️ No image passed strict filtering.")
        return None
        
    except Exception as e:
        print(f"      ❌ Exception: {e}")
        return None

def main():
    print("🚀 Starting Bulk Refinement (Strict LinkedIn Mode)...")
    
    # Fetch all targets
    res = supabase.table("target_brokers").select("id, full_name, firm, profile_image").execute()
    targets = res.data
    
    updates = 0
    
    for t in targets:
        name = t.get("full_name")
        img = t.get("profile_image")
        
        # SKIP if image is missing logic, BUT
        # Logic: 
        # - Update if NULL
        # - Update if "generic" (/professional-...)
        # - Update if "feedshare" (bad previous enrichment)
        
        should_update = False
        
        if not img:
            should_update = True
        elif img.startswith("/professional-"):
            print(f"♻️  Updating Generic Image: {name}")
            should_update = True
        elif "feedshare" in img:
            print(f"♻️  Fixing Bad Feed Image: {name}")
            should_update = True
        
        # Explicitly SKIP Andrew Forchelli (Manually Set)
        if name == "Andrew Forchelli":
            print(f"🛡️  Skipping Manual Override: {name}")
            should_update = False
            
        if not should_update:
            continue
            
        real_image = get_real_image_from_serper(name, t.get("firm"))
        
        if real_image:
            # Update DB
            data = {"profile_image": real_image}
            res = supabase.table("target_brokers").update(data).eq("id", t["id"]).execute()
            print("   💾 Database updated.")
            updates += 1
        else:
            print("   ⏩ No strict match found. Keeping existing.")
            
        time.sleep(1)

    print(f"\n✅ Bulk Refinement Complete. Updated {updates} targets.")

if __name__ == "__main__":
    main()


import os
import sys
from supabase import create_client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app.core.image_proxy import ImageProxyEngine
from app.lib.safety import sanitize_external_string_for_db

URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def fix_david():
    print("--- FORCE FIX DAVID ---")
    sb = create_client(URL, KEY)
    proxy = ImageProxyEngine()
    
    # 1. Fetch new image
    name = "David Brink"
    firm = "Gallagher"
    print(f"Fetching for {name} ({firm})...")
    
    res = proxy.fetch_image(name, firm)
    new_url = res.get("imageUrl")
    
    if new_url:
        print(f"Found New URL: {new_url}")
        
        # Verify accessibility
        if proxy.verify_accessibility(new_url):
            print("Access Check: PASS")
            # Update DB
            clean_url = sanitize_external_string_for_db(new_url)
            sb.table("candidates").update({"linkedin_image_url": clean_url}).eq("full_name", name).execute()
            sb.table("target_brokers").update({"profile_image": clean_url}).eq("full_name", name).execute()
            print("Updated DB.")
        else:
            print("New URL Access Check: FAIL")
    else:
        print("No image found.")

if __name__ == "__main__":
    fix_david()

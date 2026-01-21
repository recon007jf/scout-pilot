
import requests
from supabase import create_client

# Hardcoded Service Role Key
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def inspect_hallucinations():
    admin_db = create_client(URL, KEY)
    
    # 1. Check specific problematic candidates
    names = ["Mike Chen", "Nichole Kebriti", "Francesca Redshaw", "Mike Walsh"]
    print(f"--- Inspecting Images for: {names} ---")
    
    res = admin_db.table("candidates").select("full_name, firm, linkedin_url, linkedin_image_url").in_("full_name", names).execute()
    
    for c in res.data:
        print(f"\nCandidate: {c['full_name']}")
        print(f"Firm: {c['firm']}")
        print(f"LinkedIn Profile: {c['linkedin_url']}")
        print(f"Stored Image: {c['linkedin_image_url']}")
        
        # Check if stored image looks like a verified profile pic or a random generic one
        if c['linkedin_image_url']:
            print("  [Analyst Note]: Does this URL match the pattern of a direct profile scraping?")

if __name__ == "__main__":
    inspect_hallucinations()

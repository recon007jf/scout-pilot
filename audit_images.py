
import os
from supabase import create_client

URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def check_images():
    sb = create_client(URL, KEY)
    names = ["Sean Spencer", "Tamara Johnson", "Kevin Overbey", "Doug Obenshain"]
    
    print("--- IMAGE AUDIT ---")
    for name in names:
        # Search by partial name to catch "Cpdm Tamara Johnson"
        res = sb.table("candidates").select("full_name, linkedin_image_url, profile_image, linkedin_url").ilike("full_name", f"%{name.split()[-1]}%").execute()
        
        for cand in res.data:
            # Simple match filter
            if name.split()[-1] in cand['full_name']:
                print(f"\nCandidate: {cand['full_name']}")
                print(f"  LinkedIn URL: {cand['linkedin_url']}")
                print(f"  Stored Image: {cand['linkedin_image_url']}")
                print(f"  Profile Image: {cand['profile_image']}")

if __name__ == "__main__":
    check_images()

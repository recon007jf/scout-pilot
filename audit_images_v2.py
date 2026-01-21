
import os
from supabase import create_client

URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
# Using ANON KEY
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFub24iLCJpYXQiOjE3NjUzODIxNDMsImV4cCI6MjA4MDk1ODE0M30.k2ETYCZiv0atyVIQcKjRaKcgCR0t_Se9UmWvXXGTzDs"

def check_images():
    print("--- CONNECTING ---")
    sb = create_client(URL, KEY)
    
    # 1. Test Auth
    try:
        res = sb.table("candidates").select("id").limit(1).execute()
        print(f"Auth Success. Rows: {len(res.data)}")
    except Exception as e:
        print(f"Auth Failed: {e}")
        return

    # 2. Check Specific Names
    names = ["Spencer", "Johnson", "Overbey", "Obenshain"]
    
    print("\n--- SEARCHING CANDIDATES ---")
    for name in names:
        try:
            # Using ilike on full_name
            res = sb.table("candidates").select("full_name, linkedin_image_url, profile_image, linkedin_url").ilike("full_name", f"%{name}%").execute()
            for c in res.data:
                print(f"\n[FOUND] {c['full_name']}")
                print(f"  LI Image: {c.get('linkedin_image_url')}")
                print(f"  Profile Img: {c.get('profile_image')}")
                print(f"  LinkedIn: {c.get('linkedin_url')}")
        except Exception as e:
            print(f"Search failed for {name}: {e}")

if __name__ == "__main__":
    check_images()

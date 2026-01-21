
import os
from supabase import create_client

URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def inspect_david():
    print("--- INSPECTING DAVID BRINK ---")
    sb = create_client(URL, KEY)
    
    res = sb.table("candidates").select("*").eq("full_name", "David Brink").execute()
    if res.data:
        c = res.data[0]
        print(f"Name: {c.get('full_name')}")
        print(f"Email: {c.get('email')}")
        print(f"LinkedIn: {c.get('linkedin_url')}")
        print(f"Image URL: {c.get('linkedin_image_url')}")
        print(f"Status: {c.get('status')}")
    else:
        print("David Brink not found in DB.")

if __name__ == "__main__":
    inspect_david()

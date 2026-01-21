
import os
from supabase import create_client

URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def check_draft():
    print("--- CHECKING DRAFT FOR DAVID BRINK ---")
    sb = create_client(URL, KEY)
    
    res = sb.table("candidates").select("full_name, draft_body").eq("full_name", "David Brink").execute()
    if res.data:
        c = res.data[0]
        print(f"Name: {c.get('full_name')}")
        body = c.get('draft_body')
        if body:
            print(f"Draft Length: {len(body)} chars")
            print(f"Snippet: {body[:100]}...")
        else:
            print("NO DRAFT FOUND.")
    else:
        print("David not found.")

if __name__ == "__main__":
    check_draft()

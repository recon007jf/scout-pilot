
import os
from supabase import create_client

# HARDCODED SUPER ADMIN CREDENTIALS (VERIFIED)
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
# USING ANON KEY (Service Role is busted currently)
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFub24iLCJpYXQiOjE3NjUzODIxNDMsImV4cCI6MjA4MDk1ODE0M30.k2ETYCZiv0atyVIQcKjRaKcgCR0t_Se9UmWvXXGTzDs"

def fix_data():
    print("--- FIXING DATA QUALITY ---")
    sb = create_client(URL, KEY)
    
    # 1. Fix Tamara Johnson Name & Image
    print("Fixing Tamara Johnson...")
    # Find her ID first to be safe
    tamaras = sb.table("candidates").select("id, full_name").ilike("full_name", "%Tamara Johnson%").execute().data
    
    for t in tamaras:
        print(f"Found: {t['full_name']} ({t['id']})")
        
        # New values
        update_payload = {
            "full_name": "Tamara Johnson",
            "linkedin_image_url": None, # Reset to force refresh or fallback
            "profile_image": None
        }
        
        # Update Candidates
        sb.table("candidates").update(update_payload).eq("id", t['id']).execute()
        # Update Target Brokers (Sync)
        sb.table("target_brokers").update(update_payload).eq("id", t['id']).execute()
        print(" -> Updated Name to 'Tamara Johnson' and Cleared Images")

    # 2. Fix Sean Spencer Image
    print("\nFixing Sean Spencer...")
    seans = sb.table("candidates").select("id, full_name").ilike("full_name", "%Sean Spencer%").execute().data
    
    for s in seans:
        print(f"Found: {s['full_name']} ({s['id']})")
        # Reset image
        update_payload = {
            "linkedin_image_url": None,
            "profile_image": None
        }
        sb.table("candidates").update(update_payload).eq("id", s['id']).execute()
        sb.table("target_brokers").update(update_payload).eq("id", s['id']).execute()
        print(" -> Cleared Bad/Missing Images")

if __name__ == "__main__":
    fix_data()

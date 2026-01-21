
import os
import sys
from supabase import create_client

# Hardcoded Service Role Key
URL = "https://ojzqwwaebxvpnenkthaw.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9qenF3d2FlYnh2cG5lbmt0aGF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTM4MjE0MywiZXhwIjoyMDgwOTU4MTQzfQ.IvHe8xsdiIix3XugOHBEEV_ZG1Y0x6KzJF3JL0ByUcQ"

def reset_and_rebake():
    print("--- DIVERSITY RESET PROTOCOL ---")
    sb = create_client(URL, KEY)
    today = "2026-01-20"
    
    # 1. Clear Queue
    print(f"1. Clearing Morning Queue for {today}...")
    sb.table("morning_briefing_queue").delete().eq("selected_for_date", today).execute()
    
    # 2. Reset Candidates in QUEUED state to POOL (so they can be re-fetched if valid)
    # Actually, we rely on the baker to pick from POOL.
    # If they are QUEUED, they are stuck.
    # We should reset RECENTLY Queued ones? 
    # Or just all QUEUED?
    # Let's reset all 'QUEUED' to 'POOL' to be safe.
    print("2. Resetting 'QUEUED' candidates to 'POOL'...")
    sb.table("candidates").update({"status": "POOL"}).eq("status", "QUEUED").execute()

    print("✅ RESET COMPLETE. STARTING BAKER...")
    
    # Import and run baker
    from app.jobs.draft_prep import run_draft_prep
    run_draft_prep()

if __name__ == "__main__":
    reset_and_rebake()
